import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

import streamwq as stream
from streamwq._metadata import _storage_provider
from streamwq._storage import dataset_path


class GetSitesTests(unittest.TestCase):
    def setUp(self):
        _storage_provider.cache_clear()
        self.addCleanup(_storage_provider.cache_clear)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.metadata = pa.table(
            {
                "STREAM_ID": ["site-1", "site-2", "site-1", "site-3"],
                "state_name": ["Virginia", "Maryland", "Virginia", "Virginia"],
                "WQ_parameters": ["WTemp_C, DO_mgL", "WTemp_C", "WTemp_C", None],
                "streamflow": ["Available", "Not Available", "Available", None],
                "grabsamples": ["DOC_mgL, Cl_mgL", "DOC_mgL", None, None],
                "latitude_wgs84": [38.0, 39.0, 38.0, 40.0],
                "longitude_wgs84": [-77.0, -78.0, -77.0, -79.0],
            }
        )
        self.huc_lookup = pa.table(
            {
                "STREAM_ID": ["site-1", "site-2", "site-3"],
                "HUC6": ["020700", "020600", "170900"],
                "HUC8": ["02070010", "02060006", "17090001"],
            }
        )
        self._write_dataset("metadata", self.metadata)
        self._write_dataset("huc_lookup", self.huc_lookup)
        filesystem = fs.SubTreeFileSystem(self.directory.name, fs.LocalFileSystem())
        self.provider = Mock()
        self.provider.get_s3_filesystem.return_value = filesystem
        provider = patch("streamwq._metadata._storage_provider", return_value=self.provider)
        provider.start()
        self.addCleanup(provider.stop)

    def _write_dataset(self, name, table):
        path = Path(self.directory.name) / dataset_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)

    def test_returns_unique_site_ids_in_a_single_column_arrow_table(self):
        result = stream.get_sites()

        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.column_names, ["STREAM_ID"])
        self.assertEqual(
            result.column("STREAM_ID").to_pylist(),
            ["site-1", "site-2", "site-3"],
        )

    def test_combines_metadata_filters_and_allows_alternative_values(self):
        result = stream.get_sites(
            states=["Virginia", "Maryland"],
            variables=["WTemp_C", "DO_mgL"],
            grab_variables=["Cl_mgL", "DOC_mgL"],
        )

        self.assertEqual(result.column("STREAM_ID").to_pylist(), ["site-1", "site-2"])

    def test_combines_huc_filters_with_bounding_box_and_radius(self):
        result = stream.get_sites(
            huc6=["020700", "020600"],
            huc8=["02070010", "02060006"],
            bbox=(-78.0, 37.0, -76.0, 39.0),
            center=(-77.0, 38.0),
            radius_km=100,
        )

        self.assertEqual(result.column("STREAM_ID").to_pylist(), ["site-1"])

    def test_treats_huc_alternatives_as_choices(self):
        result = stream.get_sites(
            huc6=["020600", "170900"],
            huc8=["02060006", "17090001"],
        )

        self.assertEqual(result.column("STREAM_ID").to_pylist(), ["site-2", "site-3"])

    def test_returns_no_match_error_for_valid_filters(self):
        with self.assertRaisesRegex(ValueError, "No sites matched.*Broaden"):
            stream.get_sites(states="California")

    def test_rejects_invalid_spatial_arguments_before_reading_data(self):
        invalid_requests = [
            (
                {"bbox": (-75.0, 36.0, -80.0, 40.0)},
                "bbox west must be less than east",
            ),
            ({"bbox": (-80.0, 36.0, -75.0)}, "four finite numeric values"),
            (
                {"center": (-77.0, 38.0)},
                "center requires radius_km",
            ),
            (
                {"center": (-77.0, 38.0), "radius_km": 0},
                "positive number",
            ),
            (
                {"center": (-181.0, 38.0), "radius_km": 100},
                "center longitude must be between",
            ),
        ]
        for filters, message in invalid_requests:
            with self.subTest(filters=filters):
                with self.assertRaisesRegex(ValueError, message):
                    stream.get_sites(**filters)

        self.provider.get_s3_filesystem.assert_not_called()

    def test_discovery_table_can_be_passed_unchanged_to_get_metadata(self):
        site_ids = stream.get_sites(states="Maryland")

        result = stream.get_metadata(site_ids=site_ids)

        self.assertTrue(result.equals(self.metadata.take([1]), check_metadata=True))


if __name__ == "__main__":
    unittest.main()
