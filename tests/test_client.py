import unittest
from unittest.mock import Mock

import pandas as pd

from stream_utils.catalog import dataset_path
from stream_utils.client import StreamClient
from stream_utils.sites import SITE_COLUMNS


class StreamClientTests(unittest.TestCase):
    def setUp(self):
        self.access = Mock()
        self.client = StreamClient(access=self.access)

    def test_lists_catalog_datasets(self):
        names = self.client.list_datasets()
        self.assertIn("metadata", names)
        self.assertIn("water_quality", names)
        self.assertEqual(names, sorted(names))

    def test_returns_dataset_path(self):
        self.assertEqual(self.client.path("streamflow"), dataset_path("streamflow"))

    def test_delegates_metadata_read(self):
        expected = pd.DataFrame({"STREAM_ID": ["STREAM-gauge-1"]})
        self.access.read_metadata.return_value = expected
        result = self.client.read_metadata(columns=["STREAM_ID"])
        pd.testing.assert_frame_equal(result, expected)
        self.access.read_metadata.assert_called_once_with(columns=["STREAM_ID"])

    def test_finds_sites_from_metadata(self):
        self.access.read_metadata.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1", "STREAM-gauge-2"],
                "state_name": ["Virginia", "Maryland"],
                "WQ_parameters": ["WTemp_C", "WTemp_C"],
            }
        )
        result = self.client.find_sites(states="Virginia")
        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-1"])
        self.access.read_metadata.assert_called_once_with(columns=SITE_COLUMNS)

    def test_filters_sites_by_dataset_availability(self):
        self.access.read_metadata.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["site-1", "site-2"],
                "state_name": ["Virginia", "Virginia"],
                "WQ_parameters": ["WTemp_C", "WTemp_C"],
                "streamflow": ["Available", "Not Available"],
                "grabsamples": ["DOC_mgL, Cl_mgL", "DOC_mgL"],
            }
        )

        result = self.client.find_sites(
            streamflow=True,
            grab_variables=["DOC_mgL", "Cl_mgL"],
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["site-1"])

    def test_combines_site_and_bounding_box_filters(self):
        self.access.read_metadata.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1", "STREAM-gauge-2"],
                "state_name": ["Virginia", "Virginia"],
                "WQ_parameters": ["WTemp_C", "WTemp_C"],
                "longitude_wgs84": [-78.0, -82.0],
                "latitude_wgs84": [38.0, 38.0],
            }
        )

        result = self.client.find_sites(
            states="Virginia",
            bbox=(-80.0, 36.0, -75.0, 40.0),
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-1"])

    def test_filters_sites_by_radius(self):
        self.access.read_metadata.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["near", "far"],
                "state_name": ["Virginia", "Virginia"],
                "WQ_parameters": ["WTemp_C", "WTemp_C"],
                "longitude_wgs84": [-78.0, -82.0],
                "latitude_wgs84": [38.0, 38.0],
            }
        )

        result = self.client.find_sites(
            variables="WTemp_C",
            center=(-78.0, 38.0),
            radius_km=100,
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["near"])
        self.assertIn("distance_km", result.columns)

    def test_requires_center_and_radius_together(self):
        self.access.read_metadata.return_value = pd.DataFrame()

        with self.assertRaisesRegex(ValueError, "center requires"):
            self.client.find_sites(center=(-78.0, 38.0))

    def test_selects_nearest_sites(self):
        self.access.read_metadata.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["second", "first", "third"],
                "state_name": ["Virginia"] * 3,
                "WQ_parameters": ["WTemp_C"] * 3,
                "longitude_wgs84": [-78.5, -78.0, -79.0],
                "latitude_wgs84": [38.0, 38.0, 38.0],
            }
        )

        result = self.client.find_sites(
            center=(-78.0, 38.0),
            nearest=2,
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["first", "second"])

    def test_filters_sites_by_huc(self):
        self.access.read_metadata.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1", "STREAM-gauge-2"],
                "state_name": ["Virginia", "Maryland"],
                "WQ_parameters": ["WTemp_C", "WTemp_C"],
            }
        )
        self.access.read_huc_lookup.return_value = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1", "STREAM-gauge-2"],
                "HUC6": ["020700", "020600"],
                "HUC8": ["02070010", "02060006"],
            }
        )

        result = self.client.find_sites(huc8="02070010")

        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-1"])
        self.access.read_huc_lookup.assert_called_once_with()

    def test_delegates_water_quality_read(self):
        self.client.read_water_quality(
            "STREAM-gauge-1",
            "WTemp_C",
            start="2020-01-01",
            end="2020-12-31",
            quality="A",
        )
        self.access.read_water_quality.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            variables="WTemp_C",
            start="2020-01-01",
            end="2020-12-31",
            quality="A",
        )

    def test_delegates_streamflow_read(self):
        self.client.read_streamflow(
            "STREAM-gauge-1",
            start="2020-01-01",
            end="2020-12-31",
            quality="approved",
        )
        self.access.read_streamflow.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            start="2020-01-01",
            end="2020-12-31",
            quality="approved",
        )

    def test_delegates_static_attribute_read(self):
        self.client.read_static_attributes(
            "STREAM-gauge-1",
            ["stream_order", "ele_mt_sav"],
        )
        self.access.read_static_attributes.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            columns=["stream_order", "ele_mt_sav"],
        )

    def test_delegates_watershed_read(self):
        self.client.read_watersheds(["STREAM-gauge-1", "STREAM-gauge-2"])

        self.access.read_watersheds.assert_called_once_with(
            site_ids=["STREAM-gauge-1", "STREAM-gauge-2"]
        )

    def test_delegates_grab_sample_read(self):
        self.client.read_grab_samples(
            "STREAM-gauge-1",
            ["DOC_mgL", "Cl_mgL"],
            start="2020-01-01",
            end="2020-12-31",
        )
        self.access.read_grab_samples.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            variables=["DOC_mgL", "Cl_mgL"],
            start="2020-01-01",
            end="2020-12-31",
        )

    def test_delegates_meteorology_read(self):
        self.client.read_meteorology(
            "STREAM-gauge-1",
            ["APCP_surface", "TMP_2maboveground"],
            start="2020-01-01",
            end="2020-12-31",
        )
        self.access.read_meteorology.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            variables=["APCP_surface", "TMP_2maboveground"],
            start="2020-01-01",
            end="2020-12-31",
        )

    def test_delegates_land_cover_read(self):
        self.client.read_land_cover(
            "STREAM-gauge-1",
            ["Cultivated Crops", "Deciduous Forest"],
            start="2015-01-01",
            end="2020-12-31",
        )
        self.access.read_land_cover.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            variables=["Cultivated Crops", "Deciduous Forest"],
            start="2015-01-01",
            end="2020-12-31",
        )

    def test_delegates_anthropogenic_read(self):
        self.client.read_anthropogenic(
            "STREAM-gauge-1",
            ["salt_pound", "irrig"],
            start="2000-01-01",
            end="2020-12-31",
        )
        self.access.read_anthropogenic.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            variables=["salt_pound", "irrig"],
            start="2000-01-01",
            end="2020-12-31",
        )


if __name__ == "__main__":
    unittest.main()
