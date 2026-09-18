import unittest
from unittest.mock import Mock, patch

import pandas as pd

from utils.catalog import dataset_path
from utils.client import SITE_COLUMNS, StreamClient


class StreamClientTests(unittest.TestCase):
    def setUp(self):
        self.filesystem = object()
        self.hydroshare = Mock()
        self.hydroshare.get_s3_filesystem.return_value = self.filesystem
        self.client = StreamClient(hydroshare=self.hydroshare)

    def test_lists_catalog_datasets(self):
        names = self.client.list_datasets()

        self.assertIn("metadata", names)
        self.assertIn("water_quality", names)
        self.assertEqual(names, sorted(names))

    def test_returns_dataset_path(self):
        self.assertEqual(self.client.path("streamflow"), dataset_path("streamflow"))

    @patch("utils.client.pd.read_parquet")
    def test_reads_selected_metadata_columns(self, read_parquet_mock):
        expected = pd.DataFrame({"STREAM_ID": ["STREAM-gauge-1"]})
        read_parquet_mock.return_value = expected

        result = self.client.read_metadata(columns=["STREAM_ID"])

        pd.testing.assert_frame_equal(result, expected)
        read_parquet_mock.assert_called_once_with(
            dataset_path("metadata"),
            filesystem=self.filesystem,
            columns=["STREAM_ID"],
        )

    def test_rejects_a_string_as_the_columns_argument(self):
        with self.assertRaisesRegex(ValueError, "sequence of non-empty strings"):
            self.client.read_metadata(columns="STREAM_ID")

    def test_requires_an_s3_filesystem(self):
        self.hydroshare.get_s3_filesystem.return_value = None

        with self.assertRaisesRegex(RuntimeError, "S3 filesystem is required"):
            self.client.read_metadata()

    def test_finds_sites_by_state(self):
        self.client.read_metadata = Mock(return_value=self._site_metadata())

        result = self.client.find_sites(states="Virginia")

        self.assertEqual(
            result["STREAM_ID"].tolist(),
            ["STREAM-gauge-1", "STREAM-gauge-3"],
        )
        self.client.read_metadata.assert_called_once_with(columns=SITE_COLUMNS)

    def test_finds_sites_with_all_requested_variables(self):
        self.client.read_metadata = Mock(return_value=self._site_metadata())

        result = self.client.find_sites(variables=["WTemp_C", "DO_mgL"])

        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-1"])

    def test_combines_state_and_variable_filters(self):
        self.client.read_metadata = Mock(return_value=self._site_metadata())

        result = self.client.find_sites(
            states=["Maryland"],
            variables="WTemp_C",
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-2"])

    def test_rejects_empty_filter_values(self):
        with self.assertRaisesRegex(ValueError, "states must contain non-empty strings"):
            self.client.find_sites(states=[""])

    @patch("utils.client.ds.dataset")
    def test_reads_filtered_water_quality_columns(self, dataset_mock):
        expected = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1"],
                "DateTime": [pd.Timestamp("2020-01-01")],
                "WTemp_C": [10.0],
                "Flag_WTemp_C": ["A"],
            }
        )
        table = Mock()
        table.to_pandas.return_value = expected
        dataset_mock.return_value.to_table.return_value = table

        result = self.client.read_water_quality(
            site_ids="STREAM-gauge-1",
            variables="WTemp_C",
            start="2020-01-01",
            end="2020-12-31",
        )

        pd.testing.assert_frame_equal(result, expected)
        dataset_mock.assert_called_once_with(
            dataset_path("water_quality"),
            format="parquet",
            filesystem=self.filesystem,
            partitioning="hive",
        )
        call = dataset_mock.return_value.to_table.call_args
        self.assertEqual(
            call.kwargs["columns"],
            ["STREAM_ID", "DateTime", "WTemp_C", "Flag_WTemp_C"],
        )
        self.assertIsNotNone(call.kwargs["filter"])

    def test_rejects_unknown_water_quality_variable(self):
        with self.assertRaisesRegex(ValueError, "Unknown water-quality variable"):
            self.client.read_water_quality(
                site_ids="STREAM-gauge-1",
                variables="not-a-variable",
            )

    def test_rejects_reversed_date_range(self):
        with self.assertRaisesRegex(ValueError, "start must be before"):
            self.client.read_water_quality(
                site_ids="STREAM-gauge-1",
                variables="WTemp_C",
                start="2021-01-01",
                end="2020-01-01",
            )

    def test_requires_at_least_one_site(self):
        with self.assertRaisesRegex(ValueError, "at least one site"):
            self.client.read_water_quality(site_ids=[], variables="WTemp_C")

    @staticmethod
    def _site_metadata():
        return pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1", "STREAM-gauge-2", "STREAM-gauge-3"],
                "site_name": ["Site 1", "Site 2", "Site 3"],
                "state_name": ["Virginia", "Maryland", "Virginia"],
                "latitude_wgs84": [38.0, 39.0, 37.0],
                "longitude_wgs84": [-78.0, -77.0, -79.0],
                "WQ_parameters": ["WTemp_C, DO_mgL", "WTemp_C", None],
                "streamflow": ["Available", "Not Available", "Available"],
            }
        )


if __name__ == "__main__":
    unittest.main()
