import unittest
from unittest.mock import Mock, patch

import pandas as pd

from utils.access import StreamAccess
from utils.catalog import dataset_path


class StreamAccessTests(unittest.TestCase):
    def setUp(self):
        self.filesystem = object()
        self.hydroshare = Mock()
        self.hydroshare.get_s3_filesystem.return_value = self.filesystem
        self.access = StreamAccess(hydroshare=self.hydroshare)

    @patch("utils.access.pd.read_parquet")
    def test_reads_selected_metadata_columns(self, read_parquet_mock):
        expected = pd.DataFrame({"STREAM_ID": ["STREAM-gauge-1"]})
        read_parquet_mock.return_value = expected

        result = self.access.read_metadata(columns=["STREAM_ID"])

        pd.testing.assert_frame_equal(result, expected)
        read_parquet_mock.assert_called_once_with(
            dataset_path("metadata"),
            filesystem=self.filesystem,
            columns=["STREAM_ID"],
        )

    def test_rejects_string_as_metadata_columns(self):
        with self.assertRaisesRegex(ValueError, "sequence of non-empty strings"):
            self.access.read_metadata(columns="STREAM_ID")

    def test_requires_s3_filesystem(self):
        self.hydroshare.get_s3_filesystem.return_value = None
        with self.assertRaisesRegex(RuntimeError, "S3 filesystem is required"):
            self.access.read_metadata()

    @patch("utils.access.ds.dataset")
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

        result = self.access.read_water_quality(
            site_ids="STREAM-gauge-1",
            variables="WTemp_C",
            start="2020-01-01",
            end="2020-12-31",
            quality="approved",
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
            self.access.read_water_quality("STREAM-gauge-1", "not-a-variable")

    def test_rejects_reversed_date_range(self):
        with self.assertRaisesRegex(ValueError, "start must be before"):
            self.access.read_water_quality(
                "STREAM-gauge-1",
                "WTemp_C",
                start="2021-01-01",
                end="2020-01-01",
            )

    def test_requires_at_least_one_site(self):
        with self.assertRaisesRegex(ValueError, "at least one site"):
            self.access.read_water_quality([], "WTemp_C")

    @patch("utils.access.ds.dataset")
    def test_reads_filtered_streamflow(self, dataset_mock):
        expected = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1"],
                "DateTime": [pd.Timestamp("2020-01-01")],
                "Q_m3s": [2.5],
                "Flag_Q_m3s": ["A"],
            }
        )
        table = Mock()
        table.to_pandas.return_value = expected
        dataset_mock.return_value.to_table.return_value = table

        result = self.access.read_streamflow(
            site_ids="STREAM-gauge-1",
            start="2020-01-01",
            end="2020-12-31",
            quality="approved",
        )

        pd.testing.assert_frame_equal(result, expected)
        dataset_mock.assert_called_once_with(
            dataset_path("streamflow"),
            format="parquet",
            filesystem=self.filesystem,
            partitioning="hive",
        )
        call = dataset_mock.return_value.to_table.call_args
        self.assertEqual(
            call.kwargs["columns"],
            ["STREAM_ID", "DateTime", "Q_m3s", "Flag_Q_m3s"],
        )
        self.assertIsNotNone(call.kwargs["filter"])

    def test_streamflow_requires_at_least_one_site(self):
        with self.assertRaisesRegex(ValueError, "at least one site"):
            self.access.read_streamflow([])

    def test_streamflow_rejects_reversed_date_range(self):
        with self.assertRaisesRegex(ValueError, "start must be before"):
            self.access.read_streamflow(
                "STREAM-gauge-1",
                start="2021-01-01",
                end="2020-01-01",
            )

    @patch("utils.access.ds.dataset")
    def test_reads_selected_static_attributes(self, dataset_mock):
        expected = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1"],
                "stream_order": [3],
            }
        )
        dataset_mock.return_value.schema.names = ["STREAM_ID", "stream_order"]
        table = Mock()
        table.to_pandas.return_value = expected
        dataset_mock.return_value.to_table.return_value = table

        result = self.access.read_static_attributes(
            site_ids="STREAM-gauge-1",
            columns="stream_order",
        )

        pd.testing.assert_frame_equal(result, expected)
        dataset_mock.assert_called_once_with(
            dataset_path("static_catchment_characteristics"),
            format="parquet",
            filesystem=self.filesystem,
        )
        call = dataset_mock.return_value.to_table.call_args
        self.assertEqual(call.kwargs["columns"], ["STREAM_ID", "stream_order"])
        self.assertIsNotNone(call.kwargs["filter"])

    @patch("utils.access.ds.dataset")
    def test_rejects_unknown_static_attribute(self, dataset_mock):
        dataset_mock.return_value.schema.names = ["STREAM_ID", "stream_order"]

        with self.assertRaisesRegex(ValueError, "Unknown static attribute"):
            self.access.read_static_attributes(
                site_ids="STREAM-gauge-1",
                columns="not_a_column",
            )

    def test_static_attributes_require_at_least_one_site(self):
        with self.assertRaisesRegex(ValueError, "at least one site"):
            self.access.read_static_attributes([], "stream_order")


if __name__ == "__main__":
    unittest.main()
