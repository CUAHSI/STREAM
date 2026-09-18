import unittest
from unittest.mock import Mock, patch

import pandas as pd

from utils.catalog import dataset_path
from utils.client import StreamClient


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


if __name__ == "__main__":
    unittest.main()
