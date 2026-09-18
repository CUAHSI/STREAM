import unittest
from unittest.mock import Mock

import pandas as pd

from utils.catalog import dataset_path
from utils.client import StreamClient
from utils.sites import SITE_COLUMNS


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

    def test_delegates_water_quality_read(self):
        self.client.read_water_quality(
            "STREAM-gauge-1",
            "WTemp_C",
            start="2020-01-01",
            end="2020-12-31",
        )
        self.access.read_water_quality.assert_called_once_with(
            site_ids="STREAM-gauge-1",
            variables="WTemp_C",
            start="2020-01-01",
            end="2020-12-31",
        )


if __name__ == "__main__":
    unittest.main()
