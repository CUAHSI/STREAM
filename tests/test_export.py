import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd

from streamwq._storage import RESOURCE_ID
from streamwq.export import export_subset


class ExportSubsetTests(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1"],
                "DateTime": [pd.Timestamp("2020-01-01")],
                "WTemp_C": [10.0],
            }
        )

    def test_exports_parquet_and_manifest(self):
        with TemporaryDirectory() as directory:
            data_path, metadata_path = export_subset(
                self.data,
                Path(directory) / "subset.parquet",
                query={"start": pd.Timestamp("2020-01-01"), "quality": {"A"}},
                description="Approved water-temperature subset",
            )

            result = pd.read_parquet(data_path)
            manifest = json.loads(metadata_path.read_text())
            pd.testing.assert_frame_equal(result, self.data)
            self.assertEqual(metadata_path.name, "subset.metadata.json")
            self.assertEqual(manifest["stream_resource_id"], RESOURCE_ID)
            self.assertEqual(manifest["row_count"], 1)
            self.assertEqual(manifest["columns"], list(self.data.columns))
            self.assertEqual(manifest["query"]["start"], "2020-01-01T00:00:00")
            self.assertEqual(manifest["query"]["quality"], ["A"])

    def test_exports_csv(self):
        with TemporaryDirectory() as directory:
            data_path, metadata_path = export_subset(
                self.data,
                Path(directory) / "subset.csv",
            )

            self.assertTrue(data_path.exists())
            manifest = json.loads(metadata_path.read_text())
            self.assertEqual(manifest["format"], "csv")

    def test_creates_parent_directory(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "subset.parquet"

            data_path, metadata_path = export_subset(self.data, path)

            self.assertTrue(data_path.exists())
            self.assertTrue(metadata_path.exists())

    def test_refuses_to_overwrite_by_default(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "subset.csv"
            export_subset(self.data, path)

            with self.assertRaisesRegex(FileExistsError, "would overwrite"):
                export_subset(self.data, path)

    def test_allows_explicit_overwrite(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "subset.csv"
            export_subset(self.data, path)

            export_subset(self.data.iloc[0:0], path, overwrite=True)

            manifest = json.loads(path.with_suffix(".metadata.json").read_text())
            self.assertEqual(manifest["row_count"], 0)

    def test_rejects_unsupported_extension(self):
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, ".parquet or .csv"):
                export_subset(self.data, Path(directory) / "subset.xlsx")


if __name__ == "__main__":
    unittest.main()
