import contextlib
import io
import runpy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

import streamwq as stream
from streamwq._metadata import _storage_provider
from streamwq._storage import dataset_path


class MetadataTests(unittest.TestCase):
    def setUp(self):
        _storage_provider.cache_clear()
        self.addCleanup(_storage_provider.cache_clear)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        schema = pa.schema(
            [
                pa.field("STREAM_ID", pa.string(), nullable=False),
                pa.field("Latitude", pa.float64()),
                pa.field("Longitude", pa.float64()),
                pa.field("Name", pa.string()),
            ],
            metadata={b"source": b"STREAM fixture"},
        )
        self.metadata = pa.Table.from_pylist(
            [
                {"STREAM_ID": "STREAM-gauge-1", "Latitude": 38.0, "Longitude": -77.0, "Name": "First"},
                {"STREAM_ID": "STREAM-gauge-2", "Latitude": 39.0, "Longitude": -78.0, "Name": None},
                {"STREAM_ID": "STREAM-gauge-3", "Latitude": 40.0, "Longitude": -79.0, "Name": "Third"},
            ],
            schema=schema,
        )
        path = Path(self.directory.name) / dataset_path("metadata")
        path.parent.mkdir(parents=True)
        pq.write_table(self.metadata, path)
        filesystem = fs.SubTreeFileSystem(self.directory.name, fs.LocalFileSystem())
        provider = patch("streamwq.S3hsclient.S3HydroShare")
        self.provider = provider.start()
        self.hydroshare = self.provider.return_value
        self.addCleanup(provider.stop)
        self.hydroshare.get_s3_filesystem.return_value = filesystem

    def test_omitting_site_ids_returns_all_metadata_as_arrow(self):
        result = stream.get_metadata()

        self.assertIsInstance(result, pa.Table)
        self.assertTrue(result.equals(self.metadata, check_metadata=True))

    def test_selects_sites_from_an_id_sequence_or_arrow_table(self):
        selections = [
            ("STREAM-gauge-2", [1]),
            (["STREAM-gauge-1", "STREAM-gauge-3"], [0, 2]),
            (("STREAM-gauge-3",), [2]),
            (pa.table({"STREAM_ID": ["STREAM-gauge-2"]}), [1]),
        ]
        for selection, indices in selections:
            with self.subTest(selection=selection):
                result = stream.get_metadata(site_ids=selection)

                self.assertTrue(
                    result.equals(self.metadata.take(indices), check_metadata=True)
                )

    def test_rejects_explicitly_empty_site_selections_before_connecting(self):
        selections = ["", [], (), pa.table({"STREAM_ID": pa.array([], type=pa.string())})]
        for selection in selections:
            with self.subTest(selection=selection):
                with self.assertRaisesRegex(ValueError, "site_ids.*at least one"):
                    stream.get_metadata(site_ids=selection)
        self.hydroshare.get_s3_filesystem.assert_not_called()

    def test_rejects_malformed_site_selections(self):
        selections = [
            [None],
            [1],
            ["STREAM-gauge-1", " "],
            42,
            {"STREAM_ID": "STREAM-gauge-1"},
            pa.table({"other": ["STREAM-gauge-1"]}),
            pa.table({"STREAM_ID": ["STREAM-gauge-1"], "other": [1]}),
            pa.table({"STREAM_ID": [None]}),
        ]
        for selection in selections:
            with self.subTest(selection=selection):
                with self.assertRaisesRegex(ValueError, "site_ids"):
                    stream.get_metadata(site_ids=selection)
        self.hydroshare.get_s3_filesystem.assert_not_called()

    def test_projects_columns_while_filtering_on_unprojected_site_id(self):
        result = stream.get_metadata("STREAM-gauge-2", columns=["Name", "Latitude"])

        self.assertTrue(
            result.equals(
                self.metadata.take([1]).select(["Name", "Latitude"]),
                check_metadata=True,
            )
        )

    def test_duplicate_and_unknown_ids_filter_stored_rows(self):
        result = stream.get_metadata(
            ["STREAM-gauge-2", "STREAM-gauge-2", "STREAM-unknown"]
        )

        self.assertTrue(result.equals(self.metadata.take([1]), check_metadata=True))

    def test_no_matching_ids_returns_empty_table_with_stored_schema(self):
        result = stream.get_metadata("STREAM-unknown")

        self.assertEqual(result.num_rows, 0)
        self.assertTrue(result.schema.equals(self.metadata.schema, check_metadata=True))

    def test_missing_filesystem_has_a_clear_error(self):
        self.hydroshare.get_s3_filesystem.return_value = None

        with self.assertRaisesRegex(RuntimeError, "S3 filesystem is required"):
            stream.get_metadata()

    def test_repeated_reads_reuse_the_authenticated_storage_provider(self):
        stream.get_metadata()
        stream.get_metadata("STREAM-gauge-2")

        self.provider.assert_called_once_with()

    def test_rejects_invalid_column_selections(self):
        for columns in ["STREAM_ID", [""], [None]]:
            with self.subTest(columns=columns):
                with self.assertRaisesRegex(ValueError, "columns.*sequence"):
                    stream.get_metadata(columns=columns)

    def test_documented_metadata_catalog_example_runs(self):
        path = Path(__file__).resolve().parents[1] / "examples" / "metadata_catalog.py"
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            runpy.run_path(str(path), run_name="__main__")

        self.assertIn("Water-quality variables:", output.getvalue())
        self.assertIn("STREAM-gauge-1", output.getvalue())
        self.assertIn("STREAM-gauge-2", output.getvalue())


if __name__ == "__main__":
    unittest.main()