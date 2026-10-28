import contextlib
import io
import runpy
import sys
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

import streamwq as stream
from streamwq._metadata import _storage_provider
from streamwq._storage import dataset_path


class GetDataTests(unittest.TestCase):
    def setUp(self):
        _storage_provider.cache_clear()
        self.addCleanup(_storage_provider.cache_clear)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.metadata = pa.table(
            {
                "STREAM_ID": ["site-1", "site-2"],
                "Name": ["First site", "Second site"],
            }
        )
        self.water_quality = pa.table(
            {
                "STREAM_ID": ["site-1", "site-1", "site-1"],
                "DateTime": [
                    datetime(2020, 1, 1),
                    datetime(2020, 1, 2),
                    datetime(2020, 1, 3),
                ],
                "WTemp_C": [10.0, 11.0, 12.0],
                "Flag_WTemp_C": ["A", "P", "E"],
            }
        )
        self.streamflow = pa.table(
            {
                "STREAM_ID": ["site-1", "site-1"],
                "DateTime": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
                "Q_m3s": [2.5, 3.5],
                "Flag_Q_m3s": ["A", "P"],
            }
        )
        self._write_file("metadata", self.metadata)
        self._write_partitioned_dataset("water_quality", self.water_quality)
        self._write_partitioned_dataset("streamflow", self.streamflow)
        filesystem = fs.SubTreeFileSystem(self.directory.name, fs.LocalFileSystem())
        storage_provider = types.SimpleNamespace(
            get_s3_filesystem=lambda: filesystem
        )
        hydroshare_module = types.ModuleType("streamwq.S3hsclient")
        self.hydroshare_class = Mock(return_value=storage_provider)
        hydroshare_module.S3HydroShare = self.hydroshare_class
        module_patch = patch.dict(
            sys.modules, {"streamwq.S3hsclient": hydroshare_module}
        )
        module_patch.start()
        self.addCleanup(module_patch.stop)

    def _write_file(self, name, table):
        path = Path(self.directory.name) / dataset_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)

    def _write_partitioned_dataset(self, name, table):
        path = Path(self.directory.name) / dataset_path(name)
        path.mkdir(parents=True)
        pq.write_to_dataset(table, root_path=path, partition_cols=["STREAM_ID"])

    def test_reads_water_quality_as_arrow_with_projection_date_and_quality_filters(self):
        result = stream.get_data(
            "water_quality",
            site_ids="site-1",
            columns=["DateTime", "WTemp_C"],
            start="2020-01-02",
            end="2020-01-02",
            quality="provisional",
        )

        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.column_names, ["DateTime", "WTemp_C"])
        self.assertEqual(result.column("WTemp_C").to_pylist(), [11.0])
        self.assertEqual(
            result.column("DateTime").to_pylist(),
            [datetime(2020, 1, 2)],
        )

    def test_reads_streamflow_and_defaults_to_all_schema_columns(self):
        result = stream.get_data("streamflow", site_ids="site-1")

        self.assertIsInstance(result, pa.Table)
        self.assertEqual(
            set(result.column_names),
            {"STREAM_ID", "DateTime", "Q_m3s", "Flag_Q_m3s"},
        )
        self.assertEqual(result.num_rows, 2)

    def test_accepts_sequence_and_one_column_arrow_site_selections(self):
        selections = [
            ["site-1"],
            pa.table({"STREAM_ID": ["site-1"]}),
        ]
        for site_ids in selections:
            with self.subTest(site_ids=site_ids):
                result = stream.get_data(
                    "streamflow",
                    site_ids=site_ids,
                    columns=["STREAM_ID", "Q_m3s"],
                )
                self.assertEqual(result.column("Q_m3s").to_pylist(), [2.5, 3.5])

    def test_rejects_missing_or_malformed_site_selections(self):
        selections = [
            None,
            "",
            [],
            pa.table({"STREAM_ID": pa.array([], type=pa.string())}),
            [None],
            ["site-1", " "],
            pa.table({"other": ["site-1"]}),
        ]
        for site_ids in selections:
            with self.subTest(site_ids=site_ids):
                with self.assertRaisesRegex(ValueError, "site_ids"):
                    stream.get_data("water_quality", site_ids=site_ids)
        self.hydroshare_class.assert_not_called()

    def test_rejects_mixed_known_and_unknown_ids(self):
        with self.assertRaisesRegex(ValueError, "Unknown site ID.*not-a-site"):
            stream.get_data(
                "water_quality",
                site_ids=["site-1", "not-a-site"],
            )
        with self.assertRaisesRegex(ValueError, "Unknown site ID.*not-a-site"):
            stream.get_data("water_quality", site_ids="not-a-site")

    def test_quality_filter_preserves_exact_projection_and_requires_measurements(self):
        result = stream.get_data(
            "water_quality",
            site_ids="site-1",
            columns=["WTemp_C"],
            quality="approved",
        )

        self.assertEqual(result.column_names, ["WTemp_C"])
        self.assertEqual(result.column("WTemp_C").to_pylist(), [10.0])
        with self.assertRaisesRegex(ValueError, "requires at least one measurement"):
            stream.get_data(
                "water_quality",
                site_ids="site-1",
                columns=["STREAM_ID"],
                quality="approved",
            )

    def test_returns_projected_schema_for_known_sites_without_observations(self):
        result = stream.get_data(
            "water_quality",
            site_ids="site-2",
            columns=["STREAM_ID", "DateTime", "WTemp_C"],
        )

        self.assertEqual(result.num_rows, 0)
        self.assertEqual(
            result.schema,
            pa.schema(
                [
                    pa.field("STREAM_ID", pa.string()),
                    pa.field("DateTime", pa.timestamp("us")),
                    pa.field("WTemp_C", pa.float64()),
                ]
            ),
        )

    def test_rejects_unknown_datasets_columns_and_invalid_date_ranges(self):
        invalid_requests = [
            ({"dataset": "not-a-dataset"}, "Unknown dataset"),
            ({"dataset": "meteorology"}, "supports water_quality, streamflow"),
            (
                {"dataset": "water_quality", "columns": ["not-a-column"]},
                "Unknown column",
            ),
            (
                {
                    "dataset": "water_quality",
                    "start": "not-a-date",
                },
                "start must be a valid ISO date",
            ),
            (
                {
                    "dataset": "water_quality",
                    "start": "2020-01-03",
                    "end": "2020-01-01",
                },
                "start must be before or equal to end",
            ),
        ]
        for arguments, message in invalid_requests:
            with self.subTest(arguments=arguments):
                request = {
                    "site_ids": "site-1",
                    **arguments,
                }
                with self.assertRaisesRegex(ValueError, message):
                    stream.get_data(**request)

    def test_requires_dataset_and_site_ids_with_clear_value_errors(self):
        with self.assertRaisesRegex(ValueError, "dataset is required"):
            stream.get_data(site_ids="site-1")
        with self.assertRaisesRegex(ValueError, "site_ids"):
            stream.get_data("water_quality")

    def test_rejects_timezone_aware_bounds(self):
        with self.assertRaisesRegex(ValueError, "timezone-naive"):
            stream.get_data(
                "water_quality",
                site_ids="site-1",
                start="2020-01-01T00:00:00+00:00",
            )

    def test_mixed_metadata_and_data_reads_reuse_the_storage_provider(self):
        stream.get_metadata()
        stream.get_data("streamflow", site_ids="site-1")

        self.hydroshare_class.assert_called_once_with()

    def test_documented_data_access_example_runs_against_local_fixtures(self):
        path = Path(__file__).resolve().parents[1] / "examples" / "data_access.py"
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            runpy.run_path(str(path), run_name="__main__")

        self.assertIn("Water-quality observations:", output.getvalue())
        self.assertIn("Streamflow observations:", output.getvalue())


if __name__ == "__main__":
    unittest.main()
