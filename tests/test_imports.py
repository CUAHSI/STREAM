import subprocess
import sys
import textwrap
import unittest


class CoreImportTests(unittest.TestCase):
    def test_import_does_not_eagerly_load_dataframe_or_legacy_modules(self):
        script = textwrap.dedent('''
            import sys
            import streamwq as stream
            assert stream.catalog.DATASETS
            assert callable(stream.get_sites)
            assert callable(stream.get_data)
            assert callable(stream.get_watersheds)
            assert callable(stream.helpers.to_geopandas)
            for name in ("pandas", "geopandas", "xarray", "streamwq.client",
                         "streamwq.access", "streamwq.S3hsclient"):
                assert name not in sys.modules, name
        ''')
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_module_level_access_works_without_dataframe_dependencies(self):
        script = textwrap.dedent('''
            import importlib.abc
            import sys
            from datetime import datetime

            class BlockDataframes(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname.split(".")[0] in {"pandas", "geopandas"}:
                        raise ModuleNotFoundError(fullname)

            sys.meta_path.insert(0, BlockDataframes())

            import streamwq as stream
            assert "streamwq.client" not in sys.modules
            assert "streamwq.access" not in sys.modules
            assert "streamwq.S3hsclient" not in sys.modules
            assert stream.catalog.VARIABLES["streamflow"] == ("Q_m3s",)
            assert callable(stream.get_data)

            import tempfile
            from pathlib import Path
            from unittest.mock import patch
            import json
            import pyarrow as pa
            import pyarrow.fs as fs
            import pyarrow.parquet as pq
            from streamwq.S3hsclient import S3HydroShare
            from streamwq._storage import dataset_path

            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / dataset_path("metadata")
                path.parent.mkdir(parents=True)
                pq.write_table(pa.table({"STREAM_ID": ["STREAM-gauge-1"]}), path)
                watershed_path = Path(directory) / dataset_path("watersheds")
                watershed_path.parent.mkdir(parents=True, exist_ok=True)
                watershed_table = pa.table({
                    "STREAM_ID": ["STREAM-gauge-1"],
                    "COMID": [101],
                    "area_km2": [1.5],
                    "geometry": [b"wkb"],
                }).replace_schema_metadata({
                    b"geo": json.dumps({
                        "version": "1.1.0",
                        "primary_column": "geometry",
                        "columns": {"geometry": {
                            "encoding": "WKB",
                            "geometry_types": ["Polygon"],
                            "crs": None,
                        }},
                    }).encode()
                })
                pq.write_table(watershed_table, watershed_path)
                filesystem = fs.SubTreeFileSystem(directory, fs.LocalFileSystem())
                with patch.object(S3HydroShare, "get_s3_filesystem", return_value=filesystem):
                    with patch.object(
                        S3HydroShare, "__init__", return_value=None
                    ) as initialize_provider:
                        result = stream.get_metadata("STREAM-gauge-1")
                        watersheds = stream.get_watersheds("STREAM-gauge-1")
                        data_path = Path(directory) / dataset_path("streamflow")
                        data_path.mkdir(parents=True)
                        pq.write_to_dataset(
                            pa.table({
                                "STREAM_ID": ["STREAM-gauge-1"],
                                "DateTime": [datetime(2020, 1, 1)],
                                "Q_m3s": [2.5],
                                "Flag_Q_m3s": ["A"],
                            }),
                            root_path=data_path,
                            partition_cols=["STREAM_ID"],
                        )
                        observations = stream.get_data(
                            "streamflow",
                            site_ids="STREAM-gauge-1",
                            columns=["Q_m3s"],
                        )
                assert initialize_provider.call_count == 1
                assert result.to_pylist() == [{"STREAM_ID": "STREAM-gauge-1"}]
                assert watersheds.column("STREAM_ID").to_pylist() == ["STREAM-gauge-1"]
                assert observations.column("Q_m3s").to_pylist() == [2.5]
            assert "pandas" not in sys.modules
            assert "geopandas" not in sys.modules
        ''')
        result = subprocess.run(
            [sys.executable, "-W", "ignore::DeprecationWarning", "-c", script],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()