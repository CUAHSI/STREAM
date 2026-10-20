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
            for name in ("pandas", "geopandas", "xarray", "streamwq.client",
                         "streamwq.access", "streamwq.S3hsclient"):
                assert name not in sys.modules, name
        ''')
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_metadata_and_catalog_work_without_dataframe_dependencies(self):
        script = textwrap.dedent('''
            import importlib.abc
            import sys

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

            import tempfile
            from pathlib import Path
            from unittest.mock import patch
            import pyarrow as pa
            import pyarrow.fs as fs
            import pyarrow.parquet as pq
            from streamwq._storage import dataset_path
            from streamwq.S3hsclient import S3HydroShare

            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / dataset_path("metadata")
                path.parent.mkdir(parents=True)
                pq.write_table(pa.table({"STREAM_ID": ["STREAM-gauge-1"]}), path)
                filesystem = fs.SubTreeFileSystem(directory, fs.LocalFileSystem())
                with patch.object(S3HydroShare, "get_s3_filesystem", return_value=filesystem):
                    with patch.object(S3HydroShare, "__init__", return_value=None):
                        result = stream.get_metadata("STREAM-gauge-1")
                assert result.to_pylist() == [{"STREAM_ID": "STREAM-gauge-1"}]
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