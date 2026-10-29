import contextlib
import io
import json
import runpy
import struct
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

import streamwq as stream
from streamwq._storage import dataset_path


def _geoparquet_table(schema_metadata):
    geometry_metadata = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": None,
            }
        },
    }
    if schema_metadata is None:
        metadata = {b"geo": json.dumps(geometry_metadata).encode()}
    else:
        metadata = schema_metadata
    coordinates = [
        (-78.0, 38.0),
        (-77.0, 38.0),
        (-77.0, 39.0),
        (-78.0, 39.0),
        (-78.0, 38.0),
    ]
    schema = pa.schema(
        [
            pa.field("STREAM_ID", pa.string()),
            pa.field("COMID", pa.int64()),
            pa.field("area_km2", pa.float64()),
            pa.field("geometry", pa.binary()),
        ],
        metadata=metadata,
    )
    return pa.Table.from_pylist(
        [
            {
                "STREAM_ID": "site-1",
                "COMID": 101,
                "area_km2": 1.5,
                "geometry": _polygon_wkb(coordinates),
            }
        ],
        schema=schema,
    )


def _polygon_wkb(coordinates):
    ring = b"".join(struct.pack("<dd", x, y) for x, y in coordinates)
    return struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(coordinates)) + ring


class GetWatershedsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        geometry_metadata = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Polygon"],
                    "crs": None,
                }
            },
        }
        coordinates = [
            (-78.0, 38.0),
            (-77.0, 38.0),
            (-77.0, 39.0),
            (-78.0, 39.0),
            (-78.0, 38.0),
        ]
        schema = pa.schema(
            [
                pa.field("STREAM_ID", pa.string()),
                pa.field("COMID", pa.int64()),
                pa.field("area_km2", pa.float64()),
                pa.field("geometry", pa.binary()),
            ],
            metadata={b"geo": json.dumps(geometry_metadata).encode()},
        )
        self.watersheds = pa.Table.from_pylist(
            [
                {
                    "STREAM_ID": "site-1",
                    "COMID": 101,
                    "area_km2": 1.5,
                    "geometry": _polygon_wkb(coordinates),
                },
                {
                    "STREAM_ID": "site-2",
                    "COMID": 202,
                    "area_km2": 2.5,
                    "geometry": _polygon_wkb(coordinates),
                },
            ],
            schema=schema,
        )
        path = Path(self.directory.name) / dataset_path("watersheds")
        path.parent.mkdir(parents=True)
        pq.write_table(self.watersheds, path)
        filesystem = fs.SubTreeFileSystem(self.directory.name, fs.LocalFileSystem())
        provider = types.SimpleNamespace(get_s3_filesystem=lambda: filesystem)
        self.provider_patch = patch(
            "streamwq._watersheds._storage_provider", return_value=provider
        )
        self.provider_patch.start()
        self.addCleanup(self.provider_patch.stop)

    def test_reads_selected_watersheds_as_arrow_with_geoparquet_metadata(self):
        selections = [
            ("site-1", {"site-1"}),
            (["site-2"], {"site-2"}),
            (pa.table({"STREAM_ID": ["site-1", "site-2", "site-1"]}), {"site-1", "site-2"}),
        ]
        for site_ids, expected_ids in selections:
            with self.subTest(site_ids=site_ids):
                result = stream.get_watersheds(site_ids=site_ids)

                self.assertIsInstance(result, pa.Table)
                self.assertEqual(result.column_names, ["STREAM_ID", "COMID", "area_km2", "geometry"])
                self.assertEqual(set(result.column("STREAM_ID").to_pylist()), expected_ids)
                self.assertEqual(
                    result.schema.metadata,
                    self.watersheds.schema.metadata,
                )

    def test_filters_unknown_ids_without_rejecting_known_matches(self):
        result = stream.get_watersheds(["site-1", "unknown"])

        self.assertEqual(result.column("STREAM_ID").to_pylist(), ["site-1"])

    def test_unknown_only_selection_returns_empty_table_with_spatial_metadata(self):
        result = stream.get_watersheds("unknown")

        self.assertEqual(result.num_rows, 0)
        self.assertEqual(result.schema.metadata, self.watersheds.schema.metadata)

    def test_rejects_missing_or_malformed_site_selections(self):
        selections = [
            None,
            "",
            [],
            pa.table({"STREAM_ID": pa.array([], type=pa.string())}),
            [None],
            ["site-1", " "],
            42,
            pa.table({"other": ["site-1"]}),
        ]
        for site_ids in selections:
            with self.subTest(site_ids=site_ids):
                with self.assertRaisesRegex(ValueError, "site_ids"):
                    stream.get_watersheds(site_ids)

    def test_geoarrow_conversion_restores_geometry_and_default_crs(self):
        result = stream.helpers.to_geopandas(self.watersheds.slice(0, 1))

        self.assertEqual(result.geometry.name, "geometry")
        self.assertEqual(result.geometry.iloc[0].geom_type, "Polygon")
        self.assertEqual(result.geometry.iloc[0].bounds, (-78.0, 38.0, -77.0, 39.0))
        self.assertEqual(result.crs.to_string(), "OGC:CRS84")
        self.assertEqual(result.crs.axis_info[0].abbrev, "Lon")
        self.assertEqual(result["STREAM_ID"].tolist(), ["site-1"])

    def test_geoarrow_conversion_restores_crs_from_projjson_metadata(self):
        from pyproj import CRS

        geo = json.loads(self.watersheds.schema.metadata[b"geo"])
        geo["columns"]["geometry"]["crs"] = CRS.from_epsg(5070).to_json_dict()
        table = self.watersheds.replace_schema_metadata(
            {b"geo": json.dumps(geo).encode()}
        )

        result = stream.helpers.to_geopandas(table)

        self.assertEqual(result.crs.to_epsg(), 5070)

    def test_geoarrow_conversion_rejects_missing_or_malformed_metadata(self):
        malformed_tables = [
            (pa.table({"geometry": [b"bad"]}), "GeoParquet"),
            (
                _geoparquet_table({b"geo": b"not-json"}),
                "valid JSON",
            ),
            (
                _geoparquet_table(
                    {
                        b"geo": json.dumps(
                            {
                                "version": "1.1.0",
                                "primary_column": "geometry",
                                "columns": {},
                            }
                        ).encode()
                    }
                ),
                "geometry metadata",
            ),
            (
                _geoparquet_table(
                    {
                        b"geo": json.dumps(
                            {
                                "version": "1.1.0",
                                "primary_column": "geometry",
                                "columns": {
                                    "geometry": {"encoding": "point"}
                                },
                            }
                        ).encode()
                    }
                ),
                "encoding must be WKB",
            ),
        ]
        for table, message in malformed_tables:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    stream.helpers.to_geopandas(table)

    def test_geoarrow_conversion_reports_how_to_install_geopandas(self):
        original_import = __import__

        def import_without_geopandas(name, *args, **kwargs):
            if name == "geopandas":
                raise ModuleNotFoundError("No module named 'geopandas'", name=name)
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=import_without_geopandas):
            with self.assertRaisesRegex(RuntimeError, r"streamwq\[geopandas\]"):
                stream.helpers.to_geopandas(self.watersheds)

    def test_documented_watershed_example_uses_arrow_before_conversion(self):
        path = Path(__file__).resolve().parents[1] / "examples" / "watershed_access.py"
        namespace = runpy.run_path(str(path))
        output = io.StringIO()
        with (
            patch.object(stream, "get_watersheds", return_value=self.watersheds) as read,
            patch.object(stream.helpers, "to_geopandas", return_value="GeoDataFrame") as convert,
            contextlib.redirect_stdout(output),
        ):
            namespace["main"]("site-1")

        read.assert_called_once_with(site_ids="site-1")
        convert.assert_called_once_with(self.watersheds)
        self.assertIn("Watersheds as Arrow:", output.getvalue())
        self.assertIn("Watersheds as GeoPandas:", output.getvalue())


if __name__ == "__main__":
    unittest.main()
