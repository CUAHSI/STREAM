"""Optional conversions and analysis helpers for STREAM results."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    import geopandas as gpd


def to_geopandas(table: pa.Table) -> gpd.GeoDataFrame:
    """Convert a GeoParquet Arrow table to a GeoDataFrame.

    The GeoParquet ``geo`` schema metadata identifies the active geometry and
    coordinate reference system. Install ``streamwq[geopandas]`` to use this
    optional conversion.
    """
    if not isinstance(table, pa.Table):
        raise TypeError("table must be a pyarrow.Table with GeoParquet metadata")

    geo_metadata = (table.schema.metadata or {}).get(b"geo")
    if geo_metadata is None:
        raise ValueError(
            "Arrow table is missing GeoParquet 'geo' schema metadata; "
            "use a GeoParquet table such as the result of get_watersheds"
        )
    try:
        geo = json.loads(geo_metadata)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("GeoParquet 'geo' metadata must contain valid JSON") from error
    if not isinstance(geo, dict):
        raise ValueError("GeoParquet 'geo' metadata must be a JSON object")

    primary_column = geo.get("primary_column")
    columns = geo.get("columns")
    if (
        not isinstance(primary_column, str)
        or not isinstance(columns, dict)
        or primary_column not in columns
        or primary_column not in table.column_names
    ):
        raise ValueError(
            "GeoParquet geometry metadata must identify a geometry column "
            "present in the table"
        )

    geometry_metadata = columns[primary_column]
    if not isinstance(geometry_metadata, dict):
        raise ValueError("GeoParquet geometry metadata must be a JSON object")
    if geometry_metadata.get("encoding") != "WKB":
        raise ValueError("GeoParquet geometry encoding must be WKB")
    crs = geometry_metadata.get("crs")
    if crs is None:
        crs = "OGC:CRS84"

    try:
        import geopandas as gpd
    except ImportError as error:
        raise RuntimeError(
            "GeoPandas is optional; install it with "
            "`pip install 'streamwq[geopandas]'` to convert watershed geometry"
        ) from error

    frame = table.to_pandas()
    frame[primary_column] = gpd.GeoSeries.from_wkb(
        table[primary_column].to_pylist(),
        crs=crs,
    )
    return gpd.GeoDataFrame(frame, geometry=primary_column, crs=crs)
