"""Spatial selection helpers for STREAM monitoring sites."""

from collections.abc import Sequence
import math

import numpy as np
import pandas as pd


EARTH_RADIUS_KM = 6371.0088


def filter_huc(
    sites: pd.DataFrame,
    lookup: pd.DataFrame,
    huc6: str | Sequence[str] | None = None,
    huc8: str | Sequence[str] | None = None,
) -> pd.DataFrame:
    """Join HUC codes to site metadata and filter by HUC6 or HUC8."""

    huc6_codes = _normalize_huc_codes(huc6, level=6)
    huc8_codes = _normalize_huc_codes(huc8, level=8)
    if not huc6_codes and not huc8_codes:
        return sites.reset_index(drop=True)

    required = {"STREAM_ID", "HUC6", "HUC8"}
    missing = sorted(required - set(lookup.columns))
    if missing:
        raise ValueError(f"HUC lookup is missing column(s): {', '.join(missing)}")

    huc_columns = lookup[["STREAM_ID", "HUC6", "HUC8"]].copy()
    huc_columns["HUC6"] = huc_columns["HUC6"].astype("string")
    huc_columns["HUC8"] = huc_columns["HUC8"].astype("string")
    result = sites.merge(huc_columns, on="STREAM_ID", how="inner")

    if huc6_codes:
        result = result[result["HUC6"].isin(huc6_codes)]
    if huc8_codes:
        result = result[result["HUC8"].isin(huc8_codes)]
    return result.reset_index(drop=True)


def filter_bbox(
    sites: pd.DataFrame,
    bbox: Sequence[float],
    longitude_column: str = "longitude_wgs84",
    latitude_column: str = "latitude_wgs84",
) -> pd.DataFrame:
    """Return sites inside a bounding box, including its boundary.

    Parameters
    ----------
    sites
        Site metadata containing longitude and latitude columns.
    bbox
        Four values ordered as ``(west, south, east, north)`` in WGS 84.
    """

    west, south, east, north = _validate_bbox(bbox)

    missing = [
        column
        for column in (longitude_column, latitude_column)
        if column not in sites.columns
    ]
    if missing:
        raise ValueError(f"Site metadata is missing coordinate column(s): {', '.join(missing)}")

    matches = (
        sites[longitude_column].between(west, east, inclusive="both")
        & sites[latitude_column].between(south, north, inclusive="both")
    )
    return sites.loc[matches].reset_index(drop=True)


def filter_radius(
    sites: pd.DataFrame,
    center: Sequence[float],
    radius_km: float,
    longitude_column: str = "longitude_wgs84",
    latitude_column: str = "latitude_wgs84",
) -> pd.DataFrame:
    """Return sites within a radius and add their distance from the center.

    ``center`` is ordered as ``(longitude, latitude)`` in WGS 84. Results are
    sorted from the nearest site to the farthest.
    """

    _validate_center(center)
    radius_km = _validate_radius(radius_km)
    result = _add_distances(
        sites,
        center,
        longitude_column=longitude_column,
        latitude_column=latitude_column,
    )
    result = result[result["distance_km"] <= radius_km]
    return result.sort_values("distance_km").reset_index(drop=True)


def select_nearest(
    sites: pd.DataFrame,
    center: Sequence[float],
    count: int = 1,
    longitude_column: str = "longitude_wgs84",
    latitude_column: str = "latitude_wgs84",
) -> pd.DataFrame:
    """Return the nearest ``count`` sites to a longitude/latitude point."""

    _validate_center(center)
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("nearest must be a positive integer")

    result = _add_distances(
        sites,
        center,
        longitude_column=longitude_column,
        latitude_column=latitude_column,
    )
    return (
        result.dropna(subset=["distance_km"])
        .sort_values("distance_km")
        .head(count)
        .reset_index(drop=True)
    )


def _add_distances(
    sites: pd.DataFrame,
    center: Sequence[float],
    longitude_column: str,
    latitude_column: str,
) -> pd.DataFrame:
    center_longitude, center_latitude = _validate_center(center)
    missing = [
        column
        for column in (longitude_column, latitude_column)
        if column not in sites.columns
    ]
    if missing:
        raise ValueError(f"Site metadata is missing coordinate column(s): {', '.join(missing)}")

    longitude = pd.to_numeric(sites[longitude_column], errors="coerce")
    latitude = pd.to_numeric(sites[latitude_column], errors="coerce")
    longitude_radians = np.radians(longitude)
    latitude_radians = np.radians(latitude)
    center_longitude_radians = math.radians(center_longitude)
    center_latitude_radians = math.radians(center_latitude)

    longitude_difference = longitude_radians - center_longitude_radians
    latitude_difference = latitude_radians - center_latitude_radians
    haversine = (
        np.sin(latitude_difference / 2) ** 2
        + np.cos(center_latitude_radians)
        * np.cos(latitude_radians)
        * np.sin(longitude_difference / 2) ** 2
    )

    result = sites.copy()
    result["distance_km"] = (
        2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(haversine))
    )
    return result


def _validate_bbox(bbox: Sequence[float]) -> tuple[float, float, float, float]:
    if isinstance(bbox, (str, bytes)):
        raise ValueError("bbox must contain four numeric values")

    try:
        values = tuple(float(value) for value in bbox)
    except (TypeError, ValueError):
        raise ValueError("bbox must contain four numeric values") from None

    if len(values) != 4 or not all(math.isfinite(value) for value in values):
        raise ValueError("bbox must contain four finite numeric values")

    west, south, east, north = values
    if not (-180 <= west <= 180 and -180 <= east <= 180):
        raise ValueError("bbox longitude values must be between -180 and 180")
    if not (-90 <= south <= 90 and -90 <= north <= 90):
        raise ValueError("bbox latitude values must be between -90 and 90")
    if west >= east:
        raise ValueError("bbox west must be less than east")
    if south >= north:
        raise ValueError("bbox south must be less than north")

    return west, south, east, north


def _validate_center(center: Sequence[float]) -> tuple[float, float]:
    if isinstance(center, (str, bytes)):
        raise ValueError("center must contain longitude and latitude")
    try:
        values = tuple(float(value) for value in center)
    except (TypeError, ValueError):
        raise ValueError("center must contain longitude and latitude") from None
    if len(values) != 2 or not all(math.isfinite(value) for value in values):
        raise ValueError("center must contain finite longitude and latitude")

    longitude, latitude = values
    if not -180 <= longitude <= 180:
        raise ValueError("center longitude must be between -180 and 180")
    if not -90 <= latitude <= 90:
        raise ValueError("center latitude must be between -90 and 90")
    return longitude, latitude


def _validate_radius(radius_km: float) -> float:
    try:
        radius_km = float(radius_km)
    except (TypeError, ValueError):
        raise ValueError("radius_km must be a positive number") from None
    if not math.isfinite(radius_km) or radius_km <= 0:
        raise ValueError("radius_km must be a positive number")
    return radius_km


def _normalize_huc_codes(
    values: str | Sequence[str] | None,
    level: int,
) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    else:
        values = list(values)

    normalized = []
    for value in values:
        code = str(value).strip()
        if len(code) != level or not code.isdigit():
            raise ValueError(f"HUC{level} codes must contain exactly {level} digits")
        if code not in normalized:
            normalized.append(code)
    return normalized
