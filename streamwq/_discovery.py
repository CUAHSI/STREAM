"""Arrow-based site discovery for the public STREAM API."""

from collections.abc import Sequence
import math

import pyarrow as pa

from streamwq._metadata import _get_huc_lookup, get_metadata


_EARTH_RADIUS_KM = 6371.0088


def get_sites(
    *,
    states: str | Sequence[str] | None = None,
    variables: str | Sequence[str] | None = None,
    bbox: Sequence[float] | None = None,
    center: Sequence[float] | None = None,
    radius_km: float | None = None,
    nearest: int | None = None,
    huc6: str | Sequence[str] | None = None,
    huc8: str | Sequence[str] | None = None,
    streamflow: bool | None = None,
    grab_variables: str | Sequence[str] | None = None,
) -> pa.Table:
    """Return unique site IDs matching metadata and spatial filters.

    ``bbox`` is west, south, east, north in WGS 84 longitude/latitude degrees.
    ``center`` is longitude, latitude in WGS 84 degrees. ``radius_km`` is a
    positive distance from that point. All supplied filters are combined.
    Multiple values for a filter are alternatives.
    """

    state_values = _normalize_values(states, "states")
    variable_values = _normalize_values(variables, "variables")
    grab_values = _normalize_values(grab_variables, "grab_variables")
    huc6_values = _normalize_huc_codes(huc6, 6)
    huc8_values = _normalize_huc_codes(huc8, 8)

    if streamflow is not None and not isinstance(streamflow, bool):
        raise ValueError("streamflow must be True, False, or None")
    bbox_values = _validate_bbox(bbox) if bbox is not None else None
    center_values = _validate_center(center) if center is not None else None
    radius_value = _validate_radius(radius_km) if radius_km is not None else None
    if center_values is None and (radius_value is not None or nearest is not None):
        raise ValueError("center is required with radius_km or nearest")
    if center_values is not None and radius_value is None and nearest is None:
        raise ValueError("center requires radius_km or nearest")
    if nearest is not None and (
        isinstance(nearest, bool) or not isinstance(nearest, int) or nearest <= 0
    ):
        raise ValueError("nearest must be a positive integer")

    metadata = get_metadata()
    required = {"STREAM_ID"}
    if state_values:
        required.add("state_name")
    if variable_values:
        required.add("WQ_parameters")
    if streamflow is not None:
        required.add("streamflow")
    if grab_values:
        required.add("grabsamples")
    if bbox_values is not None or center_values is not None:
        required.update(("longitude_wgs84", "latitude_wgs84"))
    missing = sorted(required - set(metadata.column_names))
    if missing:
        raise ValueError(
            f"STREAM metadata is missing column(s) needed for site discovery: "
            f"{', '.join(missing)}"
        )

    huc_by_site = _load_huc_codes(huc6_values, huc8_values)
    matching_ids: list[str] = []
    seen_ids: set[str] = set()
    distances: dict[str, float] = {}

    for row in metadata.to_pylist():
        site_id = row["STREAM_ID"]
        if not isinstance(site_id, str) or not site_id.strip() or site_id in seen_ids:
            continue
        if state_values and row["state_name"] not in state_values:
            continue
        if variable_values and not _has_any(row["WQ_parameters"], variable_values):
            continue
        if streamflow is not None and (
            _is_available(row["streamflow"]) is not streamflow
        ):
            continue
        if grab_values and not _has_any(row["grabsamples"], grab_values):
            continue
        if (huc6_values or huc8_values) and not _matches_huc(
            huc_by_site.get(site_id, ()), huc6_values, huc8_values
        ):
            continue

        distance = None
        if bbox_values is not None or center_values is not None:
            longitude = _coordinate(row["longitude_wgs84"])
            latitude = _coordinate(row["latitude_wgs84"])
            if longitude is None or latitude is None:
                continue
            if bbox_values is not None and not _inside_bbox(
                longitude, latitude, bbox_values
            ):
                continue
            if center_values is not None:
                distance = _distance_km(longitude, latitude, center_values)
                if radius_value is not None and distance > radius_value:
                    continue

        seen_ids.add(site_id)
        matching_ids.append(site_id)
        if distance is not None:
            distances[site_id] = distance

    if center_values is not None:
        matching_ids.sort(key=distances.__getitem__)
    if nearest is not None:
        matching_ids = matching_ids[:nearest]
    if not matching_ids:
        raise ValueError(
            "No sites matched the supplied filters. Broaden or change the "
            "selection and try again."
        )

    id_field = metadata.schema.field("STREAM_ID")
    result_ids = pa.array(matching_ids, type=id_field.type)
    return pa.Table.from_arrays(
        [result_ids],
        schema=pa.schema([id_field], metadata=metadata.schema.metadata),
    )


def _normalize_values(
    values: str | Sequence[str] | None,
    argument_name: str,
) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    elif not isinstance(values, Sequence):
        raise ValueError(f"{argument_name} must be a string or a sequence of strings")
    else:
        values = list(values)
    if not all(isinstance(value, str) and value.strip() for value in values):
        raise ValueError(f"{argument_name} must contain non-empty strings")
    return list(dict.fromkeys(value.strip() for value in values))


def _normalize_huc_codes(
    values: str | Sequence[str] | None,
    level: int,
) -> list[str]:
    codes = _normalize_values(values, f"huc{level}")
    for code in codes:
        if len(code) != level or not code.isdigit():
            raise ValueError(f"HUC{level} codes must contain exactly {level} digits")
    return codes


def _load_huc_codes(
    huc6_values: list[str],
    huc8_values: list[str],
) -> dict[str, list[tuple[str, str]]]:
    if not huc6_values and not huc8_values:
        return {}
    lookup = _get_huc_lookup()
    missing = sorted({"STREAM_ID", "HUC6", "HUC8"} - set(lookup.column_names))
    if missing:
        raise ValueError(f"HUC lookup is missing column(s): {', '.join(missing)}")
    by_site: dict[str, list[tuple[str, str]]] = {}
    for row in lookup.select(["STREAM_ID", "HUC6", "HUC8"]).to_pylist():
        site_id = row["STREAM_ID"]
        if isinstance(site_id, str):
            codes = (str(row["HUC6"] or ""), str(row["HUC8"] or ""))
            by_site.setdefault(site_id, []).append(codes)
    return by_site


def _matches_huc(
    site_codes: Sequence[tuple[str, str]],
    huc6_values: list[str],
    huc8_values: list[str],
) -> bool:
    return any(
        (not huc6_values or code6 in huc6_values)
        and (not huc8_values or code8 in huc8_values)
        for code6, code8 in site_codes
    )


def _has_any(value: object, alternatives: list[str]) -> bool:
    if value is None:
        return False
    available = {item.strip() for item in str(value).split(",") if item.strip()}
    return not available.isdisjoint(alternatives)


def _is_available(value: object) -> bool:
    return isinstance(value, str) and value.strip().casefold() == "available"


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
        value = float(radius_km)
    except (TypeError, ValueError):
        raise ValueError("radius_km must be a positive number") from None
    if not math.isfinite(value) or value <= 0:
        raise ValueError("radius_km must be a positive number")
    return value


def _coordinate(value: object) -> float | None:
    if not isinstance(value, (int, float, str)):
        return None
    try:
        coordinate = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coordinate):
        return None
    return coordinate


def _inside_bbox(
    longitude: float,
    latitude: float,
    bbox: tuple[float, float, float, float],
) -> bool:
    west, south, east, north = bbox
    return west <= longitude <= east and south <= latitude <= north


def _distance_km(
    longitude: float,
    latitude: float,
    center: tuple[float, float],
) -> float:
    center_longitude, center_latitude = center
    longitude_difference = math.radians(longitude - center_longitude)
    latitude_difference = math.radians(latitude - center_latitude)
    haversine = (
        math.sin(latitude_difference / 2) ** 2
        + math.cos(math.radians(center_latitude))
        * math.cos(math.radians(latitude))
        * math.sin(longitude_difference / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, haversine)))
