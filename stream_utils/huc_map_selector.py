"""Interactive, lazy-loading HUC selection for STREAM sites."""

import json

import pandas as pd


HUC_LEVELS = {"HUC6": "huc6", "HUC8": "huc8"}


def fetch_huc_boundary(
    longitude: float,
    latitude: float,
    huc_level: str = "HUC8",
):
    """Fetch the single HUC boundary containing a WGS 84 point.

    Unlike downloading all HUC boundaries before drawing a map, this function
    sends one small request only after a user selects a location.
    """

    level, field = _normalize_huc_level(huc_level)
    longitude, latitude = _validate_point(longitude, latitude)

    import pygeohydro as gh

    boundaries = gh.WBD(field, outfields=[field, "name"]).bygeom(
        (longitude, latitude)
    )
    if boundaries.empty:
        raise ValueError(f"No {level} boundary was found at the selected location")
    return boundaries.iloc[[0]].copy()


def select_sites_in_boundary(
    sites: pd.DataFrame,
    boundary,
    longitude_column: str = "longitude_wgs84",
    latitude_column: str = "latitude_wgs84",
) -> pd.DataFrame:
    """Return STREAM sites covered by one HUC boundary."""

    required = {"STREAM_ID", longitude_column, latitude_column}
    missing = sorted(required - set(sites.columns))
    if missing:
        raise ValueError(f"Site metadata is missing column(s): {', '.join(missing)}")
    if "geometry" not in boundary.columns or boundary.empty:
        raise ValueError("boundary must contain at least one geometry")

    from shapely.geometry import Point

    geometry = boundary.geometry.iloc[0]
    matches = []
    for longitude, latitude in zip(
        pd.to_numeric(sites[longitude_column], errors="coerce"),
        pd.to_numeric(sites[latitude_column], errors="coerce"),
    ):
        matches.append(
            pd.notna(longitude)
            and pd.notna(latitude)
            and geometry.covers(Point(longitude, latitude))
        )
    return sites.loc[matches].reset_index(drop=True)


def build_huc_selection_map(
    sites: pd.DataFrame,
    huc_level: str = "HUC8",
    height: str = "650px",
    boundary_fetcher=None,
):
    """Build a map that fetches one HUC boundary when the user clicks.

    Returns
    -------
    tuple
        ``(map, selection)`` where ``selection`` is updated after every
        successful click with ``huc_id``, ``boundary``, and ``sites``.
    """

    level, field = _normalize_huc_level(huc_level)
    _validate_sites(sites)
    boundary_fetcher = boundary_fetcher or fetch_huc_boundary

    import ipyleaflet
    from ipywidgets import HTML, Layout

    valid_sites = sites.dropna(
        subset=["longitude_wgs84", "latitude_wgs84"]
    ).copy()
    if valid_sites.empty:
        raise ValueError("sites must contain at least one valid coordinate")

    map_widget = ipyleaflet.Map(
        center=(
            valid_sites["latitude_wgs84"].mean(),
            valid_sites["longitude_wgs84"].mean(),
        ),
        zoom=5,
        scroll_wheel_zoom=True,
        layout=Layout(height=height, width="100%"),
        prefer_canvas=True,
    )

    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row.longitude_wgs84, row.latitude_wgs84],
            },
            "properties": {"STREAM_ID": row.STREAM_ID},
        }
        for row in valid_sites.itertuples()
    ]
    map_widget.add_layer(
        ipyleaflet.GeoJSON(
            data={"type": "FeatureCollection", "features": features},
            point_style={
                "radius": 3,
                "color": "#8b0000",
                "fillColor": "#8b0000",
                "fillOpacity": 0.7,
            },
        )
    )

    boundary_layer = ipyleaflet.GeoJSON(
        data={"type": "FeatureCollection", "features": []},
        style={
            "color": "#005ea8",
            "weight": 3,
            "fillColor": "#66b2ff",
            "fillOpacity": 0.25,
        },
    )
    map_widget.add_layer(boundary_layer)

    status = HTML(
        value=f"<b>{level}</b>: click the map to select a hydrologic unit."
    )
    map_widget.add_control(ipyleaflet.WidgetControl(widget=status, position="topright"))

    selection = {"huc_id": None, "boundary": None, "sites": None}
    boundary_cache = {}

    def on_map_click(**event):
        if event.get("type") != "click":
            return
        latitude, longitude = event["coordinates"]
        status.value = f"Loading {level} boundary..."
        try:
            from shapely.geometry import Point

            point = Point(longitude, latitude)
            cached = next(
                (
                    cached_selection
                    for cached_selection in boundary_cache.values()
                    if cached_selection["boundary"].geometry.iloc[0].covers(point)
                ),
                None,
            )
            if cached is None:
                boundary = boundary_fetcher(longitude, latitude, level)
                huc_id = _boundary_value(boundary, field)
                selected_sites = select_sites_in_boundary(valid_sites, boundary)
                cached = {
                    "huc_id": huc_id,
                    "boundary": boundary,
                    "sites": selected_sites,
                }
                boundary_cache[huc_id] = cached

            huc_id = cached["huc_id"]
            boundary = cached["boundary"]
            selected_sites = cached["sites"]

            boundary_layer.data = json.loads(boundary.to_json())
            selection.update(
                huc_id=huc_id,
                boundary=boundary,
                sites=selected_sites,
            )
            status.value = (
                f"<b>{level} {huc_id}</b><br>"
                f"{len(selected_sites):,} STREAM site(s)"
            )
        except Exception as error:
            status.value = f"<b>Selection failed:</b> {error}"

    map_widget.on_interaction(on_map_click)
    return map_widget, selection


def _normalize_huc_level(huc_level: str) -> tuple[str, str]:
    level = str(huc_level).upper()
    if level not in HUC_LEVELS:
        raise ValueError("huc_level must be 'HUC6' or 'HUC8'")
    return level, HUC_LEVELS[level]


def _validate_point(longitude: float, latitude: float) -> tuple[float, float]:
    try:
        longitude = float(longitude)
        latitude = float(latitude)
    except (TypeError, ValueError):
        raise ValueError("longitude and latitude must be numeric") from None
    if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
        raise ValueError("longitude or latitude is outside WGS 84 bounds")
    return longitude, latitude


def _validate_sites(sites: pd.DataFrame) -> None:
    required = {"STREAM_ID", "longitude_wgs84", "latitude_wgs84"}
    missing = sorted(required - set(sites.columns))
    if missing:
        raise ValueError(f"Site metadata is missing column(s): {', '.join(missing)}")


def _boundary_value(boundary, field: str) -> str:
    columns = {str(column).lower(): column for column in boundary.columns}
    if field not in columns:
        raise ValueError(f"Boundary response is missing the {field} field")
    return str(boundary.iloc[0][columns[field]])
