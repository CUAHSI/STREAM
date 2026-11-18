#!/usr/bin/env python3

"""
Helper for interactively selecting a HUC region on a map and getting back
the gages inside it. Click a HUC boundary to select it; the matching subset
of gages is tracked in the returned `selection` dict as `selection['gages_df']`.
"""

import json

import ipyleaflet
from ipywidgets import HTML, Layout
from shapely.geometry import shape


def build_huc_selection_map(gages_df, boundaries_gdf, huc_level, huc_field, height="650px"):
    """
    Returns (map, selection). `map` is an ipyleaflet Map with clickable HUC
    boundaries and gage points. `selection` is a dict that gets populated
    with 'huc_id' and 'gages_df' (the gages inside the clicked HUC) on click.
    """

    m = ipyleaflet.Map(
        center=(gages_df["latitude_wgs84"].mean(), gages_df["longitude_wgs84"].mean()),
        zoom=6,
        scroll_wheel_zoom=True,
        layout=Layout(height=height, width="100%"),
        prefer_canvas=True,
    )

    huc_layer = ipyleaflet.GeoJSON(
        data=json.loads(boundaries_gdf.to_json()),
        style={"color": "#004080", "weight": 1.5, "fillColor": "#8fb8e0", "fillOpacity": 0.25},
        hover_style={"fillOpacity": 0.5},
    )
    m.add_layer(huc_layer)

    # gage points, single GeoJSON layer (not one widget per gage) to stay fast at scale
    gage_features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [row["longitude_wgs84"], row["latitude_wgs84"]]},
            "properties": {"STREAM_ID": row["STREAM_ID"]},
        }
        for _, row in gages_df.iterrows()
    ]
    gage_layer = ipyleaflet.GeoJSON(
        data={"type": "FeatureCollection", "features": gage_features},
        point_style={"radius": 3, "color": "darkred", "fillColor": "darkred", "fillOpacity": 0.7},
    )
    m.add_layer(gage_layer)

    # highlight for the currently-selected HUC boundary
    highlight_layer = ipyleaflet.GeoJSON(
        data={"type": "FeatureCollection", "features": []},
        style={"color": "red", "weight": 3, "fillOpacity": 0},
    )
    m.add_layer(highlight_layer)

    popup = ipyleaflet.Popup(
        child=HTML(),
        auto_pan=True,
        auto_pan_padding=(40, 40),
        close_button=True,
        max_width=280,
    )

    selection = {"huc_id": None, "gages_df": None}

    def on_huc_click(event=None, feature=None, **kwargs):
        huc_id = feature["properties"][huc_field]
        name = feature["properties"].get("name", huc_id)

        subset = gages_df[gages_df[huc_level] == huc_id]
        selection["huc_id"] = huc_id
        selection["gages_df"] = subset

        highlight_layer.data = {"type": "FeatureCollection", "features": [feature]}

        centroid = shape(feature["geometry"]).centroid
        popup.child = HTML(f"<b>{huc_level} {huc_id}</b><br>{name}<br>{len(subset)} gage(s) inside")
        popup.location = (centroid.y, centroid.x)
        if popup in m.layers:
            m.remove_layer(popup)
        m.add_layer(popup)

        print(f"Selected {huc_level} {huc_id} ({name}): {len(subset)} gage(s)")

    huc_layer.on_click(on_huc_click)

    return m, selection
