#!/usr/bin/env python3

"""
Helper for plotting STREAM gauge metadata as clickable points on a map.
This is a read-only exploration map (click a gauge, see its metadata table);
it does not share the selection/download workflow in leaflet_map.py/streams.py.
"""

import ipyleaflet
from ipywidgets import HTML, Layout


def _gage_popup_html(props):
    rows = "".join(
        f"<tr><td style='font-weight:bold; padding:4px 8px; "
        f"border-bottom:1px solid #eee; word-break:break-word;'>{col}</td>"
        f"<td style='padding:4px 8px; border-bottom:1px solid #eee; "
        f"word-break:break-word;'>{val if val != '' else '—'}</td></tr>"
        for col, val in props.items()
    )
    return (
        "<div style='width:260px; max-height:250px; overflow-y:auto; font-size:12px;'>"
        "<table style='width:100%; table-layout:fixed; border-collapse:collapse;'>"
        "<colgroup><col style='width:50%'><col style='width:50%'></colgroup>"
        f"{rows}"
        "</table>"
        "</div>"
    )


def plot_gage_metadata_map(metadata_df, lat_col="latitude_wgs84", lon_col="longitude_wgs84", height="600px"):
    """
    Returns an ipyleaflet Map with one point per gauge in metadata_df.
    Click a point to see its full metadata row in a popup table.
    """

    clean_df = metadata_df.fillna("")  # NaN isn't valid JSON
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [row[lon_col], row[lat_col]]},
            "properties": row.to_dict(),
        }
        for _, row in clean_df.iterrows()
    ]
    geojson_data = {"type": "FeatureCollection", "features": features}

    m = ipyleaflet.Map(
        center=(metadata_df[lat_col].mean(), metadata_df[lon_col].mean()),
        zoom=5,
        scroll_wheel_zoom=True,
        layout=Layout(height=height, width="100%"),
    )

    geojson_layer = ipyleaflet.GeoJSON(
        data=geojson_data,
        point_style={"radius": 5, "color": "blue", "fillColor": "blue", "fillOpacity": 0.7},
    )

    popup = ipyleaflet.Popup(
        child=HTML(),
        auto_pan=True,
        auto_pan_padding=(40, 40),  # buffer (px) kept between popup and map edges
        close_button=True,
        max_width=280,
    )

    def on_gage_click(event=None, feature=None, **kwargs):
        lon, lat = feature["geometry"]["coordinates"]
        popup.child = HTML(_gage_popup_html(feature["properties"]))
        popup.location = (lat, lon)

        if popup in m.layers:
            m.remove_layer(popup)
        m.add_layer(popup)

    geojson_layer.on_click(on_gage_click)
    m.add_layer(geojson_layer)

    return m
