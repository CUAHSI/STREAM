#!/usr/bin/env python3

"""
Description: This script contains helper functions for generating  
             an interactive map containing Water Model features.

Author(s): Tony Castronova <acastronova@cuahsi.org>
"""

import json
import shapely
import requests
import ipyleaflet
import geopandas as gpd
from sidecar import Sidecar
from ipywidgets import Layout, HTML
from IPython.display import display, Javascript


class Map():
    def __init__(self, basemap=ipyleaflet.basemaps.OpenStreetMap.Mapnik, gdf=None, plot_gdf=False, name='Map'):
        self.selected_id = None
        self.selected_layer = None
        
        self.basemap = basemap
        self.name = name

        self.map = self.build_map()
        

    def build_map(self):
        defaultLayout=Layout(width='960px', height='940px')

        m = ipyleaflet.Map(
            basemap=ipyleaflet.basemap_to_tiles(ipyleaflet.basemaps.OpenStreetMap.Mapnik, layout=defaultLayout),
                center=(41., -77),
                zoom=7,
                scroll_wheel_zoom=True,
                tap=False
            )
        
        m.add_layer(
            ipyleaflet.WMSLayer(
                url='https://maps.water.noaa.gov/server/services/reference/static_nwm_flowlines/MapServer/WMSServer',
                layers='0',
                transparent=True,
                format='image/png',
                min_zoom=8,
                max_zoom=18,
                )
        )
        

        
        # Create a popup
        self.popup = ipyleaflet.Popup(
            child=HTML(),
            auto_pan=False,
            close_button=True,
            auto_close=True,
            close_on_escape_key=True
        )
        self.popup_added = False

        return m
    
    def asInlineMap(self):
        display(self.map)
        
        self.set_css_and_js()
        
    def asSideCarMap(self):
        
        sc = Sidecar(title=self.name)
        with sc:
            display(self.map)

        self.set_css_and_js()
    
    def set_css_and_js(self):
        # inject some CSS to make the loading icon spin
        display(
            HTML(
                """
            <style>
                .fa-circle-o-notch, .fa-refresh, .fa-cog {
                    animation: spin 1s linear infinite;
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            </style>
        """
            )
        )
        
        # turn off Jupyter context menu
        display(Javascript("""
            setTimeout(function() {
                var mapElements = document.querySelectorAll('.leaflet-container');
                mapElements.forEach(function(el) {
                    el.addEventListener('contextmenu', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        return false;
                    });
                });
            }, 1000);
        """))
        
    def action_after_map_click(self):
        # Method can be implemented by Subclasses to provide additional
        # additional functionality after a NWM reach has been selected.
        pass
        
    
    
    def handle_map_interaction(self, **kwargs):
        # placeholder that can be implemented.
        pass
    
            
    # setter for the selected reach
    def set_selected(self, value):
            self.selected_id = value
    
    # getter for selected reach
    def selected(self):
        return self.selected_id