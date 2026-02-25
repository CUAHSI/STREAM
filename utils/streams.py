#!/usr/bin/env python3

"""
Description: This is a script to demonstrate how STREAMS data can be downloaded
             from HydroShare.

Author(s): Tony Castronova <acastronova@cuahsi.org>
"""

from datetime import datetime
from pathlib import Path

import geopandas as gpd
import ipyleaflet
from ipyleaflet import WidgetControl
from ipywidgets import Layout, HTML
from IPython.display import display
import ipywidgets as widgets
import pyarrow.parquet as pq
import threading

from utils import S3hsclient as hsclient
from utils import leaflet_map

# Define mapping between interface labels and water quality variable names in the dataset
water_quality_vars = {
    "Water Temperature": ["WTemp_C", "Flag_WTemp_C"],
    "Specific Conductance": ["SpC_uScm", "Flag_SpC_uScm"],
    "Dissolved Oxygen": ["DO_mgL", "Flag_DO_mgL"],
    "pH": ["pH", "Flag_pH"],
    "Turbidity": ["Turb_FNU", "Flag_Turb_FNU"],
    "NO3": ["NO3_mgNL", "Flag_NO3_mgNL"],
    "fDOM": ["fDOM_QSU", "Flag_fDOM_QSU", "fDOM_RFU", "Flag_fDOM_RFU"],
    "Chla": ["Chla_ugL", "Flag_Chla_ugL"],
    "PC": ["PC_RFU", "Flag_PC_RFU"],
}

# define paths to the datasets on HydroShare, these are used in the download function
# in the future this should not be hardcoded.
hs_paths = {
    "Anthropogenic": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/dynamic_antropogenic.parquet",
    "gauges": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/gauges.parquet",
    "Grab Samples": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/grab_samples.parquet",
    "Land Use/Cover": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/lulc.parquet",
    "Streamflow": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/streamflow.parquet",
    "water_quality": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/water_quality.parquet",
    "Historical Meteorology": "tonycastronova/248ec0f13d6c4580b2faa66425cb58c3/data/contents/dynamic_historical_meteorology",
}


class StreamsMap(leaflet_map.Map):
    def __init__(self):

        # initialize HydroShare credentials
        print("Please Enter Your HydroShare Credentials")
        self.hs = hsclient.S3HydroShare()

        # initialize the parent class
        super().__init__()

        self.reach_label = None

        # track if the user click a gauge during the current event
        self.feature_selected = False

        # STREAMS Gauges
        self.gauges = gpd.read_parquet(
            hs_paths["gauges"],
            filesystem=self.hs.get_s3_filesystem(),
        )
        geo_data = ipyleaflet.GeoData(
            geo_dataframe=self.gauges,
            style={
                "color": "black",
                "radius": 8,
                "fillColor": "#3366cc",
                "opacity": 0.5,
                "weight": 1.9,
                "dashArray": "2",
                "fillOpacity": 0.6,
            },
            hover_style={"fillColor": "red", "fillOpacity": 0.2},
            point_style={
                "radius": 5,
                "color": "red",
                "fillOpacity": 0.8,
                "fillColor": "blue",
                "weight": 3,
            },
            name="Gauges",
        )

        geo_data.on_click(self.on_gauge_click)

        self.map.add(geo_data)

        self.styled_container = widgets.HTML(
            "<style>.floating-widget { z-index: 9999 !important; position: relative; }</style>"
        )
        self.map.add(WidgetControl(widget=self.styled_container, position="bottomleft"))

        self.submit = widgets.Button(
            description="Download Data", disabled=True, button_style="", icon=" "
        )
        self.submit.on_click(self.on_submit_click)

        self.map.add(WidgetControl(widget=self.submit, position="bottomleft"))

        self.gauge_label = widgets.Text(
            value="No Reach Selected",
            disabled=True,
            layout=widgets.Layout(width="300px"),
        )

        self.selection_mode = widgets.RadioButtons(
            options=["Single", "Multiple"],
            value="Single",
            orientation="horizontal",
            layout=widgets.Layout(width="250px"),
        )
        self.selection_mode.observe(self._on_selection_mode_change, names="value")
        selection_group = widgets.VBox([self.selection_mode, self.gauge_label])
        selection_accordion = widgets.Accordion(children=[selection_group])
        selection_accordion.set_title(0, "Selection Mode")
        selection_accordion.layout.width = "99%"

        self.start_date = widgets.DatePicker(
            description="Start Date",
            disabled=False,
            value=datetime(2011, 1, 1),
            layout=Layout(width="300px"),
        )

        self.end_date = widgets.DatePicker(
            description="End Date",
            disabled=False,
            value=datetime.today(),
            layout=Layout(width="300px"),
        )
        date_group = widgets.VBox([self.start_date, self.end_date])
        date_accordion = widgets.Accordion(children=[date_group])
        date_accordion.set_title(0, "Date Range")
        date_accordion.layout.width = "99%"

        # build water quality checkboxes
        self.water_temp = self.build_checkbox("Water Temperature")
        self.specific_conductance = self.build_checkbox("Specific Conductance")
        self.do = self.build_checkbox("Dissolved Oxygen")
        self.ph = self.build_checkbox("pH")
        self.turbidity = self.build_checkbox("Turbidity")
        self.no3 = self.build_checkbox("NO3")
        self.fdom = self.build_checkbox("fDOM")
        self.chla = self.build_checkbox("Chla")
        self.pc = self.build_checkbox("PC")

        water_quality_group = widgets.VBox(
            [
                widgets.HBox([self.water_temp, self.specific_conductance]),
                widgets.HBox([self.do, self.ph]),
                widgets.HBox([self.turbidity, self.no3]),
                widgets.HBox([self.fdom, self.chla]),
                widgets.HBox([self.pc]),
            ]
        )
        water_quality_accordion = widgets.Accordion(children=[water_quality_group])
        water_quality_accordion.set_title(0, "Water Quality Variables")
        water_quality_accordion.layout.width = "99%"

        # add checkboxes for other types of data
        self.streamflow = self.build_checkbox("Streamflow")
        self.lulc = self.build_checkbox("Land Use/Cover")
        self.grab = self.build_checkbox("Grab Samples")
        self.anthro = self.build_checkbox("Anthropogenic")
        self.historical_met = self.build_checkbox("Historical Meteorology")
        other_variables_group = widgets.VBox(
            [
                widgets.HBox([self.streamflow, self.grab]),
                widgets.HBox([self.lulc, self.anthro]),
                widgets.HBox([self.historical_met]),
            ]
        )
        other_variables_accordion = widgets.Accordion(children=[other_variables_group])
        other_variables_accordion.set_title(0, "Other Variables")
        other_variables_accordion.layout.width = "99%"

        # save the accordion components so they can
        # be referenced in event handlers later
        self.accordions = [
            selection_accordion,
            date_accordion,
            water_quality_accordion,
            other_variables_accordion,
        ]

        # add the accordion components to the options interface
        self.options_widgets = widgets.VBox(
            self.accordions, layout=widgets.Layout(width="350px", overflow="hidden")
        )

        # save all the checkboxes for easy lookup later
        self.wc_checkboxes = [
            self.water_temp,
            self.specific_conductance,
            self.do,
            self.ph,
            self.turbidity,
            self.no3,
            self.fdom,
            self.chla,
            self.pc,
        ]
        self.other_checkboxes = [
            self.streamflow,
            self.lulc,
            self.grab,
            self.anthro,
            self.historical_met,
        ]

        self.map.add(WidgetControl(widget=self.options_widgets, position="bottomleft"))

        # add a handler for when the map is clicked
        # this is used to activate/deactivate the
        # download button.
        self.map.on_interaction(self.on_map_click)

    def build_checkbox(
        self, description, value=False, indent=False, layout=Layout(width="50%")
    ):
        return widgets.Checkbox(
            description=description, value=value, indent=indent, layout=layout
        )

    def on_map_click(self, **kwargs):


        # show popup if right click is detected and feature is selected
        if kwargs.get("type") == "contextmenu":
            click_lat, click_lon = kwargs['coordinates']
            self.on_gauge_right_click(click_lat, click_lon)
            return

    
        # filter out all interactions except 'click'
        if kwargs.get("type") != "click":
            return

        if not self.feature_selected:
            self.gauge_label.value = "No Reach Selected"
            self.submit.disabled = True
            
        # reset the feature_selected flag for the next event
        self.feature_selected = False

    def _on_selection_mode_change(self, change):
        self.gauge_label.value = "No Reach Selected"
        self.submit.disabled = True

    def on_gauge_click(self, feature, **kwargs):

        
        # set the selected feature to True since a gauge
        # was just clicked
        self.feature_selected = True

        # get the properties of the selected gauge and
        # the lat/lon
        props = feature.get("properties", {})
        lat = props.get("latitude")
        lon = props.get("longitude")
        
        # enable the submit button
        self.submit.disabled = False

        # populate the download widget with gauge properties
        if self.selection_mode.value == "Single":
            self.gauge_label.value = props["STREAM_ID"]
        else:
            if (
                self.gauge_label.value == "No Reach Selected"
            ):  # this 'if' hacky, should fix in the future.
                self.gauge_label.value = props["STREAM_ID"]
            else:
                self.gauge_label.value += f";{props['STREAM_ID']}"

    def on_gauge_right_click(self, click_lat, click_lon):

        # Find the closest gauge to the right-click location
        self.gauges['_dist'] = (
            (self.gauges['latitude'] - click_lat) ** 2 +
            (self.gauges['longitude'] - click_lon) ** 2
        )
        closest = self.gauges.loc[self.gauges['_dist'].idxmin()]
    
        # Only show popup if click was close enough to a feature
        if closest['_dist'] > 0.0001:  # tune this threshold
            return
    
        props = closest.to_dict()
        lat = props.get('latitude')
        lon = props.get('longitude')

        # Create HTML content for the popup
        html_content = "<div style='min-width: 200px;'>"
        html_content += f"<h4>{props.get('site name', 'Gauge')}</h4>"
        html_content += f"<b>Stream ID:</b> {props.get('STREAM_ID', 'N/A')}<br>"
        html_content += f"<b>Source:</b> {props.get('source', 'N/A')} ({props.get('SourceID', 'N/A')})<br>"
        html_content += f"<b>Location:</b> {props.get('latitude', 'N/A')}, {props.get('longitude', 'N/A')}<br>"
        html_content += f"<b>State:</b> {props.get('State', 'N/A')} ({props.get('State Code', 'N/A')})<br>"
        html_content += (
            f"<b>Drainage Area:</b> {props.get('drain_sqkm', 'N/A')} km²<br>"
        )
        html_content += "</div>"

        # Update popup
        self.popup.child = HTML(html_content)
        self.popup.location = [lat, lon]

        # Add popup to map on first click
        if not self.popup_added:
            self.map.add(self.popup)  # or self.m.add(self.popup)
            self.popup_added = True
        else:
            # Reopen the popup for subsequent clicks
            self.map.remove(self.popup)
            self.map.add(self.popup)
        
        # Schedule popup close after short delay
        self._hover_timer = threading.Timer(3, self._close_popup)
        self._hover_timer.start()

    def _close_popup(self):
        if self.popup_added:
            self.map.remove(self.popup)
            self.popup_added = False
            
    def build_output_filename(self, gauge_label, variable_label):
        var_label = variable_label.replace("/", "-").replace(" ", "-").lower()
        gage_label = "-".join(gauge_label.split("-")[1:])
        return f"{gage_label}-{var_label}"

    def on_submit_click(self, button):

        # Collapse all accordions and update button
        for accordion in self.accordions:
            accordion.selected_index = None

        # change the text of the submit button so we know that downloading is happening
        self.submit.description = "Downloading..."
        self.submit.disabled = True
        self.submit.icon = "circle-o-notch"

        # make output directory
        outdir = Path("output_data")
        outdir.mkdir(exist_ok=True)

        # get download properties
        gauges = self.gauge_label.value.split(";")  # get all the selected gauges
        # gauge = self.gauge_label.value
        st = self.start_date.value
        et = self.end_date.value

        # identify water quality variables that should be saved
        wc_vars = ["DateTime"]
        for chk in self.wc_checkboxes:
            if chk.value == True:
                wc_vars.extend(water_quality_vars[chk.description])

        for gauge in gauges:
            # only download data if variables are selected
            if len(wc_vars) > 1:
                
                table = pq.read_table(
                    hs_paths["water_quality"],
                    filesystem=self.hs.get_s3_filesystem(),
                    columns=wc_vars,
                    filters=[
                        ("gauge", "=", gauge),
                        ("DateTime", ">=", st),
                        ("DateTime", "<=", et),
                    ],
                )
                subset = table.to_pandas()
                out_filename = self.build_output_filename(gauge, "water_quality")
                subset.to_csv(f"{str(outdir)}/{out_filename}.csv", index=False)

            # download other variables
            for chk in self.other_checkboxes:
                if chk.value:
                    if (chk.description == "Streamflow") or (
                        chk.description == "Grab Samples"
                    ):
                        time_column = "DateTime"
                    elif chk.description == "Historical Meteorology":
                        time_column = "time"
                    else:
                        time_column = "year"

                    
                    table = pq.read_table(
                        hs_paths[chk.description],
                        filesystem=self.hs.get_s3_filesystem(),
                        filters=[
                            ("gauge", "=", gauge),
                            (time_column, ">=", st),
                            (time_column, "<=", et),
                        ],
                    )
                    subset = table.to_pandas()

                    out_filename = self.build_output_filename(gauge, chk.description)
                    subset.to_csv(f"{str(outdir)}/{out_filename}.csv", index=False)

        # re-enable the submit button when download is complete
        self.submit.disabled = False
        self.submit.description = "Download Data"
        self.submit.icon = ""
