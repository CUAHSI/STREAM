import unittest

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from stream_utils.huc_map_selector import (
    fetch_huc_boundary,
    select_sites_in_boundary,
)


class SelectSitesInBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.sites = pd.DataFrame(
            {
                "STREAM_ID": ["inside", "edge", "outside", "missing"],
                "longitude_wgs84": [-78.0, -79.0, -81.0, None],
                "latitude_wgs84": [38.0, 38.0, 38.0, None],
            }
        )
        self.boundary = gpd.GeoDataFrame(
            {"huc8": ["02070010"]},
            geometry=[Polygon([(-79, 37), (-77, 37), (-77, 39), (-79, 39)])],
            crs="EPSG:4326",
        )

    def test_selects_sites_inside_or_on_boundary(self):
        result = select_sites_in_boundary(self.sites, self.boundary)

        self.assertEqual(result["STREAM_ID"].tolist(), ["inside", "edge"])

    def test_requires_site_columns(self):
        with self.assertRaisesRegex(ValueError, "missing column"):
            select_sites_in_boundary(
                self.sites.drop(columns="STREAM_ID"), self.boundary
            )

    def test_requires_boundary_geometry(self):
        with self.assertRaisesRegex(ValueError, "at least one geometry"):
            select_sites_in_boundary(self.sites, pd.DataFrame())


class FetchHucBoundaryTests(unittest.TestCase):
    def test_rejects_unsupported_huc_level_before_request(self):
        with self.assertRaisesRegex(ValueError, "HUC6.*HUC8"):
            fetch_huc_boundary(-78.0, 38.0, huc_level="HUC12")

    def test_rejects_invalid_coordinates_before_request(self):
        with self.assertRaisesRegex(ValueError, "outside WGS 84"):
            fetch_huc_boundary(-200.0, 38.0)


if __name__ == "__main__":
    unittest.main()
