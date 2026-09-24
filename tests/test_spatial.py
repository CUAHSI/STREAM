import unittest

import pandas as pd

from utils.spatial import filter_bbox, filter_huc, filter_radius, select_nearest


class FilterHucTests(unittest.TestCase):
    def setUp(self):
        self.sites = pd.DataFrame(
            {"STREAM_ID": ["site-1", "site-2", "site-3"]}
        )
        self.lookup = pd.DataFrame(
            {
                "STREAM_ID": ["site-1", "site-2", "site-3"],
                "HUC6": ["020700", "020700", "170900"],
                "HUC8": ["02070010", "02070011", "17090001"],
            }
        )

    def test_filters_by_huc6(self):
        result = filter_huc(self.sites, self.lookup, huc6="020700")
        self.assertEqual(result["STREAM_ID"].tolist(), ["site-1", "site-2"])

    def test_filters_by_huc8(self):
        result = filter_huc(self.sites, self.lookup, huc8=["17090001"])
        self.assertEqual(result["STREAM_ID"].tolist(), ["site-3"])

    def test_rejects_invalid_huc_length(self):
        with self.assertRaisesRegex(ValueError, "exactly 6 digits"):
            filter_huc(self.sites, self.lookup, huc6="20700")


class FilterBoundingBoxTests(unittest.TestCase):
    def setUp(self):
        self.sites = pd.DataFrame(
            {
                "STREAM_ID": ["inside", "boundary", "outside", "missing"],
                "longitude_wgs84": [-78.0, -80.0, -82.0, None],
                "latitude_wgs84": [38.0, 36.0, 38.0, None],
            }
        )

    def test_keeps_sites_inside_or_on_boundary(self):
        result = filter_bbox(self.sites, (-80.0, 36.0, -75.0, 40.0))

        self.assertEqual(result["STREAM_ID"].tolist(), ["inside", "boundary"])

    def test_rejects_wrong_number_of_values(self):
        with self.assertRaisesRegex(ValueError, "four finite numeric values"):
            filter_bbox(self.sites, (-80.0, 36.0, -75.0))

    def test_rejects_reversed_bounds(self):
        with self.assertRaisesRegex(ValueError, "west must be less than east"):
            filter_bbox(self.sites, (-75.0, 36.0, -80.0, 40.0))

    def test_rejects_coordinates_outside_wgs84(self):
        with self.assertRaisesRegex(ValueError, "longitude values"):
            filter_bbox(self.sites, (-200.0, 36.0, -75.0, 40.0))

    def test_requires_coordinate_columns(self):
        sites = pd.DataFrame({"STREAM_ID": ["site-1"]})

        with self.assertRaisesRegex(ValueError, "missing coordinate column"):
            filter_bbox(sites, (-80.0, 36.0, -75.0, 40.0))


class FilterRadiusTests(unittest.TestCase):
    def setUp(self):
        self.sites = pd.DataFrame(
            {
                "STREAM_ID": ["center", "near", "far", "missing"],
                "longitude_wgs84": [-78.0, -78.5, -82.0, None],
                "latitude_wgs84": [38.0, 38.0, 38.0, None],
            }
        )

    def test_keeps_and_sorts_sites_within_radius(self):
        result = filter_radius(
            self.sites,
            center=(-78.0, 38.0),
            radius_km=100,
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["center", "near"])
        self.assertEqual(result.iloc[0]["distance_km"], 0.0)
        self.assertGreater(result.iloc[1]["distance_km"], 0.0)

    def test_rejects_invalid_center(self):
        with self.assertRaisesRegex(ValueError, "longitude and latitude"):
            filter_radius(self.sites, center=(-78.0,), radius_km=100)

    def test_rejects_nonpositive_radius(self):
        with self.assertRaisesRegex(ValueError, "positive number"):
            filter_radius(self.sites, center=(-78.0, 38.0), radius_km=0)


class SelectNearestTests(unittest.TestCase):
    def setUp(self):
        self.sites = pd.DataFrame(
            {
                "STREAM_ID": ["far", "center", "near", "missing"],
                "longitude_wgs84": [-80.0, -78.0, -78.5, None],
                "latitude_wgs84": [38.0, 38.0, 38.0, None],
            }
        )

    def test_selects_requested_number_nearest_first(self):
        result = select_nearest(
            self.sites,
            center=(-78.0, 38.0),
            count=2,
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["center", "near"])
        self.assertIn("distance_km", result.columns)

    def test_rejects_invalid_count(self):
        with self.assertRaisesRegex(ValueError, "positive integer"):
            select_nearest(self.sites, center=(-78.0, 38.0), count=0)


if __name__ == "__main__":
    unittest.main()
