import unittest

import pandas as pd

from streamwq.sites import filter_sites


class FilterSitesTests(unittest.TestCase):
    def setUp(self):
        self.sites = pd.DataFrame(
            {
                "STREAM_ID": ["STREAM-gauge-1", "STREAM-gauge-2", "STREAM-gauge-3"],
                "state_name": ["Virginia", "Maryland", "Virginia"],
                "WQ_parameters": ["WTemp_C, DO_mgL", "WTemp_C", None],
                "streamflow": ["Available", "Not Available", None],
                "grabsamples": ["DOC_mgL, Cl_mgL", "DOC_mgL", None],
            }
        )

    def test_filters_by_state(self):
        result = filter_sites(self.sites, states="Virginia")
        self.assertEqual(
            result["STREAM_ID"].tolist(),
            ["STREAM-gauge-1", "STREAM-gauge-3"],
        )

    def test_requires_all_requested_variables(self):
        result = filter_sites(self.sites, variables=["WTemp_C", "DO_mgL"])
        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-1"])

    def test_combines_filters(self):
        result = filter_sites(
            self.sites,
            states=["Maryland"],
            variables="WTemp_C",
        )
        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-2"])

    def test_rejects_empty_filter_values(self):
        with self.assertRaisesRegex(ValueError, "states must contain non-empty strings"):
            filter_sites(self.sites, states=[""])

    def test_filters_by_streamflow_availability(self):
        available = filter_sites(self.sites, streamflow=True)
        unavailable = filter_sites(self.sites, streamflow=False)

        self.assertEqual(available["STREAM_ID"].tolist(), ["STREAM-gauge-1"])
        self.assertEqual(
            unavailable["STREAM_ID"].tolist(),
            ["STREAM-gauge-2", "STREAM-gauge-3"],
        )

    def test_filters_by_all_requested_grab_variables(self):
        result = filter_sites(
            self.sites,
            grab_variables=["DOC_mgL", "Cl_mgL"],
        )

        self.assertEqual(result["STREAM_ID"].tolist(), ["STREAM-gauge-1"])

    def test_rejects_nonboolean_streamflow_filter(self):
        with self.assertRaisesRegex(ValueError, "True, False, or None"):
            filter_sites(self.sites, streamflow="yes")


if __name__ == "__main__":
    unittest.main()
