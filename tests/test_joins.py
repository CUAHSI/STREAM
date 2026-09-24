import unittest

import pandas as pd

from stream_utils.joins import join_timeseries


class JoinTimeseriesTests(unittest.TestCase):
    def setUp(self):
        self.water_quality = pd.DataFrame(
            {
                "STREAM_ID": ["site-1", "site-1"],
                "DateTime": ["2020-01-01 01:00", "2020-01-01 00:00"],
                "value": [11.0, 10.0],
            }
        )
        self.streamflow = pd.DataFrame(
            {
                "STREAM_ID": ["site-1", "site-1"],
                "DateTime": ["2020-01-01 00:00", "2020-01-01 01:00"],
                "value": [2.0, 3.0],
            }
        )

    def test_joins_exact_keys_and_sorts_result(self):
        result = join_timeseries(self.water_quality, self.streamflow)

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result["DateTime"].tolist(),
            [pd.Timestamp("2020-01-01 00:00"), pd.Timestamp("2020-01-01 01:00")],
        )
        self.assertEqual(result["value_left"].tolist(), [10.0, 11.0])
        self.assertEqual(result["value_right"].tolist(), [2.0, 3.0])

    def test_does_not_modify_inputs(self):
        left_type = self.water_quality["DateTime"].dtype
        right_type = self.streamflow["DateTime"].dtype

        join_timeseries(self.water_quality, self.streamflow)

        self.assertEqual(self.water_quality["DateTime"].dtype, left_type)
        self.assertEqual(self.streamflow["DateTime"].dtype, right_type)

    def test_supports_custom_suffixes(self):
        result = join_timeseries(
            self.water_quality,
            self.streamflow,
            suffixes=("_wq", "_flow"),
        )

        self.assertIn("value_wq", result.columns)
        self.assertIn("value_flow", result.columns)

    def test_rejects_duplicate_keys(self):
        duplicate = pd.concat(
            [self.water_quality, self.water_quality.iloc[[0]]],
            ignore_index=True,
        )

        with self.assertRaisesRegex(ValueError, "duplicate station/timestamp"):
            join_timeseries(duplicate, self.streamflow)

    def test_rejects_missing_key_column(self):
        missing = self.water_quality.drop(columns="STREAM_ID")

        with self.assertRaisesRegex(ValueError, "missing column"):
            join_timeseries(missing, self.streamflow)

    def test_rejects_missing_key_value(self):
        missing = self.water_quality.copy()
        missing.loc[0, "DateTime"] = None

        with self.assertRaisesRegex(ValueError, "missing join keys"):
            join_timeseries(missing, self.streamflow)

    def test_rejects_nonoverlapping_periods(self):
        later = self.streamflow.copy()
        later["DateTime"] = ["2021-01-01 00:00", "2021-01-01 01:00"]

        with self.assertRaisesRegex(ValueError, "do not have a common"):
            join_timeseries(self.water_quality, later)

    def test_rejects_overlap_without_exact_matches(self):
        offset = self.streamflow.copy()
        offset["DateTime"] = ["2020-01-01 00:30", "2020-01-01 01:30"]

        with self.assertRaisesRegex(ValueError, "no exact station/timestamp matches"):
            join_timeseries(self.water_quality, offset)

    def test_rejects_invalid_suffixes(self):
        with self.assertRaisesRegex(ValueError, "two different"):
            join_timeseries(
                self.water_quality,
                self.streamflow,
                suffixes=("_same", "_same"),
            )


if __name__ == "__main__":
    unittest.main()
