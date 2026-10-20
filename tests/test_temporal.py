import unittest

import pandas as pd

from streamwq.temporal import (
    add_water_year,
    common_observation_period,
    filter_season,
    resample_timeseries,
    summarize_observation_period,
)


class AddWaterYearTests(unittest.TestCase):
    def test_uses_october_start_by_default(self):
        data = pd.DataFrame(
            {"DateTime": ["2020-09-30 23:00", "2020-10-01 00:00"]}
        )

        result = add_water_year(data)

        self.assertEqual(result["water_year"].tolist(), [2020, 2021])

    def test_supports_custom_start_month(self):
        data = pd.DataFrame({"DateTime": ["2020-06-30", "2020-07-01"]})

        result = add_water_year(data, start_month=7)

        self.assertEqual(result["water_year"].tolist(), [2020, 2021])

    def test_january_start_matches_calendar_year(self):
        data = pd.DataFrame({"DateTime": ["2020-01-01", "2020-12-31"]})

        result = add_water_year(data, start_month=1)

        self.assertEqual(result["water_year"].tolist(), [2020, 2020])

    def test_does_not_modify_original_dataframe(self):
        data = pd.DataFrame({"DateTime": ["2020-10-01"]})

        result = add_water_year(data)

        self.assertNotIn("water_year", data.columns)
        self.assertIn("water_year", result.columns)

    def test_preserves_missing_timestamps(self):
        data = pd.DataFrame({"DateTime": ["2020-10-01", None]})

        result = add_water_year(data)

        self.assertEqual(result.loc[0, "water_year"], 2021)
        self.assertTrue(pd.isna(result.loc[1, "water_year"]))

    def test_rejects_invalid_timestamp(self):
        data = pd.DataFrame({"DateTime": ["not-a-date"]})

        with self.assertRaisesRegex(ValueError, "invalid timestamps"):
            add_water_year(data)

    def test_rejects_missing_time_column(self):
        with self.assertRaisesRegex(ValueError, "missing time column"):
            add_water_year(pd.DataFrame({"time": ["2020-01-01"]}))

    def test_rejects_invalid_start_month(self):
        data = pd.DataFrame({"DateTime": ["2020-01-01"]})

        with self.assertRaisesRegex(ValueError, "integer from 1 through 12"):
            add_water_year(data, start_month=13)


class FilterSeasonTests(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame(
            {
                "DateTime": [
                    "2020-01-15",
                    "2020-04-15",
                    "2020-07-15",
                    "2020-10-15",
                    "2020-12-15",
                    None,
                ],
                "value": [1, 2, 3, 4, 5, 6],
            }
        )

    def test_filters_each_meteorological_season(self):
        expected = {
            "winter": [1, 5],
            "spring": [2],
            "summer": [3],
            "fall": [4],
        }

        for season, values in expected.items():
            with self.subTest(season=season):
                result = filter_season(self.data, season)
                self.assertEqual(result["value"].tolist(), values)

    def test_season_name_is_case_insensitive(self):
        result = filter_season(self.data, "SUMMER")

        self.assertEqual(result["value"].tolist(), [3])

    def test_does_not_modify_original_dataframe(self):
        original_type = self.data["DateTime"].dtype

        filter_season(self.data, "spring")

        self.assertEqual(self.data["DateTime"].dtype, original_type)
        self.assertEqual(len(self.data), 6)

    def test_rejects_nonstandard_season(self):
        with self.assertRaisesRegex(ValueError, "winter, spring, summer, or fall"):
            filter_season(self.data, "growing")

    def test_rejects_invalid_timestamp(self):
        data = pd.DataFrame({"DateTime": ["not-a-date"]})

        with self.assertRaisesRegex(ValueError, "invalid timestamps"):
            filter_season(data, "winter")

    def test_supports_custom_time_column(self):
        data = pd.DataFrame({"time": ["2020-06-01", "2020-12-01"]})

        result = filter_season(data, "summer", time_column="time")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "time"], pd.Timestamp("2020-06-01"))


class SummarizeObservationPeriodTests(unittest.TestCase):
    def test_summarizes_each_site(self):
        data = pd.DataFrame(
            {
                "STREAM_ID": ["site-1", "site-1", "site-2", "site-2"],
                "DateTime": [
                    "2020-01-01",
                    "2020-01-03",
                    "2021-02-01",
                    None,
                ],
            }
        )

        result = summarize_observation_period(data)

        site_1 = result[result["STREAM_ID"] == "site-1"].iloc[0]
        site_2 = result[result["STREAM_ID"] == "site-2"].iloc[0]
        self.assertEqual(site_1["observation_count"], 2)
        self.assertEqual(site_1["start_time"], pd.Timestamp("2020-01-01"))
        self.assertEqual(site_1["end_time"], pd.Timestamp("2020-01-03"))
        self.assertEqual(site_1["duration_days"], 2.0)
        self.assertEqual(site_2["observation_count"], 1)

    def test_supports_custom_columns(self):
        data = pd.DataFrame(
            {"station": ["a", "a"], "time": ["2020-01-01", "2020-01-02"]}
        )

        result = summarize_observation_period(
            data,
            time_column="time",
            group_by="station",
        )

        self.assertEqual(result.loc[0, "duration_days"], 1.0)

    def test_rejects_missing_grouping_column(self):
        data = pd.DataFrame({"DateTime": ["2020-01-01"]})

        with self.assertRaisesRegex(ValueError, "missing grouping column"):
            summarize_observation_period(data)


class CommonObservationPeriodTests(unittest.TestCase):
    def test_returns_overlap_shared_by_all_datasets(self):
        first = pd.DataFrame(
            {"DateTime": ["2020-01-01", "2020-12-31"]}
        )
        second = pd.DataFrame(
            {"DateTime": ["2020-03-01", "2021-06-30"]}
        )
        third = pd.DataFrame(
            {"DateTime": ["2019-01-01", "2020-09-30"]}
        )

        start, end = common_observation_period(first, second, third)

        self.assertEqual(start, pd.Timestamp("2020-03-01"))
        self.assertEqual(end, pd.Timestamp("2020-09-30"))

    def test_rejects_nonoverlapping_datasets(self):
        first = pd.DataFrame({"DateTime": ["2020-01-01", "2020-01-31"]})
        second = pd.DataFrame({"DateTime": ["2021-01-01", "2021-01-31"]})

        with self.assertRaisesRegex(ValueError, "do not have a common"):
            common_observation_period(first, second)

    def test_rejects_fewer_than_two_datasets(self):
        data = pd.DataFrame({"DateTime": ["2020-01-01"]})

        with self.assertRaisesRegex(ValueError, "At least two"):
            common_observation_period(data)

    def test_rejects_dataset_without_valid_timestamps(self):
        first = pd.DataFrame({"DateTime": ["2020-01-01"]})
        second = pd.DataFrame({"DateTime": [None]})

        with self.assertRaisesRegex(ValueError, "Dataset 2 has no valid timestamps"):
            common_observation_period(first, second)

    def test_supports_custom_time_column(self):
        first = pd.DataFrame({"time": ["2020-01-01", "2020-02-01"]})
        second = pd.DataFrame({"time": ["2020-01-15", "2020-03-01"]})

        start, end = common_observation_period(
            first,
            second,
            time_column="time",
        )

        self.assertEqual(start, pd.Timestamp("2020-01-15"))
        self.assertEqual(end, pd.Timestamp("2020-02-01"))


class ResampleTimeseriesTests(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame(
            {
                "STREAM_ID": ["site-1", "site-1", "site-1", "site-2"],
                "DateTime": [
                    "2020-01-01 00:00",
                    "2020-01-01 12:00",
                    "2020-02-01 00:00",
                    "2020-01-01 00:00",
                ],
                "WTemp_C": [10.0, 14.0, 20.0, 8.0],
                "Flag_WTemp_C": ["A", "A", "A", "A"],
            }
        )

    def test_resamples_daily_mean_by_site(self):
        result = resample_timeseries(
            self.data,
            value_columns="WTemp_C",
            frequency="daily",
        )

        site_1_first_day = result[
            (result["STREAM_ID"] == "site-1")
            & (result["DateTime"] == pd.Timestamp("2020-01-01"))
        ].iloc[0]
        self.assertEqual(site_1_first_day["WTemp_C"], 12.0)

    def test_resamples_monthly_with_selected_statistic(self):
        result = resample_timeseries(
            self.data,
            value_columns="WTemp_C",
            frequency="monthly",
            statistic="max",
        )

        site_1_january = result[
            (result["STREAM_ID"] == "site-1")
            & (result["DateTime"] == pd.Timestamp("2020-01-01"))
        ].iloc[0]
        self.assertEqual(site_1_january["WTemp_C"], 14.0)

    def test_sum_keeps_empty_intervals_missing(self):
        result = resample_timeseries(
            self.data,
            value_columns="WTemp_C",
            frequency="daily",
            statistic="sum",
        )

        empty_day = result[
            (result["STREAM_ID"] == "site-1")
            & (result["DateTime"] == pd.Timestamp("2020-01-02"))
        ].iloc[0]
        self.assertTrue(pd.isna(empty_day["WTemp_C"]))

    def test_rejects_quality_flag_as_value_column(self):
        with self.assertRaisesRegex(ValueError, "must be numeric"):
            resample_timeseries(
                self.data,
                value_columns="Flag_WTemp_C",
                frequency="daily",
            )

    def test_rejects_unsupported_frequency(self):
        with self.assertRaisesRegex(ValueError, "daily.*monthly"):
            resample_timeseries(
                self.data,
                value_columns="WTemp_C",
                frequency="hourly",
            )

    def test_rejects_unsupported_statistic(self):
        with self.assertRaisesRegex(ValueError, "statistic must be"):
            resample_timeseries(
                self.data,
                value_columns="WTemp_C",
                frequency="daily",
                statistic="mode",
            )


if __name__ == "__main__":
    unittest.main()
