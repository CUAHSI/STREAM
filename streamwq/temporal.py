"""Backward-compatible imports for pandas-based temporal analysis.

New code should import these helpers from :mod:`streamwq.helpers.pandas`.
"""

from streamwq.helpers.pandas import (
    METEOROLOGICAL_SEASONS,
    RESAMPLE_FREQUENCIES,
    RESAMPLE_STATISTICS,
    add_water_year,
    common_observation_period,
    filter_season,
    resample_timeseries,
    summarize_observation_period,
)

__all__ = [
    "METEOROLOGICAL_SEASONS",
    "RESAMPLE_FREQUENCIES",
    "RESAMPLE_STATISTICS",
    "add_water_year",
    "common_observation_period",
    "filter_season",
    "resample_timeseries",
    "summarize_observation_period",
]
