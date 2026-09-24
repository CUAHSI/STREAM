"""Temporal analysis helpers for STREAM datasets."""

from collections.abc import Sequence

import pandas as pd


METEOROLOGICAL_SEASONS = {
    "winter": (12, 1, 2),
    "spring": (3, 4, 5),
    "summer": (6, 7, 8),
    "fall": (9, 10, 11),
}

RESAMPLE_FREQUENCIES = {
    "daily": "D",
    "monthly": "MS",
}

RESAMPLE_STATISTICS = {"mean", "median", "min", "max", "sum"}


def add_water_year(
    data: pd.DataFrame,
    time_column: str = "DateTime",
    start_month: int = 10,
    output_column: str = "water_year",
) -> pd.DataFrame:
    """Return a copy with a water-year column derived from timestamps.

    Water years are labeled by the calendar year in which they end. With the
    default October start, October 1, 2020 belongs to water year 2021.
    """

    if time_column not in data.columns:
        raise ValueError(f"Data is missing time column {time_column!r}")
    if isinstance(start_month, bool) or not isinstance(start_month, int):
        raise ValueError("start_month must be an integer from 1 through 12")
    if not 1 <= start_month <= 12:
        raise ValueError("start_month must be an integer from 1 through 12")
    if not isinstance(output_column, str) or not output_column.strip():
        raise ValueError("output_column must be a non-empty string")

    result = data.copy()
    timestamps = pd.to_datetime(result[time_column], errors="coerce")
    invalid = result[time_column].notna() & timestamps.isna()
    if invalid.any():
        raise ValueError(f"Column {time_column!r} contains invalid timestamps")

    result[time_column] = timestamps
    years = timestamps.dt.year.astype("Int64")
    if start_month == 1:
        result[output_column] = years
    else:
        begins_next_water_year = (timestamps.dt.month >= start_month).astype("Int64")
        result[output_column] = years + begins_next_water_year

    return result


def filter_season(
    data: pd.DataFrame,
    season: str,
    time_column: str = "DateTime",
) -> pd.DataFrame:
    """Return observations from one standard meteorological season.

    Seasons are winter (December-February), spring (March-May), summer
    (June-August), and fall (September-November). Matching is case-insensitive.
    """

    if not isinstance(season, str):
        raise ValueError("season must be winter, spring, summer, or fall")
    season_name = season.strip().lower()
    if season_name not in METEOROLOGICAL_SEASONS:
        raise ValueError("season must be winter, spring, summer, or fall")
    if time_column not in data.columns:
        raise ValueError(f"Data is missing time column {time_column!r}")

    result = data.copy()
    timestamps = pd.to_datetime(result[time_column], errors="coerce")
    invalid = result[time_column].notna() & timestamps.isna()
    if invalid.any():
        raise ValueError(f"Column {time_column!r} contains invalid timestamps")

    result[time_column] = timestamps
    months = METEOROLOGICAL_SEASONS[season_name]
    matches = timestamps.dt.month.isin(months)
    return result.loc[matches].reset_index(drop=True)


def summarize_observation_period(
    data: pd.DataFrame,
    time_column: str = "DateTime",
    group_by: str = "STREAM_ID",
) -> pd.DataFrame:
    """Summarize record count and observation period for each group."""

    if group_by not in data.columns:
        raise ValueError(f"Data is missing grouping column {group_by!r}")

    result = _with_validated_timestamps(data, time_column)
    summary = (
        result.groupby(group_by, dropna=False)[time_column]
        .agg(observation_count="count", start_time="min", end_time="max")
        .reset_index()
    )
    summary["duration_days"] = (
        summary["end_time"] - summary["start_time"]
    ).dt.total_seconds() / 86400
    return summary


def common_observation_period(
    *datasets: pd.DataFrame,
    time_column: str = "DateTime",
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the overlapping observation period shared by all datasets."""

    if len(datasets) < 2:
        raise ValueError("At least two datasets are required")

    starts = []
    ends = []
    for position, data in enumerate(datasets, start=1):
        prepared = _with_validated_timestamps(data, time_column)
        timestamps = prepared[time_column].dropna()
        if timestamps.empty:
            raise ValueError(f"Dataset {position} has no valid timestamps")
        starts.append(timestamps.min())
        ends.append(timestamps.max())

    try:
        common_start = max(starts)
        common_end = min(ends)
    except TypeError as error:
        raise ValueError(
            "Dataset timestamps must use compatible timezone representations"
        ) from error

    if common_start > common_end:
        raise ValueError("Datasets do not have a common observation period")
    return common_start, common_end


def resample_timeseries(
    data: pd.DataFrame,
    value_columns: str | Sequence[str],
    frequency: str,
    statistic: str = "mean",
    time_column: str = "DateTime",
    group_by: str = "STREAM_ID",
) -> pd.DataFrame:
    """Aggregate numeric time-series values to daily or monthly intervals."""

    value_columns = _normalize_columns(value_columns)
    frequency_name = frequency.strip().lower() if isinstance(frequency, str) else ""
    if frequency_name not in RESAMPLE_FREQUENCIES:
        raise ValueError("frequency must be 'daily' or 'monthly'")

    statistic_name = statistic.strip().lower() if isinstance(statistic, str) else ""
    if statistic_name not in RESAMPLE_STATISTICS:
        choices = ", ".join(sorted(RESAMPLE_STATISTICS))
        raise ValueError(f"statistic must be one of: {choices}")
    if group_by not in data.columns:
        raise ValueError(f"Data is missing grouping column {group_by!r}")

    missing = [column for column in value_columns if column not in data.columns]
    if missing:
        raise ValueError(f"Data is missing value column(s): {', '.join(missing)}")
    nonnumeric = [
        column
        for column in value_columns
        if not pd.api.types.is_numeric_dtype(data[column])
    ]
    if nonnumeric:
        raise ValueError(
            f"Value column(s) must be numeric: {', '.join(nonnumeric)}"
        )

    prepared = _with_validated_timestamps(data, time_column)
    prepared = prepared.dropna(subset=[time_column])
    rule = RESAMPLE_FREQUENCIES[frequency_name]
    frames = []
    for group_value, group in prepared.groupby(group_by, dropna=False):
        resampler = group.set_index(time_column)[value_columns].resample(rule)
        if statistic_name == "sum":
            # Empty intervals should remain missing rather than becoming zero.
            resampled = resampler.sum(min_count=1).reset_index()
        else:
            resampled = resampler.agg(statistic_name).reset_index()
        resampled.insert(0, group_by, group_value)
        frames.append(resampled)

    if not frames:
        return pd.DataFrame(columns=[group_by, time_column, *value_columns])
    return pd.concat(frames, ignore_index=True)


def _with_validated_timestamps(
    data: pd.DataFrame,
    time_column: str,
) -> pd.DataFrame:
    if time_column not in data.columns:
        raise ValueError(f"Data is missing time column {time_column!r}")

    result = data.copy()
    timestamps = pd.to_datetime(result[time_column], errors="coerce")
    invalid = result[time_column].notna() & timestamps.isna()
    if invalid.any():
        raise ValueError(f"Column {time_column!r} contains invalid timestamps")
    result[time_column] = timestamps
    return result


def _normalize_columns(columns: str | Sequence[str]) -> list[str]:
    if isinstance(columns, str):
        columns = [columns]
    else:
        columns = list(columns)
    if not columns or not all(
        isinstance(column, str) and column.strip() for column in columns
    ):
        raise ValueError("value_columns must contain at least one column name")
    return [column.strip() for column in columns]
