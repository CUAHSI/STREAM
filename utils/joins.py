"""Safe joins for STREAM time-series datasets."""

import pandas as pd

from utils.temporal import common_observation_period


def join_timeseries(
    left: pd.DataFrame,
    right: pd.DataFrame,
    site_column: str = "STREAM_ID",
    time_column: str = "DateTime",
    suffixes: tuple[str, str] = ("_left", "_right"),
) -> pd.DataFrame:
    """Inner join two time series on exact station and timestamp matches.

    Each input must contain at most one row for a given station and timestamp.
    The function returns a new DataFrame sorted by station and time.
    """

    _validate_suffixes(suffixes)
    left_prepared = _prepare_for_join(
        left,
        name="left",
        site_column=site_column,
        time_column=time_column,
    )
    right_prepared = _prepare_for_join(
        right,
        name="right",
        site_column=site_column,
        time_column=time_column,
    )

    common_observation_period(
        left_prepared,
        right_prepared,
        time_column=time_column,
    )

    try:
        result = left_prepared.merge(
            right_prepared,
            on=[site_column, time_column],
            how="inner",
            suffixes=suffixes,
            validate="one_to_one",
        )
    except (TypeError, ValueError) as error:
        raise ValueError(f"Unable to join time series: {error}") from error

    if result.empty:
        raise ValueError(
            "Datasets overlap in time but have no exact station/timestamp matches"
        )

    return result.sort_values([site_column, time_column]).reset_index(drop=True)


def _prepare_for_join(
    data: pd.DataFrame,
    name: str,
    site_column: str,
    time_column: str,
) -> pd.DataFrame:
    missing = [
        column for column in (site_column, time_column) if column not in data.columns
    ]
    if missing:
        raise ValueError(f"{name.title()} dataset is missing column(s): {', '.join(missing)}")

    result = data.copy()
    timestamps = pd.to_datetime(result[time_column], errors="coerce")
    invalid = result[time_column].notna() & timestamps.isna()
    if invalid.any():
        raise ValueError(f"{name.title()} dataset contains invalid timestamps")
    result[time_column] = timestamps

    if result[site_column].isna().any() or result[time_column].isna().any():
        raise ValueError(f"{name.title()} dataset contains missing join keys")

    duplicates = result.duplicated([site_column, time_column], keep=False)
    if duplicates.any():
        raise ValueError(
            f"{name.title()} dataset contains duplicate station/timestamp keys"
        )
    return result


def _validate_suffixes(suffixes: tuple[str, str]) -> None:
    if (
        not isinstance(suffixes, tuple)
        or len(suffixes) != 2
        or not all(isinstance(value, str) and value for value in suffixes)
        or suffixes[0] == suffixes[1]
    ):
        raise ValueError("suffixes must contain two different non-empty strings")
