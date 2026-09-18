"""Site selection helpers for STREAM metadata."""

from collections.abc import Sequence

import pandas as pd


SITE_COLUMNS = [
    "STREAM_ID",
    "site_name",
    "state_name",
    "latitude_wgs84",
    "longitude_wgs84",
    "WQ_parameters",
    "streamflow",
]


def filter_sites(
    sites: pd.DataFrame,
    states: str | Sequence[str] | None = None,
    variables: str | Sequence[str] | None = None,
) -> pd.DataFrame:
    """Filter site metadata by state and available water-quality variables."""

    states = _normalize_filter(states, "states")
    variables = _normalize_filter(variables, "variables")
    matches = pd.Series(True, index=sites.index)

    if states:
        matches &= sites["state_name"].isin(states)

    if variables:
        available_variables = sites["WQ_parameters"].fillna("").map(
            lambda value: {
                variable.strip() for variable in str(value).split(",") if variable.strip()
            }
        )
        matches &= available_variables.map(
            lambda available: all(variable in available for variable in variables)
        )

    return sites.loc[matches].reset_index(drop=True)


def _normalize_filter(
    values: str | Sequence[str] | None,
    argument_name: str,
) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    else:
        values = list(values)
    if not all(isinstance(value, str) and value.strip() for value in values):
        raise ValueError(f"{argument_name} must contain non-empty strings")
    return [value.strip() for value in values]
