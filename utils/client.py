"""A small Python client for accessing the STREAM Parquet resource."""

from collections.abc import Sequence

import pandas as pd
import pyarrow.dataset as ds

from utils.catalog import (
    DATASETS,
    QUALITY_FLAG_COLUMNS,
    WATER_QUALITY_VARIABLES,
    dataset_path,
)


SITE_COLUMNS = [
    "STREAM_ID",
    "site_name",
    "state_name",
    "latitude_wgs84",
    "longitude_wgs84",
    "WQ_parameters",
    "streamflow",
]


class StreamClient:
    """Provide simple access to datasets in the STREAM Parquet resource.

    Parameters
    ----------
    hydroshare
        An existing HydroShare client. When omitted, the user is prompted for
        HydroShare credentials by :class:`utils.S3hsclient.S3HydroShare`.
        Passing a client is also useful when testing with a mock.
    """

    def __init__(self, hydroshare=None):
        if hydroshare is None:
            # Import only when a real connection is requested. This keeps the
            # client importable in documentation and tests that use a mock.
            from utils.S3hsclient import S3HydroShare

            hydroshare = S3HydroShare()

        self._hydroshare = hydroshare

    def list_datasets(self) -> list[str]:
        """Return the dataset names understood by this client."""

        return sorted(DATASETS)

    def path(self, name: str) -> str:
        """Return the HydroShare S3 path for a dataset."""

        return dataset_path(name)

    def read_metadata(self, columns: Sequence[str] | None = None) -> pd.DataFrame:
        """Read site metadata from HydroShare into a pandas DataFrame.

        Parameters
        ----------
        columns
            Optional column names to read. Selecting only the columns needed
            by an analysis reduces the amount of transferred data.
        """

        if columns is not None:
            if isinstance(columns, str) or not all(
                isinstance(column, str) and column for column in columns
            ):
                raise ValueError("columns must be a sequence of non-empty strings")
            columns = list(columns)

        filesystem = self._hydroshare.get_s3_filesystem()
        if filesystem is None:
            raise RuntimeError("A HydroShare S3 filesystem is required to read metadata")

        try:
            return pd.read_parquet(
                self.path("metadata"),
                filesystem=filesystem,
                columns=columns,
            )
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM metadata: {error}") from error

    def find_sites(
        self,
        states: str | Sequence[str] | None = None,
        variables: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Find monitoring sites by state and available water-quality variables.

        A site must contain every requested variable. Both filters are optional,
        and a single state or variable may be supplied as a string.
        """

        states = self._normalize_filter(states, "states")
        variables = self._normalize_filter(variables, "variables")

        sites = self.read_metadata(columns=SITE_COLUMNS)
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

    def read_water_quality(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read filtered hourly water-quality observations.

        Parameters
        ----------
        site_ids
            One STREAM site identifier or a sequence of identifiers.
        variables
            One water-quality variable or a sequence of variables.
        start, end
            Optional inclusive date limits understood by pandas, such as
            ``"2020-01-01"``.
        """

        site_ids = self._normalize_filter(site_ids, "site_ids")
        variables = self._normalize_filter(variables, "variables")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")
        if not variables:
            raise ValueError("variables must contain at least one variable")

        unknown = sorted(set(variables) - set(WATER_QUALITY_VARIABLES))
        if unknown:
            available = ", ".join(WATER_QUALITY_VARIABLES)
            raise ValueError(
                f"Unknown water-quality variable(s): {', '.join(unknown)}. "
                f"Available variables: {available}"
            )

        start_time = self._parse_date(start, "start")
        end_time = self._parse_date(end, "end")
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        columns = ["STREAM_ID", "DateTime"]
        for variable in variables:
            columns.extend([variable, QUALITY_FLAG_COLUMNS[variable]])

        filesystem = self._hydroshare.get_s3_filesystem()
        if filesystem is None:
            raise RuntimeError("A HydroShare S3 filesystem is required to read water quality")

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("DateTime") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("DateTime") <= end_time.to_pydatetime()

        try:
            dataset = ds.dataset(
                self.path("water_quality"),
                format="parquet",
                filesystem=filesystem,
                partitioning="hive",
            )
            table = dataset.to_table(columns=columns, filter=expression)
            return table.to_pandas()
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM water quality: {error}") from error

    @staticmethod
    def _normalize_filter(
        values: str | Sequence[str] | None,
        argument_name: str,
    ) -> list[str]:
        """Convert a single string or sequence into a validated list."""

        if values is None:
            return []
        if isinstance(values, str):
            values = [values]
        else:
            values = list(values)

        if not all(isinstance(value, str) and value.strip() for value in values):
            raise ValueError(f"{argument_name} must contain non-empty strings")

        return [value.strip() for value in values]

    @staticmethod
    def _parse_date(value, argument_name: str):
        """Convert a user-supplied date into a pandas Timestamp."""

        if value is None:
            return None
        try:
            return pd.Timestamp(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{argument_name} must be a valid date") from error
