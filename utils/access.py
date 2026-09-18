"""Low-level HydroShare and Parquet access for STREAM datasets."""

from collections.abc import Sequence

import pandas as pd
import pyarrow.dataset as ds

from utils.catalog import QUALITY_FLAG_COLUMNS, WATER_QUALITY_VARIABLES, dataset_path
from utils.quality import normalize_quality


class StreamAccess:
    """Read STREAM Parquet data from a HydroShare S3 filesystem."""

    def __init__(self, hydroshare=None):
        if hydroshare is None:
            from utils.S3hsclient import S3HydroShare

            hydroshare = S3HydroShare()
        self._hydroshare = hydroshare

    def read_metadata(self, columns: Sequence[str] | None = None) -> pd.DataFrame:
        """Read site metadata, optionally selecting columns."""

        if columns is not None:
            if isinstance(columns, str) or not all(
                isinstance(column, str) and column for column in columns
            ):
                raise ValueError("columns must be a sequence of non-empty strings")
            columns = list(columns)

        filesystem = self._filesystem("metadata")
        try:
            return pd.read_parquet(
                dataset_path("metadata"),
                filesystem=filesystem,
                columns=columns,
            )
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM metadata: {error}") from error

    def read_water_quality(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
        quality: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Read selected water-quality variables, sites, and dates."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        variables = self._normalize_values(variables, "variables")
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
        quality_flags = normalize_quality(quality)
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        columns = ["STREAM_ID", "DateTime"]
        for variable in variables:
            columns.extend([variable, QUALITY_FLAG_COLUMNS[variable]])

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("DateTime") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("DateTime") <= end_time.to_pydatetime()
        if quality_flags is not None:
            for variable in variables:
                expression &= ds.field(QUALITY_FLAG_COLUMNS[variable]).isin(quality_flags)

        filesystem = self._filesystem("water quality")
        try:
            dataset = ds.dataset(
                dataset_path("water_quality"),
                format="parquet",
                filesystem=filesystem,
                partitioning="hive",
            )
            return dataset.to_table(columns=columns, filter=expression).to_pandas()
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM water quality: {error}") from error

    def _filesystem(self, dataset_name: str):
        filesystem = self._hydroshare.get_s3_filesystem()
        if filesystem is None:
            raise RuntimeError(
                f"A HydroShare S3 filesystem is required to read {dataset_name}"
            )
        return filesystem

    @staticmethod
    def _normalize_values(
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

    @staticmethod
    def _parse_date(value, argument_name: str):
        if value is None:
            return None
        try:
            return pd.Timestamp(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{argument_name} must be a valid date") from error
