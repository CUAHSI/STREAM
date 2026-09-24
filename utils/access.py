"""Low-level HydroShare and Parquet access for STREAM datasets."""

from collections.abc import Sequence

import pandas as pd
import pyarrow.dataset as ds

from utils.catalog import (
    ANTHROPOGENIC_VARIABLES,
    GRAB_SAMPLE_VARIABLES,
    LAND_COVER_VARIABLES,
    METEOROLOGY_VARIABLES,
    QUALITY_FLAG_COLUMNS,
    WATER_QUALITY_VARIABLES,
    dataset_path,
)
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

    def read_huc_lookup(self) -> pd.DataFrame:
        """Read the station-to-HUC6/HUC8 lookup table."""

        filesystem = self._filesystem("HUC lookup")
        try:
            return pd.read_parquet(
                dataset_path("huc_lookup"),
                filesystem=filesystem,
                columns=["STREAM_ID", "HUC6", "HUC8"],
            )
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM HUC lookup: {error}") from error

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

    def read_streamflow(
        self,
        site_ids: str | Sequence[str],
        start=None,
        end=None,
        quality: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Read filtered hourly streamflow observations."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")

        start_time = self._parse_date(start, "start")
        end_time = self._parse_date(end, "end")
        quality_flags = normalize_quality(quality)
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("DateTime") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("DateTime") <= end_time.to_pydatetime()
        if quality_flags is not None:
            expression &= ds.field(QUALITY_FLAG_COLUMNS["Q_m3s"]).isin(quality_flags)

        filesystem = self._filesystem("streamflow")
        try:
            dataset = ds.dataset(
                dataset_path("streamflow"),
                format="parquet",
                filesystem=filesystem,
                partitioning="hive",
            )
            columns = ["STREAM_ID", "DateTime", "Q_m3s", "Flag_Q_m3s"]
            return dataset.to_table(columns=columns, filter=expression).to_pandas()
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM streamflow: {error}") from error

    def read_static_attributes(
        self,
        site_ids: str | Sequence[str],
        columns: str | Sequence[str],
    ) -> pd.DataFrame:
        """Read selected static catchment attributes for one or more sites."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        columns = self._normalize_values(columns, "columns")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")
        if not columns:
            raise ValueError("columns must contain at least one attribute")

        selected_columns = ["STREAM_ID"]
        selected_columns.extend(
            column for column in columns if column != "STREAM_ID"
        )

        filesystem = self._filesystem("static catchment attributes")
        try:
            dataset = ds.dataset(
                dataset_path("static_catchment_characteristics"),
                format="parquet",
                filesystem=filesystem,
            )
            unknown = sorted(set(selected_columns) - set(dataset.schema.names))
            if unknown:
                raise ValueError(
                    f"Unknown static attribute column(s): {', '.join(unknown)}"
                )

            expression = ds.field("STREAM_ID").isin(site_ids)
            return dataset.to_table(
                columns=selected_columns,
                filter=expression,
            ).to_pandas()
        except ValueError:
            raise
        except Exception as error:
            raise RuntimeError(
                f"Unable to read STREAM static catchment attributes: {error}"
            ) from error

    def read_watersheds(
        self,
        site_ids: str | Sequence[str],
    ):
        """Read watershed geometries for one or more STREAM sites."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")

        filesystem = self._filesystem("watersheds")
        try:
            return _read_geoparquet(
                dataset_path("watersheds"),
                filesystem=filesystem,
                columns=["STREAM_ID", "COMID", "area_km2", "geometry"],
                filters=[("STREAM_ID", "in", site_ids)],
            )
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM watersheds: {error}") from error

    def read_grab_samples(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read selected grab-sample measurements, sites, and dates."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        variables = self._normalize_values(variables, "variables")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")
        if not variables:
            raise ValueError("variables must contain at least one variable")

        unknown = sorted(set(variables) - set(GRAB_SAMPLE_VARIABLES))
        if unknown:
            available = ", ".join(GRAB_SAMPLE_VARIABLES)
            raise ValueError(
                f"Unknown grab-sample variable(s): {', '.join(unknown)}. "
                f"Available variables: {available}"
            )

        start_time = self._parse_date(start, "start")
        end_time = self._parse_date(end, "end")
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("DateTime") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("DateTime") <= end_time.to_pydatetime()

        columns = ["STREAM_ID", "DateTime", *variables]
        filesystem = self._filesystem("grab samples")
        try:
            dataset = ds.dataset(
                dataset_path("grab_samples"),
                format="parquet",
                filesystem=filesystem,
            )
            return dataset.to_table(columns=columns, filter=expression).to_pandas()
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM grab samples: {error}") from error

    def read_meteorology(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read selected hourly meteorological variables, sites, and dates."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        variables = self._normalize_values(variables, "variables")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")
        if not variables:
            raise ValueError("variables must contain at least one variable")

        unknown = sorted(set(variables) - set(METEOROLOGY_VARIABLES))
        if unknown:
            available = ", ".join(METEOROLOGY_VARIABLES)
            raise ValueError(
                f"Unknown meteorology variable(s): {', '.join(unknown)}. "
                f"Available variables: {available}"
            )

        start_time = self._parse_date(start, "start")
        end_time = self._parse_date(end, "end")
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("time") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("time") <= end_time.to_pydatetime()

        columns = ["STREAM_ID", "time", *variables]
        filesystem = self._filesystem("meteorology")
        try:
            dataset = ds.dataset(
                dataset_path("meteorology"),
                format="parquet",
                filesystem=filesystem,
                partitioning="hive",
            )
            return dataset.to_table(columns=columns, filter=expression).to_pandas()
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM meteorology: {error}") from error

    def read_land_cover(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str] | None = None,
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read annual watershed land-cover fractions."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")

        if variables is None:
            selected_variables = list(LAND_COVER_VARIABLES)
        else:
            selected_variables = self._normalize_values(variables, "variables")
            if not selected_variables:
                raise ValueError("variables must contain at least one variable")

        unknown = sorted(set(selected_variables) - set(LAND_COVER_VARIABLES))
        if unknown:
            available = ", ".join(LAND_COVER_VARIABLES)
            raise ValueError(
                f"Unknown land-cover variable(s): {', '.join(unknown)}. "
                f"Available variables: {available}"
            )

        start_time = self._parse_date(start, "start")
        end_time = self._parse_date(end, "end")
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("DateTime") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("DateTime") <= end_time.to_pydatetime()

        columns = ["STREAM_ID", "DateTime", *selected_variables]
        filesystem = self._filesystem("land cover")
        try:
            dataset = ds.dataset(
                dataset_path("land_cover"),
                format="parquet",
                filesystem=filesystem,
            )
            return dataset.to_table(columns=columns, filter=expression).to_pandas()
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM land cover: {error}") from error

    def read_anthropogenic(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str] | None = None,
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read annual watershed-scale anthropogenic variables."""

        site_ids = self._normalize_values(site_ids, "site_ids")
        if not site_ids:
            raise ValueError("site_ids must contain at least one site")

        if variables is None:
            selected_variables = list(ANTHROPOGENIC_VARIABLES)
        else:
            selected_variables = self._normalize_values(variables, "variables")
            if not selected_variables:
                raise ValueError("variables must contain at least one variable")

        unknown = sorted(set(selected_variables) - set(ANTHROPOGENIC_VARIABLES))
        if unknown:
            available = ", ".join(ANTHROPOGENIC_VARIABLES)
            raise ValueError(
                f"Unknown anthropogenic variable(s): {', '.join(unknown)}. "
                f"Available variables: {available}"
            )

        start_time = self._parse_date(start, "start")
        end_time = self._parse_date(end, "end")
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start must be before or equal to end")

        expression = ds.field("STREAM_ID").isin(site_ids)
        if start_time is not None:
            expression &= ds.field("DateTime") >= start_time.to_pydatetime()
        if end_time is not None:
            expression &= ds.field("DateTime") <= end_time.to_pydatetime()

        columns = ["STREAM_ID", "DateTime", *selected_variables]
        filesystem = self._filesystem("anthropogenic data")
        try:
            dataset = ds.dataset(
                dataset_path("anthropogenic"),
                format="parquet",
                filesystem=filesystem,
            )
            return dataset.to_table(columns=columns, filter=expression).to_pandas()
        except Exception as error:
            raise RuntimeError(
                f"Unable to read STREAM anthropogenic data: {error}"
            ) from error

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


def _read_geoparquet(*args, **kwargs):
    """Import GeoPandas only when watershed geometry is requested."""

    try:
        import geopandas as gpd
    except ImportError as error:
        raise RuntimeError(
            "GeoPandas is required to read STREAM watershed geometries"
        ) from error
    return gpd.read_parquet(*args, **kwargs)
