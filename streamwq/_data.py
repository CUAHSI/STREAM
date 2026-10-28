"""Arrow-native access to water-quality and streamflow observations."""

from collections.abc import Sequence
from datetime import date, datetime
from typing import TypeGuard

import pyarrow as pa

from streamwq import catalog
from streamwq._metadata import _storage_provider, get_metadata
from streamwq._storage import QUALITY_FLAG_COLUMNS, dataset_path
from streamwq.quality import normalize_quality


_SUPPORTED_DATASETS = ("water_quality", "streamflow")


def get_data(
    dataset: str | None = None,
    site_ids: str | Sequence[str] | pa.Table | None = None,
    *,
    columns: Sequence[str] | None = None,
    start: str | date | datetime | None = None,
    end: str | date | datetime | None = None,
    quality: str | Sequence[str] | None = None,
) -> pa.Table:
    """Return selected water-quality or streamflow observations as Arrow.

    Omitted ``columns`` selects every column in the Parquet schema. Explicit
    columns are returned exactly as requested. Date bounds are inclusive and
    timezone-naive; they are interpreted in the timestamp's stored time basis.
    Quality filters apply to each selected measurement using its paired
    quality flag; a missing flag column is an error. The output contains
    quality flags only when they are part of the selected projection.
    """
    dataset_name = _validate_dataset(dataset)
    selected_ids = _normalize_site_ids(site_ids)
    selected_columns = _normalize_columns(columns)
    start_time = _parse_date(start, "start")
    end_time = _parse_date(end, "end")
    if start_time is not None and end_time is not None and start_time > end_time:
        raise ValueError("start must be before or equal to end")
    quality_flags = normalize_quality(quality)

    metadata = get_metadata(site_ids=selected_ids, columns=["STREAM_ID"])
    known_ids = set(metadata.column("STREAM_ID").to_pylist())
    unknown_ids = sorted(set(selected_ids) - known_ids)
    if unknown_ids:
        raise ValueError(f"Unknown site ID(s): {', '.join(unknown_ids)}")

    import pyarrow.dataset as ds

    filesystem = _storage_provider().get_s3_filesystem()
    if filesystem is None:
        raise RuntimeError(f"S3 filesystem is required to read {dataset_name}")

    try:
        observation_dataset = ds.dataset(
            dataset_path(dataset_name),
            format="parquet",
            filesystem=filesystem,
            partitioning="hive",
        )
    except (OSError, pa.ArrowException) as error:
        raise RuntimeError(
            f"Unable to open STREAM {dataset_name} observations: {error}"
        ) from error

    schema_names = observation_dataset.schema.names
    required = {"STREAM_ID", "DateTime"}
    missing_required = sorted(required - set(schema_names))
    if missing_required:
        raise RuntimeError(
            f"STREAM {dataset_name} schema is missing required column(s): "
            f"{', '.join(missing_required)}"
        )

    if selected_columns is None:
        projection = schema_names
    else:
        unknown_columns = sorted(set(selected_columns) - set(schema_names))
        if unknown_columns:
            raise ValueError(
                f"Unknown column(s) for {dataset_name}: {', '.join(unknown_columns)}"
            )
        projection = selected_columns

    expression = ds.field("STREAM_ID").isin(selected_ids)
    if start_time is not None:
        expression &= ds.field("DateTime") >= start_time
    if end_time is not None:
        expression &= ds.field("DateTime") <= end_time

    if quality_flags is not None:
        selected_measurements = [
            variable
            for variable in catalog.VARIABLES[dataset_name]
            if variable in projection
        ]
        if not selected_measurements:
            raise ValueError(
                "quality filtering requires at least one measurement column "
                "in the selected projection"
            )
        for variable in selected_measurements:
            flag_column = QUALITY_FLAG_COLUMNS[variable]
            if flag_column not in schema_names:
                raise ValueError(
                    f"Quality filtering for {variable!r} requires column "
                    f"{flag_column!r}, which is not present in {dataset_name}"
                )
            expression &= ds.field(flag_column).isin(quality_flags)

    try:
        return observation_dataset.to_table(columns=projection, filter=expression)
    except (OSError, pa.ArrowException) as error:
        raise RuntimeError(
            f"Unable to read STREAM {dataset_name} observations: {error}"
        ) from error


def _validate_dataset(dataset: str | None) -> str:
    if not isinstance(dataset, str) or not dataset.strip():
        raise ValueError(
            "dataset is required; choose 'water_quality' or 'streamflow'"
        )
    dataset = dataset.strip()
    if dataset not in catalog.DATASETS:
        choices = ", ".join(catalog.DATASETS)
        raise ValueError(f"Unknown dataset {dataset!r}. Available datasets: {choices}")
    if dataset not in _SUPPORTED_DATASETS:
        choices = ", ".join(_SUPPORTED_DATASETS)
        raise ValueError(
            f"get_data currently supports {choices}; {dataset!r} is not supported"
        )
    return dataset


def _normalize_site_ids(
    site_ids: object,
) -> list[str]:
    if site_ids is None:
        raise ValueError("site_ids is required and must contain at least one site ID")
    if isinstance(site_ids, str):
        selected = [site_ids]
    elif _is_arrow_table(site_ids):
        if site_ids.column_names != ["STREAM_ID"]:
            raise ValueError("site_ids must be a one-column STREAM_ID Arrow table")
        selected = site_ids.column("STREAM_ID").to_pylist()
    elif isinstance(site_ids, Sequence):
        selected = list(site_ids)
    else:
        raise ValueError(
            "site_ids must be an ID, a sequence, or a STREAM_ID Arrow table"
        )
    if not selected:
        raise ValueError("site_ids must contain at least one non-empty site ID")
    if any(
        not isinstance(site_id, str) or not site_id.strip() for site_id in selected
    ):
        raise ValueError(
            "site_ids must contain non-empty strings without null or blank IDs"
        )
    return list(dict.fromkeys(site_id.strip() for site_id in selected))


def _is_arrow_table(value: object) -> TypeGuard[pa.Table]:
    return isinstance(value, pa.Table)


def _normalize_columns(columns: Sequence[str] | None) -> list[str] | None:
    if columns is None:
        return None
    if isinstance(columns, str) or not isinstance(columns, Sequence):
        raise ValueError("columns must be a sequence of non-empty strings")
    normalized = list(columns)
    if not normalized or any(
        not isinstance(column, str) or not column.strip() for column in normalized
    ):
        raise ValueError("columns must contain at least one non-empty column name")
    if len(set(normalized)) != len(normalized):
        raise ValueError("columns must not contain duplicate names")
    return normalized


def _parse_date(value: str | date | datetime | None, argument_name: str) -> datetime | None:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, date):
            parsed = datetime.combine(value, datetime.min.time())
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value)
        else:
            raise ValueError
    except (TypeError, ValueError) as error:
        raise ValueError(f"{argument_name} must be a valid ISO date or datetime") from error
    if parsed.utcoffset() is not None:
        raise ValueError(f"{argument_name} must be timezone-naive")
    return parsed
