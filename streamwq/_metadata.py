"""Arrow-native site metadata access."""

from collections.abc import Sequence
from functools import cache

import pyarrow as pa

from streamwq._storage import dataset_path


@cache
def _storage_provider():
    from streamwq.S3hsclient import S3HydroShare

    return S3HydroShare()


def get_metadata(
    site_ids: str | Sequence[str] | pa.Table | None = None,
    *,
    columns: Sequence[str] | None = None,
) -> pa.Table:
    """Return all or selected site metadata as a materialized Arrow table."""
    import pyarrow.dataset as ds

    if columns is not None:
        if isinstance(columns, str) or not isinstance(columns, Sequence) or not all(
            isinstance(column, str) and column for column in columns
        ):
            raise ValueError("columns must be a sequence of non-empty strings")
        columns = list(columns)

    expression = None
    if site_ids is not None:
        if isinstance(site_ids, str):
            selected = [site_ids]
        elif isinstance(site_ids, Sequence):
            selected = list(site_ids)
        elif isinstance(site_ids, pa.Table):
            if site_ids.column_names != ["STREAM_ID"]:
                raise ValueError("site_ids must be a one-column STREAM_ID Arrow table")
            selected = site_ids.column("STREAM_ID").to_pylist()
        else:
            raise ValueError(
                "site_ids must be an ID, a sequence, or a STREAM_ID Arrow table"
            )
        if not selected:
            raise ValueError("site_ids must contain at least one non-empty site ID")
        if any(
            not isinstance(identifier, str) or not identifier.strip()
            for identifier in selected
        ):
            raise ValueError(
                "site_ids must contain at least one non-empty site ID; "
                "all IDs must be strings"
            )
        expression = ds.field("STREAM_ID").isin(selected)

    filesystem = _storage_provider().get_s3_filesystem()
    if filesystem is None:
        raise RuntimeError("S3 filesystem is required to read STREAM metadata")
    try:
        dataset = ds.dataset(
            dataset_path("metadata"), format="parquet", filesystem=filesystem
        )
        return dataset.to_table(columns=columns, filter=expression)
    except Exception as error:
        raise RuntimeError(f"Unable to read STREAM metadata: {error}") from error


def _get_huc_lookup() -> pa.Table:
    """Read the small HUC lookup table as Arrow for site discovery."""
    import pyarrow.dataset as ds

    filesystem = _storage_provider().get_s3_filesystem()
    if filesystem is None:
        raise RuntimeError("S3 filesystem is required to read the STREAM HUC lookup")
    try:
        dataset = ds.dataset(
            dataset_path("huc_lookup"), format="parquet", filesystem=filesystem
        )
        return dataset.to_table(columns=["STREAM_ID", "HUC6", "HUC8"])
    except Exception as error:
        raise RuntimeError(f"Unable to read STREAM HUC lookup: {error}") from error