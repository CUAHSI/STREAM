"""Arrow-native access to STREAM watershed geometries."""

from collections.abc import Sequence
from typing import TypeGuard

import pyarrow as pa

from streamwq._metadata import _storage_provider
from streamwq._storage import dataset_path


def get_watersheds(
    site_ids: str | Sequence[str] | pa.Table | None = None,
) -> pa.Table:
    """Return watershed geometries for selected sites as a GeoParquet Arrow table.

    Unknown site IDs are filtered out, matching the legacy watershed reader.
    """
    selected_ids = _normalize_site_ids(site_ids)
    import pyarrow.dataset as ds

    filesystem = _storage_provider().get_s3_filesystem()
    if filesystem is None:
        raise RuntimeError("S3 filesystem is required to read STREAM watersheds")

    try:
        dataset = ds.dataset(
            dataset_path("watersheds"),
            format="parquet",
            filesystem=filesystem,
        )
        columns = ["STREAM_ID", "COMID", "area_km2", "geometry"]
        missing = sorted(set(columns) - set(dataset.schema.names))
        if missing:
            raise RuntimeError(
                "STREAM watersheds schema is missing required column(s): "
                f"{', '.join(missing)}"
            )
        return dataset.to_table(
            columns=columns,
            filter=ds.field("STREAM_ID").isin(selected_ids),
        )
    except (OSError, pa.ArrowException) as error:
        raise RuntimeError(f"Unable to read STREAM watersheds: {error}") from error


def _normalize_site_ids(
    site_ids: str | Sequence[str] | pa.Table | None,
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
