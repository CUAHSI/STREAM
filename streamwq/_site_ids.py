"""Shared validation for explicitly selected site identifiers."""

from collections.abc import Sequence
from typing import TypeGuard

import pyarrow as pa


def normalize_site_ids(site_ids: object) -> list[str]:
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
