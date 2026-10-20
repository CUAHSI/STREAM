"""Reproducible export helpers for STREAM data subsets."""

from datetime import date, datetime, timezone
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
from typing import Any

import pandas as pd

from streamwq._storage import RESOURCE_ID


def export_subset(
    data: pd.DataFrame,
    path: str | Path,
    query: dict[str, Any] | None = None,
    description: str | None = None,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Export a DataFrame and a JSON provenance manifest.

    The output format is inferred from a ``.parquet`` or ``.csv`` suffix. The
    returned tuple contains the data path and metadata path.
    """

    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a pandas DataFrame or GeoDataFrame")
    if query is not None and not isinstance(query, dict):
        raise TypeError("query must be a dictionary or None")
    if description is not None and not isinstance(description, str):
        raise TypeError("description must be a string or None")

    data_path = Path(path)
    suffix = data_path.suffix.lower()
    if suffix not in {".parquet", ".csv"}:
        raise ValueError("Export path must end with .parquet or .csv")
    metadata_path = data_path.with_suffix(".metadata.json")

    existing = [candidate for candidate in (data_path, metadata_path) if candidate.exists()]
    if existing and not overwrite:
        names = ", ".join(str(candidate) for candidate in existing)
        raise FileExistsError(f"Export would overwrite existing file(s): {names}")

    data_path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == ".parquet":
        data.to_parquet(data_path, index=False)
        output_format = "parquet"
    else:
        data.to_csv(data_path, index=False)
        output_format = "csv"

    manifest = {
        "stream_resource_id": RESOURCE_ID,
        "stream_resource_url": f"https://www.hydroshare.org/resource/{RESOURCE_ID}/",
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_file": data_path.name,
        "format": output_format,
        "row_count": len(data),
        "column_count": len(data.columns),
        "columns": list(data.columns),
        "query": query or {},
        "description": description,
        "package_version": _package_version(),
    }
    metadata_path.write_text(
        json.dumps(manifest, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )
    return data_path, metadata_path


def _package_version() -> str | None:
    try:
        return version("streamwq")
    except PackageNotFoundError:
        return None


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
