"""Arrow-native access to STREAM watershed geometries."""

from collections.abc import Sequence

import pyarrow as pa

from streamwq._metadata import _storage_provider
from streamwq._site_ids import normalize_site_ids
from streamwq._storage import dataset_path


def get_watersheds(
    site_ids: str | Sequence[str] | pa.Table | None = None,
) -> pa.Table:
    """Return watershed geometries for selected sites as a GeoParquet Arrow table.

    Unknown site IDs are filtered out, matching the legacy watershed reader.
    """
    selected_ids = normalize_site_ids(site_ids)
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
