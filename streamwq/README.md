# streamwq Python interface

The four readers return materialized Arrow tables. No client needs to be
created, and pandas and GeoPandas are optional:

```python
import streamwq as stream

print(stream.catalog.DATASETS)
print(stream.catalog.VARIABLES["water_quality"])

site_ids = stream.get_sites(states="Virginia", variables="WTemp_C").slice(0, 3)
metadata = stream.get_metadata(site_ids)
observations = stream.get_data(
    "water_quality",
    site_ids,
    columns=["STREAM_ID", "DateTime", "WTemp_C"],
    quality="approved",
)
watersheds = stream.get_watersheds(site_ids)
```

Site discovery returns a one-column `STREAM_ID` table and raises `ValueError`
if no sites match. Matching metadata does not guarantee observations for any
particular dataset, time window, or quality filter. `get_data` and
`get_watersheds` require a non-empty selection. `get_metadata()` without a
selection reads all metadata. Metadata and watershed reads omit unknown site
IDs; `get_data` rejects the entire selection if any ID is unknown. A valid
selection with no matching data returns an empty Arrow table with its schema.

`catalog.VARIABLES` lists measurement variables, not the complete column list.
Inspect the result's `schema` for selectable columns, including identifiers,
timestamps, quality flags, or static attributes. See the [project README](
../README.md) for all seven tabular datasets and their filters.

## Optional analysis

Install `pip install "streamwq[pandas]"` for native Arrow `to_pandas()` and
the helpers in `streamwq.helpers.pandas`. This example runs without network
access:

```python
import pyarrow as pa
from streamwq.helpers.pandas import add_water_year, summarize_observation_period

table = pa.table({
    "STREAM_ID": ["site-1", "site-1"],
    "DateTime": ["2020-09-30", "2020-10-01"],
    "Q_m3s": [2.5, 3.0],
})
observations = add_water_year(table.to_pandas())
print(summarize_observation_period(observations))
```

The same module provides `filter_season`, `common_observation_period`, and
`resample_timeseries`. Existing dataframe joins, export and map helpers are
separate from access; convert explicitly before passing data to them.

For watershed geometry, install `pip install "streamwq[geopandas]"` and call
`stream.helpers.to_geopandas(watersheds)`. The conversion restores the active
geometry and coordinate reference system from GeoParquet metadata. The
[watershed example](../examples/watershed_access.py) shows both Arrow-only and
GeoPandas use.
