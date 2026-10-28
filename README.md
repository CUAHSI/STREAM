# streamwq

Arrow-first Python access to the analysis-ready STREAM Parquet dataset on
HydroShare. Import the package as `stream`: `import streamwq as stream`.
Site discovery, metadata, and water-quality/streamflow access use the
module-level Arrow API. Other observation readers and analysis modules are
still being migrated and retain their existing pandas-based interfaces.

- [STREAM Parquet resource](https://www.hydroshare.org/resource/4553ef8a14884e4199c7018975b8faaa/)
- [STREAM dataset documentation](https://www.hydroshare.org/resource/0ca12d1860894579bb11eacf408a8830/)

## Install the package

From a checkout, using Python 3.14:

```bash
python -m pip install .
python -c "import streamwq as stream; print(stream.catalog.DATASETS)"
```

Core access requires Arrow 25 and the HydroShare storage dependencies, not
pandas or GeoPandas. Install `'.[pandas]'` for native Arrow `.to_pandas()`
conversion. The old `stream_utils` import package is not provided.

For notebooks and the full legacy analysis environment, use Pixi below.

## Step 1. Install Pixi

Pixi manages Python and all project dependencies. Follow the official
[Pixi installation instructions](https://pixi.sh/latest/installation/) or use
one of these commands.

macOS and Linux:

```bash
curl -fsSL https://pixi.sh/install.sh | sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm -useb https://pixi.sh/install.ps1 | iex"
```

Restart the terminal after installation, then confirm that Pixi is available:

```bash
pixi --version
```

## Step 2. Set up the project

From the repository root:

```bash
pixi install
pixi run start
```

`pixi run start` launches JupyterLab. Open the notebooks in
`notebooks/tutorials/` to begin. The Pixi workspace is named `streamwq`.
Remote access prompts for HydroShare credentials on the first metadata read
and reuses that authenticated provider for subsequent metadata and data reads
in the process. Result tables are not cached. Importing the package and
inspecting its catalog require no connection.

## Basic usage

```python
import streamwq as stream

print(stream.catalog.DATASETS)
print(stream.catalog.VARIABLES["water_quality"])

metadata = stream.get_metadata()  # Materialized pyarrow.Table, all sites
site_ids = metadata.select(["STREAM_ID"]).slice(0, 2)
selected = stream.get_metadata(site_ids)
print(selected.schema)

# Find candidate sites and pass their IDs directly to metadata or data access.
candidates = stream.get_sites(states="Virginia", variables=["WTemp_C", "DO_mgL"])
candidate_metadata = stream.get_metadata(site_ids=candidates)
observations = stream.get_data(
    "water_quality",
    site_ids=candidates,
    columns=["STREAM_ID", "DateTime", "WTemp_C", "Flag_WTemp_C"],
    start="2020-01-01",
    end="2020-12-31",
    quality="approved",
)
```

Run the complete example with `pixi run python examples/metadata_catalog.py`.
Run both observation examples with `pixi run python examples/data_access.py`.

### Metadata selection

`get_metadata(site_ids=None, *, columns=None)` returns a materialized
`pyarrow.Table` directly from Parquet without converting through pandas.
Omit `site_ids` (or pass `None`) for all sites. A selection can be a single
non-empty ID, a sequence of IDs, or an Arrow table containing exactly one
column named `STREAM_ID`. Empty selections, null IDs, non-string IDs, and
blank IDs raise `ValueError` before storage access. `STREAM_ID` remains the
stored identifier field; it is not renamed.

Use `columns=["STREAM_ID", "site_name"]` to project fields. Filtering can use
`STREAM_ID` even when that column is not projected. Matching rows retain the
stored Arrow field types and schema metadata. Selections filter stored rows:
repeated requested IDs do not duplicate rows, and unknown IDs do not add rows.
An all-unknown selection returns a zero-row table with the selected schema.
No ordering guarantee is added. Observation access independently validates
every selected ID and rejects a request containing any unknown ID.

### Site discovery

`get_sites(...)` returns a materialized `pyarrow.Table` with exactly one
column, `STREAM_ID`, and no repeated IDs. Pass that table unchanged to
`get_metadata(site_ids=...)`. Supported filters are `states`, `variables`,
`streamflow`, `grab_variables`, `huc6`, `huc8`, `bbox`, `center`,
`radius_km`, and `nearest`. Different filters narrow the selection together
(AND); multiple values within one filter are alternatives (OR). For example,
`variables=["WTemp_C", "DO_mgL"]` finds sites with either listed variable.

The `bbox` order is `(west, south, east, north)` in WGS 84 degrees, with
longitude values from -180 to 180 and latitude values from -90 to 90.
`center` is `(longitude, latitude)` in WGS 84 degrees. A `center` must be
paired with either a positive `radius_km` or a positive integer `nearest`;
when both `radius_km` and `nearest` are supplied, nearest sites are selected
from within the radius. Bounding-box and center/radius selections can be
combined.

Site discovery uses metadata availability fields to identify candidates. It
does not check observations, and does not guarantee data for a later dataset,
date range, or quality-filter request. Invalid selections raise a
descriptive `ValueError`; a valid selection matching no sites also raises a
`ValueError` with guidance to broaden or change the selection.

### Water-quality and streamflow data

`get_data(dataset, site_ids=None, *, columns=None, start=None, end=None,
quality=None)` currently supports the explicit dataset names `water_quality`
and `streamflow`. `site_ids` is required and accepts one non-empty ID, a
sequence, or a one-column `STREAM_ID` Arrow table such as the result from
`get_sites`. Every ID is checked against site metadata before observations are
read; if any are unknown, the whole request raises `ValueError` listing them.
A known site with no matching observations returns a zero-row table retaining
the requested schema.

All filters are applied during the Parquet read, before the result is
materialized as `pyarrow.Table`. `columns` is optional: omitted means all
columns in that dataset's Parquet schema; an explicit sequence is the exact
output projection. Unknown columns are rejected. `catalog.VARIABLES` lists
measurement variables, not the complete column schema. `STREAM_ID`,
`DateTime`, quality flags, and any additional selectable columns are discovered
from the Parquet schema, which is the authority for valid columns. Inspect
`table.schema` on a result (including an empty result) to see the schema.

`start` and `end` accept ISO date/datetime strings or Python date/datetime
values. Both boundaries are inclusive; a date-only value means midnight at
the start of that date. Bounds must be timezone-naive and are interpreted in
the stored timestamp's time basis. `quality` accepts flag codes (`"A"`, `"P"`,
`"E"`) or their names (`"approved"`, `"provisional"`, `"erroneous"`). It
filters every selected measurement by its corresponding quality flag.
Quality-flag columns are included in output only when selected explicitly or
when the default all-column projection is used. Quality filtering requires at
least one measurement column in the selected projection.

The runnable [data-access example](examples/data_access.py) demonstrates both
datasets and passes the discovery table directly to `get_data`. Discovery
selects candidate sites; it does not guarantee observations in either dataset.

### Public catalog

`stream.catalog.DATASETS` is a tuple of supported dataset names.
`stream.catalog.VARIABLES` is a read-only mapping from each dataset name to a
tuple of its enumerated measurement variables. The named constants such as
`WATER_QUALITY_VARIABLES` remain available. An empty tuple for `metadata`,
`static_catchment_characteristics`, `watersheds`, or `huc_lookup` means there
is no enumerated measurement-variable list; it does not mean no columns exist.
Inspect the returned table's schema for metadata fields. Storage paths,
resource identifiers, and connection configuration are private implementation
details, not public catalog entries. Catalog names describe supported datasets;
`get_data` currently reads water quality and streamflow; the other catalog
datasets are being added in later migration steps.

### Migration status

This is a breaking package rename, not a compatibility release. Existing
observation tutorials now import `streamwq` mechanically but still demonstrate
the legacy `streamwq.client.StreamClient` and pandas helpers. They require
the full Pixi environment. Module-level site discovery, metadata, and
water-quality/streamflow access do not require creating a client, and no
advanced configurable client is introduced by this migration.

## Repository layout

```text
notebooks/
  tutorials/          Maintained user tutorials
  parquet-workflow/   Dataset creation and upload workflow
  workshop-archive/   Historical workshop examples and prototypes
streamwq/             Reusable Python package
examples/             Module-level access examples
tests/                Unit tests
```

## Run tests

```bash
pixi run test
pixi run typecheck
```

Tests use local Arrow/Parquet fixtures and substitute the existing HydroShare
filesystem provider. They do not require live network access or credentials.

## Reproducible exports

Use `export_subset` to save a Parquet or CSV subset with a JSON provenance.

```python
from streamwq.export import export_subset

export_subset(
    selected.to_pandas(),
    "outputs/site_metadata.parquet",
    query={"site_ids": site_ids.column("STREAM_ID").to_pylist()},
)
```
