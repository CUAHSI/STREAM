# streamwq

Arrow-first Python access to the analysis-ready STREAM Parquet dataset on
HydroShare. Import the package as `stream`: `import streamwq as stream`.
Site metadata and dataset/variable discovery use the new module-level API.
The remaining observation, selection, and analysis modules are still being
migrated and retain their existing pandas-based interfaces under `streamwq`.

- [STREAM Parquet resource](https://www.hydroshare.org/resource/4553ef8a14884e4199c7018975b8faaa/)
- [STREAM dataset documentation](https://www.hydroshare.org/resource/0ca12d1860894579bb11eacf408a8830/)

## Install the package

From a checkout, using Python 3.14:

```bash
python -m pip install .
python -c "import streamwq as stream; print(stream.catalog.DATASETS)"
```

Core metadata access requires Arrow 25 and the HydroShare storage dependencies,
not pandas or GeoPandas. Install `'.[pandas]'` for native Arrow `.to_pandas()`
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
and reuses that authenticated provider for subsequent metadata reads in the
process. Metadata results are not cached. Importing the package and inspecting
its catalog require no connection.

## Basic usage

```python
import streamwq as stream

print(stream.catalog.DATASETS)
print(stream.catalog.VARIABLES["water_quality"])

metadata = stream.get_metadata()  # Materialized pyarrow.Table, all sites
site_ids = metadata.select(["STREAM_ID"]).slice(0, 2)
selected = stream.get_metadata(site_ids)
print(selected.schema)
```

Run the complete example with `pixi run python examples/metadata_catalog.py`.

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
No ordering guarantee is added. Unknown-ID validation for observation access
is a separate part of the migration.

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
this slice adds only metadata access, not the future unified `get_data` API.

### Migration status

This is a breaking package rename, not a compatibility release. Existing
observation tutorials now import `streamwq` mechanically but still demonstrate
the legacy `streamwq.client.StreamClient` and pandas helpers. They require
the full Pixi environment. Ordinary metadata access does not require creating
a client, and no advanced configurable client is introduced by this slice.

## Repository layout

```text
notebooks/
  tutorials/          Maintained user tutorials
  parquet-workflow/   Dataset creation and upload workflow
  workshop-archive/   Historical workshop examples and prototypes
streamwq/             Reusable Python package
examples/             Module-level metadata/catalog example
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
