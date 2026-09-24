# STREAM Python Utilities

Python utilities and tutorials for querying the analysis-ready Parquet version
of the STREAM dataset on HydroShare. The client supports filtered access to
water quality, streamflow, grab samples, meteorology, land cover,
anthropogenic variables, catchment attributes, and watershed geometries. 

- [STREAM Parquet resource](https://www.hydroshare.org/resource/4553ef8a14884e4199c7018975b8faaa/)
- [STREAM dataset documentation](https://www.hydroshare.org/resource/0ca12d1860894579bb11eacf408a8830/)

All dependencies are managed through Pixi. If you are not familiar with Pixi, follow Step 1 
to install it. If Pixi is already installed on your machine, go directly to Step 2.

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
`notebooks/tutorials/` to begin. The client prompts for HydroShare credentials
when remote data access is requested.

## Basic usage

```python
from utils.client import StreamClient

stream = StreamClient()

sites = stream.find_sites(
    variables=["WTemp_C", "DO_mgL"],
    streamflow=True,
)

water_quality = stream.read_water_quality(
    site_ids=sites["STREAM_ID"].head(3).tolist(),
    variables=["WTemp_C", "DO_mgL"],
    start="2020-01-01",
    end="2020-12-31",
    quality="approved",
)
```

## Repository layout

```text
notebooks/
  tutorials/          Maintained user tutorials
  parquet-workflow/   Dataset creation and upload workflow
  workshop-archive/   Historical workshop examples and prototypes
utils/                Reusable Python modules
tests/                Unit tests
```

## Run tests

```bash
pixi run python -m unittest discover -s tests
```

## Reproducible exports

Use `export_subset` to save a Parquet or CSV subset with a JSON provenance.

```python
from utils.export import export_subset

export_subset(
    water_quality,
    "outputs/water_quality.parquet",
    query={"start": "2020-01-01", "end": "2020-12-31"},
)
```
