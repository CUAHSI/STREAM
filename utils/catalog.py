"""Names and HydroShare paths for datasets in the STREAM resource.

This module is intentionally limited to metadata about the datasets.  Reading
and filtering the Parquet files will be added separately.
"""

RESOURCE_ID = "4553ef8a14884e4199c7018975b8faaa"
RESOURCE_OWNER = "tonycastronova"

_CONTENTS_ROOT = f"{RESOURCE_OWNER}/{RESOURCE_ID}/data/contents"


# Public names used by Python code mapped to names in the HydroShare resource.
DATASETS: dict[str, str] = {
    "metadata": "metadata.parquet",
    "static_catchment_characteristics": "static_catchment_characteristics.parquet",
    "watersheds": "watersheds.parquet",
    "water_quality": "waterquality_timeseries",
    "meteorology": "dynamic_historical_meteorology",
    "land_cover": "dynamic_lulc.parquet",
    "anthropogenic": "dynamic_anthropogenic.parquet",
    "streamflow": "streamflow",
    "grab_samples": "grab_samples.parquet",
}


# Only datasets with a time dimension are included here.
TIME_COLUMNS: dict[str, str] = {
    "water_quality": "DateTime",
    "meteorology": "time",
    "land_cover": "year",
    "anthropogenic": "year",
    "streamflow": "DateTime",
    "grab_samples": "DateTime",
}


# Measurement columns mapped to their corresponding source quality flags.
# Grab samples and supporting predictor datasets do not have paired flags.
QUALITY_FLAG_COLUMNS: dict[str, str] = {
    "WTemp_C": "Flag_WTemp_C",
    "SpC_uScm": "Flag_SpC_uScm",
    "DO_mgL": "Flag_DO_mgL",
    "pH": "Flag_pH",
    "Turb_FNU": "Flag_Turb_FNU",
    "Turb_NTU": "Flag_Turb_NTU",
    "NO3_mgNL": "Flag_NO3_mgNL",
    "PO4_mgPL": "Flag_PO4_mgPL",
    "fDOM_QSU": "Flag_fDOM_QSU",
    "fDOM_RFU": "Flag_fDOM_RFU",
    "Chla_ugL": "Flag_Chla_ugL",
    "Chla_RFU": "Flag_Chla_RFU",
    "PC_RFU": "Flag_PC_RFU",
    "Q_m3s": "Flag_Q_m3s",
}


# Variables stored in the continuous water-quality dataset. Streamflow has a
# quality flag too, but is stored in its own dataset.
WATER_QUALITY_VARIABLES: tuple[str, ...] = tuple(
    variable for variable in QUALITY_FLAG_COLUMNS if variable != "Q_m3s"
)


def dataset_path(name: str) -> str:
    """Return the full HydroShare S3 path for a named STREAM dataset.

    Parameters
    ----------
    name
        One of the names in :data:`DATASETS`, such as ``"metadata"`` or
        ``"streamflow"``.
    """

    try:
        resource_path = DATASETS[name]
    except KeyError as error:
        available = ", ".join(sorted(DATASETS))
        raise ValueError(
            f"Unknown STREAM dataset {name!r}. Available datasets: {available}"
        ) from error

    return f"{_CONTENTS_ROOT}/{resource_path}"
