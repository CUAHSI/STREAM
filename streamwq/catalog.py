"""Supported STREAM dataset names and measurement variables.

Non-measurement columns, including static catchment attributes, are discoverable
from the schema returned by :func:`streamwq.get_data`.
"""

from collections.abc import Mapping as _Mapping
from types import MappingProxyType as _MappingProxyType

from streamwq._storage import (
    ANTHROPOGENIC_VARIABLES,
    GRAB_SAMPLE_VARIABLES,
    LAND_COVER_VARIABLES,
    METEOROLOGY_VARIABLES,
    WATER_QUALITY_VARIABLES,
)

DATASETS: tuple[str, ...] = (
    "metadata",
    "static_catchment_characteristics",
    "watersheds",
    "water_quality",
    "meteorology",
    "land_cover",
    "anthropogenic",
    "streamflow",
    "grab_samples",
    "huc_lookup",
)

VARIABLES: _Mapping[str, tuple[str, ...]] = _MappingProxyType(
    {
        "metadata": (),
        "static_catchment_characteristics": (),
        "watersheds": (),
        "water_quality": WATER_QUALITY_VARIABLES,
        "meteorology": METEOROLOGY_VARIABLES,
        "land_cover": LAND_COVER_VARIABLES,
        "anthropogenic": ANTHROPOGENIC_VARIABLES,
        "streamflow": ("Q_m3s",),
        "grab_samples": GRAB_SAMPLE_VARIABLES,
        "huc_lookup": (),
    }
)

__all__ = [
    "DATASETS",
    "VARIABLES",
    "WATER_QUALITY_VARIABLES",
    "GRAB_SAMPLE_VARIABLES",
    "METEOROLOGY_VARIABLES",
    "LAND_COVER_VARIABLES",
    "ANTHROPOGENIC_VARIABLES",
]