"""Arrow-first access to STREAM. Recommended import: ``import streamwq as stream``."""

from streamwq import catalog
from streamwq._data import get_data
from streamwq._discovery import get_sites
from streamwq._metadata import get_metadata

__all__ = ["catalog", "get_data", "get_metadata", "get_sites"]
