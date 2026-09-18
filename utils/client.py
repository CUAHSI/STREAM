"""Public Python client for the STREAM Parquet resource."""

from collections.abc import Sequence

import pandas as pd

from utils.access import StreamAccess
from utils.catalog import DATASETS, dataset_path
from utils.sites import SITE_COLUMNS, filter_sites


class StreamClient:
    """Provide a simple user-facing interface to STREAM datasets."""

    def __init__(self, hydroshare=None, access=None):
        self._access = access or StreamAccess(hydroshare=hydroshare)

    def list_datasets(self) -> list[str]:
        """Return the dataset names understood by this client."""

        return sorted(DATASETS)

    def path(self, name: str) -> str:
        """Return the HydroShare S3 path for a dataset."""

        return dataset_path(name)

    def read_metadata(self, columns: Sequence[str] | None = None) -> pd.DataFrame:
        """Read site metadata into a pandas DataFrame."""

        return self._access.read_metadata(columns=columns)

    def find_sites(
        self,
        states: str | Sequence[str] | None = None,
        variables: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Find sites by state and available water-quality variables."""

        sites = self.read_metadata(columns=SITE_COLUMNS)
        return filter_sites(sites, states=states, variables=variables)

    def read_water_quality(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
        quality: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Read filtered hourly water-quality observations."""

        return self._access.read_water_quality(
            site_ids=site_ids,
            variables=variables,
            start=start,
            end=end,
            quality=quality,
        )

    def read_streamflow(
        self,
        site_ids: str | Sequence[str],
        start=None,
        end=None,
        quality: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Read filtered hourly streamflow observations."""

        return self._access.read_streamflow(
            site_ids=site_ids,
            start=start,
            end=end,
            quality=quality,
        )

    def read_static_attributes(
        self,
        site_ids: str | Sequence[str],
        columns: str | Sequence[str],
    ) -> pd.DataFrame:
        """Read selected static catchment attributes."""

        return self._access.read_static_attributes(
            site_ids=site_ids,
            columns=columns,
        )
