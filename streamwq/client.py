"""Public Python client for the STREAM Parquet resource."""

from collections.abc import Sequence

import pandas as pd

from streamwq.access import StreamAccess
from streamwq._storage import DATASETS, dataset_path
from streamwq.sites import SITE_COLUMNS, filter_sites
from streamwq.spatial import filter_bbox, filter_huc, filter_radius, select_nearest


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
        bbox: Sequence[float] | None = None,
        center: Sequence[float] | None = None,
        radius_km: float | None = None,
        nearest: int | None = None,
        huc6: str | Sequence[str] | None = None,
        huc8: str | Sequence[str] | None = None,
        streamflow: bool | None = None,
        grab_variables: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Find sites by state and available water-quality variables."""

        sites = self.read_metadata(columns=SITE_COLUMNS)
        sites = filter_sites(
            sites,
            states=states,
            variables=variables,
            streamflow=streamflow,
            grab_variables=grab_variables,
        )
        if huc6 is not None or huc8 is not None:
            lookup = self._access.read_huc_lookup()
            sites = filter_huc(sites, lookup, huc6=huc6, huc8=huc8)
        if bbox is not None:
            sites = filter_bbox(sites, bbox)
        if center is None and (radius_km is not None or nearest is not None):
            raise ValueError("center is required with radius_km or nearest")
        if center is not None and radius_km is None and nearest is None:
            raise ValueError("center requires radius_km or nearest")
        if radius_km is not None:
            sites = filter_radius(sites, center=center, radius_km=radius_km)
        if nearest is not None:
            sites = select_nearest(sites, center=center, count=nearest)
        return sites

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

    def read_watersheds(
        self,
        site_ids: str | Sequence[str],
    ):
        """Read watershed geometries for selected sites."""

        return self._access.read_watersheds(site_ids=site_ids)

    def read_grab_samples(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read filtered grab-sample measurements."""

        return self._access.read_grab_samples(
            site_ids=site_ids,
            variables=variables,
            start=start,
            end=end,
        )

    def read_meteorology(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str],
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read filtered hourly meteorological observations."""

        return self._access.read_meteorology(
            site_ids=site_ids,
            variables=variables,
            start=start,
            end=end,
        )

    def read_land_cover(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str] | None = None,
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read annual watershed land-cover fractions."""

        return self._access.read_land_cover(
            site_ids=site_ids,
            variables=variables,
            start=start,
            end=end,
        )

    def read_anthropogenic(
        self,
        site_ids: str | Sequence[str],
        variables: str | Sequence[str] | None = None,
        start=None,
        end=None,
    ) -> pd.DataFrame:
        """Read annual watershed-scale anthropogenic variables."""

        return self._access.read_anthropogenic(
            site_ids=site_ids,
            variables=variables,
            start=start,
            end=end,
        )
