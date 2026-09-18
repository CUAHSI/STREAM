"""A small Python client for accessing the STREAM Parquet resource."""

from collections.abc import Sequence

import pandas as pd

from utils.catalog import DATASETS, dataset_path


class StreamClient:
    """Provide simple access to datasets in the STREAM Parquet resource.

    Parameters
    ----------
    hydroshare
        An existing HydroShare client. When omitted, the user is prompted for
        HydroShare credentials by :class:`utils.S3hsclient.S3HydroShare`.
        Passing a client is also useful when testing with a mock.
    """

    def __init__(self, hydroshare=None):
        if hydroshare is None:
            # Import only when a real connection is requested. This keeps the
            # client importable in documentation and tests that use a mock.
            from utils.S3hsclient import S3HydroShare

            hydroshare = S3HydroShare()

        self._hydroshare = hydroshare

    def list_datasets(self) -> list[str]:
        """Return the dataset names understood by this client."""

        return sorted(DATASETS)

    def path(self, name: str) -> str:
        """Return the HydroShare S3 path for a dataset."""

        return dataset_path(name)

    def read_metadata(self, columns: Sequence[str] | None = None) -> pd.DataFrame:
        """Read site metadata from HydroShare into a pandas DataFrame.

        Parameters
        ----------
        columns
            Optional column names to read. Selecting only the columns needed
            by an analysis reduces the amount of transferred data.
        """

        if columns is not None:
            if isinstance(columns, str) or not all(
                isinstance(column, str) and column for column in columns
            ):
                raise ValueError("columns must be a sequence of non-empty strings")
            columns = list(columns)

        filesystem = self._hydroshare.get_s3_filesystem()
        if filesystem is None:
            raise RuntimeError("A HydroShare S3 filesystem is required to read metadata")

        try:
            return pd.read_parquet(
                self.path("metadata"),
                filesystem=filesystem,
                columns=columns,
            )
        except Exception as error:
            raise RuntimeError(f"Unable to read STREAM metadata: {error}") from error
