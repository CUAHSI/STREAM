"""Run with ``pixi run python examples/metadata_catalog.py``."""

import streamwq as stream


def main() -> None:
    print("Datasets:", stream.catalog.DATASETS)
    print("Water-quality variables:", stream.catalog.VARIABLES["water_quality"])
    metadata = stream.get_metadata()
    print("Metadata schema:", metadata.schema)
    site_ids = metadata.select(["STREAM_ID"]).slice(0, 2)
    selected = stream.get_metadata(site_ids)
    print(selected)


if __name__ == "__main__":
    main()