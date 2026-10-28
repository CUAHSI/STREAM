"""Run with ``pixi run python examples/metadata_catalog.py``."""

import streamwq as stream


def main() -> None:
    print("Datasets:", stream.catalog.DATASETS)
    print("Water-quality variables:", stream.catalog.VARIABLES["water_quality"])
    site_ids = stream.get_sites()
    print("Candidate site IDs:", site_ids)
    selected = stream.get_metadata(site_ids=site_ids)
    print("Selected metadata schema:", selected.schema)
    print(selected)


if __name__ == "__main__":
    main()