"""Run with ``pixi run python examples/watershed_access.py``."""

import streamwq as stream


def main(site_ids: str | list[str], *, convert: bool = False) -> None:
    watersheds = stream.get_watersheds(site_ids=site_ids)
    print("Watersheds as Arrow:")
    print(watersheds)

    if convert:
        geo_data = stream.helpers.to_geopandas(watersheds)
        print("Watersheds as GeoPandas:")
        print(geo_data)


if __name__ == "__main__":
    main("STREAM-gauge-1")
