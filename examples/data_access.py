"""Run with ``pixi run python examples/data_access.py``."""

import streamwq as stream


def main() -> None:
    site_ids = stream.get_sites().slice(0, 1)
    print("Selected site:", site_ids)

    water_quality = stream.get_data(
        "water_quality",
        site_ids=site_ids,
        columns=["STREAM_ID", "DateTime", "WTemp_C", "Flag_WTemp_C"],
        quality="approved",
    )
    streamflow = stream.get_data(
        "streamflow",
        site_ids=site_ids,
        columns=["STREAM_ID", "DateTime", "Q_m3s", "Flag_Q_m3s"],
        quality="approved",
    )
    grab_samples = stream.get_data(
        "grab_samples",
        site_ids=site_ids,
        columns=["STREAM_ID", "DateTime", "SSC_mgL"],
    )
    print("Water-quality observations:")
    print(water_quality)
    print("Streamflow observations:")
    print(streamflow)
    print("Grab samples:")
    print(grab_samples)


if __name__ == "__main__":
    main()
