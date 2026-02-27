#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
from tqdm import tqdm
import requests


def build_wc_var_availability_df(df):
    """
    Build water-quality variable availability by gage as a wide boolean table.

    Returns
    -------
    pandas.DataFrame
        Columns:
        - STREAM_ID
        - one column per variable (True/False)
    """

    cols = [
        "WTemp_C",
        "SpC_uScm",
        "DO_mgL",
        "pH",
        "Turb_FNU",
        "STREAM_ID",
        "Turb_NTU",
        "NO3_mgNL",
        "fDOM_QSU",
        "fDOM_RFU",
        # DOC?
        # Q?
    ]

    # subset the data to only inlude the variable columns above.
    df = df[cols]

    # count the number of records for each variable by gage.
    counts = df.groupby("STREAM_ID").count()

    return counts


def call_geoconnex(url):
    req = requests.get(url)

    # if a non-200 code is returned, skip the site.
    if req.status_code != 200:
        print(f"Error processing {url}")

    # get the response in JSON to make processing the data easier
    return req.json()


def get_geoconnex_metadata(
    df,
    service="hu06",
    result_key="huc6",
    buffer=0.00001,
    buffer_increment=0.0005,
    retries=5,
    base_url="https://reference.geoconnex.us/collections",
    output_format="json",
):
    attributes = {}
    errors = {}

    for idx, row in tqdm(
        df.iterrows(), total=len(df), desc=f"Querying Geoconnex {service}"
    ):

        lat = row.latitude_wgs84
        lon = row.longitude_wgs84

        # build a bounding box that surrounds the lat/lon using the format
        # min_lon, min_lat, max_lon, max_lat. Buffer these points.
        bbox = f"{lon-buffer},{lat-buffer},{lon+buffer},{lat+buffer}"

        url = f"{base_url}/{service}/items?f={output_format}&bbox={bbox}"

        res = call_geoconnex(url)

        if res["numberReturned"] == 0:
            retry = 1
            while retry <= retries:
                retry += 1
                lat = row.latitude_wgs84
                lon = row.longitude_wgs84

                bbox = f"{lon-(buffer + retry*buffer_increment)},{lat-(buffer + retry*buffer_increment)},{lon+(buffer + retry*buffer_increment)},{lat+ (buffer + retry*buffer_increment)}"
                url = f"{base_url}/{service}/items?f={output_format}&bbox={bbox}"
                res = call_geoconnex(url)

                if res["numberReturned"] > 0:
                    break

        # if more than one object is returned, notify the user and then skip the site.
        if res["numberReturned"] != 1:
            msg = f'Gauge {row.STREAM_ID} returned {res["numberReturned"]} features.'
            errors[row.STREAM_ID] = msg
            continue

        attributes[idx] = res["features"][0]["properties"][result_key]

    return attributes, errors


if __name__ == "__main__":
    parquet_dir = Path("processed-data/MRB")

    metadata_df = pd.read_parquet(parquet_dir / "metadata.parquet")
    # vaa = metadata_df[["STREAM_ID", "State", "drainagearea_sqkm"]]
    vaa = metadata_df[["STREAM_ID", "state_name", "drainagearea_sqkm"]]

    # add HUCs
    attribs, errors = get_geoconnex_metadata(metadata_df, "hu12", "huc12")
    vaa["huc12"] = vaa.index.map(attribs)
    vaa["huc10"] = vaa.index.map({k: v[0:10] for k, v in attribs.items()})
    vaa["huc08"] = vaa.index.map({k: v[0:8] for k, v in attribs.items()})
    vaa["huc06"] = vaa.index.map({k: v[0:6] for k, v in attribs.items()})
    vaa["huc04"] = vaa.index.map({k: v[0:4] for k, v in attribs.items()})
    vaa["huc02"] = vaa.index.map({k: v[0:2] for k, v in attribs.items()})
    vaa.to_parquet(parquet_dir / "vaa.parquet")

    # add nhd comids
    if "nhdv2_comid" not in metadata_df.columns:
        attribs, errors = get_geoconnex_metadata(
            metadata_df, "mainstems", "head_nhdpv2_comid", buffer=0.0001, retries=10
        )
        cleaned_attribs = {k: v.split("/")[-1] for k, v in attribs.items()}
        vaa["nhdv2_comid"] = vaa.index.map(cleaned_attribs)
        vaa.to_parquet(parquet_dir / "vaa.parquet")

    wq_df = pd.read_parquet(Path(parquet_dir) / "water_quality.parquet")
    df_counts = build_wc_var_availability_df(wq_df)

    df_counts = df_counts.rename(
        columns=lambda c: f"{c}_count" if c != "STREAM_ID" else c
    )
    vaa = vaa.merge(df_counts, on="STREAM_ID", how="left")
    vaa.to_parquet(parquet_dir / "vaa.parquet")
