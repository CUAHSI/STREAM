#!/usr/bin/env python3


from typing import Union

import time
import pandas
import geopandas
from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc

import warnings

warnings.simplefilter(action="ignore", category=pandas.errors.DtypeWarning)


# import utils.S3hsclient as hsclient

input_data_dir = Path("data")
output_data_dir = Path("processed-data")

# make sure output directory exists
output_data_dir.mkdir(exist_ok=True)


def create_single_parquet(
    input_data_dir: Path,
    output_filename: Path,
    date_cols: list = [],
    sort_by: Union[str, None] = None,
):
    """
    Reads all CSV files in the input directory,
    concatenates them into a single DataFrame,
    and saves it as a Parquet file.

    Arguments
    =========
    input_data_dir: str - The input directory containing the CSV files.
    output_filename: str - The filename for the output Parquet file.
    date_cols: list - A list of column names to parse as dates.
    sort_by: str or None - The column name to sort the DataFrame by before saving

    Returns
    =======
    None

    """
    dfs = []
    for f in input_data_dir.glob("*.csv"):
        df_temp = pandas.read_csv(f, parse_dates=date_cols)
        df_temp["STREAM_ID"] = f.stem
        dfs.append(df_temp)

    df = pandas.concat(dfs)

    if sort_by is not None:
        df.sort_values(by=sort_by, inplace=True)

    df.to_parquet(output_filename, index=False)


def create_hive_parquet(
    input_data_dir: Path,
    output_dir: Path,
    date_cols: list = [],
    sort_by: Union[str, None] = None,
):
    """
    Reads all CSV files in the input directory,
    concatenates them into a single DataFrame,
    and saves it as a hive-partitioned Parquet file.

    Arguments
    =========
    input_data_dir: str - The input directory containing the CSV files.
    output_filename: str - The filename for the output Parquet file.
    date_cols: list - A list of column names to parse as dates.
    sort_by: str or None - The column name to sort the DataFrame by before saving

    Returns
    =======
    None

    """

    print("    Loading data csv data into Pandas...", end="", flush=True)
    st = time.time()
    dfs = []
    for f in input_data_dir.glob("*.csv"):
        df_temp = pandas.read_csv(f, parse_dates=date_cols)
        df_temp["STREAM_ID"] = f.stem
        if sort_by is not None:
            df_temp.sort_values(by=sort_by, inplace=True)
        dfs.append(df_temp)

    df = pandas.concat(dfs)

    #    df.to_parquet("hive_temp.parquet")
    print(f"done [elapsed time: {time.time() - st:.2f} seconds]")

    # convert the pandas dataframe to a pyarrow table
    print("    Converting DataFrame into PyArrow table...", end="", flush=True)
    st = time.time()
    # table = pq.read_table("hive_temp.parquet")
    table = pa.Table.from_pandas(df)
    print(f"done [elapsed time: {time.time() - st:.2f} seconds]")

    table = table.set_column(
        table.schema.get_field_index("STREAM_ID"),
        "STREAM_ID",
        pc.cast(table["STREAM_ID"], pa.large_string()),
    )

    print("    Writing PyArrow table to Parquet files...", end="", flush=True)
    st = time.time()

    parquet_format = ds.ParquetFileFormat()

    partitioning = ds.partitioning(
        pa.schema(
            [
                ("STREAM_ID", pa.large_string()),
            ]
        ),
        flavor="hive",  # This creates STREAM_ID=X/ paths
    )

    ds.write_dataset(
        table,
        base_dir=output_dir,
        format=parquet_format,
        partitioning=partitioning,
        existing_data_behavior="overwrite_or_ignore",
        max_rows_per_file=5_000_000,
        max_rows_per_group=250_000,
        use_threads=False,
        max_open_files=32,
    )
    print(f"done [elapsed time: {time.time() - st:.2f} seconds]")


# process metadata file. we will create a parquet file for the
# metadata and also a shapefile for the gauge locations.
# the shapefile will be used in the dashboard to display
# the gauge locations on a map.

print("Processing metadata...", end="", flush=True)
df = pandas.read_csv(f"{input_data_dir}/01_metadata.csv")
outfile = output_data_dir / "metadata.parquet"
df.to_parquet(outfile, index=False)

gdf = geopandas.GeoDataFrame(
    df,
    geometry=geopandas.points_from_xy(df["longitude_wgs84"], df["latitude_wgs84"]),
    crs="EPSG:4326",  # WGS 84
)

gdf = gdf.rename(
    columns={
        "latitude_wgs84": "latitude",
        "longitude_wgs84": "longitude",
        "drainagearea_sqkm": "drain_sqkm",
    }
)
gdf.to_file(f"{output_data_dir}/gauges.shp")
print("done")

# process land use land cover data and dynamic_antropogenic data.
# These both have a date columnm named "year"
print("Processing land use land cover data...", end="", flush=True)
create_single_parquet(
    input_data_dir / "07_lulc", output_data_dir / "lulc.parquet", date_cols=["year"]
)
print("done")

print("Processing dynamic anthropogenic data...", end="", flush=True)
create_single_parquet(
    input_data_dir / "08_dynamic_anthropogenic",
    output_data_dir / "dynamic_antropogenic.parquet",
    date_cols=["year"],
)
print("done")


# process water quality data. This has a date column named
# "DateTime" and we will sort the data by this column before saving.
print("Processing water quality data...", end="", flush=True)
create_single_parquet(
    input_data_dir / "02_waterquality_timeseries",
    output_data_dir / "water_quality.parquet",
    date_cols=["DateTime"],
    sort_by="DateTime",
)
print("done")


# process streamflow discharge data. This has a date column named "DateTime" and we will sort
print("Processing streamflow discharge data...", end="", flush=True)
create_single_parquet(
    input_data_dir / "09_streamflow_discharge",
    output_data_dir / "streamflow.parquet",
    date_cols=["DateTime"],
    sort_by="DateTime",
)
print("done")

# process grab sample data. This has a date column named "DateTime" and we will sort
print("Processing grab sample data...", end="", flush=True)
create_single_parquet(
    input_data_dir / "10_grab_samples",
    output_data_dir / "grab_samples.parquet",
    date_cols=["DateTime"],
    sort_by="DateTime",
)
print("done")

# process meteorology data. This has a date column named "time" and we will sort
print("Processing meteorology data...", end="", flush=True)
create_hive_parquet(
    input_data_dir / "05_dynamic_historical_meteorology",
    output_data_dir / "dynamic_historical_meteorology",
    date_cols=["time"],
    sort_by="time",
)
print("done")
