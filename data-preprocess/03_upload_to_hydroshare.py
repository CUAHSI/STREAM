#!/usr/bin/env python3

import time
from pathlib import Path
import S3hsclient as hsclient


hs = hsclient.S3HydroShare()

resource_id = input("Enter the resource ID to upload to: ")
data_directory = input("Enter the local directory containing the data to upload: ")

resource = hs.resource(resource_id)

print("This resource has the following files:")
for f in resource.s3_ls():
    print(f" - {f}")

confirm = input("Are you sure you want to upload the data to this resource? (y/n) ")
if confirm.lower() == "y":

    for f in Path(data_directory).glob("*"):
        print(f"Uploading {f.name}...", end="", flush=True)
        st = time.time()

        _ = resource._hs_session.s3.put(
            str(f), f"{resource.s3_path}{f.name}", recursive=True
        )
        print(f"done [elapsed time: {time.time() - st:.2f} seconds]")
