import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
]

cwd = os.getcwd()
download_path = os.path.join(cwd, "downloads")

os.makedirs(download_path, exist_ok=True)


def fetchuri(uri):
    """Fetching the zip files"""
    response = requests.get(uri)
    response.raise_for_status()
    """Saving it into the memory, not downloading it"""
    zip_bytes = io.BytesIO(response.content)
    extract_csv(zip_bytes)


def extract_csv(zip_bytes):
    """Opening zipfile in reading mode"""
    with zipfile.ZipFile(zip_bytes) as file:
        # print(file.namelist())

        for file_name in file.namelist():
            """Only downloading the root folder csv file"""
            if file_name.endswith("/") or "._" in file_name:
                continue

            if ".csv" in file_name:
                file_bytes = file.read(file_name)
                file_name = os.path.join(download_path, os.path.basename(file_name))

                with open(file_name, "wb") as save_file:
                    save_file.write(file_bytes)
            else:
                print("No csv found in the ZIP")


def main():
    # your code here
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = [executor.submit(fetchuri, uri) for uri in download_uris]

        for future in as_completed(results):
            try:
                future.result()  # raises exception if fetchuri fails
            except Exception as e:
                print(f"[ERROR] Thread error: {e}")


if __name__ == "__main__":
    main()
