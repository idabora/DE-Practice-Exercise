import gzip
import io
import os

import requests


def dowanload_gz_file(base_url, key):
    """Download the gz file from web crawler and returns the first link"""

    zip_file = requests.get(base_url + key)
    zip_file.raise_for_status()
    zip_bytes = io.BytesIO(zip_file.content)

    with gzip.open(zip_bytes, "rb") as file:
        content = file.read()

    os.makedirs("gzip_file", exist_ok=True)
    zip_folder_path = os.path.join(os.getcwd(), "gzip_file/wet.paths")

    with open(zip_folder_path, "wb") as file:
        file.write(content)

    with open(zip_folder_path, "r") as file:
        first_line = file.readline()
        print(first_line)

    return first_line.strip()


# https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/segments/1642320299852.23/wet/CC-MAIN-20220116093137-20220116123137-00000.warc.wet.gz
def common_crawl(base_url, url):
    response = requests.get(base_url+url)
    response.raise_for_status()

    file_bytes = io.BytesIO(response.content)

    with gzip.open(file_bytes, 'rt') as file:
        for line in file:
            print(line.strip()) 


def main():
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    base_url = "https://data.commoncrawl.org/"

    url = dowanload_gz_file(base_url, key)
    common_crawl(base_url, url)


if __name__ == "__main__":
    main()
