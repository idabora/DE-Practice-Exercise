import gzip
import io

import boto3
import requests


def print_common_crawl_data(uri):
    response = requests.get(uri)
    response.raise_for_status()


def read_gz_file(bucket, key):

    s3_client = boto3.client("s3")
    print("HERE")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content_gzip = response["Body"].read()

    fileobj = io.BytesIO(file_content_gzip)

    with gzip.GzipFile(fileobj, mode="rb") as f:
        first_link = f.readline().decode("utf-8")
        return first_link


def main():
    # your code here
    bucket = "commoncrawl"
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    base_url = "https://data.commoncrawl.org/"
    first_link = read_gz_file(bucket, key)
    uri = base_url + first_link
    print(uri)
    # print_common_crawl_data(uri)


if __name__ == "__main__":
    main()
