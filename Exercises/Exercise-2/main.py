import datetime
import io
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

"""Weather report URL"""
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def get_hourly_dry_bulb_temperature(link):
    response = requests.get(link)
    response.raise_for_status()

    cwd = os.getcwd()
    csv_file = os.path.join(cwd, "mycsvfile.csv")

    # csv_file = os.path.join(cwd, datetime.datetime.now())

    with open(csv_file, "wb") as file:
        file.write(response.content)

    weather_report = pd.read_csv(csv_file)
    # weather_report = pd.read_csv(link, low_memory=False)
    """Extracting and returning the highest value in HourlyDryBulbTemperature column """
    highest_temp = (
        weather_report["HourlyDryBulbTemperature"].sort_values(ascending=False).iloc[0]
    )

    return highest_temp


def get_link():
    """Extract and return the csv file number(link) from the td tag"""

    response = requests.get(url)
    response.raise_for_status()

    html_page = BeautifulSoup(response.text, "html.parser")

    for row in html_page.find_all("tr"):
        tds = row.find_all("td")
        """Skipping if some tr tag dont have any td in it"""
        if not tds:
            continue

        date = tds[1].get_text()

        if date == "2024-01-19 15:06":
            link = tds[0].find("a")["href"]
            return link
    return None


def main():
    # your code here
    csv_link = get_link()
    """Creating the downloading link"""
    csv_link = url + csv_link
    print(get_hourly_dry_bulb_temperature(csv_link))


if __name__ == "__main__":
    main()
