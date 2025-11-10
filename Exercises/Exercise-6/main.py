import glob
import os
import zipfile
from datetime import timedelta

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    asc,
    avg,
    col,
    desc,
    lit,
    month,
    to_date,
    trim,
    when,
    year,
)
from pyspark.sql.types import FloatType

columns = []
data = []


# What is the `average` trip duration per day?
def get_trip_avg(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    df.show(5)
    result_df = df.groupBy("date").agg(F.avg(col("tripduration")))
    result_df.coalesce(1).write.mode('overwrite')\
        .option("header", "true")\
        .csv("reports/solution1.csv")


def trips_each_day(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    result_df = df.groupBy("date").agg(F.count("trip_id").alias("trip_count"))
    result_df.coalesce(1).write.mode("overwrite")\
        .option("header", "true")\
        .csv('reports/solution2.csv')


def popular_trip_station(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    df = df.withColumn("month", month(col("date")))

    df = df.groupBy("month", "from_station_name").agg(F.count("*").alias("trip_count"))
    result_df = df.groupBy("month").agg(F.max("trip_count"))
    result_df.coalesce(1).write.mode("overwrite")\
        .option("header", "true")\
        .csv('reports/solution3.csv')


# What were the top 3 trip stations each day for the last two weeks?
def top_3_trip_stations(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    df_count = df.groupBy("date", "to_station_name") \
                 .agg(F.count("*").alias("trip_count"))
    win = Window.partitionBy("date").orderBy(F.col("trip_count").desc())
    result_df = df_count.withColumn("rank", F.row_number().over(win)) \
                        .filter(F.col("rank") <= 3).orderBy(F.col("date").desc())
    last_date = result_df.agg(F.max("date")).collect()[0][0]
    date_14_days_ago = last_date - timedelta(days=14)
    result_df = result_df.select("date", "to_station_name", "rank").filter(col("date") > date_14_days_ago)
    result_df.coalesce(1).write.mode("overwrite")\
        .option("header", "true")\
        .csv('reports/solution4.csv')


# Do `Male`s or `Female`s take longer trips on average?
def longer_trips_on_average(df):
    avg_bike_ride = df.agg(F.avg("tripduration").alias("avg_bike_ride")).collect()[0][0]
    df_filtered = df.filter( (col("gender").isNotNull()) & (trim(col("gender")) != ""))
    gender_avg_trips = df_filtered.groupBy("gender").agg(F.avg("tripduration").alias("avg_by_gender"))

    gender_avg_trips = gender_avg_trips.withColumn("Greater than avg", when(col("avg_by_gender") > lit(avg_bike_ride), "YES").otherwise("NO")).filter((col("gender").isNotNull()) & (trim(col("gender")) != ""))
    gender_avg_trips.coalesce(1).write.mode("overwrite")\
        .option("header", "true")\
        .csv('reports/solution5.csv')


def top_10_ages(df):
    df = df.withColumn("age", F.year(F.current_date()) - col("birthyear"))
    df = df.filter(col("age").cast(FloatType()).isNotNull())
    # df.select("age","tripduration").dis
    # win = df.withColumn()


def main():
    spark = SparkSession.builder.appName("Exercise6")\
            .enableHiveSupport()\
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
            .getOrCreate()

    os.makedirs('reports', exist_ok=True)
    zip_files = glob.glob("data/*.zip")
    for file in zip_files:
        with zipfile.ZipFile(file, 'r') as f:
            for name in f.namelist():
                if name.startswith("Divvy_Trips"):
                    with f.open(name) as csv_file:
                        df = pd.read_csv(csv_file)
                        df['gender'] = df['gender'].fillna('Unknown').astype(str)
                        spark_df = spark.createDataFrame(df)
                        # spark_df.show(5)
                    # What is the `average` trip duration per day?
                    get_trip_avg(spark_df)

                    # How many trips were taken each day?
                    trips_each_day(spark_df)

                    # What was the most popular starting trip station for each month?
                    popular_trip_station(spark_df)

                    # What were the top 3 trip stations each day for the last two weeks?
                    top_3_trip_stations(spark_df)

                    # Do `Male`s or `Female`s take longer trips on average?
                    longer_trips_on_average(spark_df)

                    # What is the top 10 ages of those that take the longest trips, and shortest?
                    # top_10_ages(spark_df)

if __name__ == "__main__":
    main()
    print(columns)
    print(data)
