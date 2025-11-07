import glob
import os
import zipfile

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, col, month, to_date

columns = []
data = []


# What is the `average` trip duration per day?
def get_trip_avg(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    df.show(5)
    result_df = df.groupBy("date").agg(F.avg(col("tripduration")))
    result_df.coalesce(1).write.mode('overwrite')\
        .option("header", "true")\
        .csv("reports/sol1.csv")


def trips_each_day(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    result_df = df.groupBy("date").agg(F.count("trip_id").alias("trip_count"))
    result_df.coalesce(1).write.mode("overwrite")\
        .option("header", "true")\
        .csv('reports/solution2.csv')


def popular_trip_station(df):
    df = df.withColumn("date", to_date(col("start_time"), 'yyyy-MM-dd'))
    df = df.withColumn("month", month(col("date")))

    df = df.groupBy("month","from_station_name")\
        .count("*").alias("trip_count")

    df.groupBy("months").agg(F.max("trip_count")).show(5)

    # win = Window.partitionBy("month","from_station_name")
    # df.withColumn("popular", F.count("*").over(win)).show(5)


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().config("spark.sql.legacy.timeParserPolicy", "LEGACY") .getOrCreate()
    os.makedirs('reports', exist_ok=True)
    zip_files = glob.glob("data/*.zip")
    for file in zip_files:
        with zipfile.ZipFile(file, 'r') as f:
            for name in f.namelist():
                if name.startswith("Divvy_Trips"):
                    with f.open(name) as csv_file:
                        df = pd.read_csv(csv_file)
                        spark_df = spark.createDataFrame(df)
                        # spark_df.show(5)
                    # What is the `average` trip duration per day?
                    get_trip_avg(spark_df)
                    # How many trips were taken each day?
                    trips_each_day(spark_df)
                    # What was the most popular starting trip station for each month?
                    popular_trip_station(spark_df)

if __name__ == "__main__":
    main()
    print(columns)
    print(data)
