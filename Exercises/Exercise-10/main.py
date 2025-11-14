import great_expectations as ge
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp, unix_timestamp
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Create a SparkSession
spark = SparkSession.builder.appName("BikeRideDuration").getOrCreate()

# Define the schema based on the provided CSV structure
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

input_csv_path = "data/202306-divvy-tripdata.csv"

df = spark.read.csv(
    input_csv_path,
    header=True,
    schema=schema,
    mode="DROPMALFORMED"
)

df = df.withColumn(
    "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
)

df = df.withColumn(
    "duration_seconds",
    unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
)

df = df.withColumn(
    "date", date_format(col("started_at"), "yyyy-MM-dd")
)

daily_durations = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)

ge_df = SparkDFDataset(daily_durations)
ge_df.expect_column_values_not_to_be_null("started_at")
ge_df.expect_column_values_not_to_be_null("ended_at")
ge_df.expect_column_values_to_be_between("duration_seconds",min_value=1,max_value=86400)

# validation = ge_df.validate()

if not ge_df.validate()["success"]:
    raise Exception("Data quality failed!")

output_parquet_path = "results/output_file.csv"
daily_durations.write.mode("overwrite").csv(output_parquet_path)


