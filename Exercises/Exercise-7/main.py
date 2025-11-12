import os
import zipfile

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.window import Window


def extract_csv_file(spark):
    folder_path = os.path.join(os.getcwd(), 'data')
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv.zip'):
            with zipfile.ZipFile(folder_path+'/'+file_name, 'r') as zip_file:
                for file in zip_file.infolist():
                    if not file.filename.startswith("__"):
                        with zip_file.open(file.filename) as f:
                            data = f.read().decode('utf-8')
                        rows = [row for row in data.split("\n") if row.strip() != ""]
                        rdd = spark.sparkContext.parallelize(rows)
                        header = rdd.first()
                        columns = header.split(",")
                        data_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(","))
                        df = data_rdd.toDF(columns)
                        return df, file.filename


def add_file_name(spark, df, filename):
    os.makedirs(os.path.join(os.getcwd(), 'solutions'), exist_ok=True)
    df = df.withColumn("source_file", F.lit(filename))
    df.write.csv(os.path.join(os.getcwd(), 'solutions', 'addcolumn'), header=True, mode='overwrite')
    return df


# Pull the `date` located inside the string of the `source_file` column. Final data-type must be 
# `date` or `timestamp`, not a `string`. Call the new column `file_date`.
def date_in_column(spark, df):
    df = df.withColumn("file_date", F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)))
    # df.select("source_file", "file_date").show(5, truncate=False)
    print(dict(df.dtypes)['file_date'])
    return df


# Add a new column called `brand`. It will be based on the column `model`. If the
# column `model` has a space ... aka ` ` in it, split on that `space`. The value
#    found before the space ` ` will be considered the `brand`. If there is no
#    space to split on, fill in a value called `unknown` for the `brand`.
def add_brand(df):
    split_model = F.split("model", " ")
    df = df.withColumn("brand", F.when(F.size(split_model) > 1, split_model[0]).otherwise("unknown"))
    return df


# Inspect a column called `capacity_bytes`. Create a secondary DataFrame that
# relates `capacity_bytes` to the `model` column, create "buckets" / "rankings" for
#    those models with the most capacity to the least. Bring back that 
#    data as a column called `storage_ranking` into the main dataset.
def capacity_bytes(df):
    df = df.withColumn("capacity_bytes", F.col("capacity_bytes").cast(LongType()))
    secondary_df = df.select("model", "capacity_bytes")
    secondary_df = secondary_df.withColumn("capacity_bytes", F.col("capacity_bytes").cast(LongType()))
    # win = Window.partitionBy("model").orderBy("capacity_bytes")
    win = Window.orderBy(F.col("capacity_bytes").desc())
    secondary_df = secondary_df.withColumn("rank", F.dense_rank().over(win))
    secondary_df = secondary_df.orderBy(F.col("capacity_bytes").desc())
    bucket_count = secondary_df.select(F.countDistinct("rank")).collect()[0][0]
    print(bucket_count)
    secondary_df.write\
        .format("parquet")\
        .mode("overwrite")\
        .bucketBy(bucket_count, "rank")\
        .sortBy("rank")\
        .saveAsTable("secondary_table")
    
    # new_df = df.orderBy(F.col("capacity_bytes").desc()).withColumn("storage_ranking",F.lit(secondary_df.select("capacity_bytes")))
    ranked_df = (
        df.join(
            secondary_df.select("model", "capacity_bytes", "rank"),
            on=["model", "capacity_bytes"],
            how="left"
        )
    )
    ranked_df.coalesce(1).write.mode("overwrite").csv("solutions/rankColumn/rank.csv", header=True)


def primary_key(df):

    result = df.withColumn('unique_hash', F.hash('serial_number'))

    result.write.csv("solutions/hashColumn", header=True, mode="overwrite", )
    return result


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    df, filename = extract_csv_file(spark)
    df = add_file_name(spark, df, filename)
    df = date_in_column(spark, df)
    df = add_brand(df)
    # capacity_bytes(df)
    primary_key(df)


if __name__ == "__main__":
    main()
