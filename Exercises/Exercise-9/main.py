import polars as pl


def rides_per_day(df):

    df = df.with_columns([
        pl.col("started_at").dt.date().alias("date")
    ])

    result_df = df.group_by("date").agg([
                    pl.count().alias("ride_counts")
                ]).sort("date")

    print("Number bike rides per day-", result_df)
    return result_df


# Calculate the average, max, and minimum number of rides per week of the dataset.
def avg_min_max(df):
    df = df.with_columns([
        pl.col("date").dt.week().alias("week")
    ]).sort("date")

    df = df.group_by("week").agg([
                pl.mean("ride_counts").alias("Avg"),
                pl.min("ride_counts").alias("min"),
                pl.max("ride_counts").alias("Max")
        ]).sort("week")

    print("The average, max, and minimum number of rides per week of the dataset-",df)


# For each day, calculate how many rides that day is above or below the same day last week.
def above_below_count(df):
    df = df.with_columns([
        pl.col("ride_counts").shift(7).alias("last_week_counts")
    ])

    df = df.with_columns([
        (pl.col("ride_counts").cast(pl.Int32) - pl.col("last_week_counts").cast(pl.Int32)).alias("diff")
    ])
    print("How many rides that day is above or below the same day last week", df)


def main():
    df = pl.read_csv(
        "./data/202306-divvy-tripdata.csv",
        infer_schema=True,
        infer_schema_length=1000,
        try_parse_dates=True
    )
    df = rides_per_day(df)

    # Calculate the average, max, and minimum number of rides per week of the dataset.
    avg_min_max(df)

    # For each day, calculate how many rides that day is above or below the same day last week.
    above_below_count(df)

if __name__ == "__main__":
    main()
