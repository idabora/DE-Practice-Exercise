import duckdb


def correct_data_types(conn, df):
    conn.sql("""ALTER TABLE electric_vehicle ALTER COLUMN "DOL Vehicle ID" TYPE BIGINT;""")
    conn.sql("""ALTER TABLE electric_vehicle ALTER COLUMN "2020 Census Tract" TYPE BIGINT;""")


def convert_into_csv(conn):
    conn.sql("SELECT * FROM electric_vehicle").write_csv("data/modified.csv")
    # conn.read_csv("data/modified.csv").show()


# 3. Calculate the following analytics.
#  - Count the number of electric cars per city.
#  - Find the top 3 most popular electric vehicles.
#  - Find the most popular electric vehicle in each postal code.
#  - Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.
def analytics_on_modified_data(conn):
    conn.sql("""SELECT City, count(*) AS "Number of cars" FROM electric_vehicle GROUP BY "City";""").show()

    conn.sql("""SELECT Make, Model, count(*) FROM electric_vehicle GROUP BY ("Make", "Model") ORDER BY count(*) DESC LIMIT 3;""").show()

    # conn.sql("""SELECT "Postal Code",Make, max(Make) AS "count_number" FROM electric_vehicle GROUP BY ("Postal Code", "Make") ORDER BY "Postal Code" DESC;""").show()

    conn.sql("""WITH ranked AS (
                SELECT
                    "Postal Code",
                    Make,
                    RANK() OVER(PARTITION BY "Postal Code" ORDER BY COUNT(*) DESC) AS rnk
                FROM electric_vehicle
                GROUP BY "Postal Code", Make
            )
            SELECT
                "Postal Code",
                Make
            FROM ranked
            WHERE rnk = 1;""").show()

    conn.sql("""COPY(SELECT "Model Year", count(*) as "Car Count" 
             from electric_vehicle GROUP BY "Model Year" ORDER BY "Car Count")
             TO 'data/my_parquet_file.parquet' (FORMAT PARQUET,OVERWRITE, PARTITION_BY 
             'Model Year')""")


def main():

    conn = duckdb.connect()
    
    conn.sql("create table electric_vehicle as select * from read_csv_auto('data/Electric_Vehicle_Population_Data.csv')")
    df = conn.sql("select * from electric_vehicle").df()

    # 1. create a DuckDB Table including DDL and correct data types that will hold the data in this CSV file.
    # - inspect data types and make DDL that makes sense. Don't just `String` everything.
    correct_data_types(conn, df)

    # 2.Read the provided `CSV` file into the table you created.
    convert_into_csv(conn)

    analytics_on_modified_data(conn)# 3.Analytics on the modifies data

if __name__ == "__main__":
    main()
