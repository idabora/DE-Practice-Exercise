import os

import pandas as pd
import psycopg2

sql_file_path = os.path.join(os.getcwd(), 'create_tables.sql')
files_path = os.path.join(os.getcwd(), 'data')


def create_table(cur, conn, path):
    """Create sql script to CREATE table, add PRIMARY KEY , FOREIGN KEY and INDEXES"""

    with open('create_tables.sql', 'r') as file:
        commands = file.read()

    statements = [stmt.strip() for stmt in commands.split(';') if stmt.strip()]
    for stmt in statements:
        cur.execute(stmt)
    
    conn.commit()


def insert_data(cur, conn):
    """INSERT ing data into SQL tables"""
    for file in os.listdir(files_path):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(files_path, file))
            table_name = file.split(".")[0]
            
            # Generate insert query dynamically
            cols = ','.join(df.columns)
            vals_template = ','.join(['%s'] * len(df.columns))
            query = f"INSERT INTO {table_name} ({cols}) VALUES ({vals_template})"
            
            cur.executemany(query, df.values.tolist())
            conn.commit()


def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    """For data type of each column of the datble"""
    dtype_mapping = {
        "int64": "INTEGER",
        "object": "VARCHAR(100)",
    }

    with open(sql_file_path, "w") as f:
        for file in os.listdir(files_path):

            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join(files_path, file))
                table_name = file.split(".")[0]

                cols = []
                for col, dtype in df.dtypes.items():
                    sql_type = dtype_mapping.get(str(dtype), "VARCHAR(100)")
                    cols.append(f"{col} {sql_type}")

                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)});\n"
                f.write(create_table_query)
        """Adding sql commands into sql script file"""
        # 2. Add primary keys
        f.write("ALTER TABLE accounts ADD PRIMARY KEY (customer_id);\n")
        f.write("ALTER TABLE products ADD PRIMARY KEY (product_id);\n")
        f.write("ALTER TABLE transactions ADD PRIMARY KEY (transaction_id);\n")

        # 3. Add foreign keys
        f.write("ALTER TABLE transactions ADD FOREIGN KEY (account_id) REFERENCES accounts(customer_id);\n")
        f.write("ALTER TABLE transactions ADD FOREIGN KEY (product_id) REFERENCES products(product_id);\n")

        # 4. Create indexes
        f.write("CREATE INDEX first_name_idx ON accounts(first_name);\n")
        f.write("CREATE INDEX product_code_idx ON products(product_code);\n")

    create_table(cur, conn, sql_file_path)
    insert_data(cur, conn)


if __name__ == "__main__":
    #sql script creation
    main()
