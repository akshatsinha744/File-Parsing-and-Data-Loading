import pandas as pd
from sqlalchemy import create_engine
import psycopg2

hostname = 'localhost'
database = 'demo'
username = 'postgres'
pwd = '250857'
port_id = 5432
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()
    table_name = 'q7_csv'


    def load_to_postgresql(csv_file, columns, table_name, postgres_uri):
        data = pd.read_csv(csv_file, usecols=columns)

        engine = create_engine(postgres_uri)

        data.to_sql(table_name, engine, index=False, if_exists='replace')
        print(f"Data successfully loaded into the table '{table_name}'.")

    csv_file = r"C:\Users\Admin\Desktop\Akshat\question7_log_file.csv"

    columns = ["timestamp", "log_level"]

    postgres_uri = "postgresql://postgres:250857@localhost:5432/demo"

    load_to_postgresql(csv_file, columns, table_name, postgres_uri)

except Exception as error:
    print("Error:", error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()