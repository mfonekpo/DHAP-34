from airflow.sdk import task
from pendulum import duration
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import os


load_dotenv()


@task(
    retries=2,
    retry_delay=duration(seconds=10),
)
def load():
    """
    Task to load cleaned email thread details into a Postgres database.

    The task checks if the cleaned CSV file exists and if the path exists.
    It then connects to Postgres, creates the table if it does not exist,
    reads the cleaned data, and inserts the data into the table.

    The task uses environment variables for the Postgres connection details.
    The task also uses a DDL file to create the table.

    The task retries twice with a 10 second delay between retries if there is an error.
    """
    DDL_FILE_PATH = "/opt/airflow/config/"
    CLEANED_CSV_FILE_PATH = "/opt/airflow/sample_data/"

    # check if cleaned csv file exists
    csv_path = Path(CLEANED_CSV_FILE_PATH)
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} -> directory dos not exist")
    if not any(csv_path.glob("*email_thread_details_clean.csv")):
        raise FileNotFoundError(f"{csv_path} -> no CSV files found")
    else:
        print("file and path check successful. Path exist...")

    # Check iff DDL file exists
    ddl_path = Path(DDL_FILE_PATH)
    if not ddl_path.exists():
        raise FileNotFoundError(f"{ddl_path} -> directory dos not exist")
    if not any(ddl_path.glob("*create_table.sql")):
        raise FileNotFoundError(f"{ddl_path} -> no DDL files found")
    else:
        print("DDL file and path check successful. Path exist...")
    
    # connect to postgres
    conn = psycopg2.connect(
        dbname=os.getenv("EXT_POSTGRES_DB"),
        user=os.getenv("EXT_POSTGRES_USER"),
        password=os.getenv("EXT_POSTGRES_PASSWORD"),
        host=os.getenv("EXT_POSTGRES_HOST"),
        port=os.getenv("EXT_POSTGRES_PORT"),
    )
    cursor = conn.cursor()

    # create table
    with open(f"{DDL_FILE_PATH}create_table.sql", "r") as f:
        sql = f.read()
        cursor.execute(sql)

    # read cleaned data
    df = pd.read_csv(f"{CLEANED_CSV_FILE_PATH}/email_thread_details_clean.csv")

    # insert data into table
    insert_sql = """
        INSERT INTO public.email_thread_details
        (thread_id, subject, "timestamp", "from", "to", body)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (thread_id, "timestamp") DO NOTHING;
    """

    records = list(
        df[[
            "thread_id",
            "subject",
            "timestamp",
            "from",
            "to",
            "body",
        ]].itertuples(index=False, name=None)
    )

    cursor.executemany(insert_sql, records)
    conn.commit()

    cursor.close()
    conn.close()