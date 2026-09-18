from airflow.sdk import task
from pathlib import Path
import pendulum
from pendulum import duration

@task(
    retries=2,
    retry_delay=pendulum.duration(seconds=10),
)
def file_check():
    csv_path = Path("/opt/airflow/sample_data")
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} -> directory dos not exist")
    if not any(csv_path.glob("*email_thread_details.csv")):
        raise FileNotFoundError(f"{csv_path} -> no CSV files found")
    else:
        print("file and path check successful. Path exist...")
