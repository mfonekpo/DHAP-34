from airflow.sdk import dag
from pendulum import duration
from file_check import file_check
from validation import validation
from transform import transform
from load import load

@dag(
    dag_id="email_thread_ingest",
    description="DAG to ingest email thread summary dataset",
    schedule="@once",
)
def email_thread_ingest_dag():
    """
    This DAG is responsible for ingesting the email thread summary dataset.
    It consists of two tasks: file_check and validation.
    The file_check task checks if the necessary files exist in the right location.
    The validation task validates the schema of the ingested data against the expected schema.
    """
    file_check_task = file_check()
    validation_task = validation()
    transform_task = transform()
    load_task = load()

    file_check_task >> validation_task >> transform_task >> load_task

email_thread_ingest_dag()
