from airflow.sdk import task
from pendulum import duration
import pandas as pd

@task(
        retries=2,
        retry_delay=duration(seconds=10),
)
def transform():
    """
    This function reads an email thread details CSV file, strips whitespace from string columns,
    and fills null values in critical columns with default values.
    """
    csv_file = "/opt/airflow/sample_data/email_thread_details.csv"
    df = pd.read_csv(csv_file)

    # Strip whitespace from string columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    
    # check for nulls in critical columns and fill with default values
    df = df.dropna(subset=['thread_id'], axis=0, how='any')  # drop rows where thread_id is null
    df['subject'].fillna('No Subject', inplace=True)
    df['body'].fillna('No Content', inplace=True)

    # dropping duplicates
    cleaned_df = df.drop_duplicates()

    # Save the transformed data back to CSV
    transformed_csv_file = "/opt/airflow/sample_data/email_thread_details_clean.csv"
    cleaned_df.to_csv(transformed_csv_file, index=False),
    print("Data transformation complete and saved to", transformed_csv_file)