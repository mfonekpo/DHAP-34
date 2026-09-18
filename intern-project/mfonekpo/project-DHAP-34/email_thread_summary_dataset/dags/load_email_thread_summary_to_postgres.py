"""DHAP-34: approved local CSV -> validate all rows -> clean -> PostgreSQL."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_dataset_status():
    import os
    from pathlib import Path
    from airflow.exceptions import AirflowSkipException
    from email_pipeline.core import read_manifest

    root = Path(os.environ.get("PIPELINE_PROJECT_DIR", "/opt/airflow/project"))
    manifest = read_manifest(root / "manifest.yaml")
    if manifest["status"] != "done":
        raise AirflowSkipException("Dataset is not approved: manifest status must be done")
    return manifest


def read_csv(ti, run_id, **_):
    import hashlib
    import os
    import shutil
    from pathlib import Path
    from email_pipeline.core import ValidationError, read_schema, sha256_file

    manifest = ti.xcom_pull(task_ids="check_dataset_status")
    root = Path(os.environ.get("PIPELINE_PROJECT_DIR", "/opt/airflow/project")).resolve()
    work_root = Path(os.environ.get("PIPELINE_WORK_DIR", "/opt/airflow/work"))
    # A stable, filesystem-safe directory keeps retries local to their own run.
    work = work_root / hashlib.sha256(run_id.encode()).hexdigest()
    work.mkdir(parents=True, exist_ok=True)
    artifact = {"work_dir": str(work)}
    for key, name in (("csv_path", "input.csv"), ("schema_path", "schema.yaml"), ("ddl_path", "ddl.sql")):
        source = (root / manifest[key]).resolve()
        if not source.is_relative_to(root) or not source.is_file():
            raise ValidationError(f"Manifest {key} must point to a file inside the dataset folder")
        target = work / name
        temporary = target.with_suffix(target.suffix + ".tmp")
        shutil.copyfile(source, temporary)
        temporary.replace(target)
        artifact[key] = str(target)
    schema = read_schema(artifact["schema_path"])
    if schema.get("dataset") != manifest["dataset"] or schema["table"] != manifest["target_table"]:
        raise ValidationError("Manifest and schema dataset/table declarations disagree")
    artifact["source_sha256"] = sha256_file(artifact["csv_path"])
    return artifact


def validate_schema(ti, **_):
    from email_pipeline.core import ValidationError, read_schema, validate_csv

    artifact = ti.xcom_pull(task_ids="read_csv")
    result = validate_csv(artifact["csv_path"], read_schema(artifact["schema_path"]))
    if result["sha256"] != artifact["source_sha256"]:
        raise ValidationError("Input snapshot changed during validation")
    return result


def transform(ti, **_):
    from pathlib import Path
    from email_pipeline.core import ValidationError, read_schema, sha256_file, transform_csv

    artifact = ti.xcom_pull(task_ids="read_csv")
    validated = ti.xcom_pull(task_ids="validate_schema")
    if sha256_file(artifact["csv_path"]) != validated["sha256"]:
        raise ValidationError("Input snapshot changed after validation")
    cleaned_path = Path(artifact["work_dir"]) / "cleaned.csv"
    result = transform_csv(artifact["csv_path"], cleaned_path, read_schema(artifact["schema_path"]))
    if result["input_rows"] != validated["input_rows"]:
        raise ValidationError("Transformed row count differs from validated input")
    return {**result, "cleaned_path": str(cleaned_path)}


def load_to_postgres(ti, **_):
    import logging
    from contextlib import closing
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from email_pipeline.core import ValidationError, load_snapshot, read_schema, sha256_file

    artifact = ti.xcom_pull(task_ids="read_csv")
    transformed = ti.xcom_pull(task_ids="transform")
    if sha256_file(transformed["cleaned_path"]) != transformed["sha256"]:
        raise ValidationError("Cleaned snapshot changed after transformation")
    hook = PostgresHook(postgres_conn_id="warehouse_postgres")
    with closing(hook.get_conn()) as connection:
        result = load_snapshot(connection, transformed["cleaned_path"],
                               read_schema(artifact["schema_path"]), artifact["ddl_path"],
                               transformed["output_rows"])
    logging.info("Snapshot loaded: input=%s loaded=%s duplicates_removed=%s source_sha256=%s",
                 transformed["input_rows"], result["loaded_rows"],
                 transformed["duplicates_removed"], artifact["source_sha256"])
    return {**result, "source_sha256": artifact["source_sha256"]}


with DAG(
    dag_id="load_email_thread_summary_to_postgres",
    description="Validate an approved local CSV and atomically replace its PostgreSQL snapshot",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"owner": "mfonekpo", "retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["DHAP-34", "local-csv", "postgres"],
) as dag:
    status_task = PythonOperator(task_id="check_dataset_status", python_callable=check_dataset_status)
    read_task = PythonOperator(task_id="read_csv", python_callable=read_csv)
    validate_task = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres,
                               retries=2, retry_delay=timedelta(seconds=30))
    status_task >> read_task >> validate_task >> transform_task >> load_task
