"""Exercise the real DAG, PostgreSQL reruns, validation failures, and status gate.

Run through the airflow-cli Compose service after starting the local stack.
This intentionally replaces the local target table with the configured CSV twice.
Malformed and unapproved fixtures are temporary; the source CSV is never edited.
"""

import csv
import importlib.util
import json
import os
import shutil
import tempfile
from contextlib import closing
from pathlib import Path

import pendulum
import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from email_pipeline.core import read_manifest, sha256_file


def fingerprint(table):
    with closing(PostgresHook(postgres_conn_id="warehouse_postgres").get_conn()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL(
                "SELECT COUNT(*), md5(string_agg(record_id, '' ORDER BY record_id)) FROM {}"
            ).format(sql.Identifier(*table.split("."))))
            count, digest = cursor.fetchone()
    return {"rows": count, "record_ids_md5": digest}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def run_dag(dag):
    run = dag.test(execution_date=pendulum.now("UTC"))
    states = {task.task_id: task.state for task in run.get_task_instances()}
    return run, states


def main():
    root = Path(os.environ.get("PIPELINE_PROJECT_DIR", "/opt/airflow/project"))
    manifest = read_manifest(root / "manifest.yaml")
    dag_path = Path("/opt/airflow/dags/load_email_thread_summary_to_postgres.py")
    spec = importlib.util.spec_from_file_location("acceptance_dag", dag_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = module.dag
    report = {"dag_id": dag.dag_id, "source_sha256": sha256_file(root / manifest["csv_path"])}

    for label in ("first_load", "rerun"):
        run, states = run_dag(dag)
        require(run.state == "success" and all(state == "success" for state in states.values()),
                f"{label} failed: {states}")
        report[label] = {"run_id": run.run_id, "tasks": states,
                         **fingerprint(manifest["target_table"])}
    require(report["first_load"]["rows"] > 0, "Target table is empty")
    require(report["first_load"]["rows"] == report["rerun"]["rows"] and
            report["first_load"]["record_ids_md5"] == report["rerun"]["record_ids_md5"],
            "Rerun changed the target snapshot")
    reference = manifest.get("reference_snapshot", {})
    if report["source_sha256"] == reference.get("sha256"):
        require(report["rerun"]["rows"] == reference["rows"], "Reference row count mismatch")

    with tempfile.TemporaryDirectory(prefix="dhap34-acceptance-") as directory:
        fixture = Path(directory)
        for field in ("schema_path", "ddl_path"):
            shutil.copyfile(root / manifest[field], fixture / Path(manifest[field]).name)
        fixture_manifest = {**manifest, "csv_path": "malformed.csv", "schema_path": "schema.yaml",
                            "ddl_path": "ddl.sql"}
        (fixture / "manifest.yaml").write_text(yaml.safe_dump(fixture_manifest))
        with (fixture / "malformed.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["thread_id", "subject", "timestamp", "from", "to", "body"])
            writer.writerow(["1", "Synthetic test", "not-a-timestamp", "a@example.test", "b@example.test", "Test"])
        previous_root = os.environ.get("PIPELINE_PROJECT_DIR")
        os.environ["PIPELINE_PROJECT_DIR"] = str(fixture)
        try:
            run, states = run_dag(dag)
            require(run.state == "failed" and states["validate_schema"] == "failed" and
                    states["load_to_postgres"] == "upstream_failed",
                    f"Malformed CSV did not fail at validation: {states}")
            require(fingerprint(manifest["target_table"])["record_ids_md5"] == report["rerun"]["record_ids_md5"],
                    "Malformed CSV changed the target")
            report["malformed_csv"] = {"run_id": run.run_id, "tasks": states, "target_unchanged": True}
            fixture_manifest["status"] = "pending"
            (fixture / "manifest.yaml").write_text(yaml.safe_dump(fixture_manifest))
            (fixture / "malformed.csv").unlink()
            run, states = run_dag(dag)
            require(all(state == "skipped" for state in states.values()),
                    f"Unapproved dataset was not skipped: {states}")
            require(fingerprint(manifest["target_table"])["record_ids_md5"] == report["rerun"]["record_ids_md5"],
                    "Unapproved dataset changed the target")
            report["pending_dataset"] = {"run_id": run.run_id, "tasks": states, "target_unchanged": True}
        finally:
            if previous_root is None:
                os.environ.pop("PIPELINE_PROJECT_DIR", None)
            else:
                os.environ["PIPELINE_PROJECT_DIR"] = previous_root

    destination = Path(os.environ.get("PIPELINE_WORK_DIR", "/opt/airflow/work")) / "acceptance-report.json"
    destination.write_text(json.dumps(report, indent=2) + "\n")
    print("ACCEPTANCE PASSED")
    print(json.dumps(report, indent=2))
    print(f"Report saved to {destination}")


if __name__ == "__main__":
    main()
