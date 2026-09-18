"""Airflow DAG import, task dependency, and dataset approval contracts."""

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "dags"))
import email_pipeline  # noqa: E402

DAG_DIRECTORY = Path(email_pipeline.__file__).resolve().parents[1]
DAG_FILE = DAG_DIRECTORY / "load_email_thread_summary_to_postgres.py"
DAG_ID = "load_email_thread_summary_to_postgres"


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Airflow is available in the Docker image")
class DagContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        specification = importlib.util.spec_from_file_location("dhap34_test_dag", DAG_FILE)
        cls.module = importlib.util.module_from_spec(specification)
        # A DAG must remain importable even before its source/config files exist.
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.dict(os.environ, {"PIPELINE_PROJECT_DIR": temporary}):
                specification.loader.exec_module(cls.module)

    def test_dag_bag_imports_cleanly_with_expected_safe_schedule(self):
        from airflow.models import DagBag

        bag = DagBag(
            dag_folder=str(DAG_FILE),
            include_examples=False,
            read_dags_from_db=False,
            safe_mode=False,
        )
        self.assertEqual(bag.import_errors, {})
        self.assertIn(DAG_ID, bag.dags)
        dag = bag.dags[DAG_ID]
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertIsNone(dag.schedule_interval)
        graph = {
            "check_dataset_status": {"read_csv"},
            "read_csv": {"validate_schema"},
            "validate_schema": {"transform"},
            "transform": {"load_to_postgres"},
            "load_to_postgres": set(),
        }
        self.assertEqual(set(dag.task_ids), set(graph))
        for task_id, downstream in graph.items():
            self.assertEqual(dag.get_task(task_id).downstream_task_ids, downstream)

    def check_status(self, status):
        manifest = yaml.safe_load((PROJECT / "manifest.yaml").read_text(encoding="utf-8"))
        manifest["status"] = status
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")
            with mock.patch.dict(os.environ, {"PIPELINE_PROJECT_DIR": temporary}):
                with mock.patch("email_pipeline.core.validate_csv", side_effect=AssertionError("Status check read CSV")):
                    with mock.patch("email_pipeline.core.load_snapshot", side_effect=AssertionError("Status check loaded database")):
                        return self.module.check_dataset_status()

    def test_unapproved_status_skips_without_a_csv_or_database(self):
        from airflow.exceptions import AirflowSkipException

        for status in ["pending", "in_progress", "blocked"]:
            with self.subTest(status=status):
                with self.assertRaises(AirflowSkipException):
                    self.check_status(status)

    def test_done_status_allows_the_pipeline_to_continue(self):
        self.assertEqual(self.check_status("done")["status"], "done")


if __name__ == "__main__":
    unittest.main()
