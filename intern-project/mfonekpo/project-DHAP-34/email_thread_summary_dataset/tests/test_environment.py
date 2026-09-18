"""Credential setup and delimiter-safe database connection tests."""

import base64
import importlib.util
import json
import stat
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))

from airflow_env import configured_environment, database_port  # noqa: E402
from init_env import create_environment  # noqa: E402


SECRET_KEYS = [
    "METADATA_DB_PASSWORD",
    "WAREHOUSE_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_WEBSERVER_SECRET_KEY",
]


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.template = self.directory / ".env.example"
        self.output = self.directory / ".env"
        self.template.write_text(
            "# Local configuration\nWAREHOUSE_DB_PORT=5432\n"
            + "".join(f"{key}=replace-me\n" for key in SECRET_KEYS),
            encoding="utf-8",
        )

    def test_generated_secrets_are_private_and_fernet_key_is_valid(self):
        create_environment(self.output, self.template)
        environment = dict(
            line.split("=", 1)
            for line in self.output.read_text(encoding="utf-8").splitlines()
            if "=" in line
        )
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o600)
        self.assertEqual(environment["WAREHOUSE_DB_PORT"], "5432")
        for key in SECRET_KEYS:
            self.assertNotEqual(environment[key], "replace-me")
            self.assertGreaterEqual(len(environment[key]), 32)
        self.assertEqual(len(base64.urlsafe_b64decode(environment["AIRFLOW_FERNET_KEY"])), 32)
        self.assertNotEqual(environment["METADATA_DB_PASSWORD"], environment["WAREHOUSE_DB_PASSWORD"])

    def test_existing_credentials_are_never_overwritten(self):
        self.output.write_text("existing credentials\n", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            create_environment(self.output, self.template)
        self.assertEqual(self.output.read_text(encoding="utf-8"), "existing credentials\n")

    def test_incomplete_template_does_not_create_credentials(self):
        self.template.write_text("WAREHOUSE_DB_PASSWORD=replace-me\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            create_environment(self.output, self.template)
        self.assertFalse(self.output.exists())

    def test_invalid_ports_are_rejected(self):
        for value in ["", "0", "65536", "abc", "5432.0"]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    database_port({"PG_PORT": value}, "PG_PORT")


@unittest.skipUnless(importlib.util.find_spec("sqlalchemy"), "SQLAlchemy is available in the Docker image")
class ConnectionEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.environment = {
            "METADATA_DB_USER": "airflow",
            "METADATA_DB_PASSWORD": "meta:@:/?#% $\\'\"",
            "METADATA_DB_HOST": "postgres-metadata",
            "METADATA_DB_PORT": "5432",
            "METADATA_DB_NAME": "airflow",
            "WAREHOUSE_DB_USER": "warehouse",
            "WAREHOUSE_DB_PASSWORD": "target:@:/?#% $\\'\"",
            "WAREHOUSE_DB_HOST": "postgres-warehouse",
            "WAREHOUSE_DB_PORT": "5432",
            "WAREHOUSE_DB_NAME": "emails",
            "UNRELATED_SETTING": "keep-me",
        }

    def test_special_characters_survive_both_connection_formats(self):
        from sqlalchemy.engine import make_url

        original = dict(self.environment)
        result = configured_environment(self.environment)
        metadata = make_url(result["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
        warehouse = json.loads(result["AIRFLOW_CONN_WAREHOUSE_POSTGRES"])
        self.assertEqual(metadata.password, self.environment["METADATA_DB_PASSWORD"])
        self.assertEqual(metadata.host, self.environment["METADATA_DB_HOST"])
        self.assertEqual(metadata.port, 5432)
        self.assertEqual(warehouse["password"], self.environment["WAREHOUSE_DB_PASSWORD"])
        self.assertEqual(warehouse["host"], self.environment["WAREHOUSE_DB_HOST"])
        self.assertEqual(warehouse["port"], 5432)
        self.assertEqual(result["UNRELATED_SETTING"], "keep-me")
        self.assertEqual(self.environment, original)

    def test_empty_password_is_rejected_without_exposing_other_secrets(self):
        self.environment["WAREHOUSE_DB_PASSWORD"] = ""
        with self.assertRaises(ValueError) as raised:
            configured_environment(self.environment)
        self.assertIn("WAREHOUSE_DB_PASSWORD", str(raised.exception))
        self.assertNotIn(self.environment["METADATA_DB_PASSWORD"], str(raised.exception))


if __name__ == "__main__":
    unittest.main()
