"""Run the test suite, optionally enabling tests against the target PostgreSQL.

With --postgres, credentials are read from WAREHOUSE_DB_* environment variables
and encoded using psycopg2's DSN builder. The DSN is never logged or persisted.
Database tests create and remove their own randomly named test tables.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import unittest

from airflow_env import database_port, required


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def enable_postgres_tests() -> None:
    from psycopg2.extensions import make_dsn

    os.environ["TEST_POSTGRES_DSN"] = make_dsn(
        host=required(os.environ, "WAREHOUSE_DB_HOST"),
        port=database_port(os.environ, "WAREHOUSE_DB_PORT"),
        dbname=required(os.environ, "WAREHOUSE_DB_NAME"),
        user=required(os.environ, "WAREHOUSE_DB_USER"),
        password=required(os.environ, "WAREHOUSE_DB_PASSWORD"),
        connect_timeout=10,
        application_name="dhap34_acceptance_tests",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--postgres",
        action="store_true",
        help="Enable integration tests against the configured target database.",
    )
    args = parser.parse_args()
    if args.postgres:
        try:
            enable_postgres_tests()
        except ValueError as exc:
            parser.exit(2, f"Cannot configure PostgreSQL tests: {exc}\n")
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(PROJECT_ROOT / "dags"))
    suite = unittest.defaultTestLoader.discover(str(PROJECT_ROOT / "tests"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
