"""Construct connection values safely, then delegate to Airflow's entrypoint.

Passwords containing URL delimiters are encoded by SQLAlchemy for metadata and
represented as JSON for the target Airflow Connection. No secrets are printed.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping


def required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value:
        raise ValueError(f"Required environment variable {name} is empty.")
    return value


def database_port(environment: Mapping[str, str], name: str) -> int:
    try:
        port = int(required(environment, name))
    except ValueError:
        raise ValueError(f"{name} must be an integer port between 1 and 65535.") from None
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be an integer port between 1 and 65535.")
    return port


def configured_environment(environment: Mapping[str, str]) -> dict[str, str]:
    from sqlalchemy.engine import URL

    configured = dict(environment)
    metadata = URL.create(
        "postgresql+psycopg2",
        username=required(environment, "METADATA_DB_USER"),
        password=required(environment, "METADATA_DB_PASSWORD"),
        host=required(environment, "METADATA_DB_HOST"),
        port=database_port(environment, "METADATA_DB_PORT"),
        database=required(environment, "METADATA_DB_NAME"),
    )
    configured["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = metadata.render_as_string(
        hide_password=False
    )
    configured["AIRFLOW_CONN_WAREHOUSE_POSTGRES"] = json.dumps(
        {
            "conn_type": "postgres",
            "host": required(environment, "WAREHOUSE_DB_HOST"),
            "port": database_port(environment, "WAREHOUSE_DB_PORT"),
            "login": required(environment, "WAREHOUSE_DB_USER"),
            "password": required(environment, "WAREHOUSE_DB_PASSWORD"),
            "schema": required(environment, "WAREHOUSE_DB_NAME"),
            "extra": {"connect_timeout": 10, "application_name": "dhap34_email_pipeline"},
        }
    )
    return configured


def main() -> int:
    try:
        environment = configured_environment(os.environ)
    except ValueError as exc:
        print(f"Airflow environment configuration error: {exc}", file=sys.stderr)
        return 2
    arguments = sys.argv[1:] or ["airflow", "version"]
    os.execve("/entrypoint", ["/entrypoint", *arguments], environment)
    return 0  # os.execve only returns if execution fails.


if __name__ == "__main__":
    raise SystemExit(main())
