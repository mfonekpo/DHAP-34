"""Create private local credentials using only the Python standard library.

Run from any directory with ``python3 path/to/scripts/init_env.py``. Existing
credentials are never replaced, because database volumes retain their passwords.
"""

from __future__ import annotations

import argparse
import base64
import os
from pathlib import Path
import secrets


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def create_environment(destination: Path, template: Path) -> None:
    """Atomically create a mode-0600 dotenv file, refusing any existing path."""
    replacements = {
        "METADATA_DB_PASSWORD": secrets.token_urlsafe(32),
        "WAREHOUSE_DB_PASSWORD": secrets.token_urlsafe(32),
        "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii"),
        "AIRFLOW_WEBSERVER_SECRET_KEY": secrets.token_urlsafe(48),
    }
    lines = []
    found = set()
    for line in template.read_text(encoding="utf-8").splitlines():
        key, separator, _ = line.partition("=")
        if separator and key in replacements:
            line = f"{key}={replacements[key]}"
            found.add(key)
        lines.append(line)
    if found != replacements.keys():
        raise ValueError("The .env.example template is missing required secret placeholders.")
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / ".env")
    args = parser.parse_args()
    try:
        create_environment(args.output, PROJECT_ROOT / ".env.example")
    except FileExistsError:
        parser.exit(1, f"Refusing to overwrite existing credentials: {args.output}\n")
    except (OSError, ValueError) as exc:
        parser.exit(1, f"Cannot create local credentials: {exc}\n")
    print(f"Created {args.output} with private, randomly generated credentials.")
    print("Read AIRFLOW_ADMIN_USERNAME and AIRFLOW_ADMIN_PASSWORD in that file to sign in.")
    print("Start the stack from the dataset folder: docker compose up --build -d")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
