"""Strict, streaming CSV ingestion with an atomic PostgreSQL snapshot load."""

import csv
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

import yaml


class ValidationError(ValueError):
    """A configuration or input record does not satisfy the data contract."""


def _read_yaml(path):
    with Path(path).open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    if not isinstance(document, dict):
        raise ValidationError("Configuration must be a YAML mapping")
    return document


def read_manifest(path):
    manifest = _read_yaml(path)
    required = ("dataset", "owner", "source", "status", "csv_path", "schema_path",
                "ddl_path", "target_table")
    if any(not manifest.get(key) for key in required):
        raise ValidationError("Manifest is missing required dataset metadata")
    if manifest["status"] not in {"done", "pending", "in_progress", "blocked"}:
        raise ValidationError("Manifest status must be done, pending, in_progress, or blocked")
    if manifest.get("status_policy") != "ingest_only_when_done":
        raise ValidationError("Unsupported manifest status policy")
    if manifest.get("load_mode") != "replace_snapshot":
        raise ValidationError("Only replace_snapshot loads are supported")
    return manifest


def read_schema(path):
    schema = _read_yaml(path)
    columns = schema.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ValidationError("Schema must declare source columns")
    names = []
    for column in columns:
        if not isinstance(column, dict) or not isinstance(column.get("name"), str):
            raise ValidationError("Every source column needs a name")
        names.append(column["name"])
        if not isinstance(column.get("nullable"), bool):
            raise ValidationError("Every source column needs boolean nullability")
        dtype = column.get("type", "")
        if dtype not in {"integer", "timestamp", "text"} and not re.fullmatch(r"varchar\([1-9][0-9]*\)", str(dtype)):
            raise ValidationError(f"Unsupported column type: {dtype}")
    if len(names) != len(set(names)) or "record_id" in names:
        raise ValidationError("Source column names must be unique and exclude record_id")
    if schema.get("primary_key") != ["record_id"] or schema.get("generated_columns") != [
        {"name": "record_id", "type": "char(64)", "nullable": False,
         "strategy": "sha256_canonical_row"}
    ]:
        raise ValidationError("Schema must declare the generated record_id primary key")
    if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", schema.get("table", "")):
        raise ValidationError("Table must be a schema-qualified SQL identifier")
    return schema


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize(value, column, row_number):
    value = value.strip()
    name, dtype = column["name"], column["type"]
    prefix = f"Record {row_number}, column {name}"
    if "\x00" in value:
        raise ValidationError(f"{prefix}: NUL characters are not supported by PostgreSQL")
    if not value:
        if not column["nullable"]:
            raise ValidationError(f"{prefix}: required value is empty")
        return None
    if dtype == "integer":
        if not re.fullmatch(r"[+-]?[0-9]+", value):
            raise ValidationError(f"{prefix}: invalid integer")
        # Check length before conversion (also avoids Python's large-int limit).
        digits = value.lstrip("+-").lstrip("0") or "0"
        if len(digits) > 10:
            raise ValidationError(f"{prefix}: integer exceeds PostgreSQL INTEGER range")
        number = int(("-" if value.startswith("-") else "") + digits)
        if not -(2**31) <= number < 2**31:
            raise ValidationError(f"{prefix}: integer exceeds PostgreSQL INTEGER range")
        return str(number)
    if dtype == "timestamp":
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?", value):
            raise ValidationError(f"{prefix}: expected a timestamp without a timezone")
        try:
            return datetime.fromisoformat(value).isoformat(sep=" ")
        except ValueError:
            raise ValidationError(f"{prefix}: invalid timestamp") from None
    match = re.fullmatch(r"varchar\(([0-9]+)\)", dtype)
    if match and len(value) > int(match[1]):
        raise ValidationError(f"{prefix}: value exceeds {dtype}")
    return value


def _records(csv_path, schema):
    # Some source bodies exceed csv's default 128 KiB field limit.
    csv.field_size_limit(16 * 1024 * 1024)
    expected = [column["name"] for column in schema["columns"]]
    try:
        with Path(csv_path).open(encoding=schema.get("encoding", "utf-8-sig"), newline="") as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader, None)
            if header != expected:
                raise ValidationError("CSV header must exactly match schema column names and order")
            count = 0
            for count, row in enumerate(reader, start=1):
                if len(row) != len(expected):
                    raise ValidationError(f"Record {count}: wrong number of CSV fields")
                yield [_normalize(value, column, count)
                       for value, column in zip(row, schema["columns"])]
            if not count:
                raise ValidationError("CSV must contain at least one record; refusing an empty snapshot")
    except (csv.Error, UnicodeError):
        # Never include raw records/email contents in task logs.
        raise ValidationError("Malformed CSV quoting, encoding, or oversized field") from None


def validate_csv(csv_path, schema):
    count = sum(1 for _ in _records(csv_path, schema))
    return {"input_rows": count, "sha256": sha256_file(csv_path)}


def transform_csv(csv_path, output_path, schema):
    output_path = Path(output_path)
    if Path(csv_path).resolve() == output_path.resolve():
        raise ValidationError("Transformed output must not overwrite the input CSV")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    input_rows = 0
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="",
                                         dir=output_path.parent, delete=False) as stream:
            temporary_path = Path(stream.name)
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(["record_id"] + [column["name"] for column in schema["columns"]])
            for input_rows, values in enumerate(_records(csv_path, schema), start=1):
                canonical = json.dumps(values, ensure_ascii=False, separators=(",", ":"))
                record_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                if record_id not in seen:
                    seen.add(record_id)
                    writer.writerow([record_id] + values)
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return {"input_rows": input_rows, "output_rows": len(seen),
            "duplicates_removed": input_rows - len(seen), "sha256": sha256_file(output_path)}


def load_snapshot(connection, cleaned_path, schema, ddl_path, expected_rows):
    """Replace the table in one transaction; exceptions restore the old snapshot.

    The caller owns the connection. PostgreSQL COPY keeps large email bodies out
    of SQL literals and task logs. A count/DDL/COPY failure rolls everything back.
    """
    from psycopg2 import Error, sql

    if connection.autocommit:
        raise ValidationError("Snapshot loads require autocommit disabled")
    if not isinstance(expected_rows, int) or expected_rows < 1:
        raise ValidationError("Refusing to load an empty snapshot")
    table = sql.Identifier(*schema["table"].split("."))
    names = ["record_id"] + [column["name"] for column in schema["columns"]]
    columns = sql.SQL(", ").join(map(sql.Identifier, names))
    with Path(cleaned_path).open(encoding="utf-8", newline="") as stream:
        if next(csv.reader(stream), None) != names:
            raise ValidationError("Cleaned CSV header does not match the target schema")
        stream.seek(0)
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(Path(ddl_path).read_text(encoding="utf-8"))
                    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(table))
                    cursor.copy_expert(
                        sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')")
                        .format(table, columns).as_string(connection), stream,
                    )
                    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(table))
                    loaded = cursor.fetchone()[0]
                    if loaded != expected_rows:
                        raise ValidationError("Loaded row count does not match the transformed snapshot")
        except Error as exc:
            # COPY diagnostics may contain private message contents. The database
            # context manager has already rolled back before this error is raised.
            raise ValidationError(
                f"PostgreSQL snapshot load failed (SQLSTATE {exc.pgcode or 'unavailable'}); "
                "transaction rolled back. Check target DDL and database availability."
            ) from None
    return {"loaded_rows": loaded}
