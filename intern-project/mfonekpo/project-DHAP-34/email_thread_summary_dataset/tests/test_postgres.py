"""Real PostgreSQL acceptance tests, enabled by TEST_POSTGRES_DSN.

The tests create and drop only randomly named tables. Use a disposable test
database. The DSN is never written or printed by this suite.
"""

import copy
import csv
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "dags"))

from email_pipeline.core import (  # noqa: E402
    ValidationError,
    load_snapshot,
    read_schema,
    transform_csv,
)


@unittest.skipUnless(os.environ.get("TEST_POSTGRES_DSN"), "TEST_POSTGRES_DSN is not configured")
class PostgresAcceptanceTests(unittest.TestCase):
    def setUp(self):
        import psycopg2
        from psycopg2 import sql

        self.psycopg2 = psycopg2
        self.sql = sql
        self.dsn = os.environ["TEST_POSTGRES_DSN"]
        self.table_name = "dhap34_test_" + uuid.uuid4().hex
        self.table_identifier = sql.Identifier("public", self.table_name)
        self.schema = copy.deepcopy(read_schema(PROJECT / "schema.yaml"))
        self.schema["table"] = "public." + self.table_name
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(self.drop_table)
        self.directory = Path(self.temporary.name)
        self.ddl = self.directory / "ddl.sql"
        self.ddl.write_text(
            f'CREATE TABLE IF NOT EXISTS public."{self.table_name}" ('
            'record_id char(64) PRIMARY KEY, '
            'thread_id integer NOT NULL, subject text, '
            '"timestamp" timestamp NOT NULL, "from" text NOT NULL, '
            '"to" text NOT NULL, body text);',
            encoding="utf-8",
        )
        self.source = self.directory / "input.csv"
        self.cleaned = self.directory / "cleaned.csv"
        with self.source.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([column["name"] for column in self.schema["columns"]])
            writer.writerows(
                [
                    ["42", "", "2026-07-06 08:09:10", "a@example.test", "b@example.test", 'First, "quoted" message\nSecond line'],
                    ["42", "NULL", "2026-07-06 08:09:10", "a@example.test", "b@example.test", "Separate message"],
                ]
            )
        transform_csv(self.source, self.cleaned, self.schema)

    def drop_table(self):
        connection = self.psycopg2.connect(self.dsn)
        try:
            with connection.cursor() as cursor:
                cursor.execute(self.sql.SQL("DROP TABLE IF EXISTS {}").format(self.table_identifier))
            connection.commit()
        finally:
            connection.close()

    def load(self, expected_rows=2):
        connection = self.psycopg2.connect(self.dsn)
        try:
            return load_snapshot(connection, self.cleaned, self.schema, self.ddl, expected_rows)
        finally:
            connection.close()

    def records(self):
        connection = self.psycopg2.connect(self.dsn)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    self.sql.SQL('SELECT record_id, thread_id, subject, "timestamp", "from", "to", body FROM {} ORDER BY record_id').format(self.table_identifier)
                )
                return cursor.fetchall()
        finally:
            connection.close()

    def test_rerun_is_idempotent_and_preserves_distinct_messages(self):
        self.load()
        first = self.records()
        self.load()
        self.assertEqual(self.records(), first)
        self.assertEqual(len(first), 2)
        self.assertEqual(len({row[0] for row in first}), 2)
        self.assertEqual({row[2] for row in first}, {None, "NULL"})
        self.assertIn('First, "quoted" message\nSecond line', {row[-1] for row in first})

    def test_count_mismatch_rolls_back_to_previous_snapshot(self):
        self.load()
        before = self.records()
        with self.assertRaises(ValidationError):
            self.load(expected_rows=999)
        self.assertEqual(self.records(), before)

    def test_copy_failure_rolls_back_truncate(self):
        self.load()
        before = self.records()
        with self.cleaned.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))
        rows[1][1] = "not-an-integer"
        with self.cleaned.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle).writerows(rows)
        with self.assertRaises((ValidationError, self.psycopg2.Error)):
            self.load()
        self.assertEqual(self.records(), before)


if __name__ == "__main__":
    unittest.main()
