"""Contract tests for CSV validation and lossless, deterministic cleaning.

Run from the dataset directory with ``python -m unittest discover -s tests -v``.
These tests do not require Airflow, Docker, or a database.
"""

import csv
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "dags"))

from email_pipeline.core import (  # noqa: E402
    ValidationError,
    read_manifest,
    read_schema,
    transform_csv,
    validate_csv,
)


HEADER = ["thread_id", "subject", "timestamp", "from", "to", "body"]
BASE_ROW = [
    "42",
    "Quarterly report",
    "2026-07-06 08:09:10",
    "sender@example.test",
    "recipient@example.test",
    "Please review the attached report.",
]


class CsvContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.source = self.directory / "input.csv"
        self.cleaned = self.directory / "cleaned.csv"
        self.schema = read_schema(PROJECT / "schema.yaml")

    def write_csv(self, rows, header=HEADER):
        with self.source.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)
        return self.source

    def read_cleaned(self):
        with self.cleaned.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def assert_invalid_row(self, column, value):
        row = BASE_ROW.copy()
        row[HEADER.index(column)] = value
        self.write_csv([row])
        with self.assertRaises(ValidationError):
            validate_csv(self.source, self.schema)

    def test_valid_input_reports_number_of_source_records(self):
        second = BASE_ROW.copy()
        second[0] = "43"
        self.write_csv([BASE_ROW, second])
        self.assertEqual(validate_csv(self.source, self.schema)["input_rows"], 2)

    def test_utf8_bom_is_accepted_without_changing_the_header(self):
        self.write_csv([BASE_ROW])
        self.source.write_bytes(b"\xef\xbb\xbf" + self.source.read_bytes())
        self.assertEqual(validate_csv(self.source, self.schema)["input_rows"], 1)
        transform_csv(self.source, self.cleaned, self.schema)
        self.assertEqual(self.read_cleaned()[0]["body"], BASE_ROW[-1])

    def test_missing_extra_duplicate_and_reordered_headers_fail(self):
        headers = [
            HEADER[:-1],
            HEADER + ["unexpected"],
            HEADER[:-1] + ["to"],
            [HEADER[1], HEADER[0]] + HEADER[2:],
        ]
        for header in headers:
            with self.subTest(header=header):
                self.write_csv([BASE_ROW], header=header)
                with self.assertRaises(ValidationError):
                    validate_csv(self.source, self.schema)

    def test_missing_or_extra_fields_in_a_record_fail(self):
        for row in [BASE_ROW[:-1], BASE_ROW + ["unexpected"]]:
            with self.subTest(row=row):
                self.write_csv([row])
                with self.assertRaises(ValidationError):
                    validate_csv(self.source, self.schema)

    def test_invalid_and_out_of_range_integers_fail(self):
        for value in ["abc", "1.5", "1e3", "2147483648", "-2147483649"]:
            with self.subTest(value=value):
                self.assert_invalid_row("thread_id", value)

    def test_postgres_integer_boundaries_are_accepted(self):
        for value in ["-2147483648", "2147483647"]:
            with self.subTest(value=value):
                row = BASE_ROW.copy()
                row[0] = value
                self.write_csv([row])
                self.assertEqual(validate_csv(self.source, self.schema)["input_rows"], 1)

    def test_empty_required_values_fail_after_trimming(self):
        for column in ["thread_id", "timestamp", "from", "to"]:
            for value in ["", "  \t "]:
                with self.subTest(column=column, value=value):
                    self.assert_invalid_row(column, value)

    def test_varchar_limits_are_checked_before_loading(self):
        for column in ["subject", "from"]:
            with self.subTest(column=column):
                self.assert_invalid_row(column, "x" * 256)

    def test_postgres_incompatible_nul_is_rejected(self):
        for column in ["subject", "from", "to", "body"]:
            with self.subTest(column=column):
                self.assert_invalid_row(column, "before\x00after")

    def test_validation_checks_beyond_the_first_fifty_records(self):
        invalid = BASE_ROW.copy()
        invalid[0] = "invalid"
        self.write_csv([BASE_ROW] * 75 + [invalid])
        with self.assertRaises(ValidationError):
            validate_csv(self.source, self.schema)

    def test_header_only_input_cannot_erase_the_existing_snapshot(self):
        self.write_csv([])
        with self.assertRaises(ValidationError):
            validate_csv(self.source, self.schema)

    def test_transform_refuses_to_overwrite_the_source(self):
        self.write_csv([BASE_ROW])
        original = self.source.read_bytes()
        with self.assertRaises(ValidationError):
            transform_csv(self.source, self.source, self.schema)
        self.assertEqual(self.source.read_bytes(), original)

    def test_invalid_dates_and_timezone_aware_timestamps_fail(self):
        for value in [
            "not-a-date",
            "2026-02-30 12:00:00",
            "2026-07-06 25:00:00",
            "2026-07-06T08:09:10+01:00",
            "2026-07-06T08:09:10Z",
        ]:
            with self.subTest(value=value):
                self.assert_invalid_row("timestamp", value)

    def test_unterminated_quoted_csv_record_fails(self):
        self.source.write_text(
            ",".join(HEADER) + '\n42,subject,2026-07-06 08:09:10,a,b,"unterminated\n',
            encoding="utf-8",
        )
        with self.assertRaises(ValidationError):
            validate_csv(self.source, self.schema)

    def test_long_multiline_unicode_body_survives_csv_round_trip(self):
        body = 'Résumé — "approved",\n' + ("x" * 250_000) + "\nFinal paragraph."
        row = BASE_ROW.copy()
        row[-1] = body
        self.write_csv([row])
        self.assertEqual(validate_csv(self.source, self.schema)["input_rows"], 1)
        result = transform_csv(self.source, self.cleaned, self.schema)
        self.assertEqual(result["output_rows"], 1)
        self.assertEqual(self.read_cleaned()[0]["body"], body)

    def test_cleaning_normalizes_values_and_nullable_blanks(self):
        row = [
            " 0042 ",
            " \t ",
            " 2026-07-06T08:09:10 ",
            " sender@example.test ",
            " recipient@example.test ",
            " ",
        ]
        self.write_csv([row])
        transform_csv(self.source, self.cleaned, self.schema)
        actual = self.read_cleaned()[0]
        self.assertEqual(list(actual), ["record_id"] + HEADER)
        self.assertEqual(actual["thread_id"], "42")
        self.assertEqual(actual["timestamp"], "2026-07-06 08:09:10")
        self.assertEqual(actual["from"], "sender@example.test")
        self.assertEqual(actual["to"], "recipient@example.test")
        self.assertEqual(actual["subject"], "")
        self.assertEqual(actual["body"], "")
        self.assertRegex(actual["record_id"], r"^[0-9a-f]{64}$")

    def test_literal_na_and_null_are_text_not_missing_values(self):
        for value in ["NA", "N/A", "NULL", "null", "NaN"]:
            with self.subTest(value=value):
                row = BASE_ROW.copy()
                row[1] = value
                row[-1] = value
                self.write_csv([row])
                transform_csv(self.source, self.cleaned, self.schema)
                actual = self.read_cleaned()[0]
                self.assertEqual(actual["subject"], value)
                self.assertEqual(actual["body"], value)

    def test_duplicate_normalized_records_are_removed(self):
        equivalent = [f" {value} " for value in BASE_ROW]
        equivalent[0] = "00042"
        equivalent[2] = "2026-07-06T08:09:10"
        self.write_csv([BASE_ROW, BASE_ROW, equivalent])
        result = transform_csv(self.source, self.cleaned, self.schema)
        self.assertEqual(result["input_rows"], 3)
        self.assertEqual(result["output_rows"], 1)
        self.assertEqual(result["duplicates_removed"], 2)
        self.assertEqual(len(self.read_cleaned()), 1)

    def test_distinct_messages_in_same_thread_and_second_are_preserved(self):
        second = BASE_ROW.copy()
        second[-1] = "A separate message with the same timestamp."
        self.write_csv([BASE_ROW, second])
        result = transform_csv(self.source, self.cleaned, self.schema)
        records = self.read_cleaned()
        self.assertEqual(result["output_rows"], 2)
        self.assertEqual(result["duplicates_removed"], 0)
        self.assertEqual(len({row["record_id"] for row in records}), 2)

    def test_record_ids_are_independent_of_source_order(self):
        second = BASE_ROW.copy()
        second[-1] = "Different body"
        self.write_csv([BASE_ROW, second])
        transform_csv(self.source, self.cleaned, self.schema)
        original_ids = {row["body"]: row["record_id"] for row in self.read_cleaned()}
        self.write_csv([second, BASE_ROW])
        transform_csv(self.source, self.cleaned, self.schema)
        reordered_ids = {row["body"]: row["record_id"] for row in self.read_cleaned()}
        self.assertEqual(original_ids, reordered_ids)

    def test_digest_describes_the_written_artifact(self):
        self.write_csv([BASE_ROW])
        result = transform_csv(self.source, self.cleaned, self.schema)
        self.assertEqual(result["sha256"], hashlib.sha256(self.cleaned.read_bytes()).hexdigest())

    def test_invalid_input_cannot_leave_a_loadable_partial_artifact(self):
        invalid = BASE_ROW.copy()
        invalid[0] = "invalid"
        self.write_csv([BASE_ROW, invalid])
        with self.assertRaises(ValidationError):
            transform_csv(self.source, self.cleaned, self.schema)
        self.assertFalse(self.cleaned.exists())


class MetadataContractTests(unittest.TestCase):
    def test_committed_schema_matches_the_actual_source_columns(self):
        schema = read_schema(PROJECT / "schema.yaml")
        self.assertEqual([column["name"] for column in schema["columns"]], HEADER)
        self.assertEqual(schema["primary_key"], ["record_id"])

    def test_committed_manifest_marks_dataset_as_validated(self):
        manifest = read_manifest(PROJECT / "manifest.yaml")
        self.assertEqual(manifest["status"], "done")

    def test_manifest_rejects_unknown_status(self):
        import yaml

        manifest = read_manifest(PROJECT / "manifest.yaml")
        manifest["status"] = "ready-ish"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.yaml"
            path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
            with self.assertRaises(ValidationError):
                read_manifest(path)


if __name__ == "__main__":
    unittest.main()
