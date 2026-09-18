# DHAP-34 — Local CSV to PostgreSQL

An Apache Airflow pipeline that reads the Email Thread Summary dataset downloaded
from SharePoint, validates every CSV record, cleans the values, and loads a
PostgreSQL table. Docker Compose runs Airflow 2.11.2 on Python 3.11 with
LocalExecutor and separate PostgreSQL 16 services for metadata and dataset rows.

The implementation lives in
[`intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/`](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md).
The old `extraction/` and `mfonekpo-project1/` directories have been removed from
the current checkout; their original implementation remains in Git history.

## Start here

For first-time setup, follow the [dataset runbook](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#quick-start).
It covers downloading the CSV, generating local credentials, and building the
stack using only Docker and Docker Compose.

If the dataset's `.env` and `data/email_thread_details.csv` already exist:

```bash
cd intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset
docker compose up --build -d
docker compose ps -a
```

Run Compose commands from that dataset directory. Open
[Airflow](http://localhost:8080), log in with `AIRFLOW_ADMIN_USERNAME` and
`AIRFLOW_ADMIN_PASSWORD` from its `.env`, then unpause and trigger
`load_email_thread_summary_to_postgres`. The DAG has no automatic schedule.
Initialization should exit with code 0; the other four services should be healthy.

The source CSV and `.env` are ignored by Git and are not included in a fresh
clone. Generate credentials only once; the setup script preserves existing
credentials by refusing to overwrite `.env`.

## How the pipeline works

```text
SharePoint (manual download) → local CSV
  → check_dataset_status → read_csv → validate_schema → transform → load_to_postgres
                                                                    ↓
                                                        public.email_thread_details
```

`manifest.yaml` must have `status: done` to approve ingestion. Every input record
is checked against `schema.yaml`. The loader replaces the target snapshot in one
transaction, so reruns do not add duplicates and a failed load restores the prior
snapshot. Empty or invalid files fail before replacing the table.

The corrected implementation preserves distinct messages that share a thread ID
and timestamp by deriving a SHA-256 `record_id` from all cleaned source values.

## Documentation and verification

| Need | Documentation |
| --- | --- |
| Install, start, and trigger the DAG | [Quick start](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#quick-start) |
| Check PostgreSQL row counts | [Verify the loaded data](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#verify-the-loaded-data) |
| Connect with pgAdmin | [pgAdmin connection](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#connect-with-pgadmin) |
| Understand the files and configuration | [Project files](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#project-files), [configuration](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#configuration) |
| Run tests and negative cases | [Automated acceptance checks](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#automated-acceptance-checks) |
| Update inputs, stop, or troubleshoot | [Operations and troubleshooting](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#operations-and-troubleshooting) |
| Review what changed | [Changes from the original implementation](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md#changes-from-the-original-implementation) |

The [recorded verification on 2026-09-18](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/docs/verification.md)
passed **37 tests**, loaded **21,684 records** on successive runs, preserved **55**
distinct messages that share a thread/time pair, and verified malformed-input
failure and pending-status skipping without changing the target. A separate
scheduler-triggered run also succeeded. These are recorded results for the
reference CSV; use the runbook to verify your current environment.

## Submission scope

The current remote is `mfonekpo/DHAP-34`. For the central
`Glynac-AI/airflow-dag-configs` submission, commit only the
`intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/` subtree.
Keep credentials, local CSVs, logs, and virtual environments out of the commit.
The dataset runbook maps each brief deliverable to its implementation files.
