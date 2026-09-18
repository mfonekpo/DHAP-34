# DHAP-34: Email Thread Summary CSV → PostgreSQL

A local Airflow 2.11.2 / Python 3.11 pipeline that validates every CSV record,
cleans the values, and atomically replaces a PostgreSQL snapshot. Airflow metadata
and the destination data live in separate PostgreSQL services.

## Quick start

Prerequisites: Docker Engine/Desktop with Docker Compose v2, an internet connection
for the first image build, and the CSV downloaded manually from the SharePoint
source recorded in [manifest.yaml](manifest.yaml). Allocate at least 4 GB RAM to
Docker; 6–8 GB and 10 GB free disk space are recommended. No host Python, Airflow,
PostgreSQL, or pgAdmin installation is required.

From this repository's root:

```bash
cd intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset
```

1. Place the downloaded file at `data/email_thread_details.csv`. This working
   copy already contains the original source at that path. CSVs are intentionally
   excluded from this submission subtree; a fresh clone needs the download.
2. Create local credentials. On Linux/macOS:

   ```bash
   docker run --rm --user "$(id -u):$(id -g)" \
     -v "$PWD:/project" -w /project python:3.11-slim \
     python scripts/init_env.py
   ```

   PowerShell with Docker Desktop:

   ```powershell
   docker run --rm -v "${PWD}:/project" -w /project python:3.11-slim python scripts/init_env.py
   ```

   If Python 3.11+ is already installed, `python3 scripts/init_env.py` does the
   same thing. The script fills the blank secret placeholders in `.env.example`,
   creates a private `.env`, and refuses to overwrite existing credentials.
   Open `.env` locally to read `AIRFLOW_ADMIN_USERNAME` and
   `AIRFLOW_ADMIN_PASSWORD`; never commit that file.
3. Build and start all services with one command:

   ```bash
   docker compose up --build -d
   docker compose ps -a
   ```

   The first image download can take several minutes. `airflow-init` must exit
   with code 0, and both database services, webserver, and scheduler should become
   healthy. Dependencies and database migrations run automatically.
4. Open [Airflow](http://localhost:8080), sign in using `.env`, find
   `load_email_thread_summary_to_postgres`, unpause it, and click **Trigger DAG**.
   The DAG runs manually (`schedule=None`, `catchup=False`).

   Equivalent CLI commands:

   ```bash
   docker compose run --rm airflow-cli airflow dags list-import-errors
   docker compose run --rm airflow-cli airflow dags unpause load_email_thread_summary_to_postgres
   docker compose run --rm airflow-cli airflow dags trigger load_email_thread_summary_to_postgres
   docker compose run --rm airflow-cli airflow dags list-runs -d load_email_thread_summary_to_postgres
   ```

   Use the `airflow-cli` service for commands: its entrypoint constructs the
   database connections from `.env`. A plain `docker compose exec ... airflow`
   bypasses that connection setup.

## Verify the loaded data

Run this command from the dataset directory; database credentials stay inside the
container. PostgreSQL's `psql` is included, so pgAdmin is optional.

```bash
docker compose exec -T postgres-target sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' <<'SQL'
SELECT COUNT(*) AS rows, COUNT(DISTINCT record_id) AS unique_records
FROM public.email_thread_details;

SELECT COUNT(*) - COUNT(DISTINCT (thread_id, "timestamp")) AS same_thread_same_time_rows
FROM public.email_thread_details;

SELECT thread_id, "timestamp", LENGTH(body) AS body_characters
FROM public.email_thread_details ORDER BY record_id LIMIT 5;
SQL
```

The supplied reference snapshot has **21,684 rows and 21,684 unique record IDs**.
It contains **55** additional distinct messages with an already-used
`(thread_id, timestamp)` pair. Trigger again and verify that the row count stays
21,684. New valid source snapshots may have different counts; the manifest's
reference checksum/count describe the original file, not a fixed ingestion limit.

For pgAdmin: host `localhost`, port `5433`, database/user/password from the
`WAREHOUSE_DB_*` entries in `.env`. Inside Compose, the host is `postgres-target`
and the port is `5432`. The Airflow Connection ID is `warehouse_postgres`; it is
supplied through the environment, so it is not listed as a saved UI connection.

## Pipeline and data contract

```text
SharePoint (manual download) → data/email_thread_details.csv
  → check_dataset_status → read_csv → validate_schema → transform → load_to_postgres
                                                                → PostgreSQL target
```

- **Status:** this CSV has no row-level status column. `manifest.yaml` uses
  `status: done` to mean the dataset is reviewed and approved for ingestion.
  `pending`, `in_progress`, and `blocked` skip all downstream tasks without
  opening the CSV or connecting to the target. Unknown statuses fail. `done` is
  an approval state, not a marker that prevents future loads. Each CSV is still
  validated on every run. The pipeline does not rewrite the read-only manifest.
- **Read:** copy the input, schema, and DDL into a run-specific work directory.
  XCom contains only paths, counts, and checksums. It never contains CSV records.
- **Validate:** check the exact six-column header/order and every record's field
  count, integer range, date/time format, string length, and nullability. Accept
  UTF-8 with optional BOM, quoted commas, and multiline bodies up to 16 MiB per
  field. Header-only files, malformed quoting, NULs, and bad values fail before
  touching the target. Errors identify record numbers/columns, without messages.
- **Transform:** strip outer whitespace, normalize integer and naive timestamp
  representations, turn empty nullable values into SQL NULL, and remove only
  identical normalized rows. Literal `NA`/`NULL` text is retained. No made-up
  subject/body values, dropped invalid records, or assumed timestamp timezone.
- **Identity:** `schema.yaml` separately declares the six source columns and
  derived `record_id CHAR(64)`. SHA-256 of their canonical JSON value array is
  the primary key. Neither `thread_id` nor `(thread_id, timestamp)` uniquely
  identifies a message. A nonunique index supports thread/time lookups.
- **Load:** `PostgresHook` obtains the connection. DDL, `TRUNCATE`, bulk `COPY`,
  and the output-count check share one transaction. A failure rolls everything
  back, including the truncate. Reruns replace the snapshot without duplicates;
  valid source edits/deletions are reflected in the next snapshot. An empty
  snapshot is rejected to prevent accidentally clearing the table.

Run directories prevent cross-run file collisions; `max_active_runs=1` serializes
scheduled/manual runs. Input and cleaned checksums detect changes between tasks.
Only the database load retries automatically (twice, 30 seconds apart). Schema
errors require correcting the source and triggering a new run.

## Automated acceptance checks

Run these after the stack becomes healthy. The first command checks parsing,
cleaning, the DAG, credential handling, and real PostgreSQL rollback/idempotency
using isolated temporary tables:

```bash
docker compose run --rm --no-deps --entrypoint python tests \
  /opt/airflow/project/scripts/run_tests.py --postgres
```

Run the complete DAG against the configured local CSV twice, then against a
temporary malformed CSV and a pending manifest:

```bash
docker compose run --rm airflow-cli python /opt/airflow/project/scripts/acceptance.py
```

This uses Airflow's `dag.test()` with the real metadata database, tasks, and
PostgresHook. It replaces the local target with the configured snapshot twice.
The malformed run must fail specifically at `validate_schema`, the pending
dataset must skip all tasks, and neither may change the successful snapshot.
The intentionally failed test run remains visible in Airflow. Run acceptance
while no other DAG runs are active. The source and manifest files are not edited.

The script prints `ACCEPTANCE PASSED` and saves an aggregate report at
`/opt/airflow/work/acceptance-report.json`. To read it:

```bash
docker compose run --rm --no-deps --entrypoint cat airflow-cli /opt/airflow/work/acceptance-report.json
```

For unit/DAG tests without starting PostgreSQL (database tests explicitly skip):

```bash
docker compose run --rm --no-deps tests
```

## Operations and troubleshooting

| Symptom | Action |
| --- | --- |
| Compose reports a missing environment variable | Run `scripts/init_env.py`; if `.env` already exists, fill the relevant value locally. |
| Port already allocated | Change `AIRFLOW_WEB_PORT` or `WAREHOUSE_HOST_PORT` in `.env`, then run `docker compose up -d`. |
| Initialization or UI failure | Inspect `docker compose logs --tail=100 airflow-init airflow-webserver airflow-scheduler`. |
| DAG missing | Run the `list-import-errors` command above; check the read-only `dags/` mount and wait for the next scheduler scan. |
| CSV missing | Place the file at the exact manifest path inside `data/`; Linux filenames are case-sensitive. |
| Validation fails | Use record/column information in the task log to fix the CSV. Do not weaken the schema to hide bad values. |
| All tasks skipped | The manifest must have `status: done` after dataset review. |
| Database authentication fails after editing `.env` | Existing PostgreSQL volumes retain their original passwords. Restore matching credentials or intentionally migrate/reset that local database. |
| SQLSTATE reported during load | Check target DDL and connectivity. Existing tables are not automatically migrated by `CREATE TABLE IF NOT EXISTS`; apply a reviewed migration when changing schema. |
| Old Airflow 3 table lacks `record_id` | Use this stack's separate target database. Its fresh volume avoids altering the old database. |
| A retry fails due to a missing run artifact | Trigger a fresh run from `read_csv` onward; clearing only load requires retained work artifacts. |

Stop while retaining databases and logs:

```bash
docker compose down
```

Work snapshots are retained for inspection and retries (about 80 MB per full run).
To discard **only** those artifacts, first wait for active runs to finish and stop
the stack. This prevents retrying old runs from the load step but preserves both
databases. The volume name below is fixed by this Compose project's name:

```bash
docker compose down
docker volume rm dhap34-mfonekpo-email_pipeline-work
docker compose up -d
```

Only for an intentional full local reset: `docker compose down -v` removes this
stack's metadata, target data, logs, and work volume. Keep `.env` and the local
CSV, then rerun `docker compose up -d`. Normal shutdown should omit `-v`.

## Deliverables and submission

| Brief requirement | Implementation |
| --- | --- |
| Dataset manifest/status/ownership | [manifest.yaml](manifest.yaml) |
| Header/types/nullability/key | [schema.yaml](schema.yaml) |
| PostgreSQL table DDL | [ddl.sql](ddl.sql) |
| Reproducible Airflow 2 environment | [docker-compose.yaml](docker-compose.yaml), [Dockerfile](Dockerfile), [requirements.txt](requirements.txt) |
| Credentials outside code | [.env.example](.env.example), [scripts/init_env.py](scripts/init_env.py), ignored `.env` |
| Validated, cleaned, repeatable ingestion | [DAG](dags/load_email_thread_summary_to_postgres.py), [pipeline](dags/email_pipeline/core.py) |
| End-to-end, rerun, malformed-input evidence | [acceptance script](scripts/acceptance.py), [tests](tests/), [verification report](docs/verification.md) |
| Teammate runbook | This README |

The maintained directory matches the required central-repository path:

```text
intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/
```

This checkout's remote is `mfonekpo/DHAP-34`, not `Glynac-AI/airflow-dag-configs`.
To complete the external submission, copy this directory into a checkout of the
central repository, make a feature branch, commit only this subtree, and open a
PR. Do not include `.env`, `data/*.csv`, old project directories, logs, or virtual
environments. The dataset is obtained through SharePoint rather than embedded in
the submission. No commit, push, or PR is performed by these scripts.

Suggested commit title: `DHAP-34: validate and load email CSV into PostgreSQL`.

Implementation references: [official Airflow 2.11.2 Docker guide](https://airflow.apache.org/docs/apache-airflow/2.11.2/howto/docker-compose/index.html)
and [matching Python 3.11 constraints](https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt).
