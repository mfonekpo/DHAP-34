# DHAP-34: Email Thread Summary CSV → PostgreSQL

A local Airflow 2.11.2 / Python 3.11 pipeline that validates every CSV record,
cleans the values, and atomically replaces a PostgreSQL snapshot. Airflow metadata
and the destination data live in separate PostgreSQL services.

This directory is the runnable project. The repository-root README is an entry
point; all Compose commands below run from this directory. Shell examples use
Bash/Zsh unless marked PowerShell. See [project files](#project-files),
[pgAdmin setup](#connect-with-pgadmin), [tests](#automated-acceptance-checks), and
[changes from the original implementation](#changes-from-the-original-implementation).

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

1. Ensure `data/email_thread_details.csv` exists. If it is missing, download the
   source from SharePoint and place it at that exact path. CSVs are excluded from
   Git; a fresh clone needs the download. The SharePoint download is manual and
   requires access to the source; the DAG does not fetch the file.
2. **First setup only:** create local credentials if `.env` does not exist.
   If you have already run the project, keep your existing `.env` and proceed to
   step 3. On Linux/macOS:

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
   same thing. The script reads `.env.example`, writes generated secrets into a
   private `.env`, and refuses to overwrite existing credentials. The template
   itself is not modified.
   Do not copy the blank template to `.env` before running the script. If you
   intentionally configure `.env` by hand, supply every required secret.
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

   | Service | Expected status |
   | --- | --- |
   | `airflow-init` | `Exited (0)`; this is a successful one-time initialization |
   | `airflow-webserver` | Running and healthy |
   | `airflow-scheduler` | Running and healthy |
   | `postgres-metadata` | Running and healthy |
   | `postgres-target` | Running and healthy |

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

   A successful normal run has all five tasks marked **success**. Open the DAG's
   Grid/Graph view and the individual task logs to investigate a failed task.
   `list-import-errors` should report no import errors; `list-runs` shows whether
   a triggered run is queued, running, successful, or failed. Container health
   alone does not prove that the CSV has been loaded.

## Project files

```text
email_thread_summary_dataset/
├── manifest.yaml                 # ownership, source, approval status, file paths
├── schema.yaml                   # source contract and generated primary key
├── ddl.sql                       # target table and nonunique thread/time index
├── dags/
│   ├── load_email_thread_summary_to_postgres.py  # Airflow orchestration
│   └── email_pipeline/core.py     # validation, transformation, transactional load
├── data/                         # local source CSV; only .gitkeep is committed
├── docker-compose.yaml           # services, mounts, ports, and named volumes
├── Dockerfile                    # extends the official Airflow image
├── requirements.txt              # pinned packages under Airflow constraints
├── .env.example                  # configuration names, defaults, blank secrets
├── scripts/
│   ├── init_env.py               # create private local credentials once
│   ├── airflow_env.py            # construct connections and run the entrypoint
│   ├── run_tests.py              # unit/DAG tests, optional database tests
│   └── acceptance.py             # full DAG, rerun, malformed-input, status checks
├── tests/                        # CSV, DAG, environment, PostgreSQL test suites
└── docs/                         # dated verification and aggregate test evidence
```

## Configuration

Compose reads `.env` from this directory. These defaults are defined in
[.env.example](.env.example); use your actual `.env` if you have changed them.

| Setting | Default or purpose |
| --- | --- |
| `AIRFLOW_WEB_PORT` | Host UI port `8080` |
| `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` | UI login; `admin` and a generated password |
| `WAREHOUSE_DB_HOST` / `WAREHOUSE_DB_PORT` | Internal Docker endpoint `postgres-target:5432` |
| `WAREHOUSE_HOST_PORT` | Published host database port `5433` |
| `WAREHOUSE_DB_NAME` / `WAREHOUSE_DB_USER` | `email_warehouse` / `email_loader` |
| `WAREHOUSE_DB_PASSWORD` | Generated target database password; also used in pgAdmin |
| `METADATA_DB_*` | Airflow's separate internal database at `postgres-metadata:5432` |
| `AIRFLOW_FERNET_KEY` / `AIRFLOW_WEBSERVER_SECRET_KEY` | Generated Airflow encryption/session keys |

The UI and target database ports bind to `127.0.0.1`. The metadata database has no
published host port. [scripts/airflow_env.py](scripts/airflow_env.py) constructs
the metadata connection URL and the JSON `AIRFLOW_CONN_WAREHOUSE_POSTGRES` value
from the discrete settings, preserving special characters in database passwords.
The target Connection ID is `warehouse_postgres`; environment connections are not
listed as saved connections in the Airflow UI.

CSV, DAG, script, and configuration mounts are read-only inside the Airflow
containers. Logs and run artifacts use named Docker volumes. The code reads
`PIPELINE_PROJECT_DIR` and `PIPELINE_WORK_DIR`, which Compose sets to
`/opt/airflow/project` and `/opt/airflow/work`. Merely adding different values for
these two names to `.env` does not override the literal Compose settings.

## Verify the loaded data

Run this Bash/Zsh command from the dataset directory; database credentials stay inside the
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

On PowerShell, run the same SQL statements in pgAdmin's Query Tool instead of
pasting the Bash heredoc above.

## Connect with pgAdmin

For pgAdmin installed on the same computer as Docker, right-click **Servers →
Register → Server**. Set the General-tab name to `DHAP-34 Email Warehouse`, then
use these Connection-tab values:

| pgAdmin field | Value with the supplied defaults |
| --- | --- |
| Host name/address | `127.0.0.1` |
| Port | `5433` (or your `WAREHOUSE_HOST_PORT`) |
| Maintenance database | `email_warehouse` (your `WAREHOUSE_DB_NAME`) |
| Username | `email_loader` (your `WAREHOUSE_DB_USER`) |
| Password | Copy only the value of `WAREHOUSE_DB_PASSWORD` from `.env` |

Click **Save**, then expand **Databases → email_warehouse → Schemas → public →
Tables → email_thread_details**. Right-click the table and choose **View/Edit
Data → First 100 Rows**, or open Query Tool on `email_warehouse` and use the
queries above. The warehouse password is separate from the Airflow UI password.
Port `5433` is the host mapping; `5432` is used between Compose services.

These host settings apply to pgAdmin on your computer. A pgAdmin container needs
its own network configuration; if attached to this project's Docker network,
its database endpoint is `postgres-target:5432`.

## Pipeline and data contract

```text
SharePoint (manual download) → data/email_thread_details.csv
  → check_dataset_status → read_csv → validate_schema → transform → load_to_postgres
                                                                → PostgreSQL target
```

The CSV header must match this order exactly; `record_id` is generated later and
must not appear in the input:

```csv
thread_id,subject,timestamp,from,to,body
```

| Source column | PostgreSQL type | Nullable | Validation |
| --- | --- | --- | --- |
| `thread_id` | `INTEGER` | No | Whole number between −2,147,483,648 and 2,147,483,647 |
| `subject` | `VARCHAR(255)` | Yes | Up to 255 characters after trimming |
| `timestamp` | `TIMESTAMP WITHOUT TIME ZONE` | No | Valid date and time; no timezone offset or `Z` suffix |
| `from` | `VARCHAR(255)` | No | Nonblank; up to 255 characters after trimming |
| `to` | `TEXT` | No | Nonblank; supports long recipient lists |
| `body` | `TEXT` | Yes | Supports quoted, multiline content |

Accepted timestamp examples are `2001-06-15 09:30:00`,
`2001-06-15T09:30:00`, and `2001-06-15 09:30:00.123456`. Seconds are required;
optional fractional seconds have one to six digits. Timezone information is not
inferred. Address fields are treated as text; email-address syntax and recipient
deliverability are not validated.

- **Status:** this CSV has no row-level status column. `manifest.yaml` uses
  `status: done` to mean the dataset is reviewed and approved for ingestion.
  `pending`, `in_progress`, and `blocked` skip all downstream tasks without
  opening the CSV or connecting to the target. Unknown statuses fail. `done` is
  an approval state, not a marker that prevents future loads. Each CSV is still
  validated on every run. The pipeline does not rewrite the read-only manifest.
- **Read:** copy the input, schema, and DDL into a run-specific work directory.
  XCom contains small manifest/configuration metadata, paths, counts, and
  checksums. It never contains CSV records.
- **Validate:** check the exact six-column header/order and every record's field
  count, integer range, date/time format, string length, and nullability. Accept
  UTF-8 with optional BOM, quoted commas, and multiline bodies. The parser limit
  is 16,777,216 characters per field; schema length limits can be smaller.
  Header-only files, malformed quoting, NULs, and bad values fail before touching
  the target. Value errors identify record numbers/columns without printing email
  contents; header, quoting, encoding, and field-size errors can be generic.
- **Transform:** strip outer whitespace, normalize integer and naive timestamp
  representations, turn empty nullable values into SQL NULL, and remove only
  identical normalized rows. Literal `NA`/`NULL` text is retained. No made-up
  subject/body values, dropped invalid records, or assumed timestamp timezone.
- **Identity:** `schema.yaml` separately declares the six source columns and
  derived `record_id CHAR(64)`. SHA-256 of their canonical JSON value array is
  the primary key. Neither `thread_id` nor `(thread_id, timestamp)` uniquely
  identifies a message. Editing any normalized source value changes that record's
  ID; it is a content identity, not a source-provided immutable message identifier.
  A nonunique index supports thread/time lookups.
- **Load:** `PostgresHook` obtains the connection. DDL, `TRUNCATE`, bulk `COPY`,
  and the output-count check share one transaction. A failure rolls everything
  back, including the truncate. Reruns replace the snapshot without duplicates;
  valid source edits/deletions are reflected in the next snapshot. An empty
  snapshot is rejected to prevent accidentally clearing the table.

Run directories prevent cross-run file collisions; `max_active_runs=1` serializes
normal scheduler-managed runs. Input and cleaned checksums detect changes between
tasks. Each task has a 10-minute execution timeout; the DAG declares a 30-minute
run timeout. Only the database load retries automatically (twice, 30 seconds
apart). Schema errors require correcting the source and triggering a new run.

Artifacts are stored under `/opt/airflow/work/<sha256-of-run-id>/`: `input.csv`,
`schema.yaml`, `ddl.sql`, and, after transformation succeeds, `cleaned.csv`.
The `read_csv` task's XCom records the actual paths. A load-only retry uses these
copies. No cleaned CSV is written back into the host's `data/` directory.

The implementation streams CSV records with Python's `csv` module and uses PyYAML
and psycopg2. pandas is included in the pinned environment but is not used by the
current pipeline. Deduplication stores the seen record IDs in memory, so memory
usage grows with the number of unique rows. The `normalization` section in
`schema.yaml` documents the policy implemented in `core.py`; changing those YAML
labels alone does not change the transformation behavior. This is a local full
snapshot pipeline; it does not perform incremental ingestion or schema migrations.

## Automated acceptance checks

Run these after the stack becomes healthy, with `manifest.yaml` set to
`status: done` and the source CSV in place. The first command checks parsing,
cleaning, the DAG, credential handling, and real PostgreSQL rollback/idempotency
using isolated temporary tables:

```bash
docker compose run --rm --no-deps --entrypoint python tests \
  /opt/airflow/project/scripts/run_tests.py --postgres
```

With the current test suite, expect `Ran 37 tests` and `OK`, with zero skips. The
three PostgreSQL tests use randomly named tables and remove them afterward; they
do not replace the dataset table. This command uses the configured warehouse, so
run it against this local development stack.

Run the complete DAG against the configured local CSV twice, then against a
temporary malformed CSV and a pending manifest:

```bash
docker compose run --rm airflow-cli python /opt/airflow/project/scripts/acceptance.py
```

This uses Airflow's `dag.test()` with the real metadata database, tasks, and
PostgresHook. It replaces the local target with the configured snapshot twice.
The malformed run must fail specifically at `validate_schema`, the pending
dataset must skip all tasks, and neither may change the successful snapshot.
The intentionally failed test run remains visible in Airflow. `dag.test()` runs
tasks in the CLI process and bypasses the scheduler/LocalExecutor. Run acceptance
while no other DAG runs are active; it does not enforce the normal scheduler's
single-active-run limit. The source and manifest files are not edited. Keep the
normal UI/CLI trigger in the quick start as a separate scheduler check.

| Acceptance case | Expected outcome |
| --- | --- |
| First load | All five tasks succeed; target contains the cleaned snapshot |
| Identical rerun | Same row count and sorted-record-ID fingerprint |
| Malformed timestamp | `validate_schema` fails; transform/load are `upstream_failed`; target unchanged |
| Pending dataset | All five tasks are `skipped`; target unchanged |

The reference count of 21,684 is enforced by the acceptance script only when the
input checksum matches `manifest.yaml`'s reference checksum. A different valid
snapshot is checked for a nonempty result and repeatability.

The script prints `ACCEPTANCE PASSED` and saves an aggregate report at
`/opt/airflow/work/acceptance-report.json`. To read it:

```bash
docker compose run --rm --no-deps --entrypoint cat airflow-cli /opt/airflow/work/acceptance-report.json
```

For unit/DAG tests without starting PostgreSQL (database tests explicitly skip):

```bash
docker compose run --rm --no-deps tests
```

This still requires `.env` for Compose interpolation and a built image. Expect
`Ran 37 tests` and `OK (skipped=3)`; the three database tests are intentionally
skipped. Running the suites in an incomplete host virtualenv can skip additional
checks, so use the Docker command for the documented result.

The [verification report](docs/verification.md) and
[saved acceptance report](docs/acceptance-report.json) record the earlier verified
run. New acceptance runs write their runtime report to the work volume; they do
not automatically update the committed evidence files.

## Operations and troubleshooting

For a new CSV snapshot, wait for active runs to finish, place the complete new
file at `data/email_thread_details.csv`, and trigger a new DAG run. The new input
must be a **complete snapshot**: records absent from it will be absent from the
target after a successful load. Leave `status: done` only for an approved dataset.
Input record counts include embedded-newline fields correctly; do not use
`wc -l` as the CSV row count.

| Change | How to apply it |
| --- | --- |
| CSV in `data/` | Finish writing/replacing the file before triggering a fresh run. No image rebuild is needed. |
| DAG or `email_pipeline/core.py` | Let the scheduler reparse the bind-mounted code, check import errors, then trigger a fresh run. |
| `manifest.yaml`, `schema.yaml`, or `ddl.sql` | Update the files consistently and recreate the Airflow containers before a new run (command below). These individual file mounts can retain an old file when an editor saves by replacing it. |
| Database table structure | Apply an explicit migration to the existing target as well as updating YAML/DDL. `CREATE TABLE IF NOT EXISTS` does not migrate an existing table. |
| `.env` values | Run `docker compose up -d` to apply changed container settings. Existing database passwords and UI accounts need their own credential updates. |
| `Dockerfile`, `requirements.txt`, or `scripts/airflow_env.py` | Run `docker compose up --build -d`; the entrypoint wrapper is copied into the image. |

After configuration-file edits, once active runs have finished:

```bash
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```

The manifest's paths are relative to the dataset directory. Keep CSV files under
the mounted `data/` directory. If you relocate schema/DDL files, also update the
Compose mounts and acceptance fixtures; the supplied setup assumes the current
filenames. If a file or configuration has changed, trigger a new run so it takes
a new snapshot. Clearing only a downstream task retains the existing run's copies.

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
| pgAdmin connection refused | Check `postgres-target` health; desktop pgAdmin uses `127.0.0.1` and `WAREHOUSE_HOST_PORT` (default `5433`). |
| pgAdmin password rejected | Use `WAREHOUSE_DB_PASSWORD`, not `AIRFLOW_ADMIN_PASSWORD`; verify it matches the initialized database volume. |
| A retry fails due to a missing run artifact | Trigger a fresh DAG run; clearing only load requires retained work artifacts. |
| An acceptance run is red | The malformed-input case is intentionally failed. Check that the script ultimately printed `ACCEPTANCE PASSED`. |

Stop while retaining databases and logs:

```bash
docker compose down
```

Work snapshots are retained for inspection and retries (about 80 MB per full run).
To discard **only** those artifacts, first wait for active runs to finish and stop
the stack. This prevents retrying old runs from the load step but preserves both
databases. The volume name below uses the default Compose project name. If you
override the name with `-p` or `COMPOSE_PROJECT_NAME`, inspect `docker volume ls`
for the corresponding volume instead:

```bash
docker compose down
docker volume rm dhap34-mfonekpo-email_pipeline-work
docker compose up -d
```

Only for an intentional full local reset: `docker compose down -v` removes this
stack's metadata, target data, logs, and work volume. Keep `.env` and the local
CSV, then rerun `docker compose up -d`. Normal shutdown should omit `-v`.

## Changes from the original implementation

| Area | Original behavior | Current behavior |
| --- | --- | --- |
| Project layout | Separate `extraction/` and `mfonekpo-project1/` folders | One runnable dataset directory under the required intern path; old folders are available in Git history |
| Runtime | Airflow 3 APIs and a Celery/Redis stack | Airflow 2.11.2 APIs, LocalExecutor, and separate metadata/target PostgreSQL services |
| Validation | Dtype/null checks on a 50-row sample | Every record checked for exact header, shape, type, range, length, and required values |
| Record identity | YAML used `thread_id`; SQL used `(thread_id, timestamp)` | Consistent generated `record_id` key; preserves the 55 distinct messages sharing thread/time in the reference CSV |
| Timestamps | Naive CSV values loaded into a timezone-aware target | Source times preserved as `TIMESTAMP WITHOUT TIME ZONE` |
| Reruns | Unconditional table creation and conflict-skipping inserts | Idempotent table creation and transactional snapshot replacement with rollback and count verification |
| Dataset status | No implemented status gate | Manifest approval gate; `done` loads, recognized non-done states skip |
| Intermediate data | Shared cleaned CSV beside the source | Separate run artifacts in a named volume, passed by path through XCom |
| Credentials and dependencies | Inconsistent environment examples and an empty requirements file | Generated secrets, matching environment names, encoded connections, and pinned Airflow-compatible dependencies |
| Verification | Manual run instructions | Unit/DAG/database suites, repeatable positive/negative acceptance cases, and dated evidence |

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
