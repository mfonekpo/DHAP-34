# DHAP-34 verification — 2026-09-18

Verified against the actual local `email_thread_details.csv`, not just a sample.
Runtime: Docker Compose v2.40.0, Airflow 2.11.2 / Python 3.11, PostgreSQL 16.15.

This is a dated record of the completed verification, not a live status report.
Compose uses the `postgres:16` tag, so a later image pull may use a different 16.x
patch release. Follow the runbook to recheck a changed environment or source file.

## Results

| Check | Observed result |
| --- | --- |
| Compose configuration | `docker compose config --quiet` passed |
| Reproducible image build | Official Airflow image, pinned dependencies, `pip check` passed |
| Startup | Initialization exited 0; scheduler, webserver, and both PostgreSQL services healthy |
| Web UI | Airflow sign-in page loaded at `http://localhost:8080` in a browser |
| Automated suite | **37 tests passed, zero skips**, including real database tests |
| First complete DAG run | All five tasks succeeded; **21,684 rows** loaded |
| Identical input rerun | **21,684 rows**, identical sorted-record-ID fingerprint |
| Malformed timestamp fixture | `validate_schema` failed; transform/load marked upstream failed; target unchanged |
| Pending manifest fixture | All five tasks skipped; target unchanged, even with source CSV absent |
| Normal scheduler/LocalExecutor run | `dhap34_scheduler_acceptance` succeeded; all five tasks succeeded |
| Destination uniqueness | **21,684 distinct record IDs** |
| Messages sharing thread/time | **55 additional distinct messages retained** |
| Transaction failure | COPY failure and post-load count mismatch each restored the previous snapshot |
| Git exclusions | New `.env` and local source CSV ignored; placeholder `.env.example` included |

The end-to-end test used the real Airflow metadata database, DAG tasks, and
PostgresHook through `dag.test()`. A separate normal CLI-triggered run verified
scheduler/LocalExecutor execution. The browser check covered the login page;
DAG discovery and execution were verified through Airflow and its metadata.

The deliberate malformed-input run is expected to appear as failed in Airflow.
It is evidence of the required negative acceptance case.

## Source and result fingerprints

- Source SHA-256: `9fb54399233cdd258c9c6eb859e083a67112dd2f5924c1eafb4616910061d75c`
- Source records: `21684`
- Normalized exact duplicates removed: `0`
- Cleaned CSV SHA-256: `dba182b6f4d5801abb03cc9636191abef218e776ee8a6bcc7f52fb3fc2bc75e3`
- Destination fingerprint (MD5 of sorted record IDs): `40e89a9b6b8a6126de84317b4e7ba537`

MD5 here is only a compact comparison of two test snapshots. Record identities
use SHA-256. See [acceptance-report.json](acceptance-report.json) for individual
Airflow run IDs and task states. Reports contain counts/hashes, not email data.

## Corrections from the original implementation

- Replaced Airflow 3 APIs/stack with the brief's required Airflow 2.x runtime.
- Added the required YAML manifest, consistent schema/DDL, and intern-project layout.
- Replaced sampled dtype inference with full-record content validation.
- Replaced the invalid natural keys with derived record IDs; preserved 55 messages.
- Preserved source timestamps without assigning an unsupported timezone.
- Replaced failing-on-rerun CREATE/row-by-row inserts with transactional DDL,
  bulk COPY, snapshot replacement, and a loaded-count check.
- Added explicit manifest-status gating, connection management, generated secrets,
  separate database services, isolated run artifacts, and checksum checks.
- Added meaningful automated tests and reproducible positive/negative DAG checks.
- Corrected shutdown guidance and documented work-volume retention and cleanup.

## Reproduce

Follow the [runbook](../README.md), then run:

```bash
docker compose run --rm --no-deps --entrypoint python tests \
  /opt/airflow/project/scripts/run_tests.py --postgres
docker compose run --rm airflow-cli python /opt/airflow/project/scripts/acceptance.py
```

## Remaining external submission

Local implementation and validation were completed in the `mfonekpo/DHAP-34`
checkout. No changes or PR to `Glynac-AI/airflow-dag-configs` were made during this
verification; central submission is a separate step under the specified intern
subtree. The old project folders have since been removed from the current
checkout; their original contents remain available in Git history.
