# DHAP-34 — Local CSV to PostgreSQL

The maintained, self-contained project is
[`intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/`](intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/README.md).

It includes the manifest, schema, SQL DDL, Airflow 2 DAG, Docker stack, automated
acceptance checks, and runbook required by DHAP-34. Follow that README to run it.

The `extraction/` and `mfonekpo-project1/` folders preserve the original submission
for reference; their old stack and DAGs are superseded. The original local CSV has
already been copied into the maintained project's ignored `data/` directory.
For submission to `Glynac-AI/airflow-dag-configs`, copy and commit only the
`intern-project/mfonekpo/project-DHAP-34/email_thread_summary_dataset/` subtree.
