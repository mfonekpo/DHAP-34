## Email Thread Summary Dataset Manifest

**Version:** 0.1.0

### datasetName
Email Thread Summary Dataset

### description:
The Email Thread Dataset consists of two main files: <br> ``email_thread_details`` and ``email_thread_summaries``. These files collectively offer a comprehensive compilation of email thread information alongsife human-generated summaries.

### schema:
[schema](./config/schema_expected.yml)

### source:
A compressed archive containing the dataset files can be downloaded from sharepoint: [dataset url](https://glynac-my.sharepoint.com/:x:/r/personal/nurdin_glynac_ai/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7B3d1c8094-2701-4fb4-9063-15cdcc659f96%7D&wdOrigin=TEAMS-MAGLEV.undefined_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1765987218511&web=1)

 - **dataset_code:** 100

 - **Analysis Status:** Complete

## Local CSV Path:
[Open project directory](./sample_data/email_thread_details.csv)

## Target Table
```sql
public.email_thread_details
```

## - PS
- Only records with ***"analysis_status_2"*** = ```complete``` are processed.
- Schema validation is enforced before loading.