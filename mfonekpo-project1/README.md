# Local Apache Airflow Setup

This sets up a reproducible Airflow environment with Docker Compose, including:
- Airflow webserver, scheduler, and metadata DB (PostgreSQL)
- Local PostgreSQL for development/testing (also used as target for pipelines)

## Prerequisites
- Docker & Docker Compose installed

## How to Start
1. Copy the ```.env``` template:  
   (Edit `.env` for custom credentials or external PG params)

   ```env
   # Local PostgreSQL configuration (used by Airflow metadata + target DB for dev)
   POSTGRES_USER=your_value

   POSTGRES_PASSWORD=your_value

   POSTGRES_DB=your_value

   # Connection parameters used by your DAGs to load data ("external" Postgres)
   # In local dev, this points to the same container

   EXT_POSTGRES_HOST=your_value
   EXT_POSTGRES_PORT=your_value

   EXT_POSTGRES_USER=your_value

   EXT_POSTGRES_PASSWORD=your_value

   EXT_POSTGRES_DB=your_value
   ```

2. Start the environment:  
   First-time initialization (creates DB and admin user):  
   `docker compose up -d` run this in detach mode

1. Access Airflow UI: http://localhost:8080  
   Login: airflow / airflow

## How to Shut Down
`docker compose down -v`  # -v removes volumes (clears DB)

## Adding DAGs
Place your DAG files in `./dags/` – they will appear in the UI instantly.

## Connecting PGAdmin to Local PostgreSQL (For testing)
before connecting, ensure the PostgreSQL container is running:
1. Install PGAdmin on your host machine.
2. Open PGAdmin and create a new server connection with:
   - **Host**: `localhost`
   - **Port**: `5433 # maps to container's 5432`
   - **Username**: this will be the same as in your `.env`
   - **Password**: this will be the same as in your `.env`
   - **Maintenance** DB: DB var is in your `.env`

## Notes
- The external PostgreSQL connection for your pipelines is configured via variables in `.env` (e.g., EXT_POSTGRES_*).
- In DAGs, create a connection ID (e.g., `my_external_pg`) using those env vars or set it in Airflow UI.