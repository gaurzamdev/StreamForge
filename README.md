# StreamForge — API Ingestion & Data Processing

StreamForge is a Python-based ETL project that collects records from a public API, processes them through a PostgreSQL data workflow, and prepares reliable data for downstream use.

The implementation focuses on the practical challenges of ingestion pipelines: inconsistent input, invalid fields, repeated records, database transaction safety, and predictable results across repeated executions.

## 1. Project Overview

The pipeline takes external user data through a series of processing stages, preserving the original input while separating valid records from rejected ones.

Its main capabilities include:

- Retrieving records from an external REST API.
- Persisting incoming JSON payloads in PostgreSQL.
- Converting nested attributes into a consistent structure.
- Applying Pydantic models to validate field values.
- Recording invalid records alongside their validation errors.
- Removing duplicate records using a deterministic selection rule.
- Synchronizing processed records into a final database table.
- Recording execution statistics and failure details.
- Testing transformation logic and database-dependent behavior with pytest.

## 2. Architecture

The processing workflow follows this sequence:

```text
Public REST API
      |
      v
Input Preparation
      |
      v
Raw PostgreSQL Staging
      |
      v
Normalization and Validation
      |
      +--------------------+
      |                    |
      v                    v
  Valid Data          Rejected Data
      |
      v
Duplicate Resolution
      |
      v
Final User Table
      |
      v
Execution Audit
```

The database tables have distinct responsibilities:

| Table | Purpose |
|---|---|
| `stg_users_raw` | Retains the original incoming records. |
| `clean_users` | Holds normalized records that pass validation. |
| `rejected_users` | Captures invalid input and associated error information. |
| `final_users` | Stores the deduplicated output dataset. |
| `etl_load_logs` | Tracks pipeline execution status and processing counters. |

## 3. Tools and Libraries

- **Python** — pipeline implementation and orchestration logic.
- **PostgreSQL** — persistent storage and relational data processing.
- **Docker Compose** — local database environment.
- **Requests** — HTTP communication with the source API.
- **Psycopg2** — PostgreSQL connectivity.
- **Pydantic** — structured data validation.
- **Pytest** — automated unit and integration tests.
- **Apache Airflow** — DAG implementation for orchestration integration.

## 4. Source Data

The pipeline retrieves sample user records from JSONPlaceholder:

`https://jsonplaceholder.typicode.com/users`

The API supplies 10 users. Additional records are introduced deliberately to exercise validation and duplicate-handling logic.

| Input category | Records |
|---|---:|
| Original API response | 10 |
| Intentionally invalid records | 5 |
| Additional duplicate records | 2 |
| **Total input** | **17** |

These controlled inputs make it possible to reproduce expected processing outcomes without depending on naturally occurring data errors.

## 5. Processing Stages

### Stage 1 — API extraction

The extraction module requests the source data and passes the returned records into the pipeline.

### Stage 2 — Raw ingestion

All 17 prepared records are written to `stg_users_raw` as JSON. Retaining the original payload allows later investigation without depending exclusively on transformed values.

### Stage 3 — Standardization

The normalization module flattens nested attributes and converts them into the structure expected by the downstream model. For example, the API's company information is represented through a `company_name` field.

### Stage 4 — Validation and rejection handling

Pydantic checks whether records conform to the defined data model.

- Records that pass validation continue through the clean-data path.
- Records that fail validation are stored in `rejected_users`.
- Validation errors identify the affected fields and explain why a record was rejected.

Both clean and rejected records are written within one database transaction, so a loading error can roll back the complete operation.

### Stage 5 — Duplicate resolution

Valid records are deduplicated using `external_id` as the business key.

Because the source API does not provide an `updated_at` attribute, the current implementation keeps the record with the highest `staging_id` for each key. This is a deterministic demonstration rule rather than a guarantee that the selected record is the most recent in the source system.

### Stage 6 — Final synchronization

The selected records are loaded into `final_users`. The loader distinguishes between three outcomes:

- **Inserted:** the key is not present in the destination.
- **Updated:** the key exists, but its stored values differ.
- **Unchanged:** the destination already contains the same values.

The `external_id` column is unique in the final table, preventing duplicate business keys in the published dataset.

### Stage 7 — Execution tracking

Each run writes its status and processing counters to `etl_load_logs`. Failed executions can also record the stage at which processing stopped and the associated error message.

## 6. Database Transactions and Repeatability

Database operations use a shared connection and cursor management utility. Successful operations are committed, exceptions trigger rollback, and database resources are closed after use.

The clean/rejected loading step is atomic: either both result sets are persisted, or neither is.

The final loader is designed to support repeatable execution. When the same data is processed again without any changes, the expected counters are:

```text
rows_inserted  = 0
rows_updated   = 0
rows_unchanged = 10
```

This behavior prevents repeated executions from creating additional final records for existing business keys.

## 7. Local Setup

### Prerequisites

Install Python, Docker Desktop, and Git before starting.

### Create the Python environment

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On macOS or Linux:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### Configure environment variables

Create a local `.env` file from `.env.example`.

```powershell
Copy-Item .env.example .env
```

Configure the database connection and source URL:

```env
POSTGRES_DB=etl_db
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=your_password_here
POSTGRES_HOST=localhost
POSTGRES_PORT=5433

API_URL=https://jsonplaceholder.typicode.com/users
```

Use the database credentials defined by your local Compose configuration.

### Start PostgreSQL

```bash
docker compose up -d
docker compose ps
```

The existing configuration exposes PostgreSQL on host port `5433`, forwarding connections to container port `5432`.

### Initialize database objects

Run these commands in PowerShell:

```powershell
Get-Content sql\001_create_tables.sql | docker exec -i users_etl_postgres psql -U etl_user -d etl_db
```

```powershell
Get-Content sql\002_create_indexes.sql | docker exec -i users_etl_postgres psql -U etl_user -d etl_db
```

These scripts create the database tables and indexes used by the pipeline.

## 8. Execute the Pipeline

From the repository root, run:

```bash
python -m src.pipeline
```

A successful execution prints a message similar to:

```text
Pipeline finished successfully. run_id=<uuid>
```

The run identifier can be used to investigate the corresponding execution and its database results.

### Inspect the latest execution

Connect to PostgreSQL:

```bash
docker exec -it users_etl_postgres psql -U etl_user -d etl_db
```

Then query the audit table:

```sql
SELECT
    status,
    rows_received,
    rows_staged,
    rows_clean,
    rows_rejected,
    rows_deduplicated,
    rows_final_candidates,
    rows_inserted,
    rows_updated,
    rows_unchanged,
    failed_step,
    error_message
FROM etl_load_logs
ORDER BY started_at DESC
LIMIT 1;
```

For a successful first run with the supplied demonstration inputs, the expected processing counts are:

| Counter | Expected value |
|---|---:|
| Records received | 17 |
| Records staged | 17 |
| Valid records before deduplication | 12 |
| Rejected records | 5 |
| Duplicate records removed | 2 |
| Final candidate records | 10 |

If those same ten final records are already present and unchanged, the subsequent run should report zero inserts, zero updates, and ten unchanged records.

## 9. Automated Testing

The repository uses pytest to check the transformation logic and database interactions.

Run the complete test suite:

```bash
python -m pytest
```

Run only database integration tests:

```bash
python -m pytest -m integration
```

Run the unit tests:

```bash
python -m pytest -m "not integration"
```

The documented test suite contains 17 tests: 15 unit tests and two integration tests.

Unit tests cover normalization, validation, rejected records, and duplicate handling. Integration tests exercise the final loader's insert, update, unchanged, and idempotency behavior. PostgreSQL must be available for the integration tests.

## 10. Airflow Integration

The repository contains an Airflow DAG at:

`dags/users_etl_dag.py`

The DAG invokes `run_users_pipeline()` from `src/pipeline.py`, keeping the processing logic separate from the orchestration layer.

The current setup includes the DAG definition but does not deploy a complete Airflow environment. Docker Compose is configured for PostgreSQL only, and the pipeline can be run directly through Python.

A full Airflow deployment with a scheduler, web interface, and supporting services remains a possible extension.

## 11. Repository Layout

```text
api-airflow-postgres-etl-pipeline/
├── dags/
│   └── users_etl_dag.py
├── sql/
│   ├── 001_create_tables.sql
│   └── 002_create_indexes.sql
├── src/
│   ├── extract/
│   │   └── users_api.py
│   ├── load/
│   │   ├── clean_rejected_loader.py
│   │   ├── final_loader.py
│   │   └── staging_loader.py
│   ├── normalize/
│   │   └── users_normalizer.py
│   ├── transform/
│   │   ├── demo_data.py
│   │   └── users_deduplication.py
│   ├── utils/
│   │   ├── db.py
│   │   ├── logger.py
│   │   ├── run_id.py
│   │   └── settings.py
│   ├── validate/
│   │   ├── users_model.py
│   │   └── users_validator.py
│   └── pipeline.py
├── tests/
│   ├── test_deduplication.py
│   ├── test_idempotency.py
│   ├── test_normalization.py
│   ├── test_rejected.py
│   └── test_validation.py
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
├── .env.example
└── README.md
```

## 12. Engineering Highlights

StreamForge demonstrates several useful data engineering practices:

- Separating extraction, transformation, validation, and loading responsibilities.
- Retaining raw input for traceability.
- Isolating invalid records instead of silently discarding them.
- Applying deterministic deduplication rules.
- Using transaction boundaries to avoid partially written validation results.
- Distinguishing inserted, modified, and unchanged destination records.
- Capturing execution metadata for debugging and auditing.
- Testing Python transformations separately from database-dependent behavior.
- Keeping orchestration code separate from the ETL implementation.

## 13. Current Scope and Future Work

The current implementation is an educational portfolio project rather than a fully deployed production service.

Potential extensions include:

- Deploying Airflow alongside PostgreSQL through Docker Compose.
- Automating SQL schema changes with a migration tool.
- Adding retry and timeout policies for API requests.
- Introducing automated tests through a GitHub Actions workflow.
- Supporting additional API sources and configurable validation rules.
- Improving monitoring and structured failure reporting.

## License

MIT
