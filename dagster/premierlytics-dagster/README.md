# premierlytics_dagster

Dagster pipeline for ingesting, transforming, and loading Premier League data into DuckDB. Data is sourced from the [olbauday/FPL-Core-Insights](https://github.com/olbauday/FPL-Core-Insights) GitHub repository and processed across three layers before being modelled with dbt.

## Pipeline overview

```
raw  →  transformation  →  loading  →  dbt
CSV       Parquet          DuckDB     Views/marts
(S3)      (S3)             (local)
```

| Layer | Group | What it does |
|---|---|---|
| `raw_*` | `raw_data` | Downloads CSV from GitHub, uploads to S3 |
| `transformed_*` | `transformed_data` | Cleans data, validates schema, writes Parquet to S3. Invalid rows are quarantined. |
| `loaded_*` | `loaded_data` | Reads Parquet from S3, loads into DuckDB `*_bronze` tables (delete-then-insert) |
| dbt | — | Runs dbt models on top of DuckDB bronze tables |

Assets are partitioned by `season × gameweek` (e.g. `2025-2026 | GW32`).

**Datasets:** `matches`, `playermatchstats`, `players`, `playerstats`, `teams`, `player_gameweek_stats`, `fixtures`

## Project structure

```
src/premierlytics_dagster/
├── definitions.py          # Dagster Definitions entry point
├── run_pipeline.py         # Production entrypoint (runs current gameweek)
├── defs/
│   ├── config.py           # Per-season dataset config (URLs, schemas, rename rules)
│   ├── jobs.py             # fpl_pipeline_job
│   ├── schedules.py        # Daily schedule (EventBridge triggers in production)
│   ├── partitions.py       # season × gameweek multi-partition
│   ├── raw/                # Raw ingestion assets
│   ├── transformation/     # Cleaning, validation, quarantine assets + checks
│   ├── loading/            # DuckDB load assets + checks
│   ├── dbt/                # dbt asset + profiles
│   ├── resources/
│   │   ├── s3.py           # S3Resource (boto3-backed, supports local MinIO via endpoint_url)
│   │   └── duckdb.py       # DuckDBResource
│   └── schemas/            # Pydantic schemas per dataset
└── helpers/
    ├── clean_data.py
    ├── validation.py
    ├── download_csv.py
    ├── current_gameweek.py
    ├── sql.py
    └── checks.py
```

## Local development

### Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Docker (for MinIO and Postgres via docker-compose)

### 1. Start backing services

From the repo root:

```bash
docker compose -f docker/docker-compose.yml up -d minio minio_init docker_postgres
```

This starts MinIO (object storage) on `localhost:9000` and Postgres (Dagster metadata) on `localhost:5432`.

### 2. Set environment variables

Copy `.env.dev` from the repo root and export the variables, or use a tool like `direnv`. Key variables:

| Variable | Local dev value | Description |
|---|---|---|
| `S3_BUCKET_NAME` | `premierlytics-dev-bucket` | S3/MinIO bucket |
| `AWS_REGION` | `us-east-1` | Region (MinIO ignores this) |
| `MINIO_ENDPOINT` | `http://localhost:9000` | Set this to use local MinIO instead of AWS S3 |
| `AWS_ACCESS_KEY_ID` | `admin` | MinIO root user (boto3 credential chain) |
| `AWS_SECRET_ACCESS_KEY` | `password` | MinIO root password (boto3 credential chain) |
| `DUCKDB_PATH` | `/data/duckdb/premierlytics.duckdb` | DuckDB file path |

> **Note:** `MINIO_ENDPOINT` is only needed for local dev. In production on EC2, leave it unset — boto3 will use the IAM role credentials and talk directly to AWS S3.

### 3. Install dependencies

```bash
uv sync
```

### 4. Run the Dagster UI

```bash
uv run dg dev
```

Open [http://localhost:3000](http://localhost:3000).

## Running tests

Tests use `moto` to mock S3 in-process — no running services required.

```bash
uv run pytest tests/ -v
```

## Production

In production the pipeline runs on an EC2 spot instance launched by AWS Lambda on a daily EventBridge schedule. The instance pulls the Docker image from ECR, runs `run_pipeline.py` for the current gameweek, syncs DuckDB to S3, then self-terminates.

The production Docker image is built from `docker/dagster/Dockerfile.prod`. Credentials are provided by the EC2 IAM instance profile — no explicit AWS keys are needed.

To build and push the image manually:

```bash
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com

docker build -f docker/dagster/Dockerfile.prod -t premierlytics-dagster:latest .

docker tag premierlytics-dagster:latest <ecr-repo-url>:latest
docker push <ecr-repo-url>:latest
```
