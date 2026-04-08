# Premierlytics Data Platform

[![CI](https://github.com/KenImade/premierlytics-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/KenImade/premierlytics-data-platform/actions/workflows/ci.yml)

A data platform for Premier League analytics. Ingests FPL and FotMob data, transforms it through a dimensional model, and serves it for analysis and FPL team-building.

> For a full writeup of the design decisions and architecture behind this project, see the case study: [End-to-End FPL Analytics Pipeline with Dagster, dbt, and AWS](https://kennethimade.dev/blog/)

## Architecture

```
GitHub (CSV source)
        │
        ▼
   Dagster pipeline  ──────────────────────────────────────────────┐
   (raw → transform → load)                                        │
        │                                                          │
        ▼                                                          ▼
     AWS S3                                                    DuckDB
  (CSV + Parquet)                                          (bronze tables)
                                                               │
                                                               ▼
                                                          dbt models
                                                   (staging → dims → facts → marts)
```

**Production flow:** EventBridge triggers a Lambda daily → Lambda starts a spot EC2 instance → EC2 pulls the Docker image from ECR, runs the pipeline, syncs DuckDB to S3, then self-terminates.

## Repository structure

```
.
├── dagster/premierlytics-dagster/   # Dagster pipeline (ingest, transform, load)
├── premierlytics_dbt/               # dbt project (dimensional model)
├── docker/                          # Docker Compose for local dev + Dockerfiles
├── infra/                           # Terraform (AWS Lambda, EC2, S3, ECR, EventBridge)
├── .github/workflows/               # CI (pytest with moto)
├── Makefile                         # Local dev shortcuts
└── .env.dev                         # Local environment variables (not committed)
```

## Data pipeline

Data is sourced from the [olbauday/FPL-Core-Insights](https://github.com/olbauday/FPL-Core-Insights) GitHub repository and processed across four stages:

| Stage | Tool | Description |
|---|---|---|
| **Raw** | Dagster | Downloads CSVs from GitHub, uploads to S3 |
| **Transformation** | Dagster | Cleans data, validates schema, writes Parquet to S3. Invalid rows quarantined. |
| **Loading** | Dagster | Reads Parquet from S3, loads into DuckDB bronze tables (delete-then-insert) |
| **Modelling** | dbt | Staging → intermediate → dimensions → facts → marts |

Assets are partitioned by `season × gameweek`. Datasets: `matches`, `playermatchstats`, `players`, `playerstats`, `teams`, `player_gameweek_stats`, `fixtures`.

## Local development

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

### 1. Configure environment

Copy `.env.dev` and verify the values. Key variables:

| Variable | Description |
|---|---|
| `S3_BUCKET_NAME` | MinIO bucket name for local dev (`premierlytics-dev-bucket`) |
| `AWS_REGION` | Region (`us-east-1` — MinIO ignores this) |
| `MINIO_ENDPOINT` | `http://minio:9000` — routes boto3 to local MinIO |
| `AWS_ACCESS_KEY_ID` | MinIO root user (used by boto3 credential chain) |
| `AWS_SECRET_ACCESS_KEY` | MinIO root password |
| `DUCKDB_PATH` | Path to DuckDB file inside the container |

### 2. Start services

```bash
make up        # start all services
make build     # rebuild images and start
make down      # stop services
make remove    # stop and remove volumes
```

This starts: Dagster webserver (`:3000`), Dagster daemon, user code gRPC server, MinIO (`:9000`), MinIO init (creates bucket), Postgres, DuckDB init, CloudBeaver (`:8978`).

Open [http://localhost:3000](http://localhost:3000) for the Dagster UI.

### 3. Rebuild user code only

After changing pipeline code:

```bash
make build-code
```

## Running tests

```bash
cd dagster/premierlytics-dagster
uv sync --locked
uv run pytest tests/ -v
```

Tests use `moto` to mock S3 in-process — no running services required.

## Production deployment

### Infrastructure

Terraform manages all AWS resources. From `infra/`:

```bash
terraform init
terraform plan
terraform apply
```

Resources created: S3 bucket, ECR repository, Lambda function, EventBridge rule, IAM roles, EC2 launch template (spot, `t3.small`).

### Building and pushing the Docker image

```bash
aws ecr get-login-password --region eu-west-1 --profile premierlytics | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.eu-west-1.amazonaws.com

docker build -f docker/dagster/Dockerfile.prod -t premierlytics-dagster:latest .

docker tag premierlytics-dagster:latest <ecr-repo-url>:latest
docker push <ecr-repo-url>:latest
```

### Triggering a manual pipeline run

```bash
make invoke-aws-lambda
```

Or from the AWS console: Lambda → `premierlytics-start-pipeline` → Test.

### Running a backfill

To backfill all historical seasons and gameweeks, SSH into a running EC2 instance or run locally:

```bash
# All seasons
python -m premierlytics_dagster.run_backfill

# Specific season up to a gameweek
python -m premierlytics_dagster.run_backfill --season 2024-2025 --max-gw 38
```

After the backfill completes, trigger `fpl_dbt_job` from the Dagster UI to rebuild the full dimensional model.

## Dagster jobs

| Job | Description |
|---|---|
| `fpl_pipeline_job` | Daily run — current gameweek end-to-end including dbt |
| `fpl_backfill_job` | Loads historical partitions into bronze (excludes dbt) |
| `fpl_dbt_job` | Runs all dbt models — use after a backfill |
