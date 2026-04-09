# TPC-DS ETL Benchmark: Windmill+DuckDB vs The World

ETL benchmark comparing **Windmill + DuckDB/Ducklake** against **Airflow, Dagster, dbt, and Snowflake** using the industry-standard TPC-DS dataset at scale factor 100 (~30 GB Parquet, 288M+ store sales rows).

## The Story

You're a mid-size retailer. You sell through stores, a catalog, and your website. Every night, you run a pipeline that ingests the day's data, validates quality, builds analytics tables, and refreshes dashboards.

This benchmark measures how long that pipeline takes — and how much of that time is wasted on orchestrator overhead vs. actual computation.

## Pipeline: 6 Stages, ~53 Tasks

```
STAGE 1: INGEST (24 parallel tasks)
   Load each of the 24 TPC-DS tables from Parquet/S3
         │
         ▼
STAGE 2: VALIDATE (8 parallel tasks)
   Per-table data quality checks (nulls, ranges, FK integrity)
         │
         ▼
STAGE 3: DENORMALIZE (3 parallel tasks)
   Join each sales channel with dimensions → wide tables
         │
         ▼
STAGE 4: AGGREGATE (7 parallel tasks)
   Build analytics tables (daily by store, monthly by category, customer LTV, etc.)
         │
         ▼
STAGE 5: ANALYTICAL QUERIES (10 parallel tasks)
   Run 10 representative TPC-DS queries (q3, q7, q19, q27, q34, q43, q46, q53, q67, q79)
         │
         ▼
STAGE 6: VERIFY (1 task)
   Check row counts across all output tables
```

## Competitors

| Stack                   | Orchestrator | Compute                       | Directory         |
| ----------------------- | ------------ | ----------------------------- | ----------------- |
| **Windmill + DuckDB**   | Windmill     | DuckDB (native SQL steps)     | `windmill/`       |
| **Airflow + DuckDB**    | Airflow      | DuckDB (via Python)           | `airflow_duckdb/` |
| **Airflow + Pandas**    | Airflow      | Pandas                        | `airflow_pandas/` |
| **Dagster + DuckDB**    | Dagster      | DuckDB (asset-based)          | `dagster_duckdb/` |
| **dbt + DuckDB**        | dbt CLI      | DuckDB (dbt-duckdb adapter)   | `dbt_duckdb/`     |
| **Airflow + Snowflake** | Airflow      | Snowflake (X-Small warehouse) | `snowflake/`      |

## Quick Start (Local)

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- DuckDB CLI (`brew install duckdb` or `pip install duckdb`)
- [Windmill CLI](https://www.windmill.dev/docs/advanced/cli) (`npm install -g windmill-cli`)

### 1. Generate TPC-DS data

```bash
# Start MinIO first (any competitor's docker-compose includes it)
docker compose -f windmill/docker-compose.yml up minio -d

# Generate SF1 for quick testing, SF100 for the real benchmark
python scripts/generate_data.py --sf 1 --endpoint localhost:9000
```

### 2. Run a benchmark

```bash
# Run Windmill benchmark locally
./scripts/run_benchmark.sh windmill local

# Run Airflow+DuckDB benchmark locally
./scripts/run_benchmark.sh airflow_duckdb local

# Run all benchmarks
for c in windmill airflow_duckdb airflow_pandas dagster_duckdb dbt_duckdb snowflake; do
  ./scripts/run_benchmark.sh $c local
done
```

### 3. Compare results

```bash
pip install -r scripts/requirements.txt
python scripts/compare.py results/ -o results/report.md
```

## Deploy on Kubernetes

### 1. Provision infrastructure

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars  # edit with your AWS settings
terraform init && terraform apply

# Configure kubectl
aws eks update-kubeconfig --name ducklake-bench --region us-east-1
```

### 2. Deploy shared infrastructure

```bash
kubectl apply -f infra/k8s/namespace.yaml
kubectl apply -f infra/k8s/resource-limits.yaml
kubectl apply -f infra/k8s/minio.yaml

# Wait for MinIO
kubectl -n bench wait --for=condition=ready pod -l app=minio --timeout=120s

# Port-forward MinIO and generate data
kubectl -n bench port-forward svc/minio 9000:9000 &
python scripts/generate_data.py --sf 100 --endpoint localhost:9000
```

### 3. Run benchmarks on K8s

```bash
./scripts/run_benchmark.sh windmill k8s
./scripts/run_benchmark.sh airflow_duckdb k8s
# etc.
```

## Hardware

All self-hosted benchmarks run on identical hardware:

|                         | Spec                                      |
| ----------------------- | ----------------------------------------- |
| **Instance**            | m6i.4xlarge (16 vCPU, 64 GB RAM)          |
| **Storage**             | gp3, 500 GB, 3000 IOPS                    |
| **Region**              | us-east-1                                 |
| **Worker limits**       | 8 CPU, 32 GB (Docker/K8s resource limits) |
| **Orchestrator limits** | 4 CPU, 8 GB                               |

## Measurement

- 5 runs per competitor, report median and p95
- OS page cache flushed between runs
- Per-task metrics: queue time, execution time, transition time
- Cost: `wall_clock_hours × $0.768/hr` for self-hosted, billed amount for Snowflake

## What We Measure

| Metric                     | Why                                                             |
| -------------------------- | --------------------------------------------------------------- |
| **End-to-end wall-clock**  | Headline number                                                 |
| **Orchestration overhead** | Sum of queue + transition times — where Windmill differentiates |
| **Pure compute time**      | Isolates engine performance (DuckDB vs Pandas vs Snowflake)     |
| **Cost per run**           | Self-hosted = time × instance rate. Cloud = billed              |
| **Peak memory**            | DuckDB efficiency vs Pandas                                     |

## Directory Structure

```
wm_ducklake_etl/
├── scripts/
│   ├── sql/                    # Shared SQL (the actual ETL logic)
│   ├── generate_data.py        # TPC-DS data generation
│   ├── generate_windmill_flow.py  # Generates Windmill flow.yaml from SQL
│   ├── run_benchmark.sh        # Main benchmark runner
│   ├── collect_*.py            # Per-competitor timing collection
│   └── compare.py              # Results aggregation + report
├── windmill/                   # Windmill + DuckDB implementation
├── airflow_duckdb/             # Airflow + DuckDB implementation
├── airflow_pandas/             # Airflow + Pandas implementation
├── dagster_duckdb/             # Dagster + DuckDB implementation
├── dbt_duckdb/                 # dbt + DuckDB implementation
├── snowflake/                  # Airflow + Snowflake implementation
├── infra/
│   ├── terraform/              # AWS EKS provisioning
│   └── k8s/                    # Shared K8s manifests (MinIO, limits)
└── results/                    # Benchmark output
```
