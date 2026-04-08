#!/usr/bin/env python3
"""
Generate the Windmill flow.yaml and all inline SQL scripts for the TPC-DS ETL benchmark.
This creates ~69 tasks organized in 6 stages with branchall fan-out.
"""

import os
import shutil

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLOW_DIR = os.path.join(BASE, "windmill", "f", "tpcds_etl", "tpcds_etl.flow")
SQL_DIR = os.path.join(BASE, "scripts", "sql")

TABLES = [
    "store_sales", "catalog_sales", "web_sales",
    "store_returns", "catalog_returns", "web_returns",
    "inventory", "customer", "customer_address", "customer_demographics",
    "household_demographics", "item", "store", "date_dim", "time_dim",
    "promotion", "warehouse", "catalog_page", "web_page", "web_site",
    "call_center", "income_band", "reason", "ship_mode",
]

CHANNELS = ["store_sales", "catalog_sales", "web_sales"]

AGGREGATES = [
    "daily_store", "monthly_category", "customer_ltv",
    "channel_comparison", "promo_roi", "return_rate", "inventory_turnover",
]

QUERIES = ["q03", "q07", "q19", "q27", "q34", "q43", "q46", "q53", "q67", "q79"]

# Validate SQL files that exist (fact + inventory have dedicated files, dims share one)
VALIDATE_FACT_TABLES = [
    "store_sales", "catalog_sales", "web_sales",
    "store_returns", "catalog_returns", "web_returns", "inventory",
]


def read_sql(path):
    with open(path) as f:
        return f.read()


def write_inline(name, content):
    path = os.path.join(FLOW_DIR, name)
    with open(path, "w") as f:
        f.write(content)


def main():
    os.makedirs(FLOW_DIR, exist_ok=True)

    # --- Generate inline SQL files ---

    # Ingest: one per table
    ingest_template = read_sql(os.path.join(SQL_DIR, "ingest", "ingest_table.sql"))
    for table in TABLES:
        sql = ingest_template.replace("{{TABLE_NAME}}", table)
        write_inline(f"ingest_{table}.duckdb.sql", sql)

    # Validate: dedicated files for facts, one shared for dimensions
    for table in VALIDATE_FACT_TABLES:
        src = os.path.join(SQL_DIR, "validate", f"validate_{table}.sql")
        content = read_sql(src)
        write_inline(f"validate_{table}.duckdb.sql", content)
    # Dimensions validation
    content = read_sql(os.path.join(SQL_DIR, "validate", "validate_dimensions.sql"))
    write_inline("validate_dimensions.duckdb.sql", content)

    # Denormalize: 3 channels
    for channel in CHANNELS:
        src = os.path.join(SQL_DIR, "denormalize", f"denorm_{channel}.sql")
        content = read_sql(src)
        write_inline(f"denorm_{channel}.duckdb.sql", content)

    # Aggregate: 7 files
    for agg in AGGREGATES:
        src = os.path.join(SQL_DIR, "aggregate", f"agg_{agg}.sql")
        content = read_sql(src)
        write_inline(f"agg_{agg}.duckdb.sql", content)

    # Queries: 10 files
    for q in QUERIES:
        src = os.path.join(SQL_DIR, "queries", f"{q}.sql")
        content = read_sql(src)
        write_inline(f"{q}.duckdb.sql", content)

    # Verify
    content = read_sql(os.path.join(SQL_DIR, "verify.sql"))
    write_inline("verify.duckdb.sql", content)

    # --- Generate flow.yaml ---

    def make_branch(summary, module_id, inline_file):
        return f"""          - summary: '{summary}'
            skip_failure: false
            modules:
              - id: {module_id}
                value:
                  type: rawscript
                  language: duckdb
                  content: !inline {inline_file}
                  lock: ''
                  input_transforms: {{}}"""

    branches_ingest = "\n".join(
        make_branch(f"Ingest {t}", f"ingest_{t}", f"ingest_{t}.duckdb.sql")
        for t in TABLES
    )

    validate_tables = VALIDATE_FACT_TABLES + ["dimensions"]
    branches_validate = "\n".join(
        make_branch(f"Validate {t}", f"validate_{t}", f"validate_{t}.duckdb.sql")
        for t in validate_tables
    )

    branches_denorm = "\n".join(
        make_branch(f"Denormalize {c}", f"denorm_{c}", f"denorm_{c}.duckdb.sql")
        for c in CHANNELS
    )

    branches_agg = "\n".join(
        make_branch(f"Aggregate {a}", f"agg_{a}", f"agg_{a}.duckdb.sql")
        for a in AGGREGATES
    )

    branches_queries = "\n".join(
        make_branch(f"TPC-DS {q.upper()}", q, f"{q}.duckdb.sql")
        for q in QUERIES
    )

    flow_yaml = f"""summary: TPC-DS ETL Benchmark
description: >
  TPC-DS SF100 ETL benchmark. 6 stages with fan-out parallelism.
  ~69 tasks: ingest (24) -> validate (8) -> denormalize (3) -> aggregate (7) -> queries (10) -> verify (1).
value:
  modules:
    - id: stage_ingest
      summary: 'Stage 1: Ingest 24 tables from S3'
      value:
        type: branchall
        parallel: true
        branches:
{branches_ingest}

    - id: stage_validate
      summary: 'Stage 2: Validate data quality'
      value:
        type: branchall
        parallel: true
        branches:
{branches_validate}

    - id: stage_denormalize
      summary: 'Stage 3: Build wide denormalized tables'
      value:
        type: branchall
        parallel: true
        branches:
{branches_denorm}

    - id: stage_aggregate
      summary: 'Stage 4: Build aggregate tables'
      value:
        type: branchall
        parallel: true
        branches:
{branches_agg}

    - id: stage_queries
      summary: 'Stage 5: Run analytical queries'
      value:
        type: branchall
        parallel: true
        branches:
{branches_queries}

    - id: stage_verify
      summary: 'Stage 6: Verify row counts'
      value:
        type: rawscript
        language: duckdb
        content: !inline verify.duckdb.sql
        lock: ''
        input_transforms: {{}}
schema:
  $schema: 'https://json-schema.org/draft/2020-12/schema'
  type: object
  order: []
  properties: {{}}
  required: []
ws_error_handler_muted: false
"""

    with open(os.path.join(FLOW_DIR, "flow.yaml"), "w") as f:
        f.write(flow_yaml)

    # Count files
    files = [f for f in os.listdir(FLOW_DIR) if f.endswith(".sql") or f == "flow.yaml"]
    print(f"Generated {len(files)} files in {FLOW_DIR}")
    print(f"  - Ingest: {len(TABLES)} tasks")
    print(f"  - Validate: {len(validate_tables)} tasks")
    print(f"  - Denormalize: {len(CHANNELS)} tasks")
    print(f"  - Aggregate: {len(AGGREGATES)} tasks")
    print(f"  - Queries: {len(QUERIES)} tasks")
    print(f"  - Verify: 1 task")
    total = len(TABLES) + len(validate_tables) + len(CHANNELS) + len(AGGREGATES) + len(QUERIES) + 1
    print(f"  Total: {total} tasks")


if __name__ == "__main__":
    main()
