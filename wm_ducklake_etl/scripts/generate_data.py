#!/usr/bin/env python3
"""
Generate TPC-DS data at a given scale factor and upload to S3/MinIO.

Usage:
  python generate_data.py --sf 100 --endpoint localhost:9000 --bucket bench-data
  python generate_data.py --sf 1   # SF1 for local testing, defaults to local MinIO
"""

import argparse
import os
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-DS data and upload to S3/MinIO")
    parser.add_argument("--sf", type=int, default=100, help="TPC-DS scale factor (default: 100)")
    parser.add_argument("--endpoint", default="localhost:9000", help="S3/MinIO endpoint")
    parser.add_argument("--bucket", default="bench-data", help="S3 bucket name")
    parser.add_argument("--access-key", default=os.environ.get("S3_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", default=os.environ.get("S3_SECRET_KEY", "minioadmin"))
    parser.add_argument("--region", default=os.environ.get("S3_REGION", "us-east-1"))
    parser.add_argument("--output-dir", default="/tmp/tpcds_data", help="Local temp directory for parquet export")
    args = parser.parse_args()

    output_dir = os.path.join(args.output_dir, f"sf{args.sf}")
    os.makedirs(output_dir, exist_ok=True)

    print(f"=== Generating TPC-DS SF{args.sf} data ===")
    t0 = time.time()

    # Generate data using DuckDB's built-in TPC-DS extension
    duckdb_sql = f"""
        INSTALL tpcds;
        LOAD tpcds;
        CALL dsdgen(sf={args.sf});
        EXPORT DATABASE '{output_dir}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    result = subprocess.run(
        ["duckdb", "-c", duckdb_sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"ERROR generating data: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - t0
    print(f"Data generated in {elapsed:.1f}s")

    # List generated files
    parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
    total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in parquet_files)
    print(f"Generated {len(parquet_files)} parquet files, total size: {total_size / (1024**3):.1f} GB")

    # Upload to S3/MinIO
    print(f"\n=== Uploading to s3://{args.bucket}/tpcds/sf{args.sf}/ ===")
    t0 = time.time()

    s3_prefix = f"tpcds/sf{args.sf}"

    # Configure mc (MinIO client) or use aws cli
    upload_env = os.environ.copy()
    upload_env.update({
        "AWS_ACCESS_KEY_ID": args.access_key,
        "AWS_SECRET_ACCESS_KEY": args.secret_key,
        "AWS_DEFAULT_REGION": args.region,
    })

    # Create bucket if it doesn't exist
    subprocess.run(
        ["aws", "s3", "mb", f"s3://{args.bucket}",
         "--endpoint-url", f"http://{args.endpoint}"],
        env=upload_env, capture_output=True
    )

    # Upload all parquet files
    result = subprocess.run(
        ["aws", "s3", "sync", output_dir, f"s3://{args.bucket}/{s3_prefix}/",
         "--endpoint-url", f"http://{args.endpoint}",
         "--exclude", "*", "--include", "*.parquet"],
        env=upload_env, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"ERROR uploading: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - t0
    print(f"Upload completed in {elapsed:.1f}s")
    print(f"\nData available at: s3://{args.bucket}/{s3_prefix}/")
    print("Tables:", ", ".join(f.replace(".parquet", "") for f in sorted(parquet_files)))


if __name__ == "__main__":
    main()
