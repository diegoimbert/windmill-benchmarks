-- Snowflake setup for TPC-DS SF100 ETL benchmark
-- Run this once as ACCOUNTADMIN or a role with CREATE WAREHOUSE / DATABASE privileges.

-- 1. Warehouse (XSMALL to keep costs low; benchmark measures total wall-clock time)
CREATE WAREHOUSE IF NOT EXISTS tpcds_bench_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME  = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- 2. Database & schema
CREATE DATABASE IF NOT EXISTS tpcds_bench;
USE DATABASE tpcds_bench;
CREATE SCHEMA IF NOT EXISTS etl;
USE SCHEMA etl;

-- 3. External stage pointing to the S3 bucket with TPC-DS SF100 parquet files
CREATE OR REPLACE STAGE tpcds_stage
  URL = 's3://bench-data/tpcds/sf100/'
  FILE_FORMAT = (TYPE = PARQUET);

-- 4. Activate warehouse for subsequent operations
USE WAREHOUSE tpcds_bench_wh;
