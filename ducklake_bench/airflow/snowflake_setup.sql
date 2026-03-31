-- Run this in Snowflake before launching the DAG
-- Creates the database and warehouse used by the benchmark

CREATE DATABASE IF NOT EXISTS BENCHMARK;
USE DATABASE BENCHMARK;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- Use a small warehouse to keep it fair — scale up if you want to test bigger
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
