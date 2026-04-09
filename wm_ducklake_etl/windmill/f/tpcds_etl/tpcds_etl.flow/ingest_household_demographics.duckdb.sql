ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace household_demographics with the actual table name
CREATE OR REPLACE TABLE household_demographics AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/household_demographics.parquet');