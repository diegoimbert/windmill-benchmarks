-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace customer_demographics with the actual table name
CREATE OR REPLACE TABLE customer_demographics AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/customer_demographics.parquet');
