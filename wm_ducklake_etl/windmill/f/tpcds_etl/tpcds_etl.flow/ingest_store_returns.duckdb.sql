-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace store_returns with the actual table name
CREATE OR REPLACE TABLE store_returns AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/store_returns.parquet');
