ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace web_returns with the actual table name
CREATE OR REPLACE TABLE web_returns AS
SELECT * FROM read_parquet('s3://ducklake-bench-data/tpcds/sf100/web_returns.parquet');
