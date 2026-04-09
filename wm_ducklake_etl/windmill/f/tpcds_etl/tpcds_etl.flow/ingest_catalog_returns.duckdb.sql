ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace catalog_returns with the actual table name
CREATE OR REPLACE TABLE catalog_returns AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/catalog_returns.parquet');
