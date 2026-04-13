ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace warehouse with the actual table name
CREATE OR REPLACE TABLE warehouse AS
SELECT * FROM read_parquet('s3://ducklake-bench-data/tpcds/sf100/warehouse.parquet');
