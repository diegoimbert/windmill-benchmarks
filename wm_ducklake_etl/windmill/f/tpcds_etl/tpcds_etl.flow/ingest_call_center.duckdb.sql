ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace call_center with the actual table name
CREATE OR REPLACE TABLE call_center AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/call_center.parquet');
