-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace reason with the actual table name
CREATE OR REPLACE TABLE reason AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/reason.parquet');
