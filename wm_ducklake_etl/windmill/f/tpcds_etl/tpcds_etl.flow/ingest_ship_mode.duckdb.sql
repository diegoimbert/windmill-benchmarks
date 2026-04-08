-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace ship_mode with the actual table name
CREATE OR REPLACE TABLE ship_mode AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/ship_mode.parquet');
