-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace time_dim with the actual table name
CREATE OR REPLACE TABLE time_dim AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/time_dim.parquet');
