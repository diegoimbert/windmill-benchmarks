-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace {{TABLE_NAME}} with the actual table name
CREATE OR REPLACE TABLE {{TABLE_NAME}} AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/{{TABLE_NAME}}.parquet');
