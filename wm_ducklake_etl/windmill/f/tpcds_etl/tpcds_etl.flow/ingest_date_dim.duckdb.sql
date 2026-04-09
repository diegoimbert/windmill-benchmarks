ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace date_dim with the actual table name
CREATE OR REPLACE TABLE date_dim AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/date_dim.parquet');