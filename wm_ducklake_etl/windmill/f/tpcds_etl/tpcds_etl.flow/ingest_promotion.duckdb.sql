-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace promotion with the actual table name
CREATE OR REPLACE TABLE promotion AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/promotion.parquet');
