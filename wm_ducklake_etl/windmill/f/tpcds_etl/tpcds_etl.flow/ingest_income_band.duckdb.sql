-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace income_band with the actual table name
CREATE OR REPLACE TABLE income_band AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/income_band.parquet');
