-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace web_site with the actual table name
CREATE OR REPLACE TABLE web_site AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/web_site.parquet');
