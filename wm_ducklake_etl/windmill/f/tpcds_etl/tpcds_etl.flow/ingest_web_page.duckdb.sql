ATTACH 'ducklake' AS dl;
USE dl;

-- Generic ingest template: load a single TPC-DS table from Parquet on S3
-- Usage: replace web_page with the actual table name
CREATE OR REPLACE TABLE web_page AS
SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/web_page.parquet');