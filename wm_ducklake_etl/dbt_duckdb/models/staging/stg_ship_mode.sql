{{ config(materialized='table') }}

SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/ship_mode.parquet')
