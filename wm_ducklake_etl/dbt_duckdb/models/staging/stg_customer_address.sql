{{ config(materialized='table') }}

SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/customer_address.parquet')
