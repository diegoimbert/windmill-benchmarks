{{ config(materialized='table') }}

SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, SUM(ss_ext_sales_price) sum_agg
FROM {{ ref('stg_date_dim') }} dt, {{ ref('stg_store_sales') }}, {{ ref('stg_item') }} item
WHERE dt.d_date_sk = stg_store_sales.ss_sold_date_sk
  AND stg_store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128
  AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100
