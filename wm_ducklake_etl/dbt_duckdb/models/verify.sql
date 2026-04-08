{{ config(materialized='table') }}

SELECT 'stg_store_sales' AS tbl, COUNT(*) AS cnt FROM {{ ref('stg_store_sales') }}
UNION ALL SELECT 'stg_catalog_sales', COUNT(*) FROM {{ ref('stg_catalog_sales') }}
UNION ALL SELECT 'stg_web_sales', COUNT(*) FROM {{ ref('stg_web_sales') }}
UNION ALL SELECT 'stg_store_returns', COUNT(*) FROM {{ ref('stg_store_returns') }}
UNION ALL SELECT 'stg_catalog_returns', COUNT(*) FROM {{ ref('stg_catalog_returns') }}
UNION ALL SELECT 'stg_web_returns', COUNT(*) FROM {{ ref('stg_web_returns') }}
UNION ALL SELECT 'stg_inventory', COUNT(*) FROM {{ ref('stg_inventory') }}
UNION ALL SELECT 'stg_customer', COUNT(*) FROM {{ ref('stg_customer') }}
UNION ALL SELECT 'stg_item', COUNT(*) FROM {{ ref('stg_item') }}
UNION ALL SELECT 'wide_store_sales', COUNT(*) FROM {{ ref('wide_store_sales') }}
UNION ALL SELECT 'wide_catalog_sales', COUNT(*) FROM {{ ref('wide_catalog_sales') }}
UNION ALL SELECT 'wide_web_sales', COUNT(*) FROM {{ ref('wide_web_sales') }}
UNION ALL SELECT 'agg_daily_store', COUNT(*) FROM {{ ref('agg_daily_store') }}
UNION ALL SELECT 'agg_monthly_category', COUNT(*) FROM {{ ref('agg_monthly_category') }}
UNION ALL SELECT 'agg_customer_ltv', COUNT(*) FROM {{ ref('agg_customer_ltv') }}
UNION ALL SELECT 'agg_channel_comparison', COUNT(*) FROM {{ ref('agg_channel_comparison') }}
UNION ALL SELECT 'agg_promo_roi', COUNT(*) FROM {{ ref('agg_promo_roi') }}
UNION ALL SELECT 'agg_return_rate', COUNT(*) FROM {{ ref('agg_return_rate') }}
UNION ALL SELECT 'agg_inventory_turnover', COUNT(*) FROM {{ ref('agg_inventory_turnover') }}
ORDER BY tbl
