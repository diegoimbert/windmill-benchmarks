{{ config(materialized='table') }}

WITH store_agg AS (
  SELECT d.d_year, d.d_moy,
    'store' AS channel,
    SUM(ss_net_paid) AS revenue,
    SUM(ss_net_profit) AS profit,
    COUNT(*) AS transactions,
    COUNT(DISTINCT ss_customer_sk) AS unique_customers
  FROM {{ ref('stg_store_sales') }} ss
  JOIN {{ ref('stg_date_dim') }} d ON ss.ss_sold_date_sk = d.d_date_sk
  GROUP BY d.d_year, d.d_moy
),
catalog_agg AS (
  SELECT d.d_year, d.d_moy,
    'catalog' AS channel,
    SUM(cs_net_paid) AS revenue,
    SUM(cs_net_profit) AS profit,
    COUNT(*) AS transactions,
    COUNT(DISTINCT cs_bill_customer_sk) AS unique_customers
  FROM {{ ref('stg_catalog_sales') }} cs
  JOIN {{ ref('stg_date_dim') }} d ON cs.cs_sold_date_sk = d.d_date_sk
  GROUP BY d.d_year, d.d_moy
),
web_agg AS (
  SELECT d.d_year, d.d_moy,
    'web' AS channel,
    SUM(ws_net_paid) AS revenue,
    SUM(ws_net_profit) AS profit,
    COUNT(*) AS transactions,
    COUNT(DISTINCT ws_bill_customer_sk) AS unique_customers
  FROM {{ ref('stg_web_sales') }} ws
  JOIN {{ ref('stg_date_dim') }} d ON ws.ws_sold_date_sk = d.d_date_sk
  GROUP BY d.d_year, d.d_moy
)
SELECT * FROM store_agg
UNION ALL
SELECT * FROM catalog_agg
UNION ALL
SELECT * FROM web_agg
ORDER BY d_year, d_moy, channel
