{{ config(materialized='table') }}

WITH store_totals AS (
  SELECT
    ss_customer_sk AS customer_sk,
    SUM(ss_net_paid) AS store_spend,
    COUNT(*) AS store_txns
  FROM {{ ref('stg_store_sales') }}
  WHERE ss_customer_sk IS NOT NULL
  GROUP BY ss_customer_sk
),
catalog_totals AS (
  SELECT
    cs_bill_customer_sk AS customer_sk,
    SUM(cs_net_paid) AS catalog_spend,
    COUNT(*) AS catalog_txns
  FROM {{ ref('stg_catalog_sales') }}
  WHERE cs_bill_customer_sk IS NOT NULL
  GROUP BY cs_bill_customer_sk
),
web_totals AS (
  SELECT
    ws_bill_customer_sk AS customer_sk,
    SUM(ws_net_paid) AS web_spend,
    COUNT(*) AS web_txns
  FROM {{ ref('stg_web_sales') }}
  WHERE ws_bill_customer_sk IS NOT NULL
  GROUP BY ws_bill_customer_sk
)
SELECT
  c.c_customer_sk,
  c.c_customer_id,
  c.c_first_name,
  c.c_last_name,
  COALESCE(st.store_spend, 0) + COALESCE(ct.catalog_spend, 0) + COALESCE(wt.web_spend, 0) AS total_spend,
  COALESCE(st.store_txns, 0) + COALESCE(ct.catalog_txns, 0) + COALESCE(wt.web_txns, 0) AS total_transactions,
  COALESCE(st.store_spend, 0) AS store_spend,
  COALESCE(ct.catalog_spend, 0) AS catalog_spend,
  COALESCE(wt.web_spend, 0) AS web_spend,
  CASE
    WHEN COALESCE(st.store_txns, 0) + COALESCE(ct.catalog_txns, 0) + COALESCE(wt.web_txns, 0) > 0
    THEN (COALESCE(st.store_spend, 0) + COALESCE(ct.catalog_spend, 0) + COALESCE(wt.web_spend, 0))
         / (COALESCE(st.store_txns, 0) + COALESCE(ct.catalog_txns, 0) + COALESCE(wt.web_txns, 0))
    ELSE 0
  END AS avg_basket_size
FROM {{ ref('stg_customer') }} c
LEFT JOIN store_totals st ON c.c_customer_sk = st.customer_sk
LEFT JOIN catalog_totals ct ON c.c_customer_sk = ct.customer_sk
LEFT JOIN web_totals wt ON c.c_customer_sk = wt.customer_sk
ORDER BY total_spend DESC
