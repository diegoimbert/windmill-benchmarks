{{ config(materialized='table') }}

WITH sales AS (
  SELECT i.i_category, i.i_class,
    COUNT(*) AS sale_count,
    SUM(ss_net_paid) AS sale_revenue
  FROM {{ ref('stg_store_sales') }} ss
  JOIN {{ ref('stg_item') }} i ON ss.ss_item_sk = i.i_item_sk
  GROUP BY i.i_category, i.i_class
),
returns AS (
  SELECT i.i_category, i.i_class,
    COUNT(*) AS return_count,
    SUM(sr_return_amt) AS return_revenue
  FROM {{ ref('stg_store_returns') }} sr
  JOIN {{ ref('stg_item') }} i ON sr.sr_item_sk = i.i_item_sk
  GROUP BY i.i_category, i.i_class
)
SELECT
  s.i_category,
  s.i_class,
  s.sale_count,
  s.sale_revenue,
  COALESCE(r.return_count, 0) AS return_count,
  COALESCE(r.return_revenue, 0) AS return_revenue,
  CASE WHEN s.sale_count > 0
    THEN ROUND(COALESCE(r.return_count, 0) * 100.0 / s.sale_count, 2)
    ELSE 0
  END AS return_rate_pct,
  CASE WHEN s.sale_revenue > 0
    THEN ROUND(COALESCE(r.return_revenue, 0) * 100.0 / s.sale_revenue, 2)
    ELSE 0
  END AS return_revenue_pct
FROM sales s
LEFT JOIN returns r ON s.i_category = r.i_category AND s.i_class = r.i_class
ORDER BY return_rate_pct DESC
