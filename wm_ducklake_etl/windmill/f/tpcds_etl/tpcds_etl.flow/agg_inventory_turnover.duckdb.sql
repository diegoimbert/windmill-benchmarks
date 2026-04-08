CREATE OR REPLACE TABLE inventory_turnover AS
WITH avg_inventory AS (
  SELECT
    inv_item_sk,
    inv_warehouse_sk,
    AVG(inv_quantity_on_hand) AS avg_qty_on_hand
  FROM inventory
  GROUP BY inv_item_sk, inv_warehouse_sk
),
sales_velocity AS (
  SELECT
    ss_item_sk,
    ss_store_sk,
    SUM(ss_quantity) AS total_sold,
    COUNT(DISTINCT ss_sold_date_sk) AS selling_days
  FROM store_sales
  WHERE ss_quantity IS NOT NULL
  GROUP BY ss_item_sk, ss_store_sk
)
SELECT
  i.i_item_id,
  i.i_product_name,
  i.i_category,
  w.w_warehouse_name,
  w.w_state,
  ai.avg_qty_on_hand,
  COALESCE(sv.total_sold, 0) AS total_sold,
  CASE WHEN ai.avg_qty_on_hand > 0
    THEN ROUND(COALESCE(sv.total_sold, 0)::DOUBLE / ai.avg_qty_on_hand, 2)
    ELSE 0
  END AS turnover_ratio
FROM avg_inventory ai
JOIN item i ON ai.inv_item_sk = i.i_item_sk
JOIN warehouse w ON ai.inv_warehouse_sk = w.w_warehouse_sk
LEFT JOIN sales_velocity sv ON ai.inv_item_sk = sv.ss_item_sk
ORDER BY turnover_ratio DESC;
