CREATE OR REPLACE TABLE inventory_turnover AS
WITH inv_by_category AS (
  SELECT
    i.i_category,
    w.w_warehouse_name,
    w.w_state,
    AVG(inv.inv_quantity_on_hand) AS avg_qty_on_hand,
    SUM(inv.inv_quantity_on_hand) AS total_on_hand
  FROM inventory inv
  JOIN item i ON inv.inv_item_sk = i.i_item_sk
  JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
  GROUP BY i.i_category, w.w_warehouse_name, w.w_state
),
sales_by_category AS (
  SELECT
    i.i_category,
    SUM(ss.ss_quantity) AS total_sold
  FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  WHERE ss.ss_quantity IS NOT NULL
  GROUP BY i.i_category
)
SELECT
  ic.i_category,
  ic.w_warehouse_name,
  ic.w_state,
  ic.avg_qty_on_hand,
  ic.total_on_hand,
  COALESCE(sc.total_sold, 0) AS total_sold,
  CASE WHEN ic.avg_qty_on_hand > 0
    THEN ROUND(COALESCE(sc.total_sold, 0)::DOUBLE / ic.avg_qty_on_hand, 2)
    ELSE 0
  END AS turnover_ratio
FROM inv_by_category ic
LEFT JOIN sales_by_category sc ON ic.i_category = sc.i_category
ORDER BY turnover_ratio DESC;
