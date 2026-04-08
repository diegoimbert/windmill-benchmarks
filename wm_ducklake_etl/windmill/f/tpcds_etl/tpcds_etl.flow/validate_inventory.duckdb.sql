SELECT
  'inventory' AS table_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN inv_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
  SUM(CASE WHEN inv_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
  SUM(CASE WHEN inv_warehouse_sk IS NULL THEN 1 ELSE 0 END) AS null_warehouse_sk,
  SUM(CASE WHEN inv_quantity_on_hand < 0 THEN 1 ELSE 0 END) AS neg_quantity
FROM inventory;
