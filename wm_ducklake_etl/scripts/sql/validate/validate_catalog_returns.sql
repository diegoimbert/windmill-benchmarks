SELECT
  'catalog_returns' AS table_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN cr_returned_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
  SUM(CASE WHEN cr_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
  SUM(CASE WHEN cr_return_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
  SUM(CASE WHEN cr_return_amount < 0 THEN 1 ELSE 0 END) AS neg_amount
FROM catalog_returns;
