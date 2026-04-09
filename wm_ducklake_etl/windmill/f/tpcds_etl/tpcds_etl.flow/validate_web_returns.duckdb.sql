ATTACH 'ducklake' AS dl;
USE dl;

SELECT
  'web_returns' AS table_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN wr_returned_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
  SUM(CASE WHEN wr_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
  SUM(CASE WHEN wr_return_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
  SUM(CASE WHEN wr_return_amt < 0 THEN 1 ELSE 0 END) AS neg_amount
FROM web_returns;