SELECT
  'web_sales' AS table_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN ws_sold_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
  SUM(CASE WHEN ws_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
  SUM(CASE WHEN ws_bill_customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer_sk,
  SUM(CASE WHEN ws_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
  SUM(CASE WHEN ws_sales_price < 0 THEN 1 ELSE 0 END) AS neg_sales_price,
  SUM(CASE WHEN ws_sold_date_sk IS NOT NULL AND d.d_date_sk IS NULL THEN 1 ELSE 0 END) AS broken_date_fk,
  SUM(CASE WHEN ws_item_sk IS NOT NULL AND i.i_item_sk IS NULL THEN 1 ELSE 0 END) AS broken_item_fk
FROM web_sales s
LEFT JOIN date_dim d ON s.ws_sold_date_sk = d.d_date_sk
LEFT JOIN item i ON s.ws_item_sk = i.i_item_sk;
