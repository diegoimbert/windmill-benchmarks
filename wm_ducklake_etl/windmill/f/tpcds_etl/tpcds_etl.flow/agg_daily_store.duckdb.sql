ATTACH 'ducklake' AS dl;
USE dl;

CREATE OR REPLACE TABLE daily_sales_by_store AS
SELECT
  d_date,
  d_day_name,
  d_weekend,
  s_store_id,
  s_store_name,
  s_state,
  COUNT(*) AS transaction_count,
  SUM(ss_quantity) AS total_units,
  SUM(ss_sales_price) AS total_sales,
  SUM(ss_net_profit) AS net_profit,
  AVG(ss_sales_price) AS avg_sale_price,
  SUM(ss_coupon_amt) AS total_coupons
FROM wide_store_sales
GROUP BY d_date, d_day_name, d_weekend, s_store_id, s_store_name, s_state
ORDER BY d_date, s_store_id;
