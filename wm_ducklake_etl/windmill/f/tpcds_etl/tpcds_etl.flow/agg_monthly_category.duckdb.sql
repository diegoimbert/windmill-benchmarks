ATTACH 'ducklake' AS dl;
USE dl;

CREATE OR REPLACE TABLE monthly_sales_by_category AS
SELECT
  d_year,
  d_moy,
  i_category,
  i_class,
  COUNT(*) AS transaction_count,
  SUM(ss_quantity) AS total_units,
  SUM(ss_sales_price) AS total_sales,
  SUM(ss_net_profit) AS net_profit,
  AVG(ss_sales_price) AS avg_sale_price,
  COUNT(DISTINCT c_customer_id) AS unique_customers
FROM wide_store_sales
GROUP BY d_year, d_moy, i_category, i_class
ORDER BY d_year, d_moy, total_sales DESC;
