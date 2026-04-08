CREATE OR REPLACE TABLE promo_roi AS
SELECT
  p_promo_id,
  p_promo_name,
  p_channel_tv,
  p_channel_radio,
  p_channel_email,
  p_discount_active,
  COUNT(*) AS promo_transactions,
  SUM(ss_sales_price) AS promo_revenue,
  SUM(ss_coupon_amt) AS total_coupon_discount,
  SUM(ss_net_profit) AS promo_profit,
  AVG(ss_sales_price) AS avg_sale_with_promo,
  (SELECT AVG(ss_sales_price) FROM store_sales WHERE ss_promo_sk IS NULL) AS avg_sale_no_promo
FROM wide_store_sales
WHERE p_promo_id IS NOT NULL
GROUP BY p_promo_id, p_promo_name, p_channel_tv, p_channel_radio, p_channel_email, p_discount_active
ORDER BY promo_revenue DESC;
