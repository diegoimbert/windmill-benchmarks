CREATE OR REPLACE TABLE wide_store_sales AS
SELECT
  ss.*,
  d.d_date, d.d_month_seq, d.d_year, d.d_moy, d.d_dom, d.d_qoy, d.d_day_name, d.d_weekend,
  c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_country, c.c_email_address,
  i.i_item_id, i.i_product_name, i.i_category, i.i_class, i.i_brand, i.i_manufact, i.i_current_price,
  s.s_store_id, s.s_store_name, s.s_city AS s_city, s.s_state AS s_state, s.s_zip AS s_zip,
  p.p_promo_id, p.p_promo_name, p.p_channel_tv, p.p_channel_radio, p.p_channel_email, p.p_discount_active,
  cd.cd_gender, cd.cd_marital_status, cd.cd_education_status,
  hd.hd_buy_potential, hd.hd_dep_count, hd.hd_vehicle_count
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
LEFT JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
LEFT JOIN store s ON ss.ss_store_sk = s.s_store_sk
LEFT JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
LEFT JOIN customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
LEFT JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk;
