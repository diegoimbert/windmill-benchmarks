{{ config(materialized='table') }}

SELECT
  cs.*,
  d.d_date, d.d_month_seq, d.d_year, d.d_moy, d.d_dom, d.d_qoy, d.d_day_name, d.d_weekend,
  c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_country, c.c_email_address,
  i.i_item_id, i.i_product_name, i.i_category, i.i_class, i.i_brand, i.i_manufact, i.i_current_price,
  p.p_promo_id, p.p_promo_name, p.p_channel_tv, p.p_channel_radio, p.p_channel_email, p.p_discount_active,
  cp.cp_department, cp.cp_catalog_number, cp.cp_catalog_page_number,
  sm.sm_type AS ship_type, sm.sm_carrier AS ship_carrier,
  w.w_warehouse_name, w.w_city AS w_city, w.w_state AS w_state
FROM {{ ref('stg_catalog_sales') }} cs
JOIN {{ ref('stg_date_dim') }} d ON cs.cs_sold_date_sk = d.d_date_sk
LEFT JOIN {{ ref('stg_customer') }} c ON cs.cs_bill_customer_sk = c.c_customer_sk
JOIN {{ ref('stg_item') }} i ON cs.cs_item_sk = i.i_item_sk
LEFT JOIN {{ ref('stg_promotion') }} p ON cs.cs_promo_sk = p.p_promo_sk
LEFT JOIN {{ ref('stg_catalog_page') }} cp ON cs.cs_catalog_page_sk = cp.cp_catalog_page_sk
LEFT JOIN {{ ref('stg_ship_mode') }} sm ON cs.cs_ship_mode_sk = sm.sm_ship_mode_sk
LEFT JOIN {{ ref('stg_warehouse') }} w ON cs.cs_warehouse_sk = w.w_warehouse_sk
