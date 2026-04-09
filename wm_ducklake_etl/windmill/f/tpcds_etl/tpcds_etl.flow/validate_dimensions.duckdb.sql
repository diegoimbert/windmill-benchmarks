ATTACH 'ducklake' AS dl;
USE dl;

-- Validate all dimension tables: check primary key uniqueness and not-null
SELECT 'customer' AS tbl, COUNT(*) AS total, COUNT(DISTINCT c_customer_sk) AS distinct_pk FROM customer
UNION ALL
SELECT 'item', COUNT(*), COUNT(DISTINCT i_item_sk) FROM item
UNION ALL
SELECT 'store', COUNT(*), COUNT(DISTINCT s_store_sk) FROM store
UNION ALL
SELECT 'date_dim', COUNT(*), COUNT(DISTINCT d_date_sk) FROM date_dim
UNION ALL
SELECT 'time_dim', COUNT(*), COUNT(DISTINCT t_time_sk) FROM time_dim
UNION ALL
SELECT 'promotion', COUNT(*), COUNT(DISTINCT p_promo_sk) FROM promotion
UNION ALL
SELECT 'warehouse', COUNT(*), COUNT(DISTINCT w_warehouse_sk) FROM warehouse
UNION ALL
SELECT 'customer_address', COUNT(*), COUNT(DISTINCT ca_address_sk) FROM customer_address
UNION ALL
SELECT 'customer_demographics', COUNT(*), COUNT(DISTINCT cd_demo_sk) FROM customer_demographics
UNION ALL
SELECT 'household_demographics', COUNT(*), COUNT(DISTINCT hd_demo_sk) FROM household_demographics
UNION ALL
SELECT 'catalog_page', COUNT(*), COUNT(DISTINCT cp_catalog_page_sk) FROM catalog_page
UNION ALL
SELECT 'web_page', COUNT(*), COUNT(DISTINCT wp_web_page_sk) FROM web_page
UNION ALL
SELECT 'web_site', COUNT(*), COUNT(DISTINCT web_site_sk) FROM web_site
UNION ALL
SELECT 'call_center', COUNT(*), COUNT(DISTINCT cc_call_center_sk) FROM call_center
UNION ALL
SELECT 'income_band', COUNT(*), COUNT(DISTINCT ib_income_band_sk) FROM income_band
UNION ALL
SELECT 'reason', COUNT(*), COUNT(DISTINCT r_reason_sk) FROM reason
UNION ALL
SELECT 'ship_mode', COUNT(*), COUNT(DISTINCT sm_ship_mode_sk) FROM ship_mode
ORDER BY tbl;