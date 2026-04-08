SELECT 'store_sales' AS tbl, COUNT(*) AS cnt FROM store_sales
UNION ALL SELECT 'catalog_sales', COUNT(*) FROM catalog_sales
UNION ALL SELECT 'web_sales', COUNT(*) FROM web_sales
UNION ALL SELECT 'store_returns', COUNT(*) FROM store_returns
UNION ALL SELECT 'catalog_returns', COUNT(*) FROM catalog_returns
UNION ALL SELECT 'web_returns', COUNT(*) FROM web_returns
UNION ALL SELECT 'inventory', COUNT(*) FROM inventory
UNION ALL SELECT 'customer', COUNT(*) FROM customer
UNION ALL SELECT 'item', COUNT(*) FROM item
UNION ALL SELECT 'wide_store_sales', COUNT(*) FROM wide_store_sales
UNION ALL SELECT 'wide_catalog_sales', COUNT(*) FROM wide_catalog_sales
UNION ALL SELECT 'wide_web_sales', COUNT(*) FROM wide_web_sales
UNION ALL SELECT 'daily_sales_by_store', COUNT(*) FROM daily_sales_by_store
UNION ALL SELECT 'monthly_sales_by_category', COUNT(*) FROM monthly_sales_by_category
UNION ALL SELECT 'customer_lifetime_value', COUNT(*) FROM customer_lifetime_value
UNION ALL SELECT 'channel_comparison', COUNT(*) FROM channel_comparison
UNION ALL SELECT 'promo_roi', COUNT(*) FROM promo_roi
UNION ALL SELECT 'return_rate_by_category', COUNT(*) FROM return_rate_by_category
UNION ALL SELECT 'inventory_turnover', COUNT(*) FROM inventory_turnover
ORDER BY tbl;
