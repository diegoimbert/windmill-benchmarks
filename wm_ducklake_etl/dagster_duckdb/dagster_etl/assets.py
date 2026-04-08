"""
TPC-DS SF100 ETL benchmark pipeline -- Dagster + DuckDB implementation.

6 stages, ~53 assets total:
  Stage 1: Ingest 24 tables from Parquet on S3
  Stage 2: Validate 8 groups (7 facts + 1 dimensions)
  Stage 3: Denormalize 3 sales channels
  Stage 4: Build 7 aggregate tables
  Stage 5: Run 10 TPC-DS analytical queries
  Stage 6: Verify row counts
"""

from dagster import asset, AssetExecutionContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

INGEST_TABLES = [
    "store_sales",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
    "inventory",
    "customer",
    "customer_address",
    "customer_demographics",
    "household_demographics",
    "item",
    "store",
    "date_dim",
    "time_dim",
    "promotion",
    "warehouse",
    "catalog_page",
    "web_page",
    "web_site",
    "call_center",
    "income_band",
    "reason",
    "ship_mode",
]


def _exec(context: AssetExecutionContext, sql: str) -> dict:
    """Execute SQL via the DuckDB resource and return the first row as metadata."""
    duckdb_res = context.resources.duckdb
    conn = duckdb_res.get_connection()
    try:
        result = conn.execute(sql).fetchall()
        description = conn.description
        if result and description:
            cols = [d[0] for d in description]
            return {cols[i]: result[0][i] for i in range(len(cols))}
        return {"rows_returned": len(result) if result else 0}
    finally:
        conn.close()


def _exec_multi(context: AssetExecutionContext, sql: str) -> list[dict]:
    """Execute SQL and return all rows."""
    duckdb_res = context.resources.duckdb
    conn = duckdb_res.get_connection()
    try:
        result = conn.execute(sql).fetchall()
        description = conn.description
        if result and description:
            cols = [d[0] for d in description]
            return [{cols[i]: row[i] for i in range(len(cols))} for row in result]
        return []
    finally:
        conn.close()


# ===========================================================================
# Stage 1 -- Ingest (24 assets)
# ===========================================================================

@asset(group_name="ingest")
def ingest_store_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE store_sales AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/store_sales.parquet');
    """)

@asset(group_name="ingest")
def ingest_catalog_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE catalog_sales AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/catalog_sales.parquet');
    """)

@asset(group_name="ingest")
def ingest_web_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE web_sales AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/web_sales.parquet');
    """)

@asset(group_name="ingest")
def ingest_store_returns(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE store_returns AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/store_returns.parquet');
    """)

@asset(group_name="ingest")
def ingest_catalog_returns(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE catalog_returns AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/catalog_returns.parquet');
    """)

@asset(group_name="ingest")
def ingest_web_returns(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE web_returns AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/web_returns.parquet');
    """)

@asset(group_name="ingest")
def ingest_inventory(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE inventory AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/inventory.parquet');
    """)

@asset(group_name="ingest")
def ingest_customer(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE customer AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/customer.parquet');
    """)

@asset(group_name="ingest")
def ingest_customer_address(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE customer_address AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/customer_address.parquet');
    """)

@asset(group_name="ingest")
def ingest_customer_demographics(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE customer_demographics AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/customer_demographics.parquet');
    """)

@asset(group_name="ingest")
def ingest_household_demographics(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE household_demographics AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/household_demographics.parquet');
    """)

@asset(group_name="ingest")
def ingest_item(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE item AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/item.parquet');
    """)

@asset(group_name="ingest")
def ingest_store(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE store AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/store.parquet');
    """)

@asset(group_name="ingest")
def ingest_date_dim(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE date_dim AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/date_dim.parquet');
    """)

@asset(group_name="ingest")
def ingest_time_dim(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE time_dim AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/time_dim.parquet');
    """)

@asset(group_name="ingest")
def ingest_promotion(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE promotion AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/promotion.parquet');
    """)

@asset(group_name="ingest")
def ingest_warehouse(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE warehouse AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/warehouse.parquet');
    """)

@asset(group_name="ingest")
def ingest_catalog_page(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE catalog_page AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/catalog_page.parquet');
    """)

@asset(group_name="ingest")
def ingest_web_page(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE web_page AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/web_page.parquet');
    """)

@asset(group_name="ingest")
def ingest_web_site(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE web_site AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/web_site.parquet');
    """)

@asset(group_name="ingest")
def ingest_call_center(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE call_center AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/call_center.parquet');
    """)

@asset(group_name="ingest")
def ingest_income_band(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE income_band AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/income_band.parquet');
    """)

@asset(group_name="ingest")
def ingest_reason(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE reason AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/reason.parquet');
    """)

@asset(group_name="ingest")
def ingest_ship_mode(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE ship_mode AS
        SELECT * FROM read_parquet('s3://bench-data/tpcds/sf100/ship_mode.parquet');
    """)


# ===========================================================================
# Stage 2 -- Validate (8 assets)
# ===========================================================================

@asset(
    group_name="validate",
    deps=[
        ingest_store_sales, ingest_date_dim, ingest_item,
    ],
)
def validate_store_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'store_sales' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN ss_sold_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN ss_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN ss_customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer_sk,
          SUM(CASE WHEN ss_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
          SUM(CASE WHEN ss_sales_price < 0 THEN 1 ELSE 0 END) AS neg_sales_price,
          (SELECT COUNT(*) FROM store_sales s
           LEFT JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
           WHERE d.d_date_sk IS NULL AND s.ss_sold_date_sk IS NOT NULL) AS broken_date_fk,
          (SELECT COUNT(*) FROM store_sales s
           LEFT JOIN item i ON s.ss_item_sk = i.i_item_sk
           WHERE i.i_item_sk IS NULL AND s.ss_item_sk IS NOT NULL) AS broken_item_fk
        FROM store_sales;
    """)

@asset(
    group_name="validate",
    deps=[
        ingest_catalog_sales, ingest_date_dim, ingest_item,
    ],
)
def validate_catalog_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'catalog_sales' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN cs_sold_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN cs_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN cs_bill_customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer_sk,
          SUM(CASE WHEN cs_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
          SUM(CASE WHEN cs_sales_price < 0 THEN 1 ELSE 0 END) AS neg_sales_price,
          (SELECT COUNT(*) FROM catalog_sales s
           LEFT JOIN date_dim d ON s.cs_sold_date_sk = d.d_date_sk
           WHERE d.d_date_sk IS NULL AND s.cs_sold_date_sk IS NOT NULL) AS broken_date_fk,
          (SELECT COUNT(*) FROM catalog_sales s
           LEFT JOIN item i ON s.cs_item_sk = i.i_item_sk
           WHERE i.i_item_sk IS NULL AND s.cs_item_sk IS NOT NULL) AS broken_item_fk
        FROM catalog_sales;
    """)

@asset(
    group_name="validate",
    deps=[
        ingest_web_sales, ingest_date_dim, ingest_item,
    ],
)
def validate_web_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'web_sales' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN ws_sold_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN ws_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN ws_bill_customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer_sk,
          SUM(CASE WHEN ws_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
          SUM(CASE WHEN ws_sales_price < 0 THEN 1 ELSE 0 END) AS neg_sales_price,
          (SELECT COUNT(*) FROM web_sales s
           LEFT JOIN date_dim d ON s.ws_sold_date_sk = d.d_date_sk
           WHERE d.d_date_sk IS NULL AND s.ws_sold_date_sk IS NOT NULL) AS broken_date_fk,
          (SELECT COUNT(*) FROM web_sales s
           LEFT JOIN item i ON s.ws_item_sk = i.i_item_sk
           WHERE i.i_item_sk IS NULL AND s.ws_item_sk IS NOT NULL) AS broken_item_fk
        FROM web_sales;
    """)

@asset(
    group_name="validate",
    deps=[ingest_store_returns],
)
def validate_store_returns(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'store_returns' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN sr_returned_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN sr_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN sr_return_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
          SUM(CASE WHEN sr_return_amt < 0 THEN 1 ELSE 0 END) AS neg_amount
        FROM store_returns;
    """)

@asset(
    group_name="validate",
    deps=[ingest_catalog_returns],
)
def validate_catalog_returns(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'catalog_returns' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN cr_returned_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN cr_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN cr_return_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
          SUM(CASE WHEN cr_return_amount < 0 THEN 1 ELSE 0 END) AS neg_amount
        FROM catalog_returns;
    """)

@asset(
    group_name="validate",
    deps=[ingest_web_returns],
)
def validate_web_returns(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'web_returns' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN wr_returned_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN wr_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN wr_return_quantity < 0 THEN 1 ELSE 0 END) AS neg_quantity,
          SUM(CASE WHEN wr_return_amt < 0 THEN 1 ELSE 0 END) AS neg_amount
        FROM web_returns;
    """)

@asset(
    group_name="validate",
    deps=[ingest_inventory],
)
def validate_inventory(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT
          'inventory' AS table_name,
          COUNT(*) AS total_rows,
          SUM(CASE WHEN inv_date_sk IS NULL THEN 1 ELSE 0 END) AS null_date_sk,
          SUM(CASE WHEN inv_item_sk IS NULL THEN 1 ELSE 0 END) AS null_item_sk,
          SUM(CASE WHEN inv_warehouse_sk IS NULL THEN 1 ELSE 0 END) AS null_warehouse_sk,
          SUM(CASE WHEN inv_quantity_on_hand < 0 THEN 1 ELSE 0 END) AS neg_quantity
        FROM inventory;
    """)

@asset(
    group_name="validate",
    deps=[
        ingest_customer, ingest_item, ingest_store, ingest_date_dim,
        ingest_time_dim, ingest_promotion, ingest_warehouse,
        ingest_customer_address, ingest_customer_demographics,
        ingest_household_demographics, ingest_catalog_page,
        ingest_web_page, ingest_web_site, ingest_call_center,
        ingest_income_band, ingest_reason, ingest_ship_mode,
    ],
)
def validate_dimensions(context: AssetExecutionContext) -> None:
    _exec_multi(context, """
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
    """)


# ===========================================================================
# Stage 3 -- Denormalize (3 assets)
# ===========================================================================

@asset(
    group_name="denormalize",
    deps=[
        validate_store_sales, validate_dimensions,
    ],
)
def denorm_store_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
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
    """)

@asset(
    group_name="denormalize",
    deps=[
        validate_catalog_sales, validate_dimensions,
    ],
)
def denorm_catalog_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE wide_catalog_sales AS
        SELECT
          cs.*,
          d.d_date, d.d_month_seq, d.d_year, d.d_moy, d.d_dom, d.d_qoy, d.d_day_name, d.d_weekend,
          c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_country, c.c_email_address,
          i.i_item_id, i.i_product_name, i.i_category, i.i_class, i.i_brand, i.i_manufact, i.i_current_price,
          p.p_promo_id, p.p_promo_name, p.p_channel_tv, p.p_channel_radio, p.p_channel_email, p.p_discount_active,
          cp.cp_department, cp.cp_catalog_number, cp.cp_catalog_page_number,
          sm.sm_type AS ship_type, sm.sm_carrier AS ship_carrier,
          w.w_warehouse_name, w.w_city AS w_city, w.w_state AS w_state
        FROM catalog_sales cs
        JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
        LEFT JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
        JOIN item i ON cs.cs_item_sk = i.i_item_sk
        LEFT JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
        LEFT JOIN catalog_page cp ON cs.cs_catalog_page_sk = cp.cp_catalog_page_sk
        LEFT JOIN ship_mode sm ON cs.cs_ship_mode_sk = sm.sm_ship_mode_sk
        LEFT JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk;
    """)

@asset(
    group_name="denormalize",
    deps=[
        validate_web_sales, validate_dimensions,
    ],
)
def denorm_web_sales(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE wide_web_sales AS
        SELECT
          ws.*,
          d.d_date, d.d_month_seq, d.d_year, d.d_moy, d.d_dom, d.d_qoy, d.d_day_name, d.d_weekend,
          c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_country, c.c_email_address,
          i.i_item_id, i.i_product_name, i.i_category, i.i_class, i.i_brand, i.i_manufact, i.i_current_price,
          p.p_promo_id, p.p_promo_name, p.p_channel_tv, p.p_channel_radio, p.p_channel_email, p.p_discount_active,
          wp.wp_type AS page_type, wp.wp_char_count, wp.wp_link_count,
          ws2.web_name, ws2.web_class, ws2.web_manager,
          sm.sm_type AS ship_type, sm.sm_carrier AS ship_carrier,
          w.w_warehouse_name, w.w_city AS w_city, w.w_state AS w_state
        FROM web_sales ws
        JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
        LEFT JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
        JOIN item i ON ws.ws_item_sk = i.i_item_sk
        LEFT JOIN promotion p ON ws.ws_promo_sk = p.p_promo_sk
        LEFT JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
        LEFT JOIN web_site ws2 ON ws.ws_web_site_sk = ws2.web_site_sk
        LEFT JOIN ship_mode sm ON ws.ws_ship_mode_sk = sm.sm_ship_mode_sk
        LEFT JOIN warehouse w ON ws.ws_warehouse_sk = w.w_warehouse_sk;
    """)


# ===========================================================================
# Stage 4 -- Aggregate (7 assets)
# ===========================================================================

@asset(
    group_name="aggregate",
    deps=[denorm_store_sales],
)
def agg_daily_store(context: AssetExecutionContext) -> None:
    _exec(context, """
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
    """)

@asset(
    group_name="aggregate",
    deps=[denorm_store_sales],
)
def agg_monthly_category(context: AssetExecutionContext) -> None:
    _exec(context, """
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
    """)

@asset(
    group_name="aggregate",
    deps=[
        ingest_store_sales, ingest_catalog_sales, ingest_web_sales,
        ingest_customer, validate_store_sales, validate_catalog_sales,
        validate_web_sales,
    ],
)
def agg_customer_ltv(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE customer_lifetime_value AS
        WITH store_totals AS (
          SELECT
            ss_customer_sk AS customer_sk,
            SUM(ss_net_paid) AS store_spend,
            COUNT(*) AS store_txns
          FROM store_sales
          WHERE ss_customer_sk IS NOT NULL
          GROUP BY ss_customer_sk
        ),
        catalog_totals AS (
          SELECT
            cs_bill_customer_sk AS customer_sk,
            SUM(cs_net_paid) AS catalog_spend,
            COUNT(*) AS catalog_txns
          FROM catalog_sales
          WHERE cs_bill_customer_sk IS NOT NULL
          GROUP BY cs_bill_customer_sk
        ),
        web_totals AS (
          SELECT
            ws_bill_customer_sk AS customer_sk,
            SUM(ws_net_paid) AS web_spend,
            COUNT(*) AS web_txns
          FROM web_sales
          WHERE ws_bill_customer_sk IS NOT NULL
          GROUP BY ws_bill_customer_sk
        )
        SELECT
          c.c_customer_sk,
          c.c_customer_id,
          c.c_first_name,
          c.c_last_name,
          COALESCE(st.store_spend, 0) + COALESCE(ct.catalog_spend, 0) + COALESCE(wt.web_spend, 0) AS total_spend,
          COALESCE(st.store_txns, 0) + COALESCE(ct.catalog_txns, 0) + COALESCE(wt.web_txns, 0) AS total_transactions,
          COALESCE(st.store_spend, 0) AS store_spend,
          COALESCE(ct.catalog_spend, 0) AS catalog_spend,
          COALESCE(wt.web_spend, 0) AS web_spend,
          CASE
            WHEN COALESCE(st.store_txns, 0) + COALESCE(ct.catalog_txns, 0) + COALESCE(wt.web_txns, 0) > 0
            THEN (COALESCE(st.store_spend, 0) + COALESCE(ct.catalog_spend, 0) + COALESCE(wt.web_spend, 0))
                 / (COALESCE(st.store_txns, 0) + COALESCE(ct.catalog_txns, 0) + COALESCE(wt.web_txns, 0))
            ELSE 0
          END AS avg_basket_size
        FROM customer c
        LEFT JOIN store_totals st ON c.c_customer_sk = st.customer_sk
        LEFT JOIN catalog_totals ct ON c.c_customer_sk = ct.customer_sk
        LEFT JOIN web_totals wt ON c.c_customer_sk = wt.customer_sk
        ORDER BY total_spend DESC;
    """)

@asset(
    group_name="aggregate",
    deps=[
        ingest_store_sales, ingest_catalog_sales, ingest_web_sales,
        ingest_date_dim, validate_store_sales, validate_catalog_sales,
        validate_web_sales,
    ],
)
def agg_channel_comparison(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE channel_comparison AS
        WITH store_agg AS (
          SELECT d.d_year, d.d_moy,
            'store' AS channel,
            SUM(ss_net_paid) AS revenue,
            SUM(ss_net_profit) AS profit,
            COUNT(*) AS transactions,
            COUNT(DISTINCT ss_customer_sk) AS unique_customers
          FROM store_sales ss JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
          GROUP BY d.d_year, d.d_moy
        ),
        catalog_agg AS (
          SELECT d.d_year, d.d_moy,
            'catalog' AS channel,
            SUM(cs_net_paid) AS revenue,
            SUM(cs_net_profit) AS profit,
            COUNT(*) AS transactions,
            COUNT(DISTINCT cs_bill_customer_sk) AS unique_customers
          FROM catalog_sales cs JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
          GROUP BY d.d_year, d.d_moy
        ),
        web_agg AS (
          SELECT d.d_year, d.d_moy,
            'web' AS channel,
            SUM(ws_net_paid) AS revenue,
            SUM(ws_net_profit) AS profit,
            COUNT(*) AS transactions,
            COUNT(DISTINCT ws_bill_customer_sk) AS unique_customers
          FROM web_sales ws JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
          GROUP BY d.d_year, d.d_moy
        )
        SELECT * FROM store_agg
        UNION ALL
        SELECT * FROM catalog_agg
        UNION ALL
        SELECT * FROM web_agg
        ORDER BY d_year, d_moy, channel;
    """)

@asset(
    group_name="aggregate",
    deps=[denorm_store_sales],
)
def agg_promo_roi(context: AssetExecutionContext) -> None:
    _exec(context, """
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
    """)

@asset(
    group_name="aggregate",
    deps=[
        ingest_store_sales, ingest_store_returns, ingest_item,
        validate_store_sales, validate_store_returns,
    ],
)
def agg_return_rate(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE return_rate_by_category AS
        WITH sales AS (
          SELECT i.i_category, i.i_class,
            COUNT(*) AS sale_count,
            SUM(ss_net_paid) AS sale_revenue
          FROM store_sales ss
          JOIN item i ON ss.ss_item_sk = i.i_item_sk
          GROUP BY i.i_category, i.i_class
        ),
        returns AS (
          SELECT i.i_category, i.i_class,
            COUNT(*) AS return_count,
            SUM(sr_return_amt) AS return_revenue
          FROM store_returns sr
          JOIN item i ON sr.sr_item_sk = i.i_item_sk
          GROUP BY i.i_category, i.i_class
        )
        SELECT
          s.i_category,
          s.i_class,
          s.sale_count,
          s.sale_revenue,
          COALESCE(r.return_count, 0) AS return_count,
          COALESCE(r.return_revenue, 0) AS return_revenue,
          CASE WHEN s.sale_count > 0
            THEN ROUND(COALESCE(r.return_count, 0) * 100.0 / s.sale_count, 2)
            ELSE 0
          END AS return_rate_pct,
          CASE WHEN s.sale_revenue > 0
            THEN ROUND(COALESCE(r.return_revenue, 0) * 100.0 / s.sale_revenue, 2)
            ELSE 0
          END AS return_revenue_pct
        FROM sales s
        LEFT JOIN returns r ON s.i_category = r.i_category AND s.i_class = r.i_class
        ORDER BY return_rate_pct DESC;
    """)

@asset(
    group_name="aggregate",
    deps=[
        ingest_inventory, ingest_item, ingest_warehouse,
        ingest_store_sales, validate_inventory,
    ],
)
def agg_inventory_turnover(context: AssetExecutionContext) -> None:
    _exec(context, """
        CREATE OR REPLACE TABLE inventory_turnover AS
        WITH avg_inventory AS (
          SELECT
            inv_item_sk,
            inv_warehouse_sk,
            AVG(inv_quantity_on_hand) AS avg_qty_on_hand
          FROM inventory
          GROUP BY inv_item_sk, inv_warehouse_sk
        ),
        sales_velocity AS (
          SELECT
            ss_item_sk,
            ss_store_sk,
            SUM(ss_quantity) AS total_sold,
            COUNT(DISTINCT ss_sold_date_sk) AS selling_days
          FROM store_sales
          WHERE ss_quantity IS NOT NULL
          GROUP BY ss_item_sk, ss_store_sk
        )
        SELECT
          i.i_item_id,
          i.i_product_name,
          i.i_category,
          w.w_warehouse_name,
          w.w_state,
          ai.avg_qty_on_hand,
          COALESCE(sv.total_sold, 0) AS total_sold,
          CASE WHEN ai.avg_qty_on_hand > 0
            THEN ROUND(COALESCE(sv.total_sold, 0)::DOUBLE / ai.avg_qty_on_hand, 2)
            ELSE 0
          END AS turnover_ratio
        FROM avg_inventory ai
        JOIN item i ON ai.inv_item_sk = i.i_item_sk
        JOIN warehouse w ON ai.inv_warehouse_sk = w.w_warehouse_sk
        LEFT JOIN sales_velocity sv ON ai.inv_item_sk = sv.ss_item_sk
        ORDER BY turnover_ratio DESC;
    """)


# ===========================================================================
# Stage 5 -- TPC-DS Analytical Queries (10 assets)
# ===========================================================================

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q03(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, SUM(ss_ext_sales_price) sum_agg
        FROM date_dim dt, store_sales, item
        WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
          AND store_sales.ss_item_sk = item.i_item_sk
          AND item.i_manufact_id = 128
          AND dt.d_moy = 11
        GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        ORDER BY dt.d_year, sum_agg DESC, brand_id
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q07(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT i_item_id,
               avg(ss_quantity) agg1,
               avg(ss_list_price) agg2,
               avg(ss_coupon_amt) agg3,
               avg(ss_sales_price) agg4
        FROM store_sales, customer_demographics, date_dim, item, promotion
        WHERE ss_sold_date_sk = d_date_sk
          AND ss_item_sk = i_item_sk
          AND ss_cdemo_sk = cd_demo_sk
          AND ss_promo_sk = p_promo_sk
          AND cd_gender = 'M'
          AND cd_marital_status = 'S'
          AND cd_education_status = 'College'
          AND (p_channel_email = 'N' OR p_channel_event = 'N')
          AND d_year = 2000
        GROUP BY i_item_id
        ORDER BY i_item_id
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q19(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
               sum(ss_ext_sales_price) ext_price
        FROM date_dim, store_sales, item, customer, customer_address, store
        WHERE d_date_sk = ss_sold_date_sk
          AND ss_item_sk = i_item_sk
          AND i_manager_id = 8
          AND d_moy = 11
          AND d_year = 1998
          AND ss_customer_sk = c_customer_sk
          AND c_current_addr_sk = ca_address_sk
          AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
          AND ss_store_sk = s_store_sk
        GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
        ORDER BY ext_price DESC, brand, brand_id, i_manufact_id, i_manufact
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q27(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT i_item_id,
               s_state, grouping(s_state) g_state,
               avg(ss_quantity) agg1,
               avg(ss_list_price) agg2,
               avg(ss_coupon_amt) agg3,
               avg(ss_sales_price) agg4
        FROM store_sales, customer_demographics, date_dim, store, item
        WHERE ss_sold_date_sk = d_date_sk
          AND ss_item_sk = i_item_sk
          AND ss_store_sk = s_store_sk
          AND ss_cdemo_sk = cd_demo_sk
          AND cd_gender = 'M'
          AND cd_marital_status = 'S'
          AND cd_education_status = 'College'
          AND d_year = 2002
          AND s_state IN ('TN','TN','TN','TN','TN','TN')
        GROUP BY ROLLUP (i_item_id, s_state)
        ORDER BY i_item_id, s_state
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q34(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt
        FROM
          (SELECT ss_ticket_number, ss_customer_sk, count(*) cnt
           FROM store_sales, date_dim, store, household_demographics
           WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
             AND store_sales.ss_store_sk = store.s_store_sk
             AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
             AND (date_dim.d_dom BETWEEN 1 AND 3 OR date_dim.d_dom BETWEEN 25 AND 28)
             AND (household_demographics.hd_buy_potential = '>10000' OR
                  household_demographics.hd_buy_potential = 'Unknown')
             AND household_demographics.hd_vehicle_count > 0
             AND (CASE WHEN household_demographics.hd_vehicle_count > 0
                  THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
                  ELSE null END) > 1.2
             AND date_dim.d_year IN (1999, 1999+1, 1999+2)
             AND store.s_county IN ('Williamson County','Williamson County','Williamson County','Williamson County',
                                    'Williamson County','Williamson County','Williamson County','Williamson County')
           GROUP BY ss_ticket_number, ss_customer_sk) dn, customer
        WHERE ss_customer_sk = c_customer_sk
          AND cnt BETWEEN 15 AND 20
        ORDER BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag DESC, ss_ticket_number;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q43(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT s_store_name, s_store_id,
               sum(CASE WHEN (d_day_name='Sunday') THEN ss_sales_price ELSE null END) sun_sales,
               sum(CASE WHEN (d_day_name='Monday') THEN ss_sales_price ELSE null END) mon_sales,
               sum(CASE WHEN (d_day_name='Tuesday') THEN ss_sales_price ELSE null END) tue_sales,
               sum(CASE WHEN (d_day_name='Wednesday') THEN ss_sales_price ELSE null END) wed_sales,
               sum(CASE WHEN (d_day_name='Thursday') THEN ss_sales_price ELSE null END) thu_sales,
               sum(CASE WHEN (d_day_name='Friday') THEN ss_sales_price ELSE null END) fri_sales,
               sum(CASE WHEN (d_day_name='Saturday') THEN ss_sales_price ELSE null END) sat_sales
        FROM date_dim, store_sales, store
        WHERE d_date_sk = ss_sold_date_sk
          AND s_store_sk = ss_store_sk
          AND s_gmt_offset = -5
          AND d_year = 2000
        GROUP BY s_store_name, s_store_id
        ORDER BY s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales,
                 thu_sales, fri_sales, sat_sales
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q46(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt, profit
        FROM
          (SELECT ss_ticket_number, ss_customer_sk, ca_city bought_city,
                  sum(ss_coupon_amt) amt, sum(ss_net_profit) profit
           FROM store_sales, date_dim, store, household_demographics, customer_address
           WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
             AND store_sales.ss_store_sk = store.s_store_sk
             AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
             AND store_sales.ss_addr_sk = customer_address.ca_address_sk
             AND (household_demographics.hd_dep_count = 4 OR
                  household_demographics.hd_vehicle_count = 3)
             AND date_dim.d_dow IN (6, 0)
             AND date_dim.d_year IN (1999, 1999+1, 1999+2)
             AND store.s_city IN ('Fairview','Midway','Fairview','Fairview','Fairview')
           GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) dn, customer, customer_address current_addr
        WHERE ss_customer_sk = c_customer_sk
          AND customer.c_current_addr_sk = current_addr.ca_address_sk
          AND current_addr.ca_city <> bought_city
        ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q53(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT * FROM
          (SELECT i_manufact_id,
                  sum(ss_sales_price) sum_sales,
                  avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
           FROM item, store_sales, date_dim, store
           WHERE ss_item_sk = i_item_sk
             AND ss_sold_date_sk = d_date_sk
             AND ss_store_sk = s_store_sk
             AND d_month_seq IN (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,
                                 1200+7,1200+8,1200+9,1200+10,1200+11)
             AND ((i_category IN ('Books','Children','Electronics')
                   AND i_class IN ('personal','portable','reference','self-help')
                   AND i_brand IN ('scholaramalgamalg #14','scholaramalgamalg #7',
                                   'exportiunivamalg #9','scholaramalgamalg #9'))
                  OR
                  (i_category IN ('Women','Music','Men')
                   AND i_class IN ('accessories','classical','fragrances','pants')
                   AND i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
                                   'importoamalg #1')))
           GROUP BY i_manufact_id, d_qoy) tmp1
        WHERE CASE WHEN avg_quarterly_sales > 0
              THEN abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
              ELSE null END > 0.1
        ORDER BY avg_quarterly_sales, sum_sales, i_manufact_id
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q67(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT * FROM
          (SELECT i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id,
                  sumsales, rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
           FROM
             (SELECT i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,
                     s_store_id, sum(coalesce(ss_sales_price*ss_quantity, 0)) sumsales
              FROM store_sales, date_dim, store, item
              WHERE ss_sold_date_sk = d_date_sk
                AND ss_item_sk = i_item_sk
                AND ss_store_sk = s_store_sk
                AND d_month_seq BETWEEN 1200 AND 1200+11
              GROUP BY ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy,
                               d_moy, s_store_id)) dw1) dw2
        WHERE rk <= 100
        ORDER BY i_category, i_class, i_brand, i_product_name, d_year,
                 d_qoy, d_moy, s_store_id, sumsales, rk
        LIMIT 100;
    """)

@asset(
    group_name="queries",
    deps=[
        denorm_store_sales, agg_daily_store, agg_monthly_category,
        agg_customer_ltv, agg_channel_comparison, agg_promo_roi,
        agg_return_rate, agg_inventory_turnover,
    ],
)
def query_q79(context: AssetExecutionContext) -> None:
    _exec(context, """
        SELECT c_last_name, c_first_name, substr(s_city, 1, 30), ss_ticket_number, amt, profit
        FROM
          (SELECT ss_ticket_number, ss_customer_sk, store.s_city,
                  sum(ss_coupon_amt) amt, sum(ss_net_profit) profit
           FROM store_sales, date_dim, store, household_demographics
           WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
             AND store_sales.ss_store_sk = store.s_store_sk
             AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
             AND (household_demographics.hd_dep_count = 6 OR
                  household_demographics.hd_vehicle_count > 2)
             AND date_dim.d_dow = 1
             AND date_dim.d_year IN (1999, 1999+1, 1999+2)
             AND store.s_number_employees BETWEEN 200 AND 295
           GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city) ms, customer
        WHERE ss_customer_sk = c_customer_sk
        ORDER BY c_last_name, c_first_name, substr(s_city, 1, 30), profit
        LIMIT 100;
    """)


# ===========================================================================
# Stage 6 -- Verify (1 asset)
# ===========================================================================

@asset(
    group_name="verify",
    deps=[
        query_q03, query_q07, query_q19, query_q27, query_q34,
        query_q43, query_q46, query_q53, query_q67, query_q79,
    ],
)
def verify_row_counts(context: AssetExecutionContext) -> None:
    rows = _exec_multi(context, """
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
    """)
    for row in rows:
        context.log.info(f"{row['tbl']}: {row['cnt']} rows")
