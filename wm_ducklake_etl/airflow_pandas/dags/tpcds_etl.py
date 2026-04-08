"""
TPC-DS SF100 ETL benchmark -- Airflow + Pandas edition.

Six stages:
  1. Ingest   - read 24 Parquet tables from S3, write to local Parquet
  2. Validate - null / range / FK checks on 8 table groups
  3. Denorm   - merge fact + dimension tables into 3 wide tables
  4. Aggregate - 7 group-by summaries
  5. Queries  - 10 analytical queries (pandas equivalents of TPC-DS SQL)
  6. Verify   - row-count sanity check
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
S3_BUCKET = os.getenv("S3_BUCKET", "bench-data")
S3_PREFIX = os.getenv("S3_PREFIX", "tpcds/sf100")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", None)

STORAGE_OPTIONS: dict | None = None
if S3_ENDPOINT:
    STORAGE_OPTIONS = {
        "client_kwargs": {"endpoint_url": S3_ENDPOINT},
        "key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    }

log = logging.getLogger(__name__)

TABLES = [
    "store_sales", "catalog_sales", "web_sales",
    "store_returns", "catalog_returns", "web_returns",
    "inventory", "customer", "customer_address",
    "customer_demographics", "household_demographics",
    "item", "store", "date_dim", "time_dim", "promotion",
    "warehouse", "catalog_page", "web_page", "web_site",
    "call_center", "income_band", "reason", "ship_mode",
]


def _s3_uri(table: str) -> str:
    return f"s3://{S3_BUCKET}/{S3_PREFIX}/{table}.parquet"


def _local(name: str) -> Path:
    return DATA_DIR / f"{name}.parquet"


def _read(name: str) -> pd.DataFrame:
    return pd.read_parquet(_local(name))


def _write(name: str, df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_local(name), index=False)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="tpcds_etl_pandas",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "tpcds", "pandas"],
)
def tpcds_etl():
    # ---- Stage 1: Ingest ------------------------------------------------
    @task()
    def ingest(table: str) -> str:
        log.info("Ingesting %s from S3", table)
        df = pd.read_parquet(_s3_uri(table), storage_options=STORAGE_OPTIONS)
        _write(table, df)
        log.info("Ingested %s: %d rows", table, len(df))
        return table

    # ---- Stage 2: Validate -----------------------------------------------
    @task()
    def validate_fact(group: str) -> dict:
        """Validate a sales/returns fact table."""
        log.info("Validating %s", group)
        df = _read(group)
        results: dict = {"table": group, "total_rows": len(df)}

        # Map column prefixes
        prefix_map = {
            "store_sales": "ss", "catalog_sales": "cs", "web_sales": "ws",
            "store_returns": "sr", "catalog_returns": "cr", "web_returns": "wr",
            "inventory": "inv",
        }
        px = prefix_map.get(group, "")

        if group in ("store_sales", "catalog_sales", "web_sales"):
            date_col = f"{px}_sold_date_sk"
            item_col = f"{px}_item_sk"
            cust_col = {
                "store_sales": "ss_customer_sk",
                "catalog_sales": "cs_bill_customer_sk",
                "web_sales": "ws_bill_customer_sk",
            }[group]
            qty_col = f"{px}_quantity"
            price_col = f"{px}_sales_price"
            results["null_date_sk"] = int(df[date_col].isna().sum())
            results["null_item_sk"] = int(df[item_col].isna().sum())
            results["null_customer_sk"] = int(df[cust_col].isna().sum())
            results["neg_quantity"] = int((df[qty_col] < 0).sum())
            results["neg_sales_price"] = int((df[price_col] < 0).sum())
            # FK checks
            date_dim = _read("date_dim")
            item = _read("item")
            valid_dates = set(date_dim["d_date_sk"].dropna())
            valid_items = set(item["i_item_sk"].dropna())
            results["broken_date_fk"] = int(
                df[date_col].dropna().apply(lambda x: x not in valid_dates).sum()
            )
            results["broken_item_fk"] = int(
                df[item_col].dropna().apply(lambda x: x not in valid_items).sum()
            )
        elif group.endswith("_returns"):
            date_col = f"{px}_returned_date_sk"
            item_col = f"{px}_item_sk"
            qty_col = f"{px}_return_quantity"
            amt_col = f"{px}_return_amt" if group != "catalog_returns" else "cr_return_amount"
            results["null_date_sk"] = int(df[date_col].isna().sum())
            results["null_item_sk"] = int(df[item_col].isna().sum())
            results["neg_quantity"] = int((df[qty_col] < 0).sum())
            results["neg_amount"] = int((df[amt_col] < 0).sum())
        elif group == "inventory":
            results["null_date_sk"] = int(df["inv_date_sk"].isna().sum())
            results["null_item_sk"] = int(df["inv_item_sk"].isna().sum())
            results["null_warehouse_sk"] = int(df["inv_warehouse_sk"].isna().sum())
            results["neg_quantity"] = int((df["inv_quantity_on_hand"] < 0).sum())

        log.info("Validation %s: %s", group, results)
        return results

    @task()
    def validate_dimensions() -> dict:
        """Check PK uniqueness on dimension tables."""
        dim_pks = {
            "customer": "c_customer_sk", "item": "i_item_sk",
            "store": "s_store_sk", "date_dim": "d_date_sk",
            "time_dim": "t_time_sk", "promotion": "p_promo_sk",
            "warehouse": "w_warehouse_sk", "customer_address": "ca_address_sk",
            "customer_demographics": "cd_demo_sk",
            "household_demographics": "hd_demo_sk",
            "catalog_page": "cp_catalog_page_sk",
            "web_page": "wp_web_page_sk", "web_site": "web_site_sk",
            "call_center": "cc_call_center_sk",
            "income_band": "ib_income_band_sk",
            "reason": "r_reason_sk", "ship_mode": "sm_ship_mode_sk",
        }
        results = {}
        for tbl, pk in dim_pks.items():
            df = _read(tbl)
            results[tbl] = {
                "total": len(df),
                "distinct_pk": int(df[pk].nunique()),
            }
        log.info("Dimension validation: %s", results)
        return results

    # ---- Stage 3: Denormalize --------------------------------------------
    @task()
    def denorm_store_sales() -> str:
        ss = _read("store_sales")
        d = _read("date_dim")[["d_date_sk", "d_date", "d_month_seq", "d_year", "d_moy", "d_dom", "d_qoy", "d_day_name", "d_weekend"]]
        c = _read("customer")[["c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_birth_country", "c_email_address"]]
        i = _read("item")[["i_item_sk", "i_item_id", "i_product_name", "i_category", "i_class", "i_brand", "i_manufact", "i_current_price"]]
        s = _read("store")[["s_store_sk", "s_store_id", "s_store_name", "s_city", "s_state", "s_zip"]]
        p = _read("promotion")[["p_promo_sk", "p_promo_id", "p_promo_name", "p_channel_tv", "p_channel_radio", "p_channel_email", "p_discount_active"]]
        cd = _read("customer_demographics")[["cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"]]
        hd = _read("household_demographics")[["hd_demo_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count"]]

        wide = (
            ss
            .merge(d, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner")
            .merge(c, left_on="ss_customer_sk", right_on="c_customer_sk", how="left")
            .merge(i, left_on="ss_item_sk", right_on="i_item_sk", how="inner")
            .merge(s, left_on="ss_store_sk", right_on="s_store_sk", how="left")
            .merge(p, left_on="ss_promo_sk", right_on="p_promo_sk", how="left")
            .merge(cd, left_on="ss_cdemo_sk", right_on="cd_demo_sk", how="left")
            .merge(hd, left_on="ss_hdemo_sk", right_on="hd_demo_sk", how="left")
        )
        _write("wide_store_sales", wide)
        log.info("wide_store_sales: %d rows", len(wide))
        return "wide_store_sales"

    @task()
    def denorm_catalog_sales() -> str:
        cs = _read("catalog_sales")
        d = _read("date_dim")[["d_date_sk", "d_date", "d_month_seq", "d_year", "d_moy", "d_dom", "d_qoy", "d_day_name", "d_weekend"]]
        c = _read("customer")[["c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_birth_country", "c_email_address"]]
        i = _read("item")[["i_item_sk", "i_item_id", "i_product_name", "i_category", "i_class", "i_brand", "i_manufact", "i_current_price"]]
        p = _read("promotion")[["p_promo_sk", "p_promo_id", "p_promo_name", "p_channel_tv", "p_channel_radio", "p_channel_email", "p_discount_active"]]
        cp = _read("catalog_page")[["cp_catalog_page_sk", "cp_department", "cp_catalog_number", "cp_catalog_page_number"]]
        sm = _read("ship_mode")[["sm_ship_mode_sk", "sm_type", "sm_carrier"]]
        w = _read("warehouse")[["w_warehouse_sk", "w_warehouse_name", "w_city", "w_state"]]

        wide = (
            cs
            .merge(d, left_on="cs_sold_date_sk", right_on="d_date_sk", how="inner")
            .merge(c, left_on="cs_bill_customer_sk", right_on="c_customer_sk", how="left")
            .merge(i, left_on="cs_item_sk", right_on="i_item_sk", how="inner")
            .merge(p, left_on="cs_promo_sk", right_on="p_promo_sk", how="left")
            .merge(cp, left_on="cs_catalog_page_sk", right_on="cp_catalog_page_sk", how="left")
            .merge(sm, left_on="cs_ship_mode_sk", right_on="sm_ship_mode_sk", how="left")
            .merge(w, left_on="cs_warehouse_sk", right_on="w_warehouse_sk", how="left")
        )
        wide = wide.rename(columns={"sm_type": "ship_type", "sm_carrier": "ship_carrier"})
        _write("wide_catalog_sales", wide)
        log.info("wide_catalog_sales: %d rows", len(wide))
        return "wide_catalog_sales"

    @task()
    def denorm_web_sales() -> str:
        ws = _read("web_sales")
        d = _read("date_dim")[["d_date_sk", "d_date", "d_month_seq", "d_year", "d_moy", "d_dom", "d_qoy", "d_day_name", "d_weekend"]]
        c = _read("customer")[["c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_birth_country", "c_email_address"]]
        i = _read("item")[["i_item_sk", "i_item_id", "i_product_name", "i_category", "i_class", "i_brand", "i_manufact", "i_current_price"]]
        p = _read("promotion")[["p_promo_sk", "p_promo_id", "p_promo_name", "p_channel_tv", "p_channel_radio", "p_channel_email", "p_discount_active"]]
        wp = _read("web_page")[["wp_web_page_sk", "wp_type", "wp_char_count", "wp_link_count"]]
        ws2 = _read("web_site")[["web_site_sk", "web_name", "web_class", "web_manager"]]
        sm = _read("ship_mode")[["sm_ship_mode_sk", "sm_type", "sm_carrier"]]
        w = _read("warehouse")[["w_warehouse_sk", "w_warehouse_name", "w_city", "w_state"]]

        wide = (
            ws
            .merge(d, left_on="ws_sold_date_sk", right_on="d_date_sk", how="inner")
            .merge(c, left_on="ws_bill_customer_sk", right_on="c_customer_sk", how="left")
            .merge(i, left_on="ws_item_sk", right_on="i_item_sk", how="inner")
            .merge(p, left_on="ws_promo_sk", right_on="p_promo_sk", how="left")
            .merge(wp, left_on="ws_web_page_sk", right_on="wp_web_page_sk", how="left")
            .merge(ws2, left_on="ws_web_site_sk", right_on="web_site_sk", how="left")
            .merge(sm, left_on="ws_ship_mode_sk", right_on="sm_ship_mode_sk", how="left")
            .merge(w, left_on="ws_warehouse_sk", right_on="w_warehouse_sk", how="left")
        )
        wide = wide.rename(columns={
            "sm_type": "ship_type", "sm_carrier": "ship_carrier",
            "wp_type": "page_type",
        })
        _write("wide_web_sales", wide)
        log.info("wide_web_sales: %d rows", len(wide))
        return "wide_web_sales"

    # ---- Stage 4: Aggregates ---------------------------------------------
    @task()
    def agg_daily_store() -> str:
        wss = _read("wide_store_sales")
        agg = (
            wss.groupby(["d_date", "d_day_name", "d_weekend", "s_store_id", "s_store_name", "s_state"])
            .agg(
                transaction_count=("ss_sales_price", "count"),
                total_units=("ss_quantity", "sum"),
                total_sales=("ss_sales_price", "sum"),
                net_profit=("ss_net_profit", "sum"),
                avg_sale_price=("ss_sales_price", "mean"),
                total_coupons=("ss_coupon_amt", "sum"),
            )
            .reset_index()
            .sort_values(["d_date", "s_store_id"])
        )
        _write("daily_sales_by_store", agg)
        log.info("daily_sales_by_store: %d rows", len(agg))
        return "daily_sales_by_store"

    @task()
    def agg_monthly_category() -> str:
        wss = _read("wide_store_sales")
        agg = (
            wss.groupby(["d_year", "d_moy", "i_category", "i_class"])
            .agg(
                transaction_count=("ss_sales_price", "count"),
                total_units=("ss_quantity", "sum"),
                total_sales=("ss_sales_price", "sum"),
                net_profit=("ss_net_profit", "sum"),
                avg_sale_price=("ss_sales_price", "mean"),
                unique_customers=("c_customer_id", "nunique"),
            )
            .reset_index()
            .sort_values(["d_year", "d_moy", "total_sales"], ascending=[True, True, False])
        )
        _write("monthly_sales_by_category", agg)
        log.info("monthly_sales_by_category: %d rows", len(agg))
        return "monthly_sales_by_category"

    @task()
    def agg_customer_ltv() -> str:
        cust = _read("customer")[["c_customer_sk", "c_customer_id", "c_first_name", "c_last_name"]]

        ss = _read("store_sales")[["ss_customer_sk", "ss_net_paid"]]
        ss = ss[ss["ss_customer_sk"].notna()]
        st = ss.groupby("ss_customer_sk").agg(store_spend=("ss_net_paid", "sum"), store_txns=("ss_net_paid", "count")).reset_index()

        cs = _read("catalog_sales")[["cs_bill_customer_sk", "cs_net_paid"]]
        cs = cs[cs["cs_bill_customer_sk"].notna()]
        ct = cs.groupby("cs_bill_customer_sk").agg(catalog_spend=("cs_net_paid", "sum"), catalog_txns=("cs_net_paid", "count")).reset_index()

        ws = _read("web_sales")[["ws_bill_customer_sk", "ws_net_paid"]]
        ws = ws[ws["ws_bill_customer_sk"].notna()]
        wt = ws.groupby("ws_bill_customer_sk").agg(web_spend=("ws_net_paid", "sum"), web_txns=("ws_net_paid", "count")).reset_index()

        ltv = (
            cust
            .merge(st, left_on="c_customer_sk", right_on="ss_customer_sk", how="left")
            .merge(ct, left_on="c_customer_sk", right_on="cs_bill_customer_sk", how="left")
            .merge(wt, left_on="c_customer_sk", right_on="ws_bill_customer_sk", how="left")
        )
        for col in ["store_spend", "catalog_spend", "web_spend", "store_txns", "catalog_txns", "web_txns"]:
            ltv[col] = ltv[col].fillna(0)
        ltv["total_spend"] = ltv["store_spend"] + ltv["catalog_spend"] + ltv["web_spend"]
        ltv["total_transactions"] = ltv["store_txns"] + ltv["catalog_txns"] + ltv["web_txns"]
        ltv["avg_basket_size"] = ltv.apply(
            lambda r: r["total_spend"] / r["total_transactions"] if r["total_transactions"] > 0 else 0, axis=1
        )
        ltv = ltv.sort_values("total_spend", ascending=False)
        _write("customer_lifetime_value", ltv[["c_customer_sk", "c_customer_id", "c_first_name", "c_last_name",
                                                 "total_spend", "total_transactions", "store_spend", "catalog_spend",
                                                 "web_spend", "avg_basket_size"]])
        log.info("customer_lifetime_value: %d rows", len(ltv))
        return "customer_lifetime_value"

    @task()
    def agg_channel_comparison() -> str:
        dd = _read("date_dim")[["d_date_sk", "d_year", "d_moy"]]

        ss = _read("store_sales")[["ss_sold_date_sk", "ss_net_paid", "ss_net_profit", "ss_customer_sk"]]
        ss = ss.merge(dd, left_on="ss_sold_date_sk", right_on="d_date_sk")
        sa = ss.groupby(["d_year", "d_moy"]).agg(
            revenue=("ss_net_paid", "sum"), profit=("ss_net_profit", "sum"),
            transactions=("ss_net_paid", "count"), unique_customers=("ss_customer_sk", "nunique"),
        ).reset_index()
        sa["channel"] = "store"

        cs = _read("catalog_sales")[["cs_sold_date_sk", "cs_net_paid", "cs_net_profit", "cs_bill_customer_sk"]]
        cs = cs.merge(dd, left_on="cs_sold_date_sk", right_on="d_date_sk")
        ca = cs.groupby(["d_year", "d_moy"]).agg(
            revenue=("cs_net_paid", "sum"), profit=("cs_net_profit", "sum"),
            transactions=("cs_net_paid", "count"), unique_customers=("cs_bill_customer_sk", "nunique"),
        ).reset_index()
        ca["channel"] = "catalog"

        ws = _read("web_sales")[["ws_sold_date_sk", "ws_net_paid", "ws_net_profit", "ws_bill_customer_sk"]]
        ws = ws.merge(dd, left_on="ws_sold_date_sk", right_on="d_date_sk")
        wa = ws.groupby(["d_year", "d_moy"]).agg(
            revenue=("ws_net_paid", "sum"), profit=("ws_net_profit", "sum"),
            transactions=("ws_net_paid", "count"), unique_customers=("ws_bill_customer_sk", "nunique"),
        ).reset_index()
        wa["channel"] = "web"

        result = pd.concat([sa, ca, wa]).sort_values(["d_year", "d_moy", "channel"])
        _write("channel_comparison", result[["d_year", "d_moy", "channel", "revenue", "profit", "transactions", "unique_customers"]])
        log.info("channel_comparison: %d rows", len(result))
        return "channel_comparison"

    @task()
    def agg_promo_roi() -> str:
        wss = _read("wide_store_sales")
        avg_no_promo = wss.loc[wss["ss_promo_sk"].isna(), "ss_sales_price"].mean()
        promo = wss[wss["p_promo_id"].notna()]
        agg = (
            promo.groupby(["p_promo_id", "p_promo_name", "p_channel_tv", "p_channel_radio", "p_channel_email", "p_discount_active"])
            .agg(
                promo_transactions=("ss_sales_price", "count"),
                promo_revenue=("ss_sales_price", "sum"),
                total_coupon_discount=("ss_coupon_amt", "sum"),
                promo_profit=("ss_net_profit", "sum"),
                avg_sale_with_promo=("ss_sales_price", "mean"),
            )
            .reset_index()
        )
        agg["avg_sale_no_promo"] = avg_no_promo
        agg = agg.sort_values("promo_revenue", ascending=False)
        _write("promo_roi", agg)
        log.info("promo_roi: %d rows", len(agg))
        return "promo_roi"

    @task()
    def agg_return_rate() -> str:
        item = _read("item")[["i_item_sk", "i_category", "i_class"]]
        ss = _read("store_sales")[["ss_item_sk", "ss_net_paid"]]
        sr = _read("store_returns")[["sr_item_sk", "sr_return_amt"]]

        sales = ss.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        s_agg = sales.groupby(["i_category", "i_class"]).agg(
            sale_count=("ss_net_paid", "count"), sale_revenue=("ss_net_paid", "sum"),
        ).reset_index()

        rets = sr.merge(item, left_on="sr_item_sk", right_on="i_item_sk")
        r_agg = rets.groupby(["i_category", "i_class"]).agg(
            return_count=("sr_return_amt", "count"), return_revenue=("sr_return_amt", "sum"),
        ).reset_index()

        result = s_agg.merge(r_agg, on=["i_category", "i_class"], how="left")
        result["return_count"] = result["return_count"].fillna(0)
        result["return_revenue"] = result["return_revenue"].fillna(0)
        result["return_rate_pct"] = (result["return_count"] * 100.0 / result["sale_count"]).round(2).where(result["sale_count"] > 0, 0)
        result["return_revenue_pct"] = (result["return_revenue"] * 100.0 / result["sale_revenue"]).round(2).where(result["sale_revenue"] > 0, 0)
        result = result.sort_values("return_rate_pct", ascending=False)
        _write("return_rate_by_category", result)
        log.info("return_rate_by_category: %d rows", len(result))
        return "return_rate_by_category"

    @task()
    def agg_inventory_turnover() -> str:
        inv = _read("inventory")[["inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand"]]
        avg_inv = inv.groupby(["inv_item_sk", "inv_warehouse_sk"]).agg(
            avg_qty_on_hand=("inv_quantity_on_hand", "mean"),
        ).reset_index()

        ss = _read("store_sales")[["ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sold_date_sk"]]
        ss = ss[ss["ss_quantity"].notna()]
        sv = ss.groupby(["ss_item_sk", "ss_store_sk"]).agg(
            total_sold=("ss_quantity", "sum"),
            selling_days=("ss_sold_date_sk", "nunique"),
        ).reset_index()

        item = _read("item")[["i_item_sk", "i_item_id", "i_product_name", "i_category"]]
        w = _read("warehouse")[["w_warehouse_sk", "w_warehouse_name", "w_state"]]

        result = (
            avg_inv
            .merge(item, left_on="inv_item_sk", right_on="i_item_sk")
            .merge(w, left_on="inv_warehouse_sk", right_on="w_warehouse_sk")
            .merge(sv, left_on="inv_item_sk", right_on="ss_item_sk", how="left")
        )
        result["total_sold"] = result["total_sold"].fillna(0)
        result["turnover_ratio"] = (result["total_sold"] / result["avg_qty_on_hand"]).round(2).where(result["avg_qty_on_hand"] > 0, 0)
        result = result.sort_values("turnover_ratio", ascending=False)
        _write("inventory_turnover", result[["i_item_id", "i_product_name", "i_category",
                                              "w_warehouse_name", "w_state", "avg_qty_on_hand",
                                              "total_sold", "turnover_ratio"]])
        log.info("inventory_turnover: %d rows", len(result))
        return "inventory_turnover"

    # ---- Stage 5: Analytical queries -------------------------------------
    @task()
    def query_q03() -> dict:
        """Sales by brand for manufact_id=128, month=11."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"]]
        dt = _read("date_dim")[["d_date_sk", "d_year", "d_moy"]]
        item = _read("item")[["i_item_sk", "i_brand_id", "i_brand", "i_manufact_id"]]

        dt = dt[dt["d_moy"] == 11]
        item = item[item["i_manufact_id"] == 128]

        merged = ss.merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
        merged = merged.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        result = (
            merged.groupby(["d_year", "i_brand", "i_brand_id"])
            .agg(sum_agg=("ss_ext_sales_price", "sum"))
            .reset_index()
            .sort_values(["d_year", "sum_agg", "i_brand_id"], ascending=[True, False, True])
            .head(100)
        )
        _write("query_q03", result)
        log.info("q03: %d rows", len(result))
        return {"query": "q03", "rows": len(result)}

    @task()
    def query_q07() -> dict:
        """Avg quantity/price/coupon/sales by item for specific demographics."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_item_sk", "ss_cdemo_sk", "ss_promo_sk",
                                    "ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_sales_price"]]
        cd = _read("customer_demographics")[["cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"]]
        dt = _read("date_dim")[["d_date_sk", "d_year"]]
        item = _read("item")[["i_item_sk", "i_item_id"]]
        promo = _read("promotion")[["p_promo_sk", "p_channel_email", "p_channel_event"]]

        cd = cd[(cd["cd_gender"] == "M") & (cd["cd_marital_status"] == "S") & (cd["cd_education_status"] == "College")]
        dt = dt[dt["d_year"] == 2000]
        promo = promo[(promo["p_channel_email"] == "N") | (promo["p_channel_event"] == "N")]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
            .merge(cd, left_on="ss_cdemo_sk", right_on="cd_demo_sk")
            .merge(promo, left_on="ss_promo_sk", right_on="p_promo_sk")
        )
        result = (
            merged.groupby("i_item_id")
            .agg(agg1=("ss_quantity", "mean"), agg2=("ss_list_price", "mean"),
                 agg3=("ss_coupon_amt", "mean"), agg4=("ss_sales_price", "mean"))
            .reset_index()
            .sort_values("i_item_id")
            .head(100)
        )
        _write("query_q07", result)
        log.info("q07: %d rows", len(result))
        return {"query": "q07", "rows": len(result)}

    @task()
    def query_q19() -> dict:
        """Brand sales where customer zip != store zip."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_ext_sales_price"]]
        dt = _read("date_dim")[["d_date_sk", "d_moy", "d_year"]]
        item = _read("item")[["i_item_sk", "i_brand_id", "i_brand", "i_manufact_id", "i_manufact", "i_manager_id"]]
        cust = _read("customer")[["c_customer_sk", "c_current_addr_sk"]]
        addr = _read("customer_address")[["ca_address_sk", "ca_zip"]]
        store = _read("store")[["s_store_sk", "s_zip"]]

        dt = dt[(dt["d_moy"] == 11) & (dt["d_year"] == 1998)]
        item = item[item["i_manager_id"] == 8]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
            .merge(cust, left_on="ss_customer_sk", right_on="c_customer_sk")
            .merge(addr, left_on="c_current_addr_sk", right_on="ca_address_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        )
        merged = merged[merged["ca_zip"].str[:5] != merged["s_zip"].str[:5]]
        result = (
            merged.groupby(["i_brand", "i_brand_id", "i_manufact_id", "i_manufact"])
            .agg(ext_price=("ss_ext_sales_price", "sum"))
            .reset_index()
            .sort_values(["ext_price", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact"],
                         ascending=[False, True, True, True, True])
            .head(100)
        )
        _write("query_q19", result)
        log.info("q19: %d rows", len(result))
        return {"query": "q19", "rows": len(result)}

    @task()
    def query_q27() -> dict:
        """Avg metrics by item/state with ROLLUP (simplified)."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_cdemo_sk",
                                    "ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_sales_price"]]
        cd = _read("customer_demographics")[["cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status"]]
        dt = _read("date_dim")[["d_date_sk", "d_year"]]
        store = _read("store")[["s_store_sk", "s_state"]]
        item = _read("item")[["i_item_sk", "i_item_id"]]

        cd = cd[(cd["cd_gender"] == "M") & (cd["cd_marital_status"] == "S") & (cd["cd_education_status"] == "College")]
        dt = dt[dt["d_year"] == 2002]
        store = store[store["s_state"] == "TN"]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
            .merge(cd, left_on="ss_cdemo_sk", right_on="cd_demo_sk")
        )
        # Detail level
        detail = (
            merged.groupby(["i_item_id", "s_state"])
            .agg(agg1=("ss_quantity", "mean"), agg2=("ss_list_price", "mean"),
                 agg3=("ss_coupon_amt", "mean"), agg4=("ss_sales_price", "mean"))
            .reset_index()
        )
        detail["g_state"] = 0
        # Item-level rollup
        item_level = (
            merged.groupby("i_item_id")
            .agg(agg1=("ss_quantity", "mean"), agg2=("ss_list_price", "mean"),
                 agg3=("ss_coupon_amt", "mean"), agg4=("ss_sales_price", "mean"))
            .reset_index()
        )
        item_level["s_state"] = None
        item_level["g_state"] = 1
        # Grand total
        grand = pd.DataFrame([{
            "i_item_id": None, "s_state": None, "g_state": 1,
            "agg1": merged["ss_quantity"].mean(),
            "agg2": merged["ss_list_price"].mean(),
            "agg3": merged["ss_coupon_amt"].mean(),
            "agg4": merged["ss_sales_price"].mean(),
        }])
        result = pd.concat([detail, item_level, grand]).sort_values(["i_item_id", "s_state"]).head(100)
        _write("query_q27", result)
        log.info("q27: %d rows", len(result))
        return {"query": "q27", "rows": len(result)}

    @task()
    def query_q34() -> dict:
        """Customers with 15-20 items per ticket at specific stores."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_ticket_number", "ss_customer_sk"]]
        dt = _read("date_dim")[["d_date_sk", "d_dom", "d_year"]]
        store = _read("store")[["s_store_sk", "s_county"]]
        hd = _read("household_demographics")[["hd_demo_sk", "hd_buy_potential", "hd_vehicle_count", "hd_dep_count"]]
        cust = _read("customer")[["c_customer_sk", "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag"]]

        dt = dt[((dt["d_dom"] >= 1) & (dt["d_dom"] <= 3)) | ((dt["d_dom"] >= 25) & (dt["d_dom"] <= 28))]
        dt = dt[dt["d_year"].isin([1999, 2000, 2001])]
        store = store[store["s_county"] == "Williamson County"]
        hd = hd[(hd["hd_buy_potential"].isin([">10000", "Unknown"])) & (hd["hd_vehicle_count"] > 0)]
        hd = hd[(hd["hd_dep_count"] / hd["hd_vehicle_count"]) > 1.2]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
            .merge(hd, left_on="ss_hdemo_sk", right_on="hd_demo_sk")
        )
        ticket_counts = merged.groupby(["ss_ticket_number", "ss_customer_sk"]).size().reset_index(name="cnt")
        ticket_counts = ticket_counts[(ticket_counts["cnt"] >= 15) & (ticket_counts["cnt"] <= 20)]
        result = ticket_counts.merge(cust, left_on="ss_customer_sk", right_on="c_customer_sk")
        result = (
            result[["c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag", "ss_ticket_number", "cnt"]]
            .sort_values(["c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag", "ss_ticket_number"],
                         ascending=[True, True, True, False, True])
        )
        _write("query_q34", result)
        log.info("q34: %d rows", len(result))
        return {"query": "q34", "rows": len(result)}

    @task()
    def query_q43() -> dict:
        """Weekly sales pivot by store, year 2000."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_store_sk", "ss_sales_price"]]
        dt = _read("date_dim")[["d_date_sk", "d_year", "d_day_name"]]
        store = _read("store")[["s_store_sk", "s_store_name", "s_store_id", "s_gmt_offset"]]

        dt = dt[dt["d_year"] == 2000]
        store = store[store["s_gmt_offset"] == -5]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        )
        days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        day_cols = ["sun_sales", "mon_sales", "tue_sales", "wed_sales", "thu_sales", "fri_sales", "sat_sales"]
        for day, col in zip(days, day_cols):
            merged[col] = merged["ss_sales_price"].where(merged["d_day_name"] == day)

        result = (
            merged.groupby(["s_store_name", "s_store_id"])
            .agg({c: "sum" for c in day_cols})
            .reset_index()
            .sort_values(["s_store_name", "s_store_id"] + day_cols)
            .head(100)
        )
        _write("query_q43", result)
        log.info("q43: %d rows", len(result))
        return {"query": "q43", "rows": len(result)}

    @task()
    def query_q46() -> dict:
        """Customers whose purchase city differs from residence city."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_addr_sk",
                                    "ss_ticket_number", "ss_customer_sk", "ss_coupon_amt", "ss_net_profit"]]
        dt = _read("date_dim")[["d_date_sk", "d_dow", "d_year"]]
        store = _read("store")[["s_store_sk", "s_city"]]
        hd = _read("household_demographics")[["hd_demo_sk", "hd_dep_count", "hd_vehicle_count"]]
        ca = _read("customer_address")[["ca_address_sk", "ca_city"]]
        cust = _read("customer")[["c_customer_sk", "c_current_addr_sk", "c_last_name", "c_first_name"]]

        dt = dt[(dt["d_dow"].isin([0, 6])) & (dt["d_year"].isin([1999, 2000, 2001]))]
        store = store[store["s_city"].isin(["Fairview", "Midway"])]
        hd = hd[(hd["hd_dep_count"] == 4) | (hd["hd_vehicle_count"] == 3)]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
            .merge(hd, left_on="ss_hdemo_sk", right_on="hd_demo_sk")
            .merge(ca, left_on="ss_addr_sk", right_on="ca_address_sk")
        )
        bought = (
            merged.groupby(["ss_ticket_number", "ss_customer_sk", "ca_city"])
            .agg(amt=("ss_coupon_amt", "sum"), profit=("ss_net_profit", "sum"))
            .reset_index()
            .rename(columns={"ca_city": "bought_city"})
        )
        result = (
            bought
            .merge(cust, left_on="ss_customer_sk", right_on="c_customer_sk")
            .merge(ca.rename(columns={"ca_city": "current_city"}), left_on="c_current_addr_sk", right_on="ca_address_sk")
        )
        result = result[result["current_city"] != result["bought_city"]]
        result = (
            result[["c_last_name", "c_first_name", "current_city", "bought_city", "ss_ticket_number", "amt", "profit"]]
            .rename(columns={"current_city": "ca_city"})
            .sort_values(["c_last_name", "c_first_name", "ca_city", "bought_city", "ss_ticket_number"])
            .head(100)
        )
        _write("query_q46", result)
        log.info("q46: %d rows", len(result))
        return {"query": "q46", "rows": len(result)}

    @task()
    def query_q53() -> dict:
        """Manufacturers whose quarterly sales deviate >10% from their average."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price"]]
        dt = _read("date_dim")[["d_date_sk", "d_month_seq", "d_qoy"]]
        item = _read("item")[["i_item_sk", "i_manufact_id", "i_category", "i_class", "i_brand"]]
        _read("store")  # join present in SQL but no filter needed beyond existence

        dt = dt[dt["d_month_seq"].between(1200, 1211)]

        cond1 = (
            item["i_category"].isin(["Books", "Children", "Electronics"])
            & item["i_class"].isin(["personal", "portable", "reference", "self-help"])
            & item["i_brand"].isin(["scholaramalgamalg #14", "scholaramalgamalg #7",
                                     "exportiunivamalg #9", "scholaramalgamalg #9"])
        )
        cond2 = (
            item["i_category"].isin(["Women", "Music", "Men"])
            & item["i_class"].isin(["accessories", "classical", "fragrances", "pants"])
            & item["i_brand"].isin(["amalgimporto #1", "edu packscholar #1",
                                     "exportiimporto #1", "importoamalg #1"])
        )
        item = item[cond1 | cond2]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        )
        quarterly = (
            merged.groupby(["i_manufact_id", "d_qoy"])
            .agg(sum_sales=("ss_sales_price", "sum"))
            .reset_index()
        )
        quarterly["avg_quarterly_sales"] = quarterly.groupby("i_manufact_id")["sum_sales"].transform("mean")
        quarterly = quarterly[
            (quarterly["avg_quarterly_sales"] > 0)
            & ((quarterly["sum_sales"] - quarterly["avg_quarterly_sales"]).abs() / quarterly["avg_quarterly_sales"] > 0.1)
        ]
        result = quarterly.sort_values(["avg_quarterly_sales", "sum_sales", "i_manufact_id"]).head(100)
        _write("query_q53", result)
        log.info("q53: %d rows", len(result))
        return {"query": "q53", "rows": len(result)}

    @task()
    def query_q67() -> dict:
        """Top-100 products by category sales (simplified ROLLUP)."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price", "ss_quantity"]]
        dt = _read("date_dim")[["d_date_sk", "d_year", "d_qoy", "d_moy", "d_month_seq"]]
        store = _read("store")[["s_store_sk", "s_store_id"]]
        item = _read("item")[["i_item_sk", "i_category", "i_class", "i_brand", "i_product_name"]]

        dt = dt[dt["d_month_seq"].between(1200, 1211)]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        )
        merged["sumsales"] = (merged["ss_sales_price"] * merged["ss_quantity"]).fillna(0)

        # Simplified: detail-level groupby only (skip full ROLLUP for pandas feasibility)
        detail = (
            merged.groupby(["i_category", "i_class", "i_brand", "i_product_name",
                            "d_year", "d_qoy", "d_moy", "s_store_id"])
            .agg(sumsales=("sumsales", "sum"))
            .reset_index()
        )
        detail["rk"] = detail.groupby("i_category")["sumsales"].rank(method="first", ascending=False)
        result = (
            detail[detail["rk"] <= 100]
            .sort_values(["i_category", "i_class", "i_brand", "i_product_name",
                          "d_year", "d_qoy", "d_moy", "s_store_id", "sumsales", "rk"])
            .head(100)
        )
        _write("query_q67", result)
        log.info("q67: %d rows", len(result))
        return {"query": "q67", "rows": len(result)}

    @task()
    def query_q79() -> dict:
        """Customer coupon/profit by ticket at stores with 200-295 employees."""
        ss = _read("store_sales")[["ss_sold_date_sk", "ss_store_sk", "ss_hdemo_sk", "ss_addr_sk",
                                    "ss_ticket_number", "ss_customer_sk", "ss_coupon_amt", "ss_net_profit"]]
        dt = _read("date_dim")[["d_date_sk", "d_dow", "d_year"]]
        store = _read("store")[["s_store_sk", "s_city", "s_number_employees"]]
        hd = _read("household_demographics")[["hd_demo_sk", "hd_dep_count", "hd_vehicle_count"]]
        cust = _read("customer")[["c_customer_sk", "c_last_name", "c_first_name"]]

        dt = dt[(dt["d_dow"] == 1) & (dt["d_year"].isin([1999, 2000, 2001]))]
        store = store[store["s_number_employees"].between(200, 295)]
        hd = hd[(hd["hd_dep_count"] == 6) | (hd["hd_vehicle_count"] > 2)]

        merged = (
            ss
            .merge(dt, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
            .merge(hd, left_on="ss_hdemo_sk", right_on="hd_demo_sk")
        )
        ticket = (
            merged.groupby(["ss_ticket_number", "ss_customer_sk", "s_city"])
            .agg(amt=("ss_coupon_amt", "sum"), profit=("ss_net_profit", "sum"))
            .reset_index()
        )
        result = (
            ticket
            .merge(cust, left_on="ss_customer_sk", right_on="c_customer_sk")
        )
        result["s_city_30"] = result["s_city"].str[:30]
        result = (
            result[["c_last_name", "c_first_name", "s_city_30", "ss_ticket_number", "amt", "profit"]]
            .sort_values(["c_last_name", "c_first_name", "s_city_30", "profit"])
            .head(100)
        )
        _write("query_q79", result)
        log.info("q79: %d rows", len(result))
        return {"query": "q79", "rows": len(result)}

    # ---- Stage 6: Verify -------------------------------------------------
    @task()
    def verify() -> dict:
        tables_to_check = [
            "store_sales", "catalog_sales", "web_sales",
            "store_returns", "catalog_returns", "web_returns",
            "inventory", "customer", "item",
            "wide_store_sales", "wide_catalog_sales", "wide_web_sales",
            "daily_sales_by_store", "monthly_sales_by_category",
            "customer_lifetime_value", "channel_comparison",
            "promo_roi", "return_rate_by_category", "inventory_turnover",
        ]
        counts = {}
        for t in tables_to_check:
            p = _local(t)
            if p.exists():
                counts[t] = len(pd.read_parquet(p))
            else:
                counts[t] = -1
                log.warning("Missing table: %s", t)
        log.info("Row counts: %s", counts)
        return counts

    # ---- Wire the DAG ----------------------------------------------------
    ingested = [ingest.override(task_id=f"ingest_{t}")(t) for t in TABLES]

    fact_groups = [
        "store_sales", "catalog_sales", "web_sales",
        "store_returns", "catalog_returns", "web_returns", "inventory",
    ]
    validated_facts = [validate_fact.override(task_id=f"validate_{g}")(g) for g in fact_groups]
    validated_dims = validate_dimensions()

    for v in validated_facts + [validated_dims]:
        for i in ingested:
            i >> v

    d_ss = denorm_store_sales()
    d_cs = denorm_catalog_sales()
    d_ws = denorm_web_sales()

    for v in validated_facts + [validated_dims]:
        v >> d_ss
        v >> d_cs
        v >> d_ws

    a1 = agg_daily_store()
    a2 = agg_monthly_category()
    a3 = agg_customer_ltv()
    a4 = agg_channel_comparison()
    a5 = agg_promo_roi()
    a6 = agg_return_rate()
    a7 = agg_inventory_turnover()

    d_ss >> a1
    d_ss >> a2
    d_ss >> a5
    for d in [d_ss, d_cs, d_ws]:
        d >> a3
        d >> a4
    d_ss >> a6
    d_ss >> a7

    q03 = query_q03()
    q07 = query_q07()
    q19 = query_q19()
    q27 = query_q27()
    q34 = query_q34()
    q43 = query_q43()
    q46 = query_q46()
    q53 = query_q53()
    q67 = query_q67()
    q79 = query_q79()

    queries = [q03, q07, q19, q27, q34, q43, q46, q53, q67, q79]
    for agg_task in [a1, a2, a3, a4, a5, a6, a7]:
        for q in queries:
            agg_task >> q

    v = verify()
    for q in queries:
        q >> v


tpcds_etl()
