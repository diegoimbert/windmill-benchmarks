from dagster import Definitions, define_asset_job, in_process_executor, load_assets_from_modules

from dagster_etl import assets
from dagster_etl.resources import DuckDBResource

all_assets = load_assets_from_modules([assets])

tpcds_etl_job = define_asset_job(
    name="tpcds_etl_job",
    selection="*",
    executor_def=in_process_executor,
)

defs = Definitions(
    assets=all_assets,
    jobs=[tpcds_etl_job],
    resources={
        "duckdb": DuckDBResource(database="/tmp/bench.duckdb"),
    },
)
