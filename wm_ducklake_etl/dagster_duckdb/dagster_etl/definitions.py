from dagster import Definitions, load_assets_from_modules

from dagster_etl import assets
from dagster_etl.resources import DuckDBResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(database="/tmp/bench.duckdb"),
    },
)
