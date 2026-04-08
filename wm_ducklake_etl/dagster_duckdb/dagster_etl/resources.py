import os

import duckdb
from dagster import ConfigurableResource, InitResourceContext


class DuckDBResource(ConfigurableResource):
    """DuckDB resource with S3 credentials configured from environment variables."""

    database: str = ":memory:"

    def _configure_s3(self, conn: duckdb.DuckDBPyConnection) -> None:
        endpoint = os.environ.get("S3_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("S3_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("S3_SECRET_KEY", "minioadmin")
        region = os.environ.get("S3_REGION", "us-east-1")

        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint = '{endpoint}';")
        conn.execute(f"SET s3_access_key_id = '{access_key}';")
        conn.execute(f"SET s3_secret_access_key = '{secret_key}';")
        conn.execute(f"SET s3_region = '{region}';")
        conn.execute("SET s3_use_ssl = false;")
        conn.execute("SET s3_url_style = 'path';")

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self.database)
        self._configure_s3(conn)
        return conn
