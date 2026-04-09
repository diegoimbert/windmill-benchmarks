FROM apache/airflow:2.10.0-python3.11

USER airflow

RUN pip install --no-cache-dir duckdb>=1.5.1

# Pre-install DuckDB extensions so tasks only need LOAD (not INSTALL)
RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL httpfs; INSTALL parquet; INSTALL ducklake; INSTALL postgres;'); conn.close()"
