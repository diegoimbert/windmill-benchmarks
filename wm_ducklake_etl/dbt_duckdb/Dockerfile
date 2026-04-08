FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core dbt-duckdb

WORKDIR /dbt

COPY dbt_project.yml profiles.yml ./
COPY models/ models/

RUN mkdir -p /data

ENTRYPOINT ["dbt"]
CMD ["run", "--threads", "8", "--profiles-dir", "/dbt"]
