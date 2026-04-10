FROM python:3.11-slim

RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    duckdb

COPY dagster_etl/ /opt/dagster/app/dagster_etl/

WORKDIR /opt/dagster/app

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
COPY dagster_home/dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY dagster_home/workspace.yaml $DAGSTER_HOME/workspace.yaml

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "dagster_etl.definitions"]
