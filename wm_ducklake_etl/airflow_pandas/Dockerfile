FROM apache/airflow:2.10.0-python3.11

USER airflow

RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    pyarrow==17.0.0 \
    s3fs==2024.6.1

COPY dags/ /opt/airflow/dags/

RUN mkdir -p /opt/airflow/data
