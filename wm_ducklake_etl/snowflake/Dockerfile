FROM apache/airflow:2.10.0-python3.11

RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.7.1

COPY dags/ /opt/airflow/dags/
