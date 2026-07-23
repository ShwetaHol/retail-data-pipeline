FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir dbt-bigquery==1.11.3 && \
    pip install --no-cache-dir "click<8.2"