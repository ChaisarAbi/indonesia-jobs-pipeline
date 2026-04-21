FROM apache/airflow:2.10.0
USER airflow
RUN pip install --no-cache-dir "apache-airflow-providers-opensearch" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.12.txt"
