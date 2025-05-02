FROM apache/airflow:2.7.2

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev python3-dev build-essential

USER airflow
RUN pip install --no-cache-dir pandas pyarrow boto3 psycopg2-binary s3fs fsspec
