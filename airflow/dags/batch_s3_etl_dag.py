from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.batch_s3_etl import export_tables

with DAG(
    dag_id="batch_full_update",
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    export_task = PythonOperator(
        task_id="export_tables",
        python_callable=export_tables
    )