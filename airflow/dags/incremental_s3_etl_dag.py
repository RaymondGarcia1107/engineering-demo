from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.incremental_s3_etl import incremental_export

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
    dag_id='incremental_s3_export',
    default_args=default_args,
    description='Incrementally export updated users and transactions from Postgres to S3',
    schedule_interval='@hourly',
    start_date=datetime(2025, 5, 1),
    catchup=False
) as dag:
    
    export_users = PythonOperator(
        task_id="export_users",
        python_callable=incremental_export,
        op_kwargs={'table_name':'users','watermark_col':'updated_at'}
    )

    export_transactions = PythonOperator(
        task_id="export_transactions",
        python_callable=incremental_export,
        op_kwargs={'table_name':'transactions', 'watermark_col':'updated_at'}
    )

    export_users >> export_transactions