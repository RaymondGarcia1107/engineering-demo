from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.incremental_s3_etl import incremental_export
from etl.s3_to_rds_etl import load_table_from_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
    dag_id='full_dag',
    default_args=default_args,
    description='Incrementally export updated users and transactions from Postgres to S3 and load into RDS',
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

    load_users_rds = PythonOperator(
        task_id='load_users',
        python_callable=load_table_from_s3,
        op_kwargs={'table_name':'users','prefix':'users/'}
    )

    load_transactions_rds = PythonOperator(
        task_id='load_transactions',
        python_callable=load_table_from_s3,
        op_kwargs={'table_name':'transactions','prefix':'transactions/'}
    )
    export_users >> load_users_rds >> export_transactions >> load_transactions_rds