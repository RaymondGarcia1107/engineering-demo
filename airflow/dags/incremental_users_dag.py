from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.incremental_s3_etl import incremental_export
from etl.incremental_rds_hist_etl import incremental_load_from_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}
with DAG(
    dag_id='incremental_users_update',
    default_args=default_args,
    description='Incrementally export updated users from Postgres to S3 and load into RDS',
    schedule_interval=None,
    start_date=datetime(2025, 5, 1),
    catchup=False
) as dag:
    
    export_users = PythonOperator(
        task_id="export_users",
        python_callable=incremental_export,
        op_kwargs={'table_name':'users','watermark_col':'updated_at'}
    )
    load_users_rds = PythonOperator(
        task_id='load_users',
        python_callable=incremental_load_from_s3,
        op_kwargs={
            'table_name':'users',
            "s3_keys": export_users.output    
        }
    )

    export_users >> load_users_rds
