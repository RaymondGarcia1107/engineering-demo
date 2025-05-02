from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator  
from datetime import datetime
from etl.batch_s3_etl import export_tables
from etl.incremental_rds_etl import incremental_load_from_s3

with DAG(
    dag_id="batch_users_full_update",
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    export_users = PythonOperator(
        task_id="export_users",
        python_callable=export_tables,
        op_kwargs={'table_name':'users'}
    )

    truncate = PostgresOperator(
        task_id="truncate_raw_users",
        postgres_conn_id="altera_data",
        sql= "TRUNCATE raw.users RESTART IDENTITY;"
    )

    load_to_raw = PythonOperator(
        task_id="load_from_s3",
        python_callable=incremental_load_from_s3,
        op_kwargs={
            "table_name":"users",
            "s3_keys": "{{ti.xcom_pull(task_ids='export_users')}}"    
        }
    )

    export_users >> truncate >> load_to_raw