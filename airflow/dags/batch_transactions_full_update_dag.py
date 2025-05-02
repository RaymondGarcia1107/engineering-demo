from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator  
from datetime import datetime
from etl.batch_s3_etl import export_tables
from etl.incremental_rds_etl import incremental_load_from_s3

with DAG(
    dag_id="batch_transactions_full_update",
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    export_transactions = PythonOperator(
        task_id="export_transactions",
        python_callable=export_tables,
        op_kwargs={'table_name':'transactions'}
    )

    truncate = PostgresOperator(
        task_id="truncate_raw_transactions",
        postgres_conn_id="altera_data",
        sql= "TRUNCATE raw.transactions RESTART IDENTITY;"
    )

    load_to_raw = PythonOperator(
        task_id="load_from_s3",
        python_callable=incremental_load_from_s3,
        op_kwargs={
            "table_name":"transactions",
            "s3_keys": "{{ti.xcom_pull(task_ids='export_transactions'}}"    
        }
    )

    export_transactions >> truncate >> load_to_raw