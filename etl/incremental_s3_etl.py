import os 
import io
import pandas as pd
import psycopg2
import boto3
from airflow.models import Variable
from datetime import datetime, timezone

def incremental_export(table_name, watermark_col):

    var_key = f"last_{table_name}_sync"
    last_sync = Variable.get(var_key, default_var="1970-01-01T00:00:00Z")
    print(f"Running incremental_export for {table_name}, since {last_sync}")
    
    conn = psycopg2.connect(
        host=os.environ["SRC_DB_HOST"],
        user=os.environ["SRC_DB_USER"],
        password=os.environ["SRC_DB_PASSWORD"],
        dbname=os.environ["SRC_DB"],
        port=int(os.environ.get("SRC_DB_PORT",5432))
    )

    sql = f"""
        select * 
        from {table_name} 
        where {watermark_col} > %s 
        order by {watermark_col} asc;
        """
    
    df = pd.read_sql(sql, conn, params=[last_sync])
    conn.close()

    if df.empty:
        print(f"No new rows for {table_name} since {last_sync}")
        return
    
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_ts = datetime.now(timezone.utc).isoformat()
    s3_path = f"{table_name}/run_date={run_date}/{table_name}--{run_ts}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index = False)
    buffer.seek(0)

    s3 = boto3.Session(region_name=os.environ["REGION"]).client('s3')
    s3.upload_fileobj(buffer, os.environ["S3_BUCKET"], s3_path)
    print(f"Uploaded {len(df)} rows to s3://{os.environ['S3_BUCKET']}/{s3_path}")

    new_sync = df[watermark_col].max().isoformat()
    Variable.set(var_key, new_sync)
    print(f"Updated watermark for {table_name} to {new_sync}")

def increment_changes():
    incremental_export("users", "updated_at")
    incremental_export("transactions", "updated_at")

if __name__ == '__main__':
    increment_changes()

