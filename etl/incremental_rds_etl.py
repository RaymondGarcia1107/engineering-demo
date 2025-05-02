import os
import io
import pandas as pd
import psycopg2
import boto3

def incremental_load_from_s3(table_name, s3_keys):

    bucket = os.environ["S3_BUCKET"]

    if not s3_keys:
        print(f"No new files to load for {table_name}")
        return
    
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        dbname=os.environ['DBT_DBNAME'],
        port=int(os.environ.get('POSTGRES_PORT', 5432))
    )
    cur = conn.cursor()

    s3 = boto3.client('s3', region_name=os.environ.get("REGION"))

    print(f"Processing s3://{bucket}/{s3_keys}")
    obj = s3.get_object(Bucket=bucket, Key=s3_keys)

    df = pd.read_parquet(io.BytesIO(obj['Body'].read()), engine='pyarrow')

    if df.empty:
        print(f"No rows found")
        return
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)

    cols = ", ".join(df.columns)
    copy_sql = f"COPY raw.{table_name} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    cur.copy_expert(copy_sql, csv_buffer)
    conn.commit()

    print(f"Loaded {len(df)} rows into raw.{table_name} from {s3_keys}")

    cur.close()
    conn.close()
