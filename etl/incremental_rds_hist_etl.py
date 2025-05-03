import os
import io
import pandas as pd
import psycopg2
import boto3

def list_s3_parquet_keys(prefix):
    bucket = os.environ['S3_BUCKET']
    client = boto3.client('s3', region_name = os.environ.get("REGION"))
    paginator = client.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []) or []:
            if obj['Key'].endswith('.parquet'):
                keys.append(obj['Key'])

    return keys

def incremental_load_from_s3(table_name, s3_keys):
    bucket = os.environ['S3_BUCKET']
    region = os.environ.get('REGION')

    if not s3_keys:
        prefix = f"{table_name}/"
        print(f"Listing all files under s3://{bucket}/{prefix}")
        s3_keys = list_s3_parquet_keys(prefix)

    if not s3_keys:
        print(f"No parquet files found under s3://{bucket}/{prefix}")
        return
    
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        dbname=os.environ['DBT_DBNAME'],
        port=int(os.environ.get('POSTGRES_PORT', 5432))
    )
    cur = conn.cursor()

    cur.execute("select s3_key from raw._load_history where table_name = %s;", (table_name,))

    loaded = { row[0] for row in cur.fetchall()}

    new_keys = [k for k in s3_keys if k not in loaded]

    if not new_keys:
        print(f"No new files to load for {table_name}")
        cur.close()
        conn.close()
        return
    
    s3 = boto3.client('s3', region_name = region)

    for key in new_keys:
        print(f'Processing s3://{bucket}/{key}')
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()), engine='pyarrow')
        if df.empty:
            print(f'Skipping empty file: {key}')
            continue

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=True)
        csv_buffer.seek(0)

        cols = ", ".join(df.columns)
        copy_sql = f"COPY raw.{table_name} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        cur.copy_expert(copy_sql, csv_buffer)
        conn.commit()
        print(f"Loaded {len(df)} rows into raw.{table_name} from {s3_keys}")

        cur.execute(
            "INSERT INTO raw._load_history (table_name, s3_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (table_name, key)
        )
        
        conn.commit()

    cur.close()
    conn.close()
