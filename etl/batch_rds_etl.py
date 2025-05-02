import os
import io
import pandas as pd
import psycopg2
import s3fs


def batch_load_table_from_s3(table_name, prefix):

    bucket = os.environ["S3_BUCKET"]
    fs = s3fs.S3FileSystem()

    keys = fs.glob(f'{bucket}/{prefix}**/*.parquet')
    s3_paths = [f's3://{key}' for key in keys]

    # Read each file and concatenate
    dfs = [pd.read_parquet(p, engine='pyarrow') for p in s3_paths]
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        print(f"No rows found")
        return
    
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        dbname=os.environ['DBT_DBNAME'],
        port=int(os.environ.get('POSTGRES_PORT', 5432))
    )

    cur = conn.cursor()

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    cols = ", ".join(df.columns)

    copy_sql = f"COPY raw.{table_name} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true);"

    cur.copy_expert(copy_sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

    print(f'Loaded {len(df)} rows into raw.{table_name}')