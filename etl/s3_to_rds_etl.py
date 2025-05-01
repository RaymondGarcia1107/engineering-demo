import os
import io
import pandas as pd
import psycopg2


def load_table_from_s3(table_name, prefix):

    bucket = os.environ["S3_BUCKET"]

    uri = f"s3://{bucket}/{prefix}"

    df = pd.read_parquet(uri, engine='pyarrow')

    if df.empty:
        print(f"No rows found under {uri}")
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
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_sql = f"COPY raw.{table_name} FROM STDIN WITH CSV;"

    cur.copy_expert(copy_sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

    print(f'Loaded {len(df)} rows into raw.{table_name} from {uri}')