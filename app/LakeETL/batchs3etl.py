import pandas as pd
import psycopg2
import boto3
import io
from postgresconfig import *


def fetch_table(sql):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def upload_df_to_s3(df, s3_path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine = "pyarrow", index=False)
    buffer.seek(0)

    session = boto3.Session(profile_name = "alteradata", region_name = REGION)
    s3 = session.client('s3')
    s3.upload_fileobj(buffer, S3_BUCKET, s3_path)
    print(f'Uploaded to s3://{S3_BUCKET}/{s3_path}')
    return None

if __name__ == "__main__":
    users_df = fetch_table("SELECT * FROM users;")
    upload_df_to_s3(users_df, "users/users.parquet")

    transactions_df = fetch_table("SELECT * FROM transactions;")
    upload_df_to_s3(transactions_df, "transactions/transactions.parquet")