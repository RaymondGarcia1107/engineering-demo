import pandas as pd
import psycopg2
import boto3
import io
import os

def fetch_table(sql):
    conn = psycopg2.connect(
        host=os.environ["SRC_DB_HOST"],
        user=os.environ["SRC_DB_USER"],
        password=os.environ["SRC_DB_PASSWORD"],
        dbname=os.environ["SRC_DB"],
        port=int(os.environ.get("SRC_DB_PORT",5432))
    )
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def upload_df_to_s3(df, s3_path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine = "pyarrow", index=False)
    buffer.seek(0)

    s3 = boto3.Session(region_name = os.environ["REGION"]).client('s3')
    s3.upload_fileobj(buffer, os.environ["S3_BUCKET"], s3_path)
    print(f'Uploaded to s3://{os.environ["S3_BUCKET"]}/{s3_path}')
    return None

def export_tables():
    users_df = fetch_table("SELECT * FROM users;")
    upload_df_to_s3(users_df, "users/users.parquet")

    transactions_df = fetch_table("SELECT * FROM transactions;")
    upload_df_to_s3(transactions_df, "transactions/transactions.parquet")

if __name__ == '__main__':
    export_tables()