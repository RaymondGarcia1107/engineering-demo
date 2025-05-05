from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import os

fake = Faker()

conn = psycopg2.connect(
    host = os.environ['SRC_DB_HOST'],
    database = os.environ['SRC_DB'],
    user = os.environ['SRC_DB_USER'],
    password = os.environ['SRC_DB_PASSWORD'],
    port = 5432
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT, 
        address TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );           
""")

data = [(fake.name(), fake.email(), fake.address()) for _ in range(9999)]

execute_values(cur, "INSERT INTO users (name, email, address) VALUES %s", data)

conn.commit()
cur.close()
conn.close()