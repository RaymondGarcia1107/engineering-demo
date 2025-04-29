from faker import Faker
import psycopg2
from psycopg2.extras import execute_values


fake = Faker()

conn = psycopg2.connect(
    host = "18.189.178.29",
    database = "random_data",
    user = "admin",
    password = "admin",
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