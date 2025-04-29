from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import random

fake = Faker()

conn = psycopg2.connect(
    host = "18.189.178.29",
    database = "random_data",
    user = "admin",
    password = "admin",
    port = 5432
)

cur = conn.cursor()

table = """CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount NUMERIC(10, 2),
    transaction_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""

cur.execute(table)
print("table created")

cur.execute("select id from users;")
print("id's received")

user_ids = [row[0] for row in cur.fetchall()]

transaction_types = ['purchase', 'refund', 'transfer', 'payment']

data = [
    (
        random.choice(user_ids),
        round(random.uniform(10.0, 10000.0), 2),
        random.choice(transaction_types),
        fake.date_time_between(start_date='-1y', end_date='now')
    )
    for _ in range(100000)
]

execute_values(cur, "INSERT INTO transactions (user_id, amount, transaction_type, created_at) VALUES %s", data)

conn.commit()
cur.close()
conn.close()

print("Complete")