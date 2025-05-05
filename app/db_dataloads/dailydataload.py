from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import random
import os

# Initializing a faker object
fake = Faker()

# Connecting to the database and setting up a cursor
conn = psycopg2.connect(
    host = os.environ['SRC_DB_HOST'],
    database = os.environ['SRC_DB'],
    user = os.environ['SRC_DB_USER'],
    password = os.environ['SRC_DB_PASSWORD'],
    port = 5432
)
cur = conn.cursor()
print("DB Connected")

# Generating fake user data
user_data = [(fake.name(), fake.email(), fake.address()) for _ in range(5)]

# Inserting into the user table and committing
execute_values(cur, "INSERT INTO users (name, email, address) VALUES %s", user_data)
conn.commit()
print("User data inserted")

# Gathering the id's to generate transactions on
cur.execute("select id from users;")
user_ids = [row[0] for row in cur.fetchall()]
print("ID's gathered")

# Generating random data
transaction_types = ['purchase', 'refund', 'transfer', 'payment']
data = [
    (
        random.choice(user_ids),
        round(random.uniform(10.0, 10000.0), 2),
        random.choice(transaction_types)
    )
    for _ in range(50)
]

# Inserting the values into the transactions table and committing
execute_values(cur, "INSERT INTO transactions (user_id, amount, transaction_type) VALUES %s", data)
conn.commit()
print("Transaction data inserted")

# Closing connections
cur.close()
conn.close()
print("Connections closed. All clear")