import psycopg2

def main():
    conn = psycopg2.connect(
        host = "3.15.239.196",
        database = "random_data",
        user = "admin",
        password = "admin",
        port = 5432
    )
    
    cur = conn.cursor()

    sql = "select version();"
    cur.execute(sql)

    version = cur.fetchone()

    print(f"Postgres version: {version}")

    cur.close()
    conn.close()

    return None

if __name__ == "__main__":
    main()