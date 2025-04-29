import psycopg2

def main():
    conn = psycopg2.connect(
        host = "18.189.178.29",
        database = "random_data",
        user = "admin",
        password = "admin",
        port = 5432
    )
    
    cur = conn.cursor()

    sql = "select * from users limit 10;"
    cur.execute(sql)

    data = cur.fetchall()

    for row in data:
        print(row)

    cur.close()
    conn.close()

    return None

if __name__ == "__main__":
    main()