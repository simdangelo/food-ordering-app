import psycopg2
import uuid
import datetime
import random
import time


def test_postgres_connection(host, database, user, password, port):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        conn.close()
        print("Connection to PostgreSQL successful!")

    except psycopg2.Error as e:
        # Print error message if connection fails
        print(f"Error connecting to PostgreSQL: {e}")


def create_postgres_schema(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute(
        """
        CREATE SCHEMA IF NOT EXISTS spark_streams;
        """
    )
    cursor.close()
    postgres_connection.commit()

def create_postgres_table(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.orders (
            id VARCHAR(255) PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            food VARCHAR(255),
            size VARCHAR(255),
            cost DECIMAL,
            time TIMESTAMP,
            order_completed INT
            );
        """)
    cursor.close()
    postgres_connection.commit()

    print("Table created successfully!")

def create_fake_orders(postgres_connection):
    # cursor = postgres_connection.cursor()

    total_iterations = 10000
    for i in range(total_iterations):

        cursor = postgres_connection.cursor()

        order_id = str(uuid.uuid4())
        username = "user_" + str(random.randint(1, 10000))
        email = username + "@example.com"
        food = random.choice(["pizza", "burger", "sushi", "taco"])
        size = random.choice(["small", "medium", "large"])
        cost = round(random.uniform(5, 20), 2)

        hour = int(random.gauss(13, 2)) if random.random() < 0.5 else int(random.gauss(20, 2))
        hour %= 24
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=90)
        random_day = start_date + datetime.timedelta(days=random.randint(0, 90))

        time_ = datetime.datetime.combine(random_day, datetime.time(hour, minute, second)).isoformat()
        order_completed = random.choice([0, 1])

        cursor.execute("""
            INSERT INTO spark_streams.orders (id, username, email, food, size, cost, time, order_completed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, username, email, food, size, cost, time_, order_completed))

        print(f"Progress: {i + 1}/{total_iterations}")

        cursor.close()
        postgres_connection.commit()

        # time.sleep(0.00000001)


conn = psycopg2.connect(
        host='localhost',
        database='database',
        user='user',
        password='password',
        port=5432
    )
host = 'localhost'
database = 'database'
user = 'user'
password = 'password'
port = 5432
test_postgres_connection(host, database, user, password, port)

create_postgres_schema(conn)
create_postgres_table(conn)

# Populate the table with fake data
create_fake_orders(conn)
