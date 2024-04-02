import faker
import psycopg2
from datetime import datetime

fake = faker.Faker()

def generate_transaction():
    return {
        "id": fake.uuid4(),
        "username": fake.name(),
        "email": fake.email(),
        "food": fake.word(),
        "size": fake.word(),
        "cost": fake.random_number(digits=2),
        "time": datetime.now().isoformat(),
        "order_completed": 1,
    }

def create_table(conn):
    cursor = conn.cursor()

    cursor.execute(
        """
        DROP SCHEMA IF EXISTS spark_streams CASCADE;
        CREATE SCHEMA IF NOT EXISTS spark_streams;
        CREATE TABLE IF NOT EXISTS spark_streams.orders (
            id VARCHAR(255) PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            food VARCHAR(255),
            size VARCHAR(255),
            cost DECIMAL,
            time TIMESTAMP,
            order_completed INT
        )
        """)

    cursor.close()
    conn.commit()

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


if __name__ == "__main__":
    host = 'localhost'
    database = 'database'
    user = 'user'
    password = 'password'
    port = 5432

    test_postgres_connection(host, database, user, password, port)

    # conn = psycopg2.connect(
    #     host='localhost',
    #     database='database',
    #     user='user',
    #     password='password',
    #     port=5432
    # )
    #
    #
    # create_table(conn)
    #
    # transaction = generate_transaction()
    # cur = conn.cursor()
    # print(transaction)
    #
    # cur.execute(
    #     """
    #     INSERT INTO spark_streams.orders(id, username, email, food, size, cost, time, order_completed)
    #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #     """, (transaction["id"], transaction["username"], transaction["email"],
    #           transaction["food"], transaction["size"], transaction["cost"], transaction["time"],
    #           transaction["order_completed"])
    # )
    #
    # cur.close()
    # conn.commit()