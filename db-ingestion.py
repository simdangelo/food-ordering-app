import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from kafka_topic import *


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.orders (
        id UUID PRIMARY KEY,
        user TEXT,
        email TEXT,
        food TEXT,
        size TEXT,
        cost INT,
        time TIMESTAMP,
        order_completed INT
        );
    """)

    print("Table created successfully!")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder
            .appName('SparkDataStreaming')
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config('spark.cassandra.connection.host', 'localhost:9042')
            .getOrCreate()
        )

        # s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'localhost:29092')
            .option('subscribe', ORDER_CONFIRMED_KAFKA_TOPIC)
            .option('startingOffsets', 'earliest')
            # .option("failOnDataLoss", "false") # in case of specific error, you need to set this option
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("user", StringType(), False),
        StructField("email", StringType(), False),
        StructField("food", StringType(), False),
        StructField("size", StringType(), False),
        StructField("cost", IntegerType(), False),
        StructField("time", TimestampType(), False),
        StructField("order_completed", IntegerType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def mysql_connection():
    import mysql.connector as mysql

    cnx = mysql.connect(
        user='user',
        password='password',
        database='mysql',
        host='0.0.0.0',
        port=8081
    )
    cursor = cnx.cursor()
    return cursor

def create_mysql_table(cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS test(id INTEGER(64) PRIMARY KEY, name VARCHAR(255))")
    print("mysql table created successfully!")



if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        # session_mysql = mysql_connection()
        # create_mysql_table(session_mysql)


        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # in case you want to write to cassandra database, as expected from the original project settings
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', 'checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'orders')
                               .start())

            # streaming_query = selection_df \
            #     .writeStream \
            #     .outputMode("append") \
            #     .format("console") \
            #     .start()

            # mysql_host = "localhost"
            # mysql_port = 3306
            # mysql_driver = "com.mysql.cj.jdbc.Driver"
            # mysql_database = "sales_db"
            # mysql_table = "total_sales_by_source_state"
            # mysql_username = getenv('MYSQL_USERNAME')
            # mysql_password = getenv('MYSQL_PASSWORD')
            # mysql_jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
            #
            # db_credentials = {
            #     "user": mysql_username,
            #     "password": mysql_password,
            #     "driver": mysql_driver
            # }
            #
            # dashboard_query = selection_df \
            #     .groupBy("food").count() \
            #     .writeStream \
            #     .outputMode("update") \
            #     .write \
            #     .jdbc(url=mysql_jdbc_url,
            #           table=mysql_table,
            #           mode="append",
            #           properties=db_credentials) \
            #     .start()


            streaming_query.awaitTermination()