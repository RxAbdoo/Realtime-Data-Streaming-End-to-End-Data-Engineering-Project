import logging
from datetime import datetime
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType


def create_spark_conn():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark Local Session Created Successfully!")
        return spark

    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        return None


def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Kafka Stream DataFrame created successfully")
        return df

    except Exception as e:
        logging.warning(f"Can't connect to Kafka: {e}")
        return None


def selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("email", StringType(), False),
        StructField("user_name", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    selected_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("id", expr("uuid()"))  # ➕ توليد UUID لكل صف

    return selected_df


def create_cassandra_conn():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Can't connect to Cassandra: {e}")
        return None


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            email TEXT,
            user_name TEXT,
            dob TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    print("Table created successfully!")


if __name__ == "__main__":
    spark_conn = create_spark_conn()

    if spark_conn:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            selection_df = selection_df_from_kafka(kafka_df)

            cassandra_session = create_cassandra_conn()
            if cassandra_session:
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                logging.info("Streaming has started...")

                # query = (selection_df.writeStream
                #          .format("org.apache.spark.sql.cassandra")
                #          .option('checkpointLocation', "file:///C:/tmp/spark_checkpoints")
                #          .option("keyspace", "spark_streams")
                #          .option("table", "users")
                #          .start())
                

                # query.awaitTermination()
