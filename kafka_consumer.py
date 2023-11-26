from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import TimestampType

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaStreamProcessing").getOrCreate()

# Add the Cassandra connector library
spark.sparkContext.addPyFile("https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.1.0/spark-cassandra-connector_2.12-3.1.0.jar")

# Define the schema to match the JSON structure
schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("temp_celsius", DoubleType(), True),
    StructField("feels_like_celsius", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("description", StringType(), True),
    StructField("sunrise_time", TimestampType(), True),
    StructField("sunset_time", TimestampType(), True)
])


# Function to write data to Cassandra
def write_to_cassandra(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="weather_data", keyspace="streams") \
        .mode("append") \
        .option("spark.cassandra.connection.host", "cassandra-container") \
        .option("spark.cassandra.connection.port", "9042") \
        .save()

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-container:9092") \
    .option("subscribe", "weather") \
    .load()

# Parse the JSON data
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Write data to Cassandra using the foreach sink
query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()


# docker cp kafka_consumer.py (41b5cd501b17345d1c2e8c8cf598beaeb919ef4e91310498dcb17838935c8eb4):/opt/spark/consumer.py