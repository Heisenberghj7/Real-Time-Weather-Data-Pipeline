from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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
    StructField("humidity", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("sunrise_time", TimestampType(), True),
    StructField("sunset_time", TimestampType(), True)
])


# Read data from Kafka as a streaming DataFrame
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-container:9092")