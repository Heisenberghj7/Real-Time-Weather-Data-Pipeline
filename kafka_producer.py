import json
from pykafka import KafkaClient
from weather_api import get_data

def initialize_kafka_producer():
    # Initialize Kafka producer
    KAFKA_BROKER_ADDRESS = "kafka-container:9092"  # Update with your Kafka broker address
    KAFKA_TOPIC = "weather"
    client = KafkaClient(hosts=KAFKA_BROKER_ADDRESS)
    topic = client.topics[KAFKA_TOPIC]
    producer = topic.get_sync_producer()

    try:
        # Get data from the API
        weather_data = get_data()
        
        # Produce the weather_data to Kafka
        producer.produce(json.dumps(weather_data).encode('utf-8'))
        print(f"Message sent to Kafka: {weather_data}")
    except Exception as e:
        print(f"Error sending message to Kafka: {str(e)}")
    finally:
        producer.stop()
