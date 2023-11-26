import json
from pykafka import KafkaClient
from weather_api import get_data
import time

# Initialize Kafka producer
KAFKA_BROKER_ADDRESS = "kafka-container:9092"  # Update with your Kafka broker address
KAFKA_TOPIC = "weather"
client = KafkaClient(hosts=KAFKA_BROKER_ADDRESS)
topic = client.topics[KAFKA_TOPIC]
producer = topic.get_sync_producer()


if __name__ == "__main__":
    while True:
        try:
            weather_data = get_data("Casablanca")
            producer.produce(json.dumps(weather_data).encode('utf-8'))
            print(f"Message sent to Kafka: {weather_data}")
        except Exception as e:
            print(f"Error sending message to Kafka: {str(e)}")

        # Wait for 10 minutes before making the next request
        time.sleep(20)
    
