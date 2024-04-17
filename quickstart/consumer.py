# Simple Kafka consumer that reads messages from the 'test' topic

from kafka import KafkaConsumer

# Create a KafkaConsumer instance
consumer = KafkaConsumer("test", bootstrap_servers="localhost:9092")

# Consume messages from the 'test' topic
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")

# Close the consumer
consumer.close()
