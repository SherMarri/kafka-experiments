"""Simple Kafka producer that publishes messages to the 'test' topic."""

from kafka import KafkaProducer

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Publish messages to the 'test' topic
for i in range(10):
    message = f"Message {i}"
    producer.send("test", message.encode("utf-8"))

# Flush the producer to ensure all messages are sent
producer.flush()

# Close the producer
producer.close()
