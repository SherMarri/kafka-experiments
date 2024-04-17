"""Kafka producer that publishes messages to the 'test' topic in a partitioned manner."""

from kafka import KafkaProducer

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Produce messages with keys to a partitioned topic
for i in range(10):
    key = f"key{i % 3}"  # Ensure messages with the same key are assigned to the same partition
    message = f"Message {i}"
    producer.send(
        "partitioned_topic", key=key.encode("utf-8"), value=message.encode("utf-8")
    )

# Flush the producer to ensure all messages are sent
producer.flush()

# Close the producer
producer.close()
