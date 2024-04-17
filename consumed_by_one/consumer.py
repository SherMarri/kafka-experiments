"""Kafka consumer by group, so that the partitioned topics can be consumed by only one consumer in the group"""

from kafka import KafkaConsumer

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    "partitioned_topic",  # Subscribe to the topic
    group_id="my_consumer_group",  # Specify the consumer group
    bootstrap_servers="localhost:9092",
)

# Consume messages from the topic
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")

# Close the consumer
consumer.close()
