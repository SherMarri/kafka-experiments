"""A kafka publisher that publishes messages to the 'order_status_updates' topic."""

from kafka import KafkaProducer
import json


# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# Publish order status update message to Kafka topic
def publish_order_status_update(order_id, status):
    message = {"order_id": order_id, "status": status}
    producer.send("order_status_updates", value=message)
    producer.flush()


# Example: Publish order status update message
order_id = "123"
status = "confirmed"
publish_order_status_update(order_id, status)
