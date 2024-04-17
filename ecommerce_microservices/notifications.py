"""A kafka consumer that reads messages from the 'order_status_updates' topic."""

from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "order_status_updates",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)


# Send notification to user
def send_user_notification(order_id, status):
    # Example: Send notification to user based on order status
    print(f"User notification: Order {order_id} is {status}")


# Send notification to seller
def send_seller_notification(order_id, status):
    # Example: Send notification to seller based on order status
    print(f"Seller notification: Order {order_id} is {status}")


# Consume messages from Kafka topic
for message in consumer:
    order_id = message.value["order_id"]
    status = message.value["status"]
    # Send notifications to both user and seller
    send_user_notification(order_id, status)
    send_seller_notification(order_id, status)
