"""Consumer module for anomaly detection."""

from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "energy_consumption",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# Consume messages from Kafka topic
for message in consumer:
    energy_data = message.value
    # Perform anomaly detection on energy consumption data
    if energy_data["energy_consumption"] > 90:
        print(
            f"Anomaly detected: High energy consumption at meter {energy_data['meter_id']}"
        )
        # Trigger control action to adjust power distribution, notify maintenance, etc.
