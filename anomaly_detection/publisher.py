"""Smart meter simulator publisher module."""

import time
import random
from kafka import KafkaProducer
import json


def simulate_energy_consumption():
    while True:
        # Simulate energy consumption data
        meter_id = random.randint(1, 100)
        energy_consumption = random.uniform(0, 100)
        timestamp = int(time.time() * 1000)

        # Create JSON message
        message = {
            "meter_id": meter_id,
            "energy_consumption": energy_consumption,
            "timestamp": timestamp,
        }

        # Send message to Kafka topic
        producer.send("energy_consumption", value=json.dumps(message).encode("utf-8"))
        time.sleep(1)


# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Start producing simulated energy consumption data
simulate_energy_consumption()
