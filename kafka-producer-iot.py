# Dummy data generator for iot sensor
import random
import time
import json
import uuid
from confluent_kafka import Producer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "iot_sensor_data"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

SENSOR_TYPES = ["temperature", "humidity", "pressure", "light", "co2"]

def generate_sensor_data():
    sensor_data = {
        "sensor_id": str(uuid.uuid4()),
        "sensor_type": random.choice(SENSOR_TYPES),
        "timestamp": int(time.time() * 1000),
        "reading": round(random.uniform(-20.0, 50.0), 2) 
    }

    if sensor_data["sensor_type"] == "humidity":
        sensor_data["reading"] = round(random.uniform(0.0, 100.0), 2)
    elif sensor_data["sensor_type"] == "pressure":
        sensor_data["reading"] = round(random.uniform(950.0, 1050.0), 2)
    elif sensor_data["sensor_type"] == "light":
        sensor_data["reading"] = round(random.uniform(0.0, 1000.0), 2)
    elif sensor_data["sensor_type"] == "co2":
        sensor_data["reading"] = round(random.uniform(300.0, 2000.0), 2)

    return sensor_data

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_data():
    while True:
        data = generate_sensor_data()
        producer.produce(KAFKA_TOPIC, json.dumps(data).encode("utf-8"), callback=delivery_report)
        # just to add some delay, can remove for more throughput
        time.sleep(random.uniform(0.5, 2.0))

    producer.flush()

if __name__ == "__main__":
    produce_data()

