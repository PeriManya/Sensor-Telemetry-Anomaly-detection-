# producer.py - Task 1: Streaming Ingestion Layer
import time
import json
import random
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

print("Task 1 Active: Streaming live sensor telemetry to Kafka...")

try:
    while True:
        has_dropout = random.random() < 0.05
        
        payload = {
            "timestamp": int(time.time() * 1000),
            "device_id": "motor_01",
            "vibration_x": None if has_dropout else round(random.uniform(-1.5, 1.5), 4),
            "acoustic_frequency": round(random.uniform(800, 3200), 2)
        }
        
        producer.produce(
            topic='sensor-telemetry-v1',
            key=payload["device_id"],
            value=json.dumps(payload)
        )
        producer.poll(0)
        time.sleep(0.1)
except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    producer.flush() 