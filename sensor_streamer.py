import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

csv_file_path = 'dataset.csv'

print("Starting sensor stream from NASA Bearing dataset...")

while True:
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Package multi-channel telemetry payload
            payload = {
                "timestamp": int(time.time() * 1000),
                "device_id": "nasa_bearing_01",
                "vibration_1": float(row['vibration_1']),
                "vibration_2": float(row['vibration_2']),
                "vibration_3": float(row['vibration_3']),
                "vibration_4": float(row['vibration_4']),
                "vibration_5": float(row['vibration_5']),
                "vibration_6": float(row['vibration_6']),
                "vibration_7": float(row['vibration_7']),
                "vibration_8": float(row['vibration_8'])
            }
            
            producer.send('sensor-topic', value=payload)
            print(f"Sent payload: {payload}")
            time.sleep(1) # Stream interval