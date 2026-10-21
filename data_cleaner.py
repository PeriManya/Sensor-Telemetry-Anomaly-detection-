import json
from kafka import KafkaConsumer
from scipy.signal import butter, filtfilt

consumer = KafkaConsumer(
    'sensor-topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Task 2 Active: Processing stream from Kafka...")

for message in consumer:
    data = message.value
    # Extract primary vibration channel for filtering or processing
    raw_val = data.get('vibration_1', 0.0)
    
    # Process or filter your data stream here
    print(f"Cleaned/Processed vibration_1: {raw_val}")