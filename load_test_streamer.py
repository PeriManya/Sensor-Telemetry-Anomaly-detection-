import json
import time
import random
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "sensor-topic"

def run_load_test(num_messages=500, delay=0.01):
    print(f"🚀 Starting High-Volume Load Test: Sending {num_messages} high-frequency packets...")
    
    for i in range(1, num_messages + 1):
        # Simulate high-frequency or stress-spike vibration telemetry
        telemetry_payload = {
            "window": i,
            "mean": random.uniform(-0.1, 0.1),
            "rms": random.uniform(0.1, 0.3),
            "std": random.uniform(0.05, 0.15),
            "peak_amplitude": random.uniform(0.5, 2.5),
            "kurtosis": random.uniform(2.0, 8.0),
            "crest_factor": random.uniform(3.0, 6.0),
            "dominant_frequency": random.uniform(100.0, 500.0),
            "anomaly_flag": 1 if i % 50 == 0 else 0  # Inject artificial spike anomalies
        }
        
        producer.send(TOPIC_NAME, value=telemetry_payload)
        print(f"[LOAD TEST] Sent packet {i}/{num_messages}")
        
        # High-speed delay (default 10ms per message to simulate high-load)
        time.sleep(delay)
        
    producer.flush()
    print("✅ Load test successfully completed! Check your consumer and dashboard stability.")

if __name__ == "__main__":
    run_load_test()