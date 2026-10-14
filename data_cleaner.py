# consumer_cleaner.py - Task 2: Cleaning, Imputation & Filtering
import json
import numpy as np
from confluent_kafka import Consumer, KafkaError
from scipy.signal import butter, filtfilt

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'telemetry-cleaning-group',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe(['sensor-telemetry-v1'])

raw_vibration_buffer = []
last_valid_vibration = 0.0

def apply_lowpass_filter(data_window):
    """Butterworth filter to smooth high-frequency noise."""
    if len(data_window) < 12:  # Increased minimum length to prevent SciPy padlen error
        return data_window[-1]
    b, a = butter(N=2, Wn=0.3, btype='low')
    filtered_signal = filtfilt(b, a, data_window)
    return float(filtered_signal[-1])

print("Task 2 Active: Processing stream from Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer Error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        raw_val = data['vibration_x']

        if raw_val is None:
            clean_val = last_valid_vibration
            dropout_flag = True
        else:
            clean_val = raw_val
            last_valid_vibration = raw_val
            dropout_flag = False

        raw_vibration_buffer.append(clean_val)
        if len(raw_vibration_buffer) > 30:
            raw_vibration_buffer.pop(0)

        filtered_val = apply_lowpass_filter(raw_vibration_buffer)

        mean_val = np.mean(raw_vibration_buffer)
        std_val = np.std(raw_vibration_buffer) if np.std(raw_vibration_buffer) > 0 else 1.0
        normalized_val = (filtered_val - mean_val) / std_val

        print(f"Raw: {raw_val} (Dropout Fixed: {dropout_flag}) | "
              f"Filtered: {filtered_val:.4f} | Z-Score: {normalized_val:.4f}")

except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()