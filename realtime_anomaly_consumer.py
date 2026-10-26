import json
import numpy as np
import pandas as pd
import joblib
from kafka import KafkaConsumer

def run_consumer():
    # Load pre-trained model and expected feature headers
    try:
        model = joblib.load('isolation_forest_model.pkl')
        model_features = joblib.load('model_features.pkl')
        print("Loaded Isolation Forest model successfully.")
    except Exception as e:
        print(f"Error loading model: {e}. Please run train_anomaly_model.py first.")
        return

    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        'sensor-topic',  # Match your Kafka topic name from sensor_streamer.py
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Kafka consumer active. Listening for live sensor telemetry stream...")
    
    buffer = []
    window_size = 50  # Number of telemetry records to evaluate in a batch window

    for message in consumer:
        data = message.value
        buffer.append(data)

        # Once buffer reaches window size, perform inference
        if len(buffer) >= window_size:
            df_window = pd.DataFrame(buffer)
            
            # Align columns with training features
            for col in model_features:
                if col not in df_window.columns:
                    df_window[col] = 0.0
            
            X_inference = df_window[model_features].fillna(0)

            # Predict anomalies (-1 for anomaly, 1 for normal)
            predictions = model.predict(X_inference)
            anomaly_count = np.sum(predictions == -1)

            # Calculate a basic Remaining Useful Life (RUL) estimation proxy
            health_score = float(np.sum(predictions == 1) / len(predictions)) * 100
            
            print(f"\n--- Processing Window of {window_size} Records ---")
            print(f"Health Score: {health_score:.2f}% | Anomalies Detected: {anomaly_count}")

            # Automated Alerting Logic for critical threshold breaches[span_5](start_span)[span_5](end_span)
            if health_score < 80.0 or anomaly_count > 5:
                print("🚨 [CRITICAL ALERT] Degradation threshold breached! Potential mechanical failure detected.")
                # You can add code here to log alerts or trigger webhooks/notifications

            # Clear buffer for the next window
            buffer = []

if __name__ == "__main__":
    run_consumer()