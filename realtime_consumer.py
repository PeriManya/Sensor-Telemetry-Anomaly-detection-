import joblib
import pandas as pd

# Load model artifacts
try:
    model = joblib.load('isolation_forest_model.pkl')
    model_features = joblib.load('model_features.pkl')
    print("Loaded Isolation Forest model successfully.")
except Exception as e:
    print(f"Error loading model: {e}")

import json
from collections import deque

import numpy as np
from kafka import KafkaConsumer

from feature_extraction import extract_features

KAFKA_TOPIC = "sensor-topic"
KAFKA_SERVER = "localhost:9092"

WINDOW_SIZE = 1024
SAMPLING_RATE = 20000
SENSOR_COLUMN = "vibration_1"

# Process only one dataset's worth of readings.
MAX_MESSAGES = 20480

# Stop if Kafka has no messages for 5 consecutive seconds.
IDLE_POLLS_LIMIT = 5


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="feature-extraction-consumer-bounded-v1",
    )  # line 44 - end of KafkaConsumer

    # --- ADD STEP 1 HERE ---
    import joblib
    import pandas as pd
    model = joblib.load('isolation_forest_model.pkl')
    model_features = joblib.load('model_features.pkl')
    # -----------------------

    readings = deque(maxlen=WINDOW_SIZE) # line 46 onwards
    total_messages = 0

    completed_windows = 0
    idle_polls = 0

    print(f"Connected to Kafka topic: {KAFKA_TOPIC}")
    print(f"Window size: {WINDOW_SIZE}")
    print(f"Maximum valid readings: {MAX_MESSAGES}")
    print("Processing messages...\n")

    try:
        while (
            total_messages < MAX_MESSAGES
            and idle_polls < IDLE_POLLS_LIMIT
        ):
            records = consumer.poll(
                timeout_ms=1000,
                max_records=500,
            )

            received_records = False

            for messages in records.values():
                for message in messages:
                    received_records = True

                    if total_messages >= MAX_MESSAGES:
                        break

                    payload = message.value

                    if SENSOR_COLUMN not in payload:
                        continue

                    try:
                        reading = float(payload[SENSOR_COLUMN])
                    except (TypeError, ValueError):
                        continue

                    if not np.isfinite(reading):
                        continue

                    readings.append(reading)
                    total_messages += 1

                    if len(readings) == WINDOW_SIZE:
                        features = extract_features(
                            list(readings),
                            sampling_rate=SAMPLING_RATE,
                        )
                        # --- INSERT STEP 2 HERE ---
                        df_features = pd.DataFrame([features])
                        for col in model_features:
                           if col not in df_features.columns:
                               df_features[col] = 0.0
                        X_inference = df_features[model_features].fillna(0)

                        prediction = model.predict(X_inference)[0]
                        is_anomaly = (prediction == -1)

                        health_score = 0.0 if is_anomaly else 100.0
                        estimated_rul_hours = max(0, round((health_score - 50) * 2.5, 1))
                        # --------------------------
                        
                        health_score = 0.0 if is_anomaly else 100.0
                        estimated_rul_hours = max(0, round((health_score - 50) * 2.5, 1))

                         # --- SAVE FEATURES TO CSV FOR DASHBOARD ---
                        row_data = {**features, 'health_score': health_score, 'rul': estimated_rul_hours, 'anomaly': int(is_anomaly)}
                        df_row = pd.DataFrame([row_data])
                        df_row.to_csv("extracted_features.csv", mode='a', header=not pd.io.common.file_exists("extracted_features.csv"), index=False)
            # ------------------------------------------

            # --- ADD ALERTING LOGIC HERE ---
                        if health_score < 80.0 or is_anomaly:
                             print(
                                   f"🚨 [CRITICAL ALERT] Degradation threshold breached! "
                                   f"Health: {health_score:.2f}% | Est. RUL: {estimated_rul_hours} hrs"
                             )
            # -------------------------------

            
                        completed_windows += 1
                        print(
                            f"\nWindow {completed_windows} | "
                            f"Health: {health_score}% | Est. RUL: {estimated_rul_hours} hrs"
                        )

            

                        completed_windows += 1

                        print(
                            f"\nWindow {completed_windows} | "
                            f"Valid readings: {total_messages}"
                        )

                        for feature, value in features.items():
                            print(f"{feature}: {value:.6f}")

                        readings.clear()

                if total_messages >= MAX_MESSAGES:
                    break

            if received_records:
                idle_polls = 0
            else:
                idle_polls += 1

        if total_messages >= MAX_MESSAGES:
            print("\nReached the maximum reading limit.")
        else:
            print("\nNo new messages received for 5 seconds.")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        print(f"\nTotal valid readings processed: {total_messages}")
        print(f"Completed windows: {completed_windows}")

        if readings:
            print(
                f"Remaining readings in incomplete window: "
                f"{len(readings)}"
            )


if __name__ == "__main__":
    main()