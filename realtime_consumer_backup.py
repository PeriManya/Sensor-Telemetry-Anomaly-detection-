
import json
import os
import time
from collections import deque

import joblib
import numpy as np
import pandas as pd
from kafka import KafkaConsumer

from feature_extraction import extract_features

# Kafka settings
KAFKA_TOPIC = "sensor-topic"
KAFKA_SERVER = "localhost:9092"

# Window settings
WINDOW_SIZE = 1024
SAMPLING_RATE = 20000
SENSOR_COLUMN = "vibration_1"
MAX_MESSAGES = 20480
IDLE_POLLS_LIMIT = 5

# Output file
OUTPUT_FILE = "extracted_features.csv"

# Rolling baseline settings
BASELINE_WINDOW = 20
MIN_BASELINE_WINDOWS = 5
baseline_values = deque(maxlen=BASELINE_WINDOW)


def main():
    # Load the trained model and its expected input columns.
    try:
        model = joblib.load("isolation_forest_model.pkl")
        model_features = joblib.load("model_features.pkl")
        model_features = list(model_features)
        print("Isolation Forest model loaded successfully.")
    except Exception as exc:
        print(f"Could not load model artifacts: {exc}")
        print(
            "Make sure isolation_forest_model.pkl and "
            "model_features.pkl are in the project folder."
        )
        return

    consumer = None
    readings = deque(maxlen=WINDOW_SIZE)

    total_messages = 0
    completed_windows = 0
    idle_polls = 0

    # Start a fresh output file for this run.
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id="feature-extraction-consumer-bounded-v1",
        )

        print(f"Connected to Kafka topic: {KAFKA_TOPIC}")
        print(f"Window size: {WINDOW_SIZE}")
        print(f"Maximum valid readings: {MAX_MESSAGES}")
        print("Waiting for sensor readings...\n")

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

                    if len(readings) < WINDOW_SIZE:
                        continue

                    # 1. Extract seven vibration features.
                    features = extract_features(
                        list(readings),
                        sampling_rate=SAMPLING_RATE,
                    )

                    # 2. Prepare features in the model's expected order.
                    feature_row = pd.DataFrame([features])
                    for column in model_features:
                        if column not in feature_row.columns:
                            feature_row[column] = 0.0

                    X_inference = (
                        feature_row[model_features]
                        .replace([np.inf, -np.inf], np.nan)
                        .fillna(0.0)
                    )

                    # 3. Predict anomaly.
                    prediction = model.predict(X_inference)[0]
                    is_anomaly = prediction == -1

                    # 4. Calculate the rolling 3-sigma baseline on RMS.
                    rms = float(features["rms"])
                    baseline_alert = False

                    if len(baseline_values) >= MIN_BASELINE_WINDOWS:
                        baseline_mean = float(np.mean(baseline_values))
                        baseline_std = float(np.std(baseline_values))

                        if baseline_std > 0:
                            baseline_alert = (
                                abs(rms - baseline_mean)
                                > 3 * baseline_std
                            )

                    baseline_values.append(rms)

                    # 5. Assign a status.
                    if is_anomaly:
                        alert_status = "CRITICAL"
                    elif baseline_alert:
                        alert_status = "WARNING"
                    else:
                        alert_status = "NORMAL"

                    # Retain the project's current illustrative health/RUL
                    # calculation. This is not a trained RUL prediction.
                    health_score = 0.0 if is_anomaly else 100.0
                    estimated_rul_hours = max(
                        0,
                        round((health_score - 50) * 2.5, 1),
                    )

                    # 6. Save actual output for the dashboard.
                    row_data = {
                        **features,
                        "window": completed_windows + 1,
                        "health_score": health_score,
                        "rul": estimated_rul_hours,
                        "anomaly": int(is_anomaly),
                        "baseline_alert": int(baseline_alert),
                        "alert_status": alert_status,
                    }

                    pd.DataFrame([row_data]).to_csv(
                        OUTPUT_FILE,
                        mode="a",
                        header=not os.path.exists(OUTPUT_FILE),
                        index=False,
                    )

                    completed_windows += 1

                    print(
                        f"\nWindow {completed_windows} | "
                        f"Status: {alert_status} | "
                        f"Health: {health_score:.1f}% | "
                        f"Estimated RUL: {estimated_rul_hours} hrs"
                    )

                    for feature, value in features.items():
                        print(f"{feature}: {value:.6f}")

                    if alert_status != "NORMAL":
                        print(
                            f"ALERT: {alert_status} | "
                            f"Isolation Forest anomaly: {is_anomaly} | "
                            f"3-sigma breach: {baseline_alert}"
                        )

                    # Start collecting the next window.
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
            print("\nNo new Kafka messages received for 5 seconds.")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    except Exception as exc:
        print(f"\nConsumer error: {exc}")

    finally:
        if consumer is not None:
            consumer.close()

        print(f"\nTotal valid readings: {total_messages}")
        print(f"Completed windows: {completed_windows}")

        if readings:
            print(
                "Readings remaining in incomplete window: "
                f"{len(readings)}"
            )


if __name__ == "__main__":
    main()
