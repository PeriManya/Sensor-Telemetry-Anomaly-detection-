import json
import os
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
SENSOR_COLUMNS = [f"vibration_{i}" for i in range(1, 9)]
MAX_MESSAGES = 20480
IDLE_POLLS_LIMIT = 5

# New Isolation Forest model
MODEL_PATH = "isolation_forest_model.pkl"
FEATURES_PATH = "model_features.pkl"

# Initial demo threshold: flag a window when 10% or more of
# its samples are identified as potential anomalies.
ANOMALY_FRACTION_THRESHOLD = 0.10

# Output file
OUTPUT_FILE = "extracted_features.csv"

# Rolling RMS baseline settings
BASELINE_WINDOW = 20
MIN_BASELINE_WINDOWS = 5


def main():
    # Load and validate the new model and feature list.
    try:
        model = joblib.load(MODEL_PATH)
        model_features = list(joblib.load(FEATURES_PATH))

        if model_features != SENSOR_COLUMNS:
            raise ValueError(
                "Model features do not match the eight sensor columns. "
                f"Expected: {SENSOR_COLUMNS}; got: {model_features}"
            )

        if getattr(model, "n_features_in_", None) != len(SENSOR_COLUMNS):
            raise ValueError(
                "The loaded model does not expect eight sensor features."
            )

        print("New Isolation Forest model loaded successfully.")
        print(f"Model: {MODEL_PATH}")
        print(f"Expected input columns: {model_features}")

    except Exception as exc:
        print(f"Could not load or validate model artifacts: {exc}")
        return

    consumer = None
    readings = deque(maxlen=WINDOW_SIZE)
    baseline_values = deque(maxlen=BASELINE_WINDOW)

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
            group_id="feature-extraction-consumer-v2",
        )

        print(f"\nConnected to Kafka topic: {KAFKA_TOPIC}")
        print(f"Window size: {WINDOW_SIZE} samples")
        print(f"Maximum valid readings: {MAX_MESSAGES}")
        print(
            "Window anomaly threshold: "
            f"{ANOMALY_FRACTION_THRESHOLD:.0%}"
        )
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

                    # Require all eight sensor channels.
                    if not all(
                        column in payload for column in SENSOR_COLUMNS
                    ):
                        continue

                    try:
                        sensor_row = {
                            column: float(payload[column])
                            for column in SENSOR_COLUMNS
                        }
                    except (TypeError, ValueError):
                        continue

                    values = np.asarray(
                        list(sensor_row.values()), dtype=float
                    )

                    if not np.isfinite(values).all():
                        continue

                    readings.append(sensor_row)
                    total_messages += 1

                    if len(readings) < WINDOW_SIZE:
                        continue

                    # 1. Calculate seven derived features from vibration_1.
                    signal = [
                        row["vibration_1"] for row in readings
                    ]

                    features = extract_features(
                        signal,
                        sampling_rate=SAMPLING_RATE,
                    )

                    # 2. Predict on all eight raw sensor channels.
                    # These columns match the model's training inputs.
                    X_inference = pd.DataFrame(
                        list(readings),
                        columns=model_features,
                    )

                    predictions = model.predict(X_inference)

                    anomalous_samples = int(
                        np.sum(predictions == -1)
                    )

                    anomaly_fraction = (
                        anomalous_samples / len(predictions)
                    )

                    is_anomaly = (
                        anomaly_fraction
                        >= ANOMALY_FRACTION_THRESHOLD
                    )

                    # 3. Calculate the rolling three-sigma RMS baseline.
                    rms = float(features["rms"])
                    baseline_alert = False

                    if len(baseline_values) >= MIN_BASELINE_WINDOWS:
                        baseline_mean = float(
                            np.mean(baseline_values)
                        )
                        baseline_std = float(
                            np.std(baseline_values)
                        )

                        if baseline_std > 0:
                            baseline_alert = (
                                abs(rms - baseline_mean)
                                > 3 * baseline_std
                            )

                    baseline_values.append(rms)

                    # 4. Assign alert status.
                    if is_anomaly:
                        alert_status = "CRITICAL"
                    elif baseline_alert:
                        alert_status = "WARNING"
                    else:
                        alert_status = "NORMAL"

                    # Illustrative values only, not trained health/RUL models.
                    health_score = 0.0 if is_anomaly else 100.0

                    estimated_rul_hours = max(
                        0,
                        round((health_score - 50) * 2.5, 1),
                    )

                    # 5. Save derived features and anomaly details.
                    row_data = {
                        **features,
                        "window": completed_windows + 1,
                        "health_score": health_score,
                        "rul": estimated_rul_hours,
                        "anomaly": int(is_anomaly),
                        "anomalous_samples": anomalous_samples,
                        "anomaly_fraction": round(
                            anomaly_fraction, 6
                        ),
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
                        f"Anomalous samples: {anomalous_samples}/"
                        f"{WINDOW_SIZE} "
                        f"({anomaly_fraction:.1%}) | "
                        f"Health: {health_score:.1f}% | "
                        f"Illustrative RUL: {estimated_rul_hours} hrs"
                    )

                    for feature, value in features.items():
                        print(f"{feature}: {value:.6f}")

                    if alert_status != "NORMAL":
                        print(
                            f"ALERT: {alert_status} | "
                            f"Isolation Forest window anomaly: "
                            f"{is_anomaly} | "
                            f"3-sigma RMS breach: {baseline_alert}"
                        )

                    # Collect the next non-overlapping window.
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

        print(f"\nTotal valid sensor rows: {total_messages}")
        print(f"Completed windows: {completed_windows}")

        if readings:
            print(
                "Rows remaining in incomplete window: "
                f"{len(readings)}"
            )

        print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()