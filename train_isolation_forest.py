import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

DATASET_PATH = "dataset.csv"

FEATURE_COLUMNS = [
    f"vibration_{i}" for i in range(1, 9)
]

MODEL_PATH = "isolation_forest_model_v2.pkl"
FEATURES_PATH = "model_features_v2.pkl"


def main():
    # Load the sensor dataset
    df = pd.read_csv(DATASET_PATH)

    # Verify that all eight sensor columns exist
    missing = [
        col for col in FEATURE_COLUMNS
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing sensor columns: {missing}"
        )

    # Keep the eight raw vibration channels
    X = df[FEATURE_COLUMNS].apply(
        pd.to_numeric, errors="coerce"
    )

    # Remove invalid or missing sensor rows
    X = X.replace(
        [float("inf"), float("-inf")], float("nan")
    ).dropna()

    if len(X) < 100:
        raise ValueError(
            "Not enough valid sensor rows for training."
        )

    print(f"Valid training rows: {len(X)}")
    print(f"Sensor features: {FEATURE_COLUMNS}")

    # Train the Isolation Forest model
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(X)

    # Save the new model and its expected feature names
    joblib.dump(model, MODEL_PATH)
    joblib.dump(FEATURE_COLUMNS, FEATURES_PATH)

    # Report how many training rows were flagged
    predictions = model.predict(X)
    anomaly_count = int((predictions == -1).sum())

    print("\nTraining completed successfully.")
    print(f"Model saved: {MODEL_PATH}")
    print(f"Feature list saved: {FEATURES_PATH}")
    print(f"Rows evaluated: {len(X)}")
    print(f"Rows flagged as potential anomalies: {anomaly_count}")
    print(
        "Potential anomaly percentage: "
        f"{anomaly_count / len(X) * 100:.2f}%"
    )


if __name__ == "__main__":
    main()