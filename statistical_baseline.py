import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Configuration
DATASET_PATH = "dataset.csv"
SIGNAL_COLUMN = "vibration_1"
WINDOW_SIZE = 100
THRESHOLD = 3

print("Loading dataset for statistical baseline analysis...")

# Load and clean the selected vibration signal
df = pd.read_csv(DATASET_PATH)

if SIGNAL_COLUMN not in df.columns:
    raise ValueError(
        f"Column '{SIGNAL_COLUMN}' not found. "
        f"Available columns: {list(df.columns)}"
    )

signal = pd.to_numeric(
    df[SIGNAL_COLUMN], errors="coerce"
).replace([np.inf, -np.inf], np.nan).dropna()

if len(signal) < WINDOW_SIZE:
    raise ValueError(
        f"Need at least {WINDOW_SIZE} valid samples; "
        f"found {len(signal)}."
    )

# Descriptive statistics
print("\n--- Descriptive Statistics ---")
print(f"Valid samples: {len(signal)}")
print(f"Mean: {signal.mean():.6f}")
print(f"Median: {signal.median():.6f}")
print(f"Standard deviation: {signal.std():.6f}")
print(f"Minimum: {signal.min():.6f}")
print(f"Maximum: {signal.max():.6f}")
print(f"25th percentile: {signal.quantile(0.25):.6f}")
print(f"75th percentile: {signal.quantile(0.75):.6f}")

# Rolling baseline using previous samples only
rolling_mean = signal.shift(1).rolling(
    WINDOW_SIZE, min_periods=WINDOW_SIZE
).mean()

rolling_std = signal.shift(1).rolling(
    WINDOW_SIZE, min_periods=WINDOW_SIZE
).std()

# Avoid division by zero and flag deviations from baseline
deviation = (signal - rolling_mean).abs()
threshold = THRESHOLD * rolling_std
anomaly_mask = (
    rolling_mean.notna()
    & rolling_std.notna()
    & (rolling_std > 0)
    & (deviation > threshold)
)

anomaly_indices = signal.index[anomaly_mask]
anomaly_count = int(anomaly_mask.sum())
eligible_count = int(rolling_mean.notna().sum())

print("\n--- Statistical Baseline Results ---")
print(f"Rolling window: {WINDOW_SIZE} samples")
print(f"Threshold: {THRESHOLD} rolling standard deviations")
print(f"Samples evaluated: {eligible_count}")
print(f"Potential anomalies: {anomaly_count}")

if eligible_count:
    print(
        f"Potential anomaly rate: "
        f"{anomaly_count / eligible_count * 100:.3f}%"
    )

# Save a results file locally
results = pd.DataFrame({
    "sample_index": signal.index,
    "vibration": signal.to_numpy(),
    "rolling_mean": rolling_mean.to_numpy(),
    "rolling_std": rolling_std.to_numpy(),
    "potential_anomaly": anomaly_mask.to_numpy()
})

results.to_csv(
    "statistical_baseline_results.csv", index=False
)
print("Results saved to statistical_baseline_results.csv")

# Plot signal and flagged points
plt.figure(figsize=(12, 5))
plt.plot(signal.index, signal, label="Vibration signal", linewidth=0.8)
plt.scatter(
    anomaly_indices,
    signal.loc[anomaly_indices],
    label="Potential anomalies",
    s=16
)
plt.title("Vibration Signal and Statistical Baseline Flags")
plt.xlabel("Sample index")
plt.ylabel("Vibration amplitude")
plt.legend()
plt.tight_layout()
plt.show()

# Plot rolling variability
plt.figure(figsize=(12, 4))
plt.plot(
    rolling_std.index,
    rolling_std,
    label="Rolling standard deviation",
    linewidth=0.8
)
plt.title(f"Rolling Variability (Window = {WINDOW_SIZE})")
plt.xlabel("Sample index")
plt.ylabel("Rolling standard deviation")
plt.legend()
plt.tight_layout()
plt.show()

print("\nStatistical baseline analysis completed successfully!")