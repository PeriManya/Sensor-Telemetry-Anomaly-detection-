
import pandas as pd
import stumpy
import matplotlib.pyplot as plt

print("Loading dataset for Matrix Profile analysis...")
df = pd.read_csv("dataset.csv")

# Extract vibration signal
signal = pd.to_numeric(
    df["vibration_1"], errors="coerce"
).dropna().to_numpy()

# Set subsequence window length
m = 150

if len(signal) <= m:
    raise ValueError(
        f"Need more than {m} valid readings; found {len(signal)}."
    )

print("Computing Matrix Profile using STUMPY...")
matrix_profile = stumpy.stump(signal, m=m)

# Extract matrix profile distances
mp_values = matrix_profile[:, 0].astype(float)

print("Matrix Profile computed successfully!")
print(
    f"Minimum distance (closest motif match): "
    f"{mp_values.min():.4f}"
)
print(
    f"Maximum distance (potential anomaly/discord): "
    f"{mp_values.max():.4f}"
)

# Plot the original signal and Matrix Profile
fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

ax[0].plot(signal, color="blue")
ax[0].set_title("Original Vibration Sensor Stream")
ax[0].set_ylabel("Amplitude")

ax[1].plot(mp_values, color="orange")
ax[1].set_title(f"Matrix Profile (Window Size m={m})")
ax[1].set_ylabel("Distance")
ax[1].set_xlabel("Sample Index")

plt.tight_layout()
plt.show()
