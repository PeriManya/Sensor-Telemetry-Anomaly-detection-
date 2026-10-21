
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pywt

print("Loading dataset for wavelet analysis...")
df = pd.read_csv("dataset.csv")

signal = pd.to_numeric(
    df["vibration_1"], errors="coerce"
).dropna().to_numpy(dtype=float)

if len(signal) < 16:
    raise ValueError("Not enough valid readings for wavelet analysis.")

# Remove the mean before decomposition
signal = signal - np.mean(signal)

# Discrete Wavelet Transform
wavelet = "db4"
max_level = pywt.dwt_max_level(len(signal), pywt.Wavelet(wavelet).dec_len)
level = min(5, max_level)

if level < 1:
    raise ValueError("Signal is too short for wavelet decomposition.")

coefficients = pywt.wavedec(signal, wavelet, level=level)

print("Wavelet analysis completed successfully!")
print(f"Samples processed: {len(signal)}")
print(f"Wavelet: {wavelet}")
print(f"Decomposition level: {level}")

for i, coeff in enumerate(coefficients):
    label = "Approximation" if i == 0 else f"Detail D{level - i + 1}"
    energy = np.sum(coeff ** 2)
    print(f"{label} coefficient count: {len(coeff)}, energy: {energy:.4f}")

# Plot the original signal and wavelet coefficients
fig, axes = plt.subplots(
    len(coefficients) + 1, 1, figsize=(12, 10)
)

axes[0].plot(signal, linewidth=0.6)
axes[0].set_title("Mean-Centered Vibration Signal")
axes[0].set_ylabel("Amplitude")

for i, coeff in enumerate(coefficients):
    label = "Approximation" if i == 0 else f"Detail D{level - i + 1}"
    axes[i + 1].plot(coeff, linewidth=0.7)
    axes[i + 1].set_title(f"{label} Wavelet Coefficients")
    axes[i + 1].set_ylabel("Coefficient")

axes[-1].set_xlabel("Coefficient Index")
plt.tight_layout()
plt.show()
