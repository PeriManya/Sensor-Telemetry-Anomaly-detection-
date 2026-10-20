
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

print("Loading dataset for frequency analysis...")
df = pd.read_csv("dataset.csv")

signal = pd.to_numeric(
    df["vibration_1"], errors="coerce"
).dropna().to_numpy(dtype=float)

fs = 20000  # Assumed sampling frequency in Hz

if len(signal) < 2:
    raise ValueError("At least two valid sensor readings are required.")

# Remove the average (DC component)
signal_centered = signal - np.mean(signal)

# Apply a Hann window to reduce spectral leakage
window = np.hanning(len(signal_centered))
windowed_signal = signal_centered * window

# Calculate the FFT
n = len(windowed_signal)
fft_values = np.fft.rfft(windowed_signal)
fft_freqs = np.fft.rfftfreq(n, d=1 / fs)
magnitude = np.abs(fft_values)

# Exclude the zero-frequency bin
nonzero_magnitude = magnitude[1:]
nonzero_freqs = fft_freqs[1:]

peak_index = np.argmax(nonzero_magnitude)
dominant_frequency = nonzero_freqs[peak_index]

print("FFT Analysis Complete!")
print(f"Total samples processed: {n}")
print(f"Mean before centering: {np.mean(signal):.6f}")
print(f"Standard deviation: {np.std(signal):.6f}")
print(f"Dominant non-zero frequency: {dominant_frequency:.2f} Hz")

plt.figure(figsize=(10, 4))
plt.plot(nonzero_freqs, nonzero_magnitude, color="blue")
plt.title("FFT Spectrum (Mean Removed, Hann Window Applied)")
plt.xlabel("Frequency (Hz)")
plt.ylabel("Magnitude")
plt.grid(True)
plt.tight_layout()
plt.show()
