import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 1. Load the cleaned dataset
print("Loading dataset for frequency analysis...")
df = pd.read_csv('dataset.csv')

# Assuming your vibration column is named 'vibration_1'
# Adjust the sampling rate (fs) based on your NASA bearing dataset specifications (e.g., 20000 Hz)
signal = df['vibration_1'].values
fs = 20000  # Sampling frequency in Hz

# 2. Perform Fast Fourier Transform (FFT)
n = len(signal)
fft_values = np.fft.rfft(signal)
fft_freqs = np.fft.rfftfreq(n, d=1/fs)

# Calculate power spectral density or magnitude spectrum
magnitude = np.abs(fft_values)

# 3. Display top frequency bands
print("FFT Analysis Complete!")
print(f"Total samples processed: {n}")
print(f"Dominant frequency peak: {fft_freqs[np.argmax(magnitude)]:.2f} Hz")

# Optional: Plot the frequency spectrum to map operational frequency bands
plt.figure(figsize=(10, 4))
plt.plot(fft_freqs, magnitude, color='b')
plt.title('Frequency Domain Analysis - FFT Spectrum')
plt.xlabel('Frequency (Hz)')
plt.ylabel('Amplitude')
plt.grid(True)
plt.show()