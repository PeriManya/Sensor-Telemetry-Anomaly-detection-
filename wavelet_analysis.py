import pandas as pd
import pywt
import matplotlib.pyplot as plt

print("Loading dataset for Wavelet Transform analysis...")
df = pd.read_csv('dataset.csv')

# Extract a slice of the vibration signal for clear time-frequency visualization
signal = df['vibration_1'].values[:1000]

# Perform Continuous Wavelet Transform (CWT) using 'morl' (Morlet wavelet)
scales = range(1, 64)
coefficients, frequencies = pywt.cwt(signal, scales, 'morl')

print("Wavelet Transform computed successfully!")

# Plot the CWT results
plt.figure(figsize=(10, 5))
plt.imshow(abs(coefficients), extent=[0, len(signal), 1, 63], cmap='PRGn', aspect='auto')
plt.title('Wavelet Transform (CWT) Time-Frequency Spectrum')
plt.xlabel('Sample Index')
plt.ylabel('Scale')
plt.colorbar(label='Amplitude Magnitude')
plt.show()