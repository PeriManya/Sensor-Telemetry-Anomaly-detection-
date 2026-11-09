import numpy as np
import pandas as pd
from scipy.stats import kurtosis
from scipy.fft import rfft, rfftfreq

def extract_features(signal, sampling_rate=20000):
    """
    Extract statistical and frequency-domain features
    from a window of vibration sensor readings.
    """

    signal = np.asarray(signal, dtype=float)
    signal = signal[np.isfinite(signal)]

    if len(signal) == 0:
        raise ValueError("Signal window contains no valid values.")

    # Statistical features
    mean_value = np.mean(signal)
    rms_value = np.sqrt(np.mean(signal ** 2))
    std_value = np.std(signal)
    peak_value = np.max(np.abs(signal))

    # Fisher kurtosis: normal distribution is approximately 0
    kurtosis_value = (
        float(kurtosis(signal, fisher=True, bias=False))
        if len(signal) > 3 and std_value > 0
        else 0.0
    )

    # Crest factor
    crest_factor = (
        peak_value / rms_value
        if rms_value > 0
        else 0.0
    )

    # Dominant frequency
    centered_signal = signal - mean_value

    if len(signal) > 1 and np.any(centered_signal):
        amplitudes = np.abs(rfft(centered_signal))
        frequencies = rfftfreq(
            len(centered_signal),
            d=1 / sampling_rate
        )

        # Ignore the zero-frequency component
        if len(amplitudes) > 1:
            peak_index = np.argmax(amplitudes[1:]) + 1
            dominant_frequency = float(frequencies[peak_index])
        else:
            dominant_frequency = 0.0
    else:
        dominant_frequency = 0.0

    return {
        "mean": float(mean_value),
        "rms": float(rms_value),
        "std": float(std_value),
        "peak_amplitude": float(peak_value),
        "kurtosis": float(kurtosis_value),
        "crest_factor": float(crest_factor),
        "dominant_frequency": dominant_frequency
    }


def extract_features_from_csv(
    csv_path="dataset.csv",
    sensor_column="vibration_1",
    window_size=1024,
    sampling_rate=20000
):
    """
    Divide a sensor CSV into windows and extract features
    from each window.
    """

    df = pd.read_csv(csv_path)

    if sensor_column not in df.columns:
        raise ValueError(
            f"Column '{sensor_column}' was not found in {csv_path}."
        )

    readings = pd.to_numeric(
        df[sensor_column],
        errors="coerce"
    ).dropna().to_numpy()

    if len(readings) == 0:
        raise ValueError("No valid sensor readings were found.")

    results = []

    for start in range(0, len(readings), window_size):
        window = readings[start:start + window_size]

        # Skip incomplete windows to keep frequency resolution consistent
        if len(window) < window_size:
            continue

        features = extract_features(
            window,
            sampling_rate=sampling_rate
        )

        features["window_start"] = start
        features["window_end"] = start + len(window) - 1
        features["sensor"] = sensor_column

        results.append(features)

    return pd.DataFrame(results)


if __name__ == "__main__":
    features_df = extract_features_from_csv()

    print("\nExtracted vibration features:")
    print(features_df.head())

    print(f"\nNumber of complete windows: {len(features_df)}")

    output_path = "extracted_features.csv"
    features_df.to_csv(output_path, index=False)

    print(f"Features saved to: {output_path}")