
import pandas as pd


def analyze_sensor_volatility(df, sensor_column):
    """Calculate basic statistics to understand sensor fluctuations."""

    if sensor_column not in df.columns:
        raise ValueError(
            f"Sensor column '{sensor_column}' was not found."
        )

    readings = pd.to_numeric(
        df[sensor_column], errors="coerce"
    ).dropna()

    if readings.empty:
        raise ValueError("No valid numeric sensor readings found.")

    results = {
        "mean": readings.mean(),
        "standard_deviation": readings.std(),
        "minimum": readings.min(),
        "maximum": readings.max(),
        "range": readings.max() - readings.min(),
        "rolling_standard_deviation": readings.rolling(
            window=10, min_periods=10
        ).std()
    }

    return results


if __name__ == "__main__":
    print("Task 2 volatility analysis module is ready.")
