
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLEANED_ROOT = PROJECT_ROOT / "data" / "cleaned"

VOLATILITY_WINDOW = 100


# --------------------------------------------------
# Analyze sensor volatility
# --------------------------------------------------

def analyze_sensor_volatility(
    df: pd.DataFrame,
    sensor_column: str,
    window: int = VOLATILITY_WINDOW,
):
    """Calculate statistics and rolling volatility for a sensor."""

    if sensor_column not in df.columns:
        raise ValueError(
            f"Sensor column '{sensor_column}' was not found."
        )

    readings = pd.to_numeric(
        df[sensor_column], errors="coerce"
    ).dropna()

    if readings.empty:
        raise ValueError("No valid numeric sensor readings found.")

    # Calculate rolling standard deviation.
    rolling_volatility = readings.rolling(
        window=window,
        min_periods=2,
    ).std()

    valid_volatility = rolling_volatility.dropna()

    # Summarize the sensor's behaviour.
    results = {
        "sensor_column": sensor_column,
        "number_of_readings": len(readings),
        "mean": readings.mean(),
        "standard_deviation": readings.std(),
        "minimum": readings.min(),
        "maximum": readings.max(),
        "range": readings.max() - readings.min(),
        "average_rolling_volatility": valid_volatility.mean(),
        "maximum_rolling_volatility": valid_volatility.max(),
    }

    return results, readings, rolling_volatility


# --------------------------------------------------
# Plot sensor readings and volatility
# --------------------------------------------------

def plot_sensor_volatility(
    readings,
    rolling_volatility,
    sensor_column,
    file_name,
):
    """Plot sensor readings and their rolling volatility."""

    fig, axes = plt.subplots(2, 1, figsize=(12, 8))

    axes[0].plot(readings.index, readings, linewidth=0.7)
    axes[0].set_title(
        f"Sensor {sensor_column} - {file_name}"
    )
    axes[0].set_xlabel("Reading number")
    axes[0].set_ylabel("Sensor value")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(
        rolling_volatility.index,
        rolling_volatility,
        linewidth=0.8,
    )
    axes[1].set_title("Rolling Volatility")
    axes[1].set_xlabel("Reading number")
    axes[1].set_ylabel("Rolling standard deviation")
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


# --------------------------------------------------
# Run analysis on one cleaned NASA file
# --------------------------------------------------

def main():
    input_folder = CLEANED_ROOT / "1st_test"

    files = sorted(input_folder.glob("*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No cleaned CSV files found in {input_folder}"
        )

    # Start with one file rather than loading the entire dataset.
    file_path = files[0]

    print("=" * 60)
    print("SENSOR VOLATILITY ANALYSIS")
    print("=" * 60)
    print(f"Input file: {file_path.name}")

    df = pd.read_csv(file_path)

    # Begin with sensor column 0.
    sensor_column = "0"

    results, readings, rolling_volatility = (
        analyze_sensor_volatility(df, sensor_column)
    )

    print("\nAnalysis results")
    print("-" * 40)

    for name, value in results.items():
        if isinstance(value, (int, float)):
            print(f"{name}: {value:.6f}")
        else:
            print(f"{name}: {value}")

    plot_sensor_volatility(
        readings,
        rolling_volatility,
        sensor_column,
        file_path.name,
    )


if __name__ == "__main__":
    main()
