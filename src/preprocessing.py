
import pandas as pd


def inspect_sensor_data(df):
    """Inspect sensor telemetry data before cleaning."""

    print("Dataset shape:", df.shape)
    print("\nColumns:", df.columns.tolist())
    print("\nData types:\n", df.dtypes)
    print("\nMissing values:\n", df.isnull().sum())
    print("\nDuplicate rows:", df.duplicated().sum())


def clean_sensor_data(df):
    """Perform basic cleaning without blindly removing anomalies."""

    cleaned = df.copy()

    # Remove exact duplicate rows
    cleaned = cleaned.drop_duplicates()

    # Replace infinite values with missing values
    cleaned = cleaned.replace(
        [float("inf"), float("-inf")],
        float("nan")
    )

    return cleaned


if __name__ == "__main__":
    print("Task 2 preprocessing module is ready.")