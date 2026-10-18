
from pathlib import Path
import re

import numpy as np
import pandas as pd


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "NASA_Bearing"
CLEANED_ROOT = PROJECT_ROOT / "data" / "cleaned"

# Rolling window used to calculate local volatility
VOLATILITY_WINDOW = 100


def read_sensor_file(file_path: Path) -> pd.DataFrame:
    """Read a whitespace-separated NASA sensor file."""
    try:
        data = pd.read_csv(
            file_path,
            sep=r"\s+",
            header=None,
            engine="python",
            on_bad_lines="skip",
        )
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        print(f"Could not read {file_path.name}: {exc}")
        return pd.DataFrame()

    # Convert every column to numeric; invalid values become NaN.
    data = data.apply(pd.to_numeric, errors="coerce")

    # Remove columns that contain no usable readings.
    data = data.dropna(axis=1, how="all")

    # Remove rows with no usable readings.
    data = data.dropna(axis=0, how="all")

    # Replace positive/negative infinity with missing values.
    data = data.replace([np.inf, -np.inf], np.nan)

    # Fill missing readings using each column's median.
    for column in data.columns:
        median = data[column].median()
        if pd.notna(median):
            data[column] = data[column].fillna(median)

    # Remove any remaining rows containing missing values.
    data = data.dropna(axis=0, how="any")

    return data.reset_index(drop=True)


def calculate_volatility(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling standard deviation for each sensor column."""
    result = data.copy()

    for column in data.columns:
        result[f"{column}_rolling_volatility"] = (
            data[column]
            .rolling(window=VOLATILITY_WINDOW, min_periods=2)
            .std()
        )

    return result


def normalize_volatility(data: pd.DataFrame) -> pd.DataFrame:
    """Scale each rolling-volatility feature between 0 and 1."""
    result = data.copy()

    volatility_columns = [
        column
        for column in result.columns
        if column.endswith("_rolling_volatility")
    ]

    for column in volatility_columns:
        values = result[column]
        minimum = values.min()
        maximum = values.max()

        if pd.isna(minimum) or pd.isna(maximum):
            continue

        if maximum > minimum:
            result[column] = (values - minimum) / (maximum - minimum)
        else:
            result[column] = 0.0

    return result


def safe_filename(name: str) -> str:
    """Make a filename safe for saving on common operating systems."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


def process_dataset() -> None:
    """Process every readable file in each NASA test folder."""
    if not RAW_ROOT.exists():
        raise FileNotFoundError(
            f"NASA dataset folder not found: {RAW_ROOT}"
        )

    CLEANED_ROOT.mkdir(parents=True, exist_ok=True)

    test_folders = [
        folder
        for folder in RAW_ROOT.iterdir()
        if folder.is_dir()
    ]

    if not test_folders:
        print(f"No test folders found inside {RAW_ROOT}")
        return

    total_files = 0
    successful_files = 0
    total_rows = 0

    for test_folder in sorted(test_folders):
        output_folder = CLEANED_ROOT / test_folder.name
        output_folder.mkdir(parents=True, exist_ok=True)

        files = sorted(
            file_path
            for file_path in test_folder.rglob("*")
            if file_path.is_file()
        )

        print(f"\nProcessing {test_folder.name}: {len(files)} files")

        for file_path in files:
            total_files += 1

            cleaned = read_sensor_file(file_path)

            if cleaned.empty:
                print(f"Skipped empty/unreadable file: {file_path.name}")
                continue

            # Preserve the original reading columns and add volatility.
            processed = calculate_volatility(cleaned)
            processed = normalize_volatility(processed)

            # Preserve nested folder structure to avoid filename clashes.
            relative_path = file_path.relative_to(test_folder)
            output_name = safe_filename(
                "__".join(relative_path.parts)
            ) + ".csv"

            output_path = output_folder / output_name

            processed.to_csv(output_path, index=False)

            successful_files += 1
            total_rows += len(processed)

        print(f"Saved results under: {output_folder}")

    print("\nPreprocessing complete.")
    print(f"Files examined: {total_files}")
    print(f"Files successfully processed: {successful_files}")
    print(f"Total cleaned rows: {total_rows}")
    print(f"Output directory: {CLEANED_ROOT}")


if __name__ == "__main__":
    process_dataset()
