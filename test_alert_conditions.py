
from collections import deque
import numpy as np

BASELINE_WINDOW = 20
MIN_BASELINE_WINDOWS = 5


def calculate_status(baseline_values, rms, is_anomaly):
    baseline_alert = False

    if len(baseline_values) >= MIN_BASELINE_WINDOWS:
        baseline_mean = float(np.mean(baseline_values))
        baseline_std = float(np.std(baseline_values))

        if baseline_std > 0:
            baseline_alert = (
                abs(rms - baseline_mean) > 3 * baseline_std
            )

    if is_anomaly:
        status = "CRITICAL"
    elif baseline_alert:
        status = "WARNING"
    else:
        status = "NORMAL"

    return status, baseline_alert


normal_rms = [
    0.100, 0.101, 0.099, 0.102,
    0.100, 0.101, 0.099, 0.100
]

tests = [
    ("Normal vibration", 0.100, False, "NORMAL"),
    ("RMS spike", 0.200, False, "WARNING"),
    ("ML anomaly", 0.100, True, "CRITICAL"),
]

all_passed = True

for name, rms, anomaly, expected in tests:
    actual, baseline_alert = calculate_status(
        normal_rms, rms, anomaly
    )

    passed = actual == expected
    all_passed = all_passed and passed

    print(
        f"{name}: expected={expected}, actual={actual} "
        f"| {'PASS' if passed else 'FAIL'}"
    )

print("\nAll tests passed!" if all_passed else "\nSome tests failed.")

