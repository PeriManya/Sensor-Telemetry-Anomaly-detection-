
import os
import time

import pandas as pd
import streamlit as st

OUTPUT_FILE = "extracted_features.csv"

st.set_page_config(
    page_title="Zaalima Development: Bearing Telemetry Dashboard",
    page_icon="⚙️",
    layout="wide",
)

st.title("⚙️ Zaalima Development: Real-Time Bearing Telemetry & Health Dashboard")
st.caption(
    "Real-time vibration monitoring, Isolation Forest anomaly detection, "
    "and rolling baseline alerts"
)

refresh_rate = st.sidebar.slider(
    "Refresh interval (seconds)",
    min_value=1,
    max_value=10,
    value=2,
)

# Load the latest consumer output
if not os.path.exists(OUTPUT_FILE):
    st.info("Waiting for sensor data. Start Kafka, the consumer, and the streamer.")
    st.stop()

try:
    df = pd.read_csv(OUTPUT_FILE)
except (pd.errors.EmptyDataError, PermissionError):
    st.info("The output file is not ready yet. Please wait.")
    st.stop()

if df.empty:
    st.info("Waiting for the first completed sensor window.")
    st.stop()

# Convert expected numeric columns safely
numeric_cols = [
    "mean", "rms", "std", "peak_amplitude", "kurtosis",
    "crest_factor", "dominant_frequency", "window",
    "health_score", "rul", "anomaly",
    "anomalous_samples", "anomaly_fraction", "baseline_alert",
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

latest = df.iloc[-1]

status = str(latest.get("alert_status", "UNKNOWN"))
health = latest.get("health_score", 0)
rul = latest.get("rul", 0)
anomaly_fraction = latest.get("anomaly_fraction", 0)

health = 0 if pd.isna(health) else float(health)
rul = 0 if pd.isna(rul) else float(rul)
anomaly_fraction = (
    0 if pd.isna(anomaly_fraction) else float(anomaly_fraction)
)

total_windows = len(df)
flagged_windows = (
    int(df["anomaly"].fillna(0).sum())
    if "anomaly" in df.columns else 0
)
total_anomalous_samples = (
    int(df["anomalous_samples"].fillna(0).sum())
    if "anomalous_samples" in df.columns else 0
)

# Current system status
st.subheader("Current System Status")

c1, c2, c3, c4 = st.columns(4)

c1.metric("Alert Status", status)
c2.metric("Health Score", f"{health:.1f}%")
c3.metric("Estimated RUL", f"{rul:.1f} hours")
c4.metric("Windows Processed", total_windows)

if status == "CRITICAL":
    st.error("Critical anomaly detected. Investigate the latest window.")
elif status == "WARNING":
    st.warning("The RMS value exceeded the rolling baseline threshold.")
elif status == "NORMAL":
    st.success("No anomaly alert was raised for the latest window.")
else:
    st.info("No recognized alert status is available.")

st.caption(
    f"Latest window: {latest.get('window', total_windows)} | "
    f"Isolation Forest anomaly: "
    f"{'Yes' if int(latest.get('anomaly', 0) or 0) else 'No'}"
)

st.divider()

# Anomaly monitoring
st.subheader("Anomaly Monitoring")

a1, a2, a3 = st.columns(3)

a1.metric("Flagged Windows", flagged_windows)
a2.metric("Total Anomalous Samples", total_anomalous_samples)
a3.metric("Latest Anomaly Fraction", f"{anomaly_fraction:.2%}")

if "window" in df.columns and "anomaly_fraction" in df.columns:
    anomaly_chart = df.set_index("window")[["anomaly_fraction"]].copy()
    anomaly_chart["anomaly_fraction"] *= 100

    st.caption("Anomalous samples as a percentage of each processed window")
    st.line_chart(anomaly_chart)

st.divider()

# Vibration statistical features
st.subheader("Vibration Feature Trends")

feature_cols = [
    col for col in [
        "rms",
        "std",
        "mean",
        "peak_amplitude",
        "kurtosis",
        "crest_factor",
    ]
    if col in df.columns
]

if feature_cols:
    feature_data = df[feature_cols].copy()

    if "window" in df.columns:
        feature_data.index = df["window"]
        feature_data.index.name = "Window"

    st.line_chart(feature_data)
else:
    st.warning("No vibration feature columns were found.")

st.divider()

# Dominant frequency displayed separately
st.subheader("Dominant Frequency Trend")

if "dominant_frequency" in df.columns:
    frequency_data = df[["dominant_frequency"]].copy()

    if "window" in df.columns:
        frequency_data.index = df["window"]
        frequency_data.index.name = "Window"

    st.line_chart(frequency_data)
else:
    st.warning("Dominant frequency data is unavailable.")

st.divider()

# Health and RUL trends
st.subheader("Health and Remaining Useful Life Trends")

trend_cols = [
    col for col in ["health_score", "rul"]
    if col in df.columns
]

if trend_cols:
    trend_data = df[trend_cols].copy()

    if "window" in df.columns:
        trend_data.index = df["window"]
        trend_data.index.name = "Window"

    st.line_chart(trend_data)

st.divider()

# Recent alerts
st.subheader("Recent Alerts")

if "alert_status" in df.columns:
    alerts = df[df["alert_status"].isin(["WARNING", "CRITICAL"])].copy()

    if alerts.empty:
        st.success("No warning or critical alerts have been recorded.")
    else:
        display_cols = [
            col for col in [
                "window",
                "alert_status",
                "anomaly",
                "anomalous_samples",
                "anomaly_fraction",
                "baseline_alert",
                "rms",
            ]
            if col in alerts.columns
        ]

        st.dataframe(
            alerts[display_cols].tail(20).iloc[::-1],
            width="stretch"
        )

st.divider()

# Recent window records
st.subheader("Recent Sensor Windows")

st.dataframe(
    df.tail(10).iloc[::-1],
    width="stretch"
)

st.caption(
    "Note: Health scores and RUL are illustrative estimates, "
    "not outputs from a trained RUL prediction model. "
    "Anomaly flags indicate potential unusual patterns, not confirmed bearing failures."
)

# Automatic refresh
time.sleep(refresh_rate)
st.rerun()
