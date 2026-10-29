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
    "Real-time vibration features, Isolation Forest anomaly detection, "
    "and rolling baseline alerts"
)

refresh_rate = st.sidebar.slider(
    "Refresh interval (seconds)", min_value=1, max_value=10, value=2
)

if not os.path.exists(OUTPUT_FILE):
    st.info(
        "Waiting for sensor data. Start Kafka, the consumer, "
        "and the sensor streamer."
    )
    st.stop()

try:
    df = pd.read_csv(OUTPUT_FILE)
except (pd.errors.EmptyDataError, PermissionError):
    st.info("The output file is not ready yet. Please wait.")
    st.stop()

if df.empty:
    st.info("Waiting for the first completed sensor window.")
    st.stop()

latest = df.iloc[-1]

status = str(latest.get("alert_status", "UNKNOWN"))
health = float(latest.get("health_score", 0))
rul = float(latest.get("rul", 0))
anomaly = int(latest.get("anomaly", 0))

st.subheader("Current System Status")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Alert Status", status)
c2.metric("Health Score", f"{health:.1f}%")
c3.metric("Estimated RUL", f"{rul:.1f} hours")
c4.metric("Windows Processed", len(df))

if status == "CRITICAL":
    st.error("Critical anomaly detected. Investigate the latest window.")
elif status == "WARNING":
    st.warning("The RMS value exceeded the rolling baseline threshold.")
elif status == "NORMAL":
    st.success("No anomaly alert was raised for the latest window.")
else:
    st.info("No recognized alert status is available.")

st.caption(
    f"Latest window: {latest.get('window', len(df))} | "
    f"Isolation Forest anomaly: {'Yes' if anomaly else 'No'}"
)

st.divider()
st.subheader("Vibration Feature Trends")

preferred = [
    "rms",
    "peak_amplitude",
    "kurtosis",
    "std",
    "mean",
    "dominant_frequency",
    "crest_factor",
]

available = [col for col in preferred if col in df.columns]

if available:
    chart_data = df[available].copy()

    if "window" in df.columns:
        chart_data.index = df["window"]
        chart_data.index.name = "Window"

    st.line_chart(chart_data)
else:
    st.warning("No recognized vibration feature columns were found.")

st.divider()
st.subheader("Health and Remaining Useful Life Trends")

trend_cols = [
    col for col in ["health_score", "rul"] if col in df.columns
]

if trend_cols:
    trend_data = df[trend_cols].copy()

    if "window" in df.columns:
        trend_data.index = df["window"]
        trend_data.index.name = "Window"

    st.line_chart(trend_data)

st.divider()
st.subheader("Recent Alerts")

if "alert_status" in df.columns:
    alerts = df[df["alert_status"] != "NORMAL"].copy()

    if alerts.empty:
        st.write("No warning or critical alerts have been recorded.")
    else:
        display_cols = [
            col
            for col in [
                "window",
                "alert_status",
                "anomaly",
                "baseline_alert",
                "rms",
                "health_score",
                "rul",
            ]
            if col in alerts.columns
        ]

        st.dataframe(
            alerts[display_cols].tail(20).iloc[::-1],
            use_container_width=True,
        )

st.caption(
    "Note: Health scores and RUL are illustrative estimates, "
    "not outputs from a trained RUL prediction model."
)

time.sleep(refresh_rate)
st.rerun()