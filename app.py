from collections import deque

import pandas as pd
import streamlit as st

from config import SAFE_TEMP_MAX, SAFE_TEMP_MIN, STORAGE, WINDOW_SIZE
from generator import SensorStreamGenerator
from processor import StreamProcessor

st.set_page_config(page_title="Cold-chain stream analytics", layout="wide")

if "processor" not in st.session_state:
    st.session_state.processor = StreamProcessor(
        window_size=WINDOW_SIZE, sink_file=STORAGE
    )

if "generator" not in st.session_state:
    st.session_state.generator = SensorStreamGenerator()

if "history" not in st.session_state:
    st.session_state.history = deque(maxlen=60)

if "is_running" not in st.session_state:
    st.session_state.is_running = True

with st.sidebar:
    st.header = "Controls"

    col_start, col_stop = st.columns(2)
    with col_start:
        if st.button("Start", use_container_width=True):
            st.session_state.is_running = True
    with col_stop:
        if st.button("Stop", use_container_width=True):
            st.session_state.is_running = False

    refresh_interval = st.slider(
        "Poll interval (seconds)", min_value=0.5, max_value=3.0, value=1.0, step=0.5
    )

    if st.button("Reset"):
        st.session_state.history.clear()
        st.session_state.processor = StreamProcessor(
            window_size=WINDOW_SIZE, sink_file=STORAGE
        )
        st.rerun()

auto_interval = refresh_interval if st.session_state.is_running else None


@st.fragment(run_every=auto_interval)
def render_dashboard():
    if st.session_state.is_running:
        raw_event = st.session_state.generator.next_record()
        analyzed_row = st.session_state.processor.process_incoming_record(raw_event)
        st.session_state.history.append(analyzed_row)

    df = pd.DataFrame(list(st.session_state.history))

    if df.empty:
        st.info("Waiting for incoming stream data...")
        return

    latest = df.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        delta = round(latest["temp_c"] - latest["roll_mean_temp"], 2)
        st.metric("Current Temp", f"{latest['temp_c']} °C", f"{delta} vs Mean")
    with col2:
        st.metric("Rolling Mean", f"{latest['roll_mean_temp']} °C")
    with col3:
        st.metric("Rolling StdDev", f"{latest['roll_std_temp']} °C")
    with col4:
        anomaly_count = st.session_state.processor.total_anomalies
        total = st.session_state.processor.total_processed
        rate = (anomaly_count / total * 100) if total > 0 else 0
        st.metric(
            "Anomalies",
            f"{anomaly_count} / {total}",
            f"{rate:.1f}% rate",
            delta_color="inverse",
        )

    if latest["is_anomaly"]:
        st.error(
            f"**ALERT [{latest['timestamp']}]**: {latest['alert_reason']} | Temp: {latest['temp_c']}°C"
        )
    else:
        st.success(
            f"Normal operation at {latest['timestamp']} | Storage sink: `{STORAGE}`"
        )

    chart_df = df.set_index("timestamp")[["temp_c", "roll_mean_temp"]].copy()
    chart_df["Safe Min"] = SAFE_TEMP_MIN
    chart_df["Safe Max"] = SAFE_TEMP_MAX

    st.subheader("Temperature trend and it's limits")
    st.line_chart(chart_df, color=["#1f77b4", "#2ca02c", "#0055ff", "#ff2200"])

    with st.expander("Show Latest", expanded=True):
        display_cols = [
            "timestamp",
            "sensor_id",
            "temp_c",
            "humidity_pct",
            "roll_mean_temp",
            "roll_std_temp",
            "is_anomaly",
            "alert_reason",
        ]
        st.dataframe(
            df[display_cols].tail(10).iloc[::-1],
            use_container_width=True,
            hide_index=True,
        )


st.title("Cold-chain telemetry real-time monitor")
render_dashboard()
