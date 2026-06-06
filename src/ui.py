"""Streamlit dashboard for the Real-Time Streaming Platform."""

import logging
import os
import time

import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Real-Time Streaming Dashboard", layout="wide")
st.title("Real-Time Streaming Platform")
st.caption("Live event processing with micro-batch aggregation")


def api_get(path: str) -> dict:
    try:
        resp = requests.get(f"{API_URL}{path}", timeout=5)
        return resp.json()
    except requests.ConnectionError:
        logger.error("Cannot reach API at %s%s", API_URL, path)
        st.error("Cannot reach the API. Is the server running?")
        st.stop()


# --- Sidebar controls ---
with st.sidebar:
    st.header("Controls")
    auto_refresh = st.checkbox("Auto-refresh (3s)", value=True)
    if st.button("Produce 500 Events", type="primary"):
        requests.post(f"{API_URL}/produce", json={"count": 500}, timeout=10)
        st.success("Produced 500 events!")

    st.divider()
    st.header("Broker Stats")
    broker = api_get("/broker/stats")
    st.metric("Topics", broker.get("num_topics", 0))
    for tname, tinfo in broker.get("topics", {}).items():
        st.metric(f"Topic '{tname}' Messages", tinfo["total_messages"])
        st.caption(f"Partitions: {tinfo['per_partition']}")

    st.divider()
    st.header("Processor Stats")
    pstats = api_get("/processor/stats")
    st.metric("Total Processed", pstats.get("total_processed", 0))
    st.metric("Windows Computed", pstats.get("windows_computed", 0))
    st.caption(f"Window size: {pstats.get('window_seconds', 5)}s")

# --- Main dashboard ---
data = api_get("/dashboard-data")
latest = data.get("latest_window", {})
windows = data.get("windows", [])

if not latest:
    st.info("Waiting for first processing window... Events are being produced.")
    time.sleep(2)
    st.rerun()

# Top-level metrics
st.header("Latest Window")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Events", latest.get("events_in_window", 0))
col2.metric("Revenue", f"${latest.get('total_revenue', 0):,.2f}")
col3.metric("Purchases", latest.get("purchase_count", 0))
col4.metric("Avg Purchase", f"${latest.get('avg_purchase', 0):,.2f}")

st.divider()

# Charts row
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Events by Type")
    type_data = latest.get("event_types", {})
    if type_data:
        fig = px.pie(
            values=list(type_data.values()),
            names=list(type_data.keys()),
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    st.subheader("Revenue by Category")
    cat_data = latest.get("category_revenue", {})
    if cat_data:
        fig = px.bar(
            x=list(cat_data.keys()),
            y=list(cat_data.values()),
            labels={"x": "Category", "y": "Revenue ($)"},
            color=list(cat_data.values()),
            color_continuous_scale="Teal",
        )
        fig.update_layout(height=350, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

# Second charts row
chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    st.subheader("Events by Region")
    region_data = latest.get("region_counts", {})
    if region_data:
        fig = px.bar(
            x=list(region_data.keys()),
            y=list(region_data.values()),
            labels={"x": "Region", "y": "Events"},
            color=list(region_data.keys()),
        )
        fig.update_layout(height=350, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

with chart_col4:
    st.subheader("Events by Device")
    device_data = latest.get("device_counts", {})
    if device_data:
        fig = px.pie(
            values=list(device_data.values()),
            names=list(device_data.keys()),
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# Historical windows timeline
if len(windows) > 1:
    st.divider()
    st.subheader("Processing Window History")
    df = pd.DataFrame(windows)
    if "events_in_window" in df.columns and "window_end" in df.columns:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["window_end"], y=df["events_in_window"],
            mode="lines+markers", name="Events",
            line=dict(color="steelblue"),
        ))
        fig.add_trace(go.Scatter(
            x=df["window_end"], y=df["total_revenue"],
            mode="lines+markers", name="Revenue ($)",
            yaxis="y2", line=dict(color="coral"),
        ))
        fig.update_layout(
            height=350,
            yaxis=dict(title="Events", side="left"),
            yaxis2=dict(title="Revenue ($)", side="right", overlaying="y"),
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

# Auto-refresh
if auto_refresh:
    time.sleep(3)
    st.rerun()
