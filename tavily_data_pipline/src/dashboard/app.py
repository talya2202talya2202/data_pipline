"""
Streamlit dashboard for Tavily agent metadata.

Clean dashboard with 4 visualizations:
1. Execution timeline – runs over time (hourly)
2. Query performance – average latency by query (top N)
3. Source usage – distribution of num_sources
4. Status overview – success vs failure and success rate trend

Data source: MongoDB (agent_metadata). Set MONGODB_URI in .env.
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from typing import Optional

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

import streamlit as st
import pandas as pd
import altair as alt

# Page config
st.set_page_config(
    page_title="Agent Metadata Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom styling for a clean look
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        padding: 1rem 1.25rem;
        border-radius: 0.5rem;
        color: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 0.5rem;
    }
    .metric-card h3 { margin: 0; font-size: 0.75rem; opacity: 0.9; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-card .value { font-size: 1.5rem; font-weight: 700; margin-top: 0.25rem; }
    .stSubheader { padding-top: 0.5rem; }
    div[data-testid="stMetricValue"] { font-size: 1.25rem; }
</style>
""", unsafe_allow_html=True)

st.title("Agent Metadata Dashboard")
st.caption("Tavily company research agent — execution metrics and performance")


def load_data(
    session_id: Optional[str] = None,
    agent_version: Optional[str] = None,
    limit: int = 500,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pd.DataFrame:
    """Load metadata from MongoDB with optional filters (date range or all time)."""
    try:
        from src.database.mongodb_client import MongoDBClient
        client = MongoDBClient()
        if date_from is not None or date_to is not None:
            start = datetime.combine(date_from or date(2000, 1, 1), datetime.min.time()).replace(tzinfo=timezone.utc)
            end = datetime.combine(date_to or date.today(), datetime.max.time()).replace(tzinfo=timezone.utc)
            if date_from is None:
                start = datetime(2000, 1, 1, tzinfo=timezone.utc)
            if date_to is None:
                end = datetime.now(timezone.utc)
            docs = client.get_metadata_by_date_range(start, end, limit=limit)
        else:
            docs = client.get_recent_metadata(limit=limit, hours=None)
        client.close()
        if not docs:
            return pd.DataFrame()
        df = pd.DataFrame(docs)
        if "timestamp_utc" in df.columns:
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        if session_id:
            df = df[df["session_id"] == session_id]
        if agent_version:
            df = df[df["agent_version"] == agent_version]
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


# Sidebar
st.sidebar.header("Filters")
time_mode = st.sidebar.radio(
    "Time range",
    options=["all", "range"],
    format_func=lambda x: "All time" if x == "all" else "From date to date",
    index=0,
)
date_from_filter: Optional[date] = None
date_to_filter: Optional[date] = None
if time_mode == "range":
    col_from, col_to = st.sidebar.columns(2)
    with col_from:
        date_from_filter = st.date_input("From", value=date.today() - timedelta(days=7), key="date_from")
    with col_to:
        date_to_filter = st.date_input("To", value=date.today(), key="date_to")
    if date_from_filter and date_to_filter and date_from_filter > date_to_filter:
        st.sidebar.warning("From date must be before To date.")
limit = st.sidebar.slider("Max records", 5, 1000, 5, help="Load 5 to see all rows in the table below; increase to load more.")

df = load_data(
    limit=limit,
    date_from=date_from_filter if time_mode == "range" else None,
    date_to=date_to_filter if time_mode == "range" else None,
)

if df.empty:
    st.warning("No data found. Run the agent to generate metadata.")
    st.stop()

session_ids = [""] + sorted(df["session_id"].dropna().unique().tolist())
agent_versions = [""] + sorted(df["agent_version"].dropna().unique().tolist())
session_filter = st.sidebar.selectbox(
    "Session", session_ids, format_func=lambda x: "All" if x == "" else (x[:12] + "…" if len(x) > 12 else x)
)
version_filter = st.sidebar.selectbox(
    "Agent version", agent_versions, format_func=lambda x: "All" if x == "" else x
)
if session_filter:
    df = df[df["session_id"] == session_filter]
if version_filter:
    df = df[df["agent_version"] == version_filter]

st.sidebar.metric("Rows loaded", len(df))
if st.sidebar.button("Refresh"):
    st.rerun()

# KPI row
total = len(df)
success = (df["status"] == "success").sum() if "status" in df.columns else 0
success_rate = (100 * success / total) if total else 0
avg_latency = df["latency_ms"].mean() if "latency_ms" in df.columns else 0
avg_sources = df["num_sources"].mean() if "num_sources" in df.columns else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total runs", total)
col2.metric("Success rate", f"{success_rate:.1f}%")
col3.metric("Avg latency", f"{avg_latency:.0f} ms")
col4.metric("Avg sources", f"{avg_sources:.1f}")

st.divider()

# 1. Execution timeline (runs per hour)
st.subheader("1. Execution timeline")
if "timestamp_utc" in df.columns and not df.empty:
    timeline = (
        df.assign(hour=df["timestamp_utc"].dt.floor("h"))
        .groupby("hour", as_index=False)
        .size()
    )
    chart_timeline = (
        alt.Chart(timeline)
        .mark_area(line=True, point=True, opacity=0.6)
        .encode(
            x=alt.X("hour:T", title="Time (UTC)"),
            y=alt.Y("size:Q", title="Runs"),
        )
        .properties(height=280)
    )
    st.altair_chart(chart_timeline, use_container_width=True)
else:
    st.info("No timestamp data.")

# 2. Query performance (avg latency by query, top 12)
st.subheader("2. Query performance (avg latency by query)")
if "query" in df.columns and "latency_ms" in df.columns and not df.empty:
    perf = (
        df.groupby("query", as_index=False)["latency_ms"]
        .agg(["mean", "count"])
        .rename(columns={"mean": "avg_latency_ms", "count": "runs"})
        .sort_values("avg_latency_ms", ascending=False)
        .head(12)
    )
    perf["query_short"] = perf["query"].apply(lambda x: (x[:50] + "…") if len(str(x)) > 50 else x)
    chart_perf = (
        alt.Chart(perf)
        .mark_bar()
        .encode(
            x=alt.X("avg_latency_ms:Q", title="Avg latency (ms)"),
            y=alt.Y("query_short:N", sort="-x", title="Query"),
            tooltip=["query:N", "avg_latency_ms:Q", "runs:Q"],
        )
        .properties(height=320)
    )
    st.altair_chart(chart_perf, use_container_width=True)
else:
    st.info("No query/latency data.")

# 3. Source count distribution
st.subheader("3. Source count distribution")
if "num_sources" in df.columns and not df.empty:
    src_counts = df["num_sources"].value_counts().sort_index().reset_index()
    src_counts.columns = ["num_sources", "count"]
    chart_sources = (
        alt.Chart(src_counts)
        .mark_bar()
        .encode(
            x=alt.X("num_sources:O", title="Number of sources"),
            y=alt.Y("count:Q", title="Runs"),
            tooltip=["num_sources:Q", "count:Q"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart_sources, use_container_width=True)
else:
    st.info("No num_sources data.")

# 4. Status overview (success vs failure + optional trend)
st.subheader("4. Status overview")
if "status" in df.columns and not df.empty:
    c1, c2 = st.columns([1, 1])
    with c1:
        status_counts = df["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        chart_status = (
            alt.Chart(status_counts)
            .mark_arc(innerRadius=50)
            .encode(
                theta=alt.Theta("count:Q"),
                color=alt.Color("status:N", scale=alt.Scale(range=["#22c55e", "#ef4444"])),
                tooltip=["status:N", "count:Q"],
            )
            .properties(height=260, title="Success vs failure")
        )
        st.altair_chart(chart_status, use_container_width=True)
    with c2:
        if "timestamp_utc" in df.columns and len(df) >= 2:
            daily = (
                df.assign(date=df["timestamp_utc"].dt.date)
                .groupby("date")["status"]
                .apply(lambda s: 100 * (s == "success").sum() / len(s))
                .reset_index()
            )
            daily.columns = ["date", "success_rate_pct"]
            chart_trend = (
                alt.Chart(daily)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("success_rate_pct:Q", title="Success rate (%)", scale=alt.Scale(domain=[0, 100])),
                )
                .properties(height=260, title="Daily success rate")
            )
            st.altair_chart(chart_trend, use_container_width=True)
        else:
            st.dataframe(status_counts, use_container_width=True, hide_index=True)
else:
    st.info("No status data.")

with st.expander("View raw data"):
    st.dataframe(df, use_container_width=True, hide_index=True)
