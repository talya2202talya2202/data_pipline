"""
Streamlit dashboard for Tavily agent pipeline.

Data source: Snowflake (agent_runs, run_steps, api_calls). Fallback: MongoDB.
Four sections: Agent Health, Agent Performance, Usage & Demand, Cost Efficiency.
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Tuple

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

st.set_page_config(
    page_title="Agent Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Theme: deep teal/blue with coral accents
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
    }
    [data-testid="stSidebar"] .stRadio label, [data-testid="stSidebar"] label, [data-testid="stSidebar"] p {
        color: #e8f4f8 !important;
    }
    [data-testid="stSidebar"] .stSlider label { color: #e8f4f8 !important; }
    h1 { color: #0f2027 !important; font-weight: 700 !important; }
    .stCaption { color: #2c5364 !important; }
    [data-testid="stMetricValue"] {
        font-size: 1.4rem !important;
        font-weight: 700 !important;
        color: #0f2027 !important;
    }
    [data-testid="stMetricLabel"] { color: #2c5364 !important; }
    h2, h3 { color: #203a43 !important; border-bottom: 2px solid #2c5364; padding-bottom: 0.25rem !important; }
    .streamlit-expanderHeader { background: #f0f7fa; border-radius: 0.5rem; }
</style>
""", unsafe_allow_html=True)

CHART_COLORS = ["#2c5364", "#3a7ca5", "#e07a5f", "#f4a261", "#81b29a", "#3d5a80"]
alt.themes.enable("none")
def altair_theme():
    return {"config": {"view": {"continuousWidth": 400, "continuousHeight": 300}, "range": {"category": CHART_COLORS}}}
alt.themes.register("custom", altair_theme)
alt.themes.enable("custom")

st.title("Agent Pipeline Dashboard")
st.caption("Tavily company research — runs, API usage, and step reliability")


def load_snowflake(
    date_from: Optional[date],
    date_to: Optional[date],
    limit: int,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], str]:
    """Load agent_runs, run_steps, api_calls from Snowflake. Returns (df_runs, df_steps, df_calls, error_msg)."""
    try:
        from src.snowflake.snowflake_client import SnowflakeClient
        client = SnowflakeClient()
        date_from_str = date_from.isoformat() if date_from else None
        date_to_str = date_to.isoformat() if date_to else None
        runs = client.get_agent_runs(limit=limit, date_from=date_from_str, date_to=date_to_str)
        client.close()
        if not runs:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), ""
        run_ids = [r["RUN_ID"] for r in runs]
        client2 = SnowflakeClient()
        steps = client2.get_run_steps(limit=5000, run_ids=run_ids)
        calls = client2.get_api_calls(limit=5000, run_ids=run_ids)
        client2.close()
        df_runs = pd.DataFrame(runs)
        df_steps = pd.DataFrame(steps) if steps else pd.DataFrame()
        df_calls = pd.DataFrame(calls) if calls else pd.DataFrame()
        for df in (df_runs, df_steps, df_calls):
            if df.empty:
                continue
            df.columns = [c.lower() for c in df.columns]
        if "started_at" in df_runs.columns:
            df_runs["started_at"] = pd.to_datetime(df_runs["started_at"], utc=True)
        if "completed_at" in df_runs.columns:
            df_runs["completed_at"] = pd.to_datetime(df_runs["completed_at"], utc=True)
        if "called_at" in df_calls.columns:
            df_calls["called_at"] = pd.to_datetime(df_calls["called_at"], utc=True)
        return df_runs, df_steps, df_calls, ""
    except Exception as e:
        return None, None, None, str(e)


def load_data_mongo(
    limit: int = 500,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pd.DataFrame:
    """Fallback: load from MongoDB (single collection)."""
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
        return df
    except Exception as e:
        st.error(f"MongoDB: {e}")
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
    date_from_filter = st.sidebar.date_input("From", value=date.today() - timedelta(days=7), key="date_from")
    date_to_filter = st.sidebar.date_input("To", value=date.today(), key="date_to")
    if date_from_filter and date_to_filter and date_from_filter > date_to_filter:
        st.sidebar.warning("From date must be before To date.")
limit = st.sidebar.slider("Max runs", 5, 1000, 100, help="Max agent runs to load")

# Load data: try Snowflake first, then MongoDB
df_runs: Optional[pd.DataFrame] = None
df_steps: Optional[pd.DataFrame] = None
df_calls: Optional[pd.DataFrame] = None
use_snowflake = False
snowflake_error = ""

df_runs, df_steps, df_calls, snowflake_error = load_snowflake(
    date_from_filter if time_mode == "range" else None,
    date_to_filter if time_mode == "range" else None,
    limit,
)

if df_runs is not None and not df_runs.empty:
    use_snowflake = True
    if snowflake_error:
        st.sidebar.caption(f"Snowflake: loaded (warning: {snowflake_error})")
else:
    if snowflake_error:
        st.sidebar.info(f"Snowflake not used: {snowflake_error[:80]}…")
    df_mongo = load_data_mongo(
        limit=limit,
        date_from=date_from_filter if time_mode == "range" else None,
        date_to=date_to_filter if time_mode == "range" else None,
    )
    if not df_mongo.empty:
        df_runs = df_mongo
        df_runs = df_runs.rename(columns={
            "query": "company_name",
            "timestamp_utc": "started_at",
            "latency_ms": "total_latency_ms",
            "num_sources": "total_api_calls",
        })
        if "company_name" not in df_runs.columns and "query" in df_mongo.columns:
            df_runs["company_name"] = df_mongo["query"]
        df_steps = pd.DataFrame()
        df_calls = pd.DataFrame()
    else:
        st.warning("No data found. Run the agent and ensure MongoDB or Snowflake is configured.")
        st.stop()

if df_runs is None or df_runs.empty:
    st.warning("No data found. Run the agent to generate metadata.")
    st.stop()

st.sidebar.metric("Runs loaded", len(df_runs))
if st.sidebar.button("Refresh"):
    st.rerun()

# ---- Agent Health ----
st.header("Agent Health")
c1, c2, c3, c4 = st.columns(4)
total_runs = len(df_runs)
success_col = "status"
if success_col in df_runs.columns:
    success_count = (df_runs[success_col] == "success").sum()
    success_rate = 100 * success_count / total_runs if total_runs else 0
else:
    success_count = total_runs
    success_rate = 100.0
c1.metric("Total runs", total_runs)
c2.metric("Success rate", f"{success_rate:.1f}%")
avg_lat = df_runs["total_latency_ms"].mean() if "total_latency_ms" in df_runs.columns else 0
c3.metric("Avg latency (ms)", f"{avg_lat:.0f}")
total_apis = df_runs["total_api_calls"].sum() if "total_api_calls" in df_runs.columns else 0
c4.metric("Total API calls", int(total_apis))

if "company_name" in df_runs.columns:
    st.subheader("Runs by company")
    count_col = "run_id" if "run_id" in df_runs.columns else "company_name"
    by_company = df_runs.groupby("company_name", as_index=False).agg(
        runs=(count_col, "count"),
    )
    if "status" in df_runs.columns:
        success_by = df_runs.groupby("company_name", as_index=False).agg(
            success_rate=("status", lambda s: 100 * (s == "success").sum() / len(s) if len(s) else 0),
        )
        by_company = by_company.merge(success_by, on="company_name")
    chart_company = alt.Chart(by_company.head(15)).mark_bar().encode(
        x=alt.X("runs:Q", title="Runs"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        color=alt.value(CHART_COLORS[0]),
        tooltip=[c for c in by_company.columns],
    ).properties(height=300)
    st.altair_chart(chart_company, use_container_width=True)

if "error_message" in df_runs.columns and df_runs["error_message"].notna().any():
    with st.expander("Recent errors"):
        cols = [c for c in ["company_name", "run_id", "error_message", "started_at"] if c in df_runs.columns]
        errs = df_runs[df_runs["error_message"].notna()][cols].head(10)
        st.dataframe(errs, use_container_width=True, hide_index=True)

# ---- Agent Performance ----
st.header("Agent Performance")
if "total_latency_ms" in df_runs.columns and "company_name" in df_runs.columns:
    st.subheader("Latency by company (avg)")
    perf = df_runs.groupby("company_name", as_index=False)["total_latency_ms"].mean().sort_values("total_latency_ms", ascending=False).head(12)
    chart_perf = alt.Chart(perf).mark_bar().encode(
        x=alt.X("total_latency_ms:Q", title="Avg latency (ms)"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        color=alt.value(CHART_COLORS[1]),
        tooltip=["company_name", "total_latency_ms"],
    ).properties(height=300)
    st.altair_chart(chart_perf, use_container_width=True)

if "started_at" in df_runs.columns:
    st.subheader("Runs over time")
    df_runs_copy = df_runs.copy()
    df_runs_copy["hour"] = pd.to_datetime(df_runs_copy["started_at"]).dt.floor("h")
    timeline = df_runs_copy.groupby("hour", as_index=False).size()
    chart_timeline = alt.Chart(timeline).mark_area(line=True, point=True, opacity=0.6).encode(
        x=alt.X("hour:T", title="Time (UTC)"),
        y=alt.Y("size:Q", title="Runs"),
    ).properties(height=260)
    st.altair_chart(chart_timeline, use_container_width=True)

if use_snowflake and df_steps is not None and not df_steps.empty and "step_name" in df_steps.columns and "latency_ms" in df_steps.columns:
    st.subheader("Step performance (latency by step)")
    step_perf = df_steps.groupby("step_name", as_index=False).agg(
        avg_latency_ms=("latency_ms", "mean"),
        runs=("latency_ms", "count"),
    )
    chart_steps = alt.Chart(step_perf).mark_bar().encode(
        x=alt.X("avg_latency_ms:Q", title="Avg latency (ms)"),
        y=alt.Y("step_name:N", sort="-x", title="Step"),
        color=alt.value(CHART_COLORS[2]),
        tooltip=["step_name", "avg_latency_ms", "runs"],
    ).properties(height=260)
    st.altair_chart(chart_steps, use_container_width=True)

# ---- Usage & Demand ----
st.header("Usage & Demand")
if "total_api_calls" in df_runs.columns and "company_name" in df_runs.columns:
    usage = df_runs.groupby("company_name", as_index=False)["total_api_calls"].sum().sort_values("total_api_calls", ascending=False).head(12)
    chart_usage = alt.Chart(usage).mark_bar().encode(
        x=alt.X("total_api_calls:Q", title="Total API calls"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        color=alt.value(CHART_COLORS[3]),
        tooltip=["company_name", "total_api_calls"],
    ).properties(height=280)
    st.altair_chart(chart_usage, use_container_width=True)

if use_snowflake and df_calls is not None and not df_calls.empty and "query_used" in df_calls.columns:
    st.subheader("Top queries (API calls)")
    top_queries = df_calls["query_used"].value_counts().head(15).reset_index()
    top_queries.columns = ["query", "calls"]
    top_queries["query_short"] = top_queries["query"].apply(lambda x: (str(x)[:60] + "…") if len(str(x)) > 60 else str(x))
    chart_q = alt.Chart(top_queries).mark_bar().encode(
        x=alt.X("calls:Q", title="API calls"),
        y=alt.Y("query_short:N", sort="-x", title="Query"),
        color=alt.value(CHART_COLORS[4]),
        tooltip=["query:N", "calls:Q"],
    ).properties(height=320)
    st.altair_chart(chart_q, use_container_width=True)

# ---- Cost Efficiency ----
st.header("Cost Efficiency")
col1, col2 = st.columns(2)
with col1:
    if "total_api_calls" in df_runs.columns:
        total_calls = int(df_runs["total_api_calls"].sum())
        st.metric("Total API calls (all runs)", total_calls)
with col2:
    if "total_latency_ms" in df_runs.columns:
        total_time_s = df_runs["total_latency_ms"].sum() / 1000
        st.metric("Total compute time (s)", f"{total_time_s:.1f}")

if "industry" in df_runs.columns and df_runs["industry"].notna().any():
    st.subheader("Runs by industry")
    by_ind = df_runs[df_runs["industry"].notna()].groupby("industry", as_index=False).size()
    by_ind.columns = ["industry", "runs"]
    chart_ind = alt.Chart(by_ind).mark_arc(innerRadius=40).encode(
        theta=alt.Theta("runs:Q"),
        color=alt.Color("industry:N", scale=alt.Scale(range=CHART_COLORS)),
        tooltip=["industry", "runs"],
    ).properties(height=280, title="Runs by industry")
    st.altair_chart(chart_ind, use_container_width=True)

with st.expander("View raw runs data"):
    st.dataframe(df_runs, use_container_width=True, hide_index=True)
if use_snowflake and df_steps is not None and not df_steps.empty:
    with st.expander("View run steps"):
        st.dataframe(df_steps, use_container_width=True, hide_index=True)
if use_snowflake and df_calls is not None and not df_calls.empty:
    with st.expander("View API calls"):
        st.dataframe(df_calls, use_container_width=True, hide_index=True)
