"""
Streamlit dashboard for Tavily agent pipeline.

Data source: Snowflake only (agent_runs, run_steps, api_calls).
Four sections: Agent Health, Agent Performance, Usage & Demand, Cost Efficiency.
"""

import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Optional, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

import os
import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(
    page_title="Agent Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Colorful theme: teal/blue base with coral/amber accents
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
    }
    [data-testid="stSidebar"] .stRadio label, [data-testid="stSidebar"] label, [data-testid="stSidebar"] p { color: #e8f4f8 !important; }
    [data-testid="stSidebar"] .stSlider label { color: #e8f4f8 !important; }
    h1 { color: #0f2027 !important; font-weight: 700 !important; }
    h2 {
        color: #203a43 !important;
        border-left: 4px solid #e07a5f;
        padding-left: 0.5rem;
        margin-top: 1.5rem !important;
    }
    .stCaption { color: #2c5364 !important; }
    [data-testid="stMetricValue"] { font-size: 1.35rem !important; font-weight: 700 !important; color: #0f2027 !important; }
    [data-testid="stMetricLabel"] { color: #2c5364 !important; }
    .streamlit-expanderHeader { background: #f0f7fa; border-radius: 0.5rem; }
</style>
""", unsafe_allow_html=True)

# Distinct colors per section for clarity
COLORS = {
    "health": ["#22c55e", "#ef4444", "#3b82f6"],      # green, red, blue
    "perf": ["#3b82f6", "#8b5cf6", "#06b6d4"],        # blue, violet, cyan
    "usage": ["#f59e0b", "#ea580c", "#84cc16"],       # amber, orange, lime
    "cost": ["#ec4899", "#14b8a6", "#6366f1"],        # pink, teal, indigo
}
CHART_COLORS = ["#2c5364", "#3a7ca5", "#e07a5f", "#f4a261", "#81b29a", "#3d5a80"]
alt.themes.enable("none")
alt.themes.register("custom", lambda: {"config": {"view": {"continuousWidth": 400, "continuousHeight": 280}, "range": {"category": CHART_COLORS}}})
alt.themes.enable("custom")

st.title("Agent Pipeline Dashboard")
st.caption("Tavily company research — health, performance, usage, cost")


def load_snowflake(
    date_from: Optional[date],
    date_to: Optional[date],
    limit: int,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], str]:
    """Load agent_runs, run_steps, api_calls from Snowflake."""
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
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
        if "started_at" in df_runs.columns:
            df_runs["started_at"] = pd.to_datetime(df_runs["started_at"], utc=True)
        if "completed_at" in df_runs.columns:
            df_runs["completed_at"] = pd.to_datetime(df_runs["completed_at"], utc=True)
        if "called_at" in df_calls.columns and not df_calls.empty:
            df_calls["called_at"] = pd.to_datetime(df_calls["called_at"], utc=True)
        return df_runs, df_steps, df_calls, ""
    except Exception as e:
        return None, None, None, str(e)


# ----- Sidebar -----
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

df_runs, df_steps, df_calls = None, None, None
use_snowflake = False
snowflake_error = ""

df_runs, df_steps, df_calls, snowflake_error = load_snowflake(
    date_from_filter if time_mode == "range" else None,
    date_to_filter if time_mode == "range" else None,
    limit,
)

if df_runs is None or df_runs.empty:
    st.error("No data from Snowflake. Check credentials (SNOWFLAKE_* in .env) and that agent_runs has data.")
    if snowflake_error:
        st.code(snowflake_error, language="text")
    st.stop()

use_snowflake = True
if snowflake_error:
    st.sidebar.caption(f"Snowflake: loaded (warning: {snowflake_error})")

db = os.getenv("SNOWFLAKE_DATABASE", "?")
schema = os.getenv("SNOWFLAKE_SCHEMA", "?")
st.sidebar.caption(f"Source: **{db}.{schema}.agent_runs** · {len(df_runs)} rows")
st.sidebar.metric("Runs loaded", len(df_runs))
if st.sidebar.button("Refresh"):
    st.rerun()

# ----- 1. Agent Health: success/failure rates, error breakdown, which companies/steps fail most -----
st.header("🏥 Agent Health")
st.caption("Success vs failure rates, error breakdown, which companies or steps fail most")

total_runs = len(df_runs)
status_col = "status"
if status_col in df_runs.columns:
    success_count = (df_runs[status_col] == "success").sum()
    failure_count = (df_runs[status_col] == "failure").sum()
    success_rate = 100 * success_count / total_runs if total_runs else 0
    failure_rate = 100 * failure_count / total_runs if total_runs else 0
else:
    success_count, failure_count = total_runs, 0
    success_rate, failure_rate = 100.0, 0.0

h1, h2, h3 = st.columns(3)
h1.metric("Total runs", total_runs)
h2.metric("Success rate", f"{success_rate:.1f}%")
h3.metric("Failure rate", f"{failure_rate:.1f}%")

# Chart: Which companies fail most (bar)
if "company_name" in df_runs.columns and status_col in df_runs.columns and failure_count > 0:
    fail_by_company = df_runs[df_runs[status_col] == "failure"].groupby("company_name", as_index=False).size()
    fail_by_company.columns = ["company_name", "failures"]
    fail_by_company = fail_by_company.sort_values("failures", ascending=False).head(10)
    chart_fail = alt.Chart(fail_by_company).mark_bar(color=COLORS["health"][1]).encode(
        x=alt.X("failures:Q", title="Failures"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        tooltip=["company_name", "failures"],
    ).properties(height=240, title="Companies with most failures")
    st.altair_chart(chart_fail, use_container_width=True)

# Chart 3 (when run_steps): Which steps fail most
if use_snowflake and df_steps is not None and not df_steps.empty and "step_name" in df_steps.columns and "status" in df_steps.columns:
    step_fail = df_steps[df_steps["status"] == "failure"].groupby("step_name", as_index=False).size()
    step_fail.columns = ["step_name", "failures"]
    if not step_fail.empty:
        step_fail = step_fail.sort_values("failures", ascending=False).head(8)
        chart_step_fail = alt.Chart(step_fail).mark_bar(color=COLORS["health"][2]).encode(
            x=alt.X("failures:Q", title="Failures"),
            y=alt.Y("step_name:N", sort="-x", title="Step"),
            tooltip=["step_name", "failures"],
        ).properties(height=220, title="Steps that fail most")
        st.altair_chart(chart_step_fail, use_container_width=True)

if "error_message" in df_runs.columns and df_runs["error_message"].notna().any():
    with st.expander("Recent errors (Agent Health)"):
        cols = [c for c in ["company_name", "run_id", "error_message", "started_at"] if c in df_runs.columns]
        st.dataframe(df_runs[df_runs["error_message"].notna()][cols].head(10), use_container_width=True, hide_index=True)

# ----- 2. Agent Performance: top 5 companies searched, industries -----
st.header("⚡ Agent Performance")
st.caption("Top companies searched (count), and runs by industry")

# Chart 1: Top 5 companies that were searched and how many times each
if "company_name" in df_runs.columns:
    count_col = "run_id" if "run_id" in df_runs.columns else "company_name"
    top5 = df_runs.groupby("company_name", as_index=False).agg(runs=(count_col, "count")).sort_values("runs", ascending=False).head(5)
    chart_top5 = alt.Chart(top5).mark_bar(color=COLORS["perf"][0]).encode(
        x=alt.X("runs:Q", title="Number of searches"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        tooltip=["company_name", "runs"],
    ).properties(height=240, title="Top 5 companies searched (how many times each)")
    st.altair_chart(chart_top5, use_container_width=True)

# Chart 2: Pie chart with industries
if "industry" in df_runs.columns and df_runs["industry"].notna().any():
    by_ind = df_runs[df_runs["industry"].notna()].groupby("industry", as_index=False).size()
    by_ind.columns = ["industry", "runs"]
    if not by_ind.empty:
        chart_ind = alt.Chart(by_ind).mark_arc(innerRadius=40).encode(
            theta=alt.Theta("runs:Q"),
            color=alt.Color("industry:N", scale=alt.Scale(range=CHART_COLORS), legend=alt.Legend(title="Industry")),
            tooltip=["industry", "runs"],
        ).properties(height=260, title="Runs by industry")
        st.altair_chart(chart_ind, use_container_width=True)

# ----- 3. Usage & Demand: runs over time, top companies -----
st.header("📈 Usage & Demand")
st.caption("Runs over time, top companies researched")

# Chart 1: Runs over time
if "started_at" in df_runs.columns:
    df_runs_copy = df_runs.copy()
    df_runs_copy["hour"] = pd.to_datetime(df_runs_copy["started_at"]).dt.floor("h")
    timeline = df_runs_copy.groupby("hour", as_index=False).size()
    chart_timeline = alt.Chart(timeline).mark_area(line=True, point=True, color=COLORS["usage"][0], opacity=0.7).encode(
        x=alt.X("hour:T", title="Time (UTC)"),
        y=alt.Y("size:Q", title="Runs"),
        tooltip=["hour:T", "size:Q"],
    ).properties(height=240, title="Runs over time")
    st.altair_chart(chart_timeline, use_container_width=True)

# Chart 2: Top companies researched
if "company_name" in df_runs.columns:
    count_col = "run_id" if "run_id" in df_runs.columns else "company_name"
    top_co = df_runs.groupby("company_name", as_index=False).agg(runs=(count_col, "count")).sort_values("runs", ascending=False).head(10)
    chart_top = alt.Chart(top_co).mark_bar(color=COLORS["usage"][1]).encode(
        x=alt.X("runs:Q", title="Runs"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        tooltip=["company_name", "runs"],
    ).properties(height=240, title="Top companies researched")
    st.altair_chart(chart_top, use_container_width=True)

# ----- 4. Cost Efficiency: API calls per run, expensive/duplicate queries -----
st.header("💰 Cost Efficiency")
st.caption("API calls per run, expensive or duplicate queries (api_calls + agent_runs.total_api_calls)")

api_col = "total_api_calls"
total_calls = int(df_runs[api_col].sum()) if api_col in df_runs.columns else 0
avg_per_run = df_runs[api_col].mean() if api_col in df_runs.columns else 0

c1, c2 = st.columns(2)
c1.metric("Total API calls (all runs)", total_calls)
c2.metric("Avg API calls per run", f"{avg_per_run:.1f}")

# Chart 1: Most frequent queries (duplicate detection) — integer counts, regular bar chart
if use_snowflake and df_calls is not None and not df_calls.empty and "query_used" in df_calls.columns:
    top_q = df_calls["query_used"].value_counts().head(12).reset_index()
    top_q.columns = ["query", "calls"]
    top_q["calls"] = top_q["calls"].astype(int)
    top_q["query_short"] = top_q["query"].apply(lambda x: (str(x)[:50] + "…") if len(str(x)) > 50 else str(x))
    chart_q = alt.Chart(top_q).mark_bar(color=COLORS["cost"][0]).encode(
        x=alt.X("calls:Q", title="API calls", scale=alt.Scale(nice=False), axis=alt.Axis(format="d", tickMinStep=1)),
        y=alt.Y("query_short:N", sort="-x", title="Query"),
        tooltip=[alt.Tooltip("query:N", title="Query"), alt.Tooltip("calls:Q", format="d", title="Calls")],
    ).properties(height=280, title="Most frequent queries (duplicate detection)")
    st.altair_chart(chart_q, use_container_width=True)
# Fallback: API usage by company from runs
elif "company_name" in df_runs.columns and api_col in df_runs.columns:
    usage_co = df_runs.groupby("company_name", as_index=False)[api_col].sum().sort_values(api_col, ascending=False).head(10)
    usage_co[api_col] = usage_co[api_col].astype(int)
    chart_usage = alt.Chart(usage_co).mark_bar(color=COLORS["cost"][1]).encode(
        x=alt.X("total_api_calls:Q", title="Total API calls", axis=alt.Axis(format="d", tickMinStep=1)),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        tooltip=["company_name", alt.Tooltip("total_api_calls:Q", format="d")],
    ).properties(height=260, title="API calls by company")
    st.altair_chart(chart_usage, use_container_width=True)

# Chart 2: Average cost (latency) per company — how expensive each company was
lat_col = "total_latency_ms"
if "company_name" in df_runs.columns and lat_col in df_runs.columns:
    expensive_co = df_runs.groupby("company_name", as_index=False)[lat_col].mean().sort_values(lat_col, ascending=False).head(10)
    chart_exp = alt.Chart(expensive_co).mark_bar(color=COLORS["cost"][2]).encode(
        x=alt.X("total_latency_ms:Q", title="Avg latency (ms)"),
        y=alt.Y("company_name:N", sort="-x", title="Company"),
        tooltip=["company_name", "total_latency_ms"],
    ).properties(height=260, title="Avg cost (latency) per company")
    st.altair_chart(chart_exp, use_container_width=True)

# ----- Raw data: one window, choose which table to see -----
st.header("📋 Raw data")
table_options = ["Runs"]
if use_snowflake and df_steps is not None and not df_steps.empty:
    table_options.append("Run steps")
if use_snowflake and df_calls is not None and not df_calls.empty:
    table_options.append("API calls")

selected_table = st.selectbox(
    "Choose table to view",
    options=table_options,
    key="raw_data_table",
)

if selected_table == "Runs":
    st.dataframe(df_runs, use_container_width=True, hide_index=True)
elif selected_table == "Run steps":
    st.dataframe(df_steps, use_container_width=True, hide_index=True)
else:
    st.dataframe(df_calls, use_container_width=True, hide_index=True)
