from __future__ import annotations
import os
from datetime import timedelta

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------- Paths to your CSVs ----------
TRANSACTIONS_CSV = r"c:\\Users\\super\\Downloads\\transactions.csv"
AUTH_CODES_CSV = r"c:\\Users\\super\\Downloads\\transactions_auth_codes.csv"
QUERY_SQL_PATH = r"c:\\Users\\super\\Downloads\\tx_dashboard\\query.sql"


def to_duckdb_path(path_str: str) -> str:
    """Convert Windows backslashes to forward slashes for DuckDB file readers."""
    return path_str.replace("\\", "/")


st.set_page_config(page_title="Transactions Live Dashboard", layout="wide")
st.title("Transactions Live Dashboard")

# ---------- Controls ----------
col_left, col_right = st.columns([1, 1])
with col_left:
    refresh_seconds = st.number_input("Auto-refresh interval (seconds)", min_value=2, max_value=300, value=10, step=1)
with col_right:
    window_choice = st.selectbox(
        "Time window",
        options=["15 minutes", "1 hour", "6 hours", "All"],
        index=1,
    )

# Trigger periodic refresh
st_autorefresh(interval=refresh_seconds * 1000, key="auto_refresh")


@st.cache_data(show_spinner=False, ttl=5)
def load_data(transactions_csv: str, auth_codes_csv: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Read both CSVs through DuckDB with a single connection for consistent typing
    con = duckdb.connect(database=":memory:")

    transactions_duck_path = to_duckdb_path(transactions_csv)
    auth_duck_path = to_duckdb_path(auth_codes_csv)

    # Load raw tables
    df_status = con.execute(
        f"""
        SELECT CAST(timestamp AS TIMESTAMP) AS ts,
               status,
               CAST(count AS BIGINT) AS count
        FROM read_csv_auto('{transactions_duck_path}', header TRUE)
        """
    ).df()

    df_auth = con.execute(
        f"""
        SELECT CAST(timestamp AS TIMESTAMP) AS ts,
               auth_code,
               CAST(count AS BIGINT) AS count
        FROM read_csv_auto('{auth_duck_path}', header TRUE)
        """
    ).df()

    # Organized view (same as query.sql) for convenience in the app
    organized_df = con.execute(
        f"""
        WITH t AS (
          SELECT CAST(timestamp AS TIMESTAMP) AS ts, status, CAST(count AS BIGINT) AS count
          FROM read_csv_auto('{transactions_duck_path}', header TRUE)
        ),
        ac AS (
          SELECT CAST(timestamp AS TIMESTAMP) AS ts, auth_code, CAST(count AS BIGINT) AS count
          FROM read_csv_auto('{auth_duck_path}', header TRUE)
        ),
        a00 AS (
          SELECT ts, count AS auth_00_count FROM ac WHERE auth_code = '00'
        )
        SELECT
          t.ts,
          SUM(CASE WHEN t.status = 'approved' THEN t.count ELSE 0 END) AS approved,
          SUM(CASE WHEN t.status = 'denied' THEN t.count ELSE 0 END) AS denied,
          SUM(CASE WHEN t.status = 'reversed' THEN t.count ELSE 0 END) AS reversed,
          SUM(CASE WHEN t.status = 'backend_reversed' THEN t.count ELSE 0 END) AS backend_reversed,
          SUM(CASE WHEN t.status = 'failed' THEN t.count ELSE 0 END) AS failed,
          SUM(CASE WHEN t.status = 'refunded' THEN t.count ELSE 0 END) AS refunded,
          a00.auth_00_count
        FROM t
        LEFT JOIN a00 USING (ts)
        GROUP BY t.ts, a00.auth_00_count
        ORDER BY t.ts
        """
    ).df()

    return df_status, df_auth, organized_df


# Validate file existence
if not os.path.exists(TRANSACTIONS_CSV) or not os.path.exists(AUTH_CODES_CSV):
    st.error("CSV files not found. Please verify the paths in the app header.")
    st.stop()

# Load data
with st.spinner("Loading data..."):
    df_status, df_auth, organized_df = load_data(TRANSACTIONS_CSV, AUTH_CODES_CSV)

if df_status.empty or df_auth.empty:
    st.warning("No data loaded from CSVs.")
    st.stop()

# Apply time window based on max timestamp present
max_ts = max(df_status["ts"].max(), df_auth["ts"].max())
if window_choice == "15 minutes":
    start_ts = max_ts - timedelta(minutes=15)
elif window_choice == "1 hour":
    start_ts = max_ts - timedelta(hours=1)
elif window_choice == "6 hours":
    start_ts = max_ts - timedelta(hours=6)
else:
    start_ts = None

if start_ts is not None:
    mask_status = df_status["ts"] >= start_ts
    mask_auth = df_auth["ts"] >= start_ts
    mask_org = organized_df["ts"] >= start_ts
    df_status = df_status.loc[mask_status]
    df_auth = df_auth.loc[mask_auth]
    organized_df = organized_df.loc[mask_org]

# ---------- KPIs ----------
col1, col2, col3, col4 = st.columns(4)
with col1:
    total_tx = int(df_status["count"].sum())
    st.metric("Total transactions (all statuses)", f"{total_tx:,}")
with col2:
    approved_total = int(df_status.query("status == 'approved'")["count"].sum())
    st.metric("Approved total", f"{approved_total:,}")
with col3:
    a00_total = int(df_auth.query("auth_code == '00'")["count"].sum())
    st.metric("Auth code '00' total", f"{a00_total:,}")
with col4:
    approval_rate = (approved_total / total_tx * 100.0) if total_tx else 0.0
    st.metric("Approval rate (%)", f"{approval_rate:.2f}%")

st.divider()

# ---------- Charts ----------
# 1) Stacked area by status over time
status_pivot = (
    df_status.pivot_table(index="ts", columns="status", values="count", aggfunc="sum")
    .fillna(0)
    .sort_index()
)
status_long = (
    status_pivot.reset_index().melt(id_vars="ts", var_name="status", value_name="count")
)
fig_status = px.area(
    status_long,
    x="ts",
    y="count",
    color="status",
    title="Transactions by status over time",
)
st.plotly_chart(fig_status, use_container_width=True)

# 2) Approved vs auth_code 00 comparison line chart
auth00 = (
    df_auth.query("auth_code == '00'")[["ts", "count"]]
    .rename(columns={"count": "auth_00"})
    .set_index("ts")
)
comparison = status_pivot.get("approved", pd.Series(dtype="float64")).to_frame("approved").join(auth00, how="outer").fillna(0).sort_index()
st.line_chart(comparison, height=300)

# 3) Current auth code distribution (latest minute)
latest_ts = df_auth["ts"].max()
latest_auth_dist = (
    df_auth[df_auth["ts"] == latest_ts][["auth_code", "count"]]
    .groupby("auth_code", as_index=False)["count"].sum()
    .sort_values("count", ascending=False)
)
fig_auth = px.bar(latest_auth_dist, x="auth_code", y="count", title=f"Auth code distribution @ {latest_ts}")
st.plotly_chart(fig_auth, use_container_width=True)

# 4) Organized table view (from SQL)
st.subheader("Organized data (SQL view)")
st.dataframe(organized_df.tail(200), use_container_width=True, height=300)

st.caption(
    "This dashboard reads the CSVs on every refresh and updates the charts. "
    "Adjust the time window or refresh interval from the controls above."
)
