import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# -------------------- CONFIG --------------------
st.set_page_config(
    page_title="Airport ETL Dashboard",
    layout="wide"
)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
    st.error("Database environment variables are not fully configured")
    st.stop()

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=2,
    pool_pre_ping=True
)

CACHE_TTL = 60 * 60 * 24  # 24 hours

# -------------------- DATA ACCESS --------------------
@st.cache_data(ttl=CACHE_TTL)
def load_global_metrics():
    query = """
        SELECT
            COUNT(*)                       AS total_flights,
            COUNT(DISTINCT destination)    AS distinct_destinations,
            COUNT(DISTINCT airline)        AS distinct_airlines,
            MIN(scheduled_time)            AS first_flight,
            MAX(scheduled_time)            AS last_flight
        FROM flights_raw;
    """
    return pd.read_sql(query, engine).iloc[0]


@st.cache_data(ttl=CACHE_TTL)
def load_stats_tables():
    df_dest = pd.read_sql(
        "SELECT destination, count FROM stats_top_destinations ORDER BY count DESC",
        engine
    )
    df_airline = pd.read_sql(
        "SELECT airline, count FROM stats_busiest_airlines ORDER BY count DESC",
        engine
    )
    df_hourly = pd.read_sql(
        "SELECT hour, flights_count FROM stats_hourly_traffic ORDER BY hour",
        engine
    )
    return df_dest, df_airline, df_hourly


@st.cache_data(ttl=CACHE_TTL)
def load_raw_data(limit: int):
    query = """
        SELECT
            airport,
            flight_number,
            destination,
            airline,
            scheduled_time,
            created_at
        FROM flights_raw
        ORDER BY scheduled_time DESC
        LIMIT %(limit)s;
    """
    return pd.read_sql(query, engine, params={"limit": limit})


# -------------------- UI --------------------
st.title("Airport ETL Dashboard")

# ---- GLOBAL METRICS ----
metrics = load_global_metrics()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total flights", int(metrics.total_flights))
col2.metric("Distinct destinations", int(metrics.distinct_destinations))
col3.metric("Distinct airlines", int(metrics.distinct_airlines))
col4.metric(
    "Data range",
    f"{metrics.first_flight:%Y-%m-%d} → {metrics.last_flight:%Y-%m-%d}"
)

st.divider()

# ---- PRECOMPUTED STATS ----
df_dest, df_airline, df_hourly = load_stats_tables()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top destinations (precomputed)")
    st.bar_chart(
        df_dest.set_index("destination"),
        use_container_width=True
    )

with col2:
    st.subheader("Top airlines (precomputed)")
    st.bar_chart(
        df_airline.set_index("airline"),
        use_container_width=True
    )

st.subheader("Hourly traffic distribution")
st.line_chart(
    df_hourly.set_index("hour"),
    use_container_width=True
)

st.divider()

# ---- RAW DATA ----
st.subheader("Raw data – flights_raw")

limit = st.selectbox(
    "Rows to display",
    [100, 500, 1000, 5000],
    index=2
)

df_raw = load_raw_data(limit)

st.dataframe(
    df_raw,
    use_container_width=True,
    height=450
)

# ---- MANUAL CACHE INVALIDATION ----
if st.button("Invalidate cache and reload"):
    st.cache_data.clear()
    st.experimental_rerun()
