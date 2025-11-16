import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

st.set_page_config(page_title="Dashboard Lotniska", layout="wide")
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    DATABASE_URL = "postgresql://etl_user:elt_pass@postgres:5432/etl_db"

@st.cache_data(ttl=60)
def load_data():
    """ Ładuje gotowe statystyki z bazy danych """
    engine = create_engine(DATABASE_URL)
    try:
        df_dest = pd.read_sql("SELECT * FROM stats_top_destinations", engine)
        df_airline = pd.read_sql("SELECT * FROM stats_busiest_airlines", engine)
        df_hourly = pd.read_sql("SELECT * FROM stats_hourly_traffic", engine)
        return df_dest, df_airline, df_hourly
    except Exception as e:
        st.error(f"Błąd ładowania danych: {e}")
        return None, None, None

st.title("Dashboard Lotniska (Projekt ETL)")

df_dest, df_airline, df_hourly = load_data()

if df_dest is not None:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Najpopularniejsze kierunki")
        st.bar_chart(df_dest.set_index('destination'), color="#FF4B4B")

    with col2:
        st.subheader("Najbardziej aktywne linie lotnicze")
        st.bar_chart(df_airline.set_index('airline'), color="#0068C9")

    st.subheader("Ruch na lotnisku w ciągu doby (loty/godzina)")
    st.line_chart(df_hourly.set_index('hour'))
    st.subheader("Podgląd tabel statystycznych")
    st.dataframe(df_dest)
    st.dataframe(df_airline)
    st.dataframe(df_hourly)