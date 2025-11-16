import pandas as pd
from sqlalchemy import create_engine
import os
import time

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    DATABASE_URL = "postgresql://etl_user:elt_pass@postgres:5432/etl_db"

engine = create_engine(DATABASE_URL)
RAW_TABLE_NAME = "flights_raw"

def run_analysis():
    print("ANALYZER: Uruchamiam cykl analizy...")
    try:
        df = pd.read_sql(f"SELECT * FROM {RAW_TABLE_NAME}", engine)

        if df.empty:
            print("ANALYZER: Tabela źródłowa jest pusta, czekam...")
            return

        top_destinations = df['destination'].value_counts().reset_index().head(10)
        top_destinations.columns = ['destination', 'count']

        busiest_airlines = df['airline'].value_counts().reset_index().head(10)
        busiest_airlines.columns = ['airline', 'count']

        df['scheduled_time'] = pd.to_datetime(df['scheduled_time'])
        hourly_traffic = df['scheduled_time'].dt.hour.value_counts().sort_index().reset_index()
        hourly_traffic.columns = ['hour', 'flights_count']
        
        top_destinations.to_sql('stats_top_destinations', engine, if_exists='replace', index=False)
        busiest_airlines.to_sql('stats_busiest_airlines', engine, if_exists='replace', index=False)
        hourly_traffic.to_sql('stats_hourly_traffic', engine, if_exists='replace', index=False)

        print("ANALYZER: Statystyki pomyślnie zaktualizowane.")

    except Exception as e:
        print(f"ANALYZER: Wystąpił błąd: {e}")

if __name__ == "__main__":
    while True:
        run_analysis()
        time.sleep(300)