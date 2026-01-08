import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_TABLE_NAME = "flights_raw"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def get_db_engine(max_retries=5):
    """Create database engine with retry logic."""
    for attempt in range(max_retries):
        try:
            engine = create_engine(DATABASE_URL)
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established")
            return engine
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to connect to database after {max_retries} attempts")
                raise
            wait_time = 2 ** attempt
            logger.warning(f"Connection failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s... Error: {e}")
            time.sleep(wait_time)


def run_analysis():
    logger.info("ANALYZER: Start analizy")

    engine = get_db_engine()
    
    try:
        df = pd.read_sql(f"SELECT * FROM {RAW_TABLE_NAME}", engine)
    except Exception as e:
        logger.error(f"ANALYZER: Błąd odczytu tabeli {RAW_TABLE_NAME}: {e}")
        return

    if df.empty:
        logger.warning("ANALYZER: Brak danych – kończę")
        return

    # Usuń wiersze z brakującymi danymi w kluczowych kolumnach
    df_clean = df.dropna(subset=['destination', 'airline', 'scheduled_time'])
    
    if df_clean.empty:
        logger.warning("ANALYZER: Brak poprawnych danych do analizy po czyszczeniu")
        return

    logger.info(f"ANALYZER: Przetwarzam {len(df_clean)} wierszy (z {len(df)} oryginalnych)")

    # Top destinations
    top_destinations = df_clean['destination'].value_counts().head(10).reset_index()
    top_destinations.columns = ['destination', 'count']

    # Busiest airlines
    busiest_airlines = df_clean['airline'].value_counts().head(10).reset_index()
    busiest_airlines.columns = ['airline', 'count']

    # Hourly traffic
    df_clean['scheduled_time'] = pd.to_datetime(df_clean['scheduled_time'])
    hourly_traffic = (
        df_clean['scheduled_time']
        .dt.hour
        .value_counts()
        .sort_index()
        .reset_index()
    )
    hourly_traffic.columns = ['hour', 'flights_count']

    # Zapisz wyniki do bazy
    try:
        top_destinations.to_sql('stats_top_destinations', engine, if_exists='replace', index=False)
        logger.info("ANALYZER: Zapisano stats_top_destinations")
        
        busiest_airlines.to_sql('stats_busiest_airlines', engine, if_exists='replace', index=False)
        logger.info("ANALYZER: Zapisano stats_busiest_airlines")
        
        hourly_traffic.to_sql('stats_hourly_traffic', engine, if_exists='replace', index=False)
        logger.info("ANALYZER: Zapisano stats_hourly_traffic")
        
        logger.info("ANALYZER: Analiza zakończona sukcesem")
    except Exception as e:
        logger.error(f"ANALYZER: Błąd zapisu do bazy: {e}")
        raise


if __name__ == "__main__":
    try:
        run_analysis()
    except Exception as e:
        logger.error(f"ANALYZER: Fatal error: {e}", exc_info=True)
        exit(1)