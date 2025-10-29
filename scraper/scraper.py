import os
import pandas as pd
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    ua = os.getenv("CHROME_USER_AGENT")
    if ua:
        opts.add_argument(f"user-agent={ua}")
    chromedriver_path = os.getenv("CHROME_DRIVER_PATH")
    service = Service(chromedriver_path) if chromedriver_path else Service()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver


def fetch_chopin_departures_selenium():
    url = "https://www.lotnisko-chopina.pl/pl/odloty.html"
    driver = _make_driver()
    try:
        driver.get(url)
        driver.implicitly_wait(5)
        prev = -1
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.flightboard.departures tr")
            if len(rows) == prev:
                break
            prev = len(rows)
            btns = driver.find_elements(By.CSS_SELECTOR, "button.btn_big.departures_more, button.departures_more")
            if not btns:
                break
            btn = btns[0]
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                driver.execute_script("arguments[0].click();", btn)
            except WebDriverException:
                break
        return driver.page_source
    finally:
        driver.quit()


def parse_departures(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table.flightboard.departures tr")
    tz = ZoneInfo("Europe/Warsaw")
    today = datetime.now(tz).strftime("%Y%m%d")
    out = []
    for r in rows:
        data_timesch = r.get("data-timesch") or r.get("data_timesch")
        if not data_timesch or len(data_timesch) < 8:
            continue
        if data_timesch[:8] != today:
            continue
        cells = r.find_all("td")
        if len(cells) < 3:
            continue
        dest = cells[1].get_text(strip=True)
        flight_no = cells[2].get_text(strip=True)
        airline = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        out.append({
            "airport": "chopin",
            "flight_number": flight_no,
            "destination": dest,
            "airline": airline,
            "scheduled_time": data_timesch,
            "data_timesch": data_timesch,
        })
    df = pd.DataFrame(out)
    logger.info("Parsed %d flights", len(df))
    return df


def _ensure_table(conn):
    # create table with composite primary key to prevent duplicates at DB level
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS flights_raw (
      airport TEXT NOT NULL,
      flight_number TEXT NOT NULL,
      destination TEXT,
      airline TEXT,
      scheduled_time TIMESTAMPTZ,
      data_timesch TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (airport, flight_number, data_timesch)
    );
    """))


def save_to_postgres(df: pd.DataFrame):
    if df is None or df.empty:
        logger.info("No data to save.")
        return

    for c in ["airport", "flight_number", "destination", "airline", "data_timesch"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().replace({"nan": None, "None": None})

    if "data_timesch" in df.columns:
        df = df[df["data_timesch"].notnull()]

    if df.empty:
        logger.info("Nothing to insert after normalization.")
        return

    db_user = os.getenv("DB_USER") or os.getenv("POSTGRES_USER") or "etl_user"
    db_pass = os.getenv("DB_PASS") or os.getenv("POSTGRES_PASSWORD") or "etl_pass"
    db_host = os.getenv("DB_HOST") or "postgres"
    db_port = os.getenv("DB_PORT") or os.getenv("POSTGRES_PORT") or "5432"
    db_name = os.getenv("DB_NAME") or os.getenv("POSTGRES_DB") or "etl_db"

    conn_str = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(conn_str)
    with engine.begin() as conn:
        _ensure_table(conn)
        insert = text("""
            INSERT INTO flights_raw (airport, flight_number, destination, airline, scheduled_time, data_timesch)
            VALUES (:airport, :flight_number, :destination, :airline, :scheduled_time::timestamptz, :data_timesch)
            ON CONFLICT (airport, flight_number, data_timesch) DO NOTHING
        """)
        for r in df.to_dict(orient="records"):
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None
            conn.execute(insert, **r)
    logger.info("Insert attempted for %d records", len(df))


if __name__ == "__main__":
    html = fetch_chopin_departures_selenium()
    df = parse_departures(html)
    save_to_postgres(df)
