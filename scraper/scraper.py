import os
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import psycopg2
from sqlalchemy import create_engine


def fetch_chopin_departures_selenium():
    url = "https://www.lotnisko-chopina.pl/pl/odloty.html"
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(), options=options)
    driver.get(url)

    last_row_count = 0
    while True:
        rows = driver.find_elements(By.CSS_SELECTOR, "table.flightboard.departures tr")
        row_count = len(rows)
        if any("Lista wpisów jest pusta" in row.text for row in rows):
            break
        if row_count == last_row_count:
            break
        last_row_count = row_count
        try:
            more_btn = driver.find_element(By.CSS_SELECTOR, ".departures_more")
            more_btn.click()
            time.sleep(1)
        except Exception:
            break

    html = driver.page_source
    driver.quit()
    return html


def parse_departures(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="flightboard departures")
    if not table:
        raise ValueError("Nie znaleziono tabeli flightboard departures na stronie")

    rows = table.find_all("tr")[1:]
    flights = []
    now = datetime.utcnow().isoformat()

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        time_col = cols[0].get_text(strip=True)
        flight_no = cols[2].get_text(strip=True)
        destination = cols[1].get_text(strip=True)
        airline = cols[4].get_text(strip=True)

        flights.append({
            "airport": "chopin",
            "scheduled_time": time_col,
            "flight_number": flight_no,
            "destination": destination,
            "airline": airline,
            "ingest_ts": now
        })

    return pd.DataFrame(flights)


def save_to_postgres(df: pd.DataFrame):
    db_user = os.getenv("DB_USER", "etl_user")
    db_pass = os.getenv("DB_PASS", "etl_pass")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "etl_db")

    conn_str = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    engine = create_engine(conn_str)

    with engine.begin() as conn:
        df.to_sql("flights_raw", conn, if_exists="append", index=False)
    print(f"✅ Zapisano {len(df)} rekordów do tabeli flights_raw")


if __name__ == "__main__":
    print("📡 Pobieram dane z lotniska Chopina...")
    html = fetch_chopin_departures_selenium()
    df = parse_departures(html)
    save_to_postgres(df)
