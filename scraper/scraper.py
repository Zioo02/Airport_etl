import os
import time
import random
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
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _make_driver():
    """Create a Chrome webdriver with basic anti-bot / stealth tweaks."""
    opts = Options()
    headless = os.getenv("CHROME_HEADLESS", "true").lower() != "false"
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    ua = os.getenv("CHROME_USER_AGENT")
    if not ua:
        ua = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ])
    opts.add_argument(f"user-agent={ua}")

    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    chromedriver_path = os.getenv("CHROME_DRIVER_PATH")
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    service = Service(chromedriver_path) if chromedriver_path else Service()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
window.chrome = window.chrome || { runtime: {} };
"""}
        )
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": ua})
    except Exception:
        pass

    return driver


def fetch_chopin_departures_selenium():
    """Load the page and click 'Załaduj więcej' with small random delays to look more human."""
    url = "https://www.lotnisko-chopina.pl/pl/odloty.html"
    driver = _make_driver()
    try:
        driver.get(url)

        wait = WebDriverWait(driver, 25)
        try:
            wait.until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "table.flightboard.departures") or
                          d.find_elements(By.CSS_SELECTOR, "button.btn_big.departures_more, button.departures_more")
            )
        except TimeoutException:
            try:
                path = os.path.join(os.path.dirname(__file__), "debug_page.html")
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(driver.page_source)
            except Exception:
                pass
            return driver.page_source

        prev = -1
        no_change_rounds = 0
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.flightboard.departures tr")
            if len(rows) == prev:
                no_change_rounds += 1
            else:
                no_change_rounds = 0
            prev = len(rows)
            if no_change_rounds >= 2:
                break

            btns = driver.find_elements(By.CSS_SELECTOR, "button.btn_big.departures_more, button.departures_more")
            if not btns:
                break
            btn = btns[0]

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(random.uniform(0.3, 0.9))
                driver.execute_script("arguments[0].click();", btn)
                try:
                    WebDriverWait(driver, 8).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.flightboard.departures tr")) > prev
                    )
                except TimeoutException:
                    time.sleep(random.uniform(0.5, 1.2))
            except WebDriverException:
                break
        time.sleep(0.5)
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

    for c in ["airport", "flight_number", "destination", "airline", "data_timesch", "scheduled_time"]:
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
            VALUES (:airport, :flight_number, :destination, :airline, :scheduled_time, :data_timesch)
            ON CONFLICT (airport, flight_number, data_timesch) DO NOTHING
        """)
        for r in df.to_dict(orient="records"):
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None
            st = r.get("scheduled_time")
            if st:
                try:
                    parsed = datetime.strptime(st, "%Y%m%d%H%M%S")
                    r["scheduled_time"] = parsed.replace(tzinfo=ZoneInfo("Europe/Warsaw"))
                except Exception:
                    r["scheduled_time"] = None

            conn.execute(insert, r)
    logger.info("Insert attempted for %d records", len(df))


if __name__ == "__main__":
    time.sleep(20)
    html = fetch_chopin_departures_selenium()
    df = parse_departures(html)
    save_to_postgres(df)
