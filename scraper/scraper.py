import os
import logging
from zoneinfo import ZoneInfo
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    WebDriverException,
)
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _make_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # reduce headless detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    user_agent = os.getenv(
        "CHROME_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    )
    options.add_argument(f"user-agent={user_agent}")

    chromedriver_path = os.getenv("CHROME_DRIVER_PATH")
    service = Service(chromedriver_path) if chromedriver_path else Service()

    driver = webdriver.Chrome(service=service, options=options)
    # optional: set small timeouts so individual waits control behavior
    driver.set_page_load_timeout(60)
    driver.set_script_timeout(30)
    return driver


def fetch_chopin_departures_selenium():
    """Pobiera pełny HTML z tabelą odlotów z lotniska Chopina (klika 'więcej' aż do końca).
    Zwraca HTML string. W razie problemów zapisuje debug_page.html z aktualnym HTML.
    """
    url = "https://www.lotnisko-chopina.pl/pl/odloty.html"
    driver = _make_driver()
    try:
        logger.info("Loading %s", url)
        driver.get(url)

        # wait for page ready
        try:
            WebDriverWait(driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")
        except TimeoutException:
            logger.warning("document.readyState != complete after timeout, continuing to element waits")

        # remove common overlays/cookie banners that can block clicks
        try:
            driver.execute_script(
                """
                document.querySelectorAll('.cookie, .cookie-consent, .consent, .overlay, .modal, .cc-window').forEach(e=>e.remove());
                document.querySelectorAll('[aria-hidden="true"]').forEach(e=>{});
                """
            )
        except WebDriverException:
            pass

        wait = WebDriverWait(driver, 30)
        # wait for either the table or the 'more' button to appear
        try:
            wait.until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "table.flightboard.departures") or d.find_elements(By.CSS_SELECTOR, ".departures_more")
            )
        except TimeoutException as e:
            # save HTML for debugging and re-raise
            html = driver.page_source
            debug_path = os.path.join(os.path.dirname(__file__), "debug_page.html")
            try:
                with open(debug_path, "w", encoding="utf-8") as fh:
                    fh.write(html)
                logger.error("Timeout waiting for table. Page saved to %s", debug_path)
            except Exception:
                logger.exception("Failed to write debug_page.html")
            raise

        # robustly click "więcej" until no new rows load
        last_count = -1
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.flightboard.departures tr")
            count = len(rows)
            if count == last_count:
                break
            last_count = count

            try:
                more_btns = driver.find_elements(By.CSS_SELECTOR, ".departures_more, .flightboard-more, .more")
                if not more_btns:
                    break
                more_btn = more_btns[0]
            except Exception:
                break

            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", more_btn)
                more_btn.click()
                # wait briefly for new rows
                try:
                    WebDriverWait(driver, 8).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.flightboard.departures tr")) > count
                    )
                except TimeoutException:
                    # no additional rows loaded -> exit loop
                    break
            except (ElementClickInterceptedException, WebDriverException):
                # try to remove overlays and retry once
                try:
                    driver.execute_script(
                        "document.querySelectorAll('.cookie, .overlay, .modal, .consent').forEach(e=>e.remove())"
                    )
                    more_btn.click()
                except Exception:
                    break

        return driver.page_source
    finally:
        driver.quit()


def parse_departures(html: str) -> pd.DataFrame:
    """Parsuje HTML i zwraca rekordy tylko z dzisiejszej daty (strefa Europe/Warsaw)."""
    soup = BeautifulSoup(html, "html.parser")
    # some rows may not have class 'tooltip', fallback to any tr inside departures table
    rows = soup.select("table.flightboard.departures tr.tooltip") or soup.select("table.flightboard.departures tr")

    flights = []
    tz = ZoneInfo("Europe/Warsaw")
    today_str = datetime.now(tz).strftime("%Y%m%d")

    for row in rows:
        data_timesch = row.get("data-timesch")
        if not data_timesch or len(data_timesch) < 8:
            continue

        date_part = data_timesch[:8]
        if date_part != today_str:
            continue

        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        dest = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        flight_no = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        airline = cells[4].get_text(strip=True) if len(cells) > 4 else (cells[-1].get_text(strip=True) if cells else "")

        try:
            scheduled_naive = datetime.strptime(data_timesch, "%Y%m%d%H%M%S")
            scheduled_local = scheduled_naive.replace(tzinfo=tz)
            scheduled_iso = scheduled_local.isoformat()
        except Exception:
            scheduled_iso = None

        flights.append({
            "airport": "chopin",
            "flight_number": flight_no,
            "destination": dest,
            "airline": airline,
            "scheduled_time": scheduled_iso,
            "data_timesch": data_timesch,
        })

    logger.info("Found %d flights for date %s", len(flights), today_str)
    return pd.DataFrame(flights)


def save_to_postgres(df: pd.DataFrame):
    """Zapisuje DataFrame do Postgresa (flights_raw)."""
    if df is None or df.empty:
        logger.warning("No data to save.")
        return

    db_user = os.getenv("DB_USER", "etl_user")
    db_pass = os.getenv("DB_PASS", "etl_pass")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "etl_db")

    conn_str = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:55432/{db_name}"
    engine = create_engine(conn_str)

    try:
        with engine.begin() as conn:
            df.to_sql("flights_raw", conn, if_exists="append", index=False)
        logger.info("Saved %d records to flights_raw", len(df))
    except Exception:
        logger.exception("Error saving to DB")


if __name__ == "__main__":
    logger.info("Fetching departures from Chopin airport...")
    try:
        html = fetch_chopin_departures_selenium()
    except TimeoutException:
        logger.error("Timeout while loading page. Check debug_page.html in scraper folder for the captured HTML.")
        raise
    df = parse_departures(html)
    if not df.empty:
        save_to_postgres(df)
    else:
        logger.warning("No flights with today's date — nothing saved.")
