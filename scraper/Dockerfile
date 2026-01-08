FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY scraper.py .

# Dodano lxml dla lepszej obsługi BeautifulSoup
RUN pip install --no-cache-dir \
    selenium \
    psycopg2-binary \
    sqlalchemy \
    pandas \
    beautifulsoup4 \
    lxml

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROME_DRIVER_PATH=/usr/bin/chromedriver

CMD ["python", "scraper.py"]