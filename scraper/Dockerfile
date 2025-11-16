FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY scraper.py /app/scraper.py

RUN pip install --no-cache-dir selenium psycopg2-binary sqlalchemy pandas beautifulsoup4

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROME_DRIVER_PATH=/usr/bin/chromedriver
ENV DB_HOST=postgres
ENV DB_NAME=etl_db
ENV DB_USER=etl_user
ENV DB_PASS=etl_pass
ENV DB_PORT=5432

CMD ["python", "scraper.py"]