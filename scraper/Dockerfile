FROM python:3.10-slim

# Zainstaluj narzędzia systemowe i Chromium
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY scraper.py /app/scraper.py

RUN pip install --no-cache-dir selenium psycopg2-binary sqlalchemy pandas beautifulsoup4

ENV DB_HOST=postgres
ENV DB_NAME=etl_db
ENV DB_USER=etl_user
ENV DB_PASS=etl_pass

CMD ["python", "scraper.py"]
