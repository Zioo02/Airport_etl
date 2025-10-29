FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install pandas psycopg2-binary
CMD ["python", "aggregator.py"]
