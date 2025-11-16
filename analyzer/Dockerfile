FROM python:3.10-slim
WORKDIR /app
RUN pip install pandas sqlalchemy psycopg2-binary
COPY analyzer.py .
CMD ["python", "analyzer.py"]