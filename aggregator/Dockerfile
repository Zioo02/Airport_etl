FROM python:3.10-slim
WORKDIR /app
RUN pip install streamlit pandas sqlalchemy psycopg2-binary
COPY aggregator.py .
EXPOSE 8501
CMD ["streamlit", "run", "aggregator.py", "--server.port=8501", "--server.address=0.0.0.0"]