# Image for the Kafka producer and the Cassandra query tool.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
