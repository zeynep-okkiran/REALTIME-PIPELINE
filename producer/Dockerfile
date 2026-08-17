FROM python:3.12-slim

WORKDIR /app

# Requirements first so the dependency layer is cached across code edits
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Log lines have to reach "docker compose logs" as they happen, not when the
# stdio buffer happens to flush.
ENV PYTHONUNBUFFERED=1

# Run unprivileged. Nothing is written to disk here - the producer only talks
# to Binance and Kafka - but there is no reason to stay root either.
RUN useradd --create-home --uid 1000 producer
USER producer

CMD ["python", "producer.py"]
