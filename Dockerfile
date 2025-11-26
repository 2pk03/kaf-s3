FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install build deps for confluent-kafka and curl for healthcheck
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc librdkafka-dev curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Install as root, then drop privileges
RUN pip install --no-cache-dir . \
    && groupadd -r app && useradd -r -g app app \
    && chown -R app:app /app

USER app

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD curl -sf http://localhost:8000/metrics || exit 1

ENTRYPOINT ["python", "-m", "s3_connector.runtime"]
