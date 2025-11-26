# Kafka S3 Connector (`kaf-s3-connector`)

A Python library to seamlessly handle large Kafka messages by offloading them to Amazon S3.

This library provides a custom Kafka Producer and Consumer that automatically handle the process of storing large message payloads in an S3 bucket and passing lightweight references through Kafka.

## Key Features

-   **Automatic S3 Offloading:** Produce messages larger than Kafka's recommended limit without manual intervention.
-   **Transparent Consumption:** Consume large messages as if they were directly in Kafka.
-   **Data Integrity:** Verifies the integrity of S3 objects using ETags to prevent data corruption.
-   **Secure by Default:** Leverages AWS IAM roles and the default `boto3` credential chain, avoiding the need to hardcode secrets.
-   **Flexible Configuration:** Built on top of `confluent-kafka-python`, allowing for full customization of Kafka client settings, including SASL and SSL.

## Installation

```bash
pip install .
```

## How it Works

1.  The `S3Producer` receives a large payload.
2.  It uploads the payload to a specified S3 bucket with a unique key.
3.  It produces a small JSON message to a Kafka topic containing the S3 bucket, key, and the object's ETag.
4.  The `S3Consumer` reads the JSON reference from Kafka.
5.  It downloads the original payload from S3.
6.  It verifies the ETag to ensure the data is not corrupted, then returns the payload to your application.

## Configuration

Configuration is handled through a single dictionary, with separate keys for `kafka` and `s3` settings.

### Security

-   **AWS Credentials:** This library is designed to be secure. **Do not** pass AWS credentials in the configuration. It uses `boto3`'s default credential discovery chain. The recommended and most secure way to provide credentials is by using an **IAM Role** attached to your compute instance (EC2, ECS, Lambda, etc.). Alternatively, you can use environment variables or a shared credentials file (`~/.aws/credentials`).
-   **Kafka Security:** All standard `confluent-kafka-python` security settings are supported and should be passed within the `kafka` dictionary. This includes SASL for authentication (e.g., `PLAIN`, `SCRAM`, and `GSSAPI` for **Kerberos**) and SSL/TLS for encryption.

### Example Configuration

```python
producer_config = {
    "kafka": {
        "bootstrap.servers": "localhost:9092",
        # Optional DLQ for delivery failures
        # "dlq_topic": "my-s3-dlq",
    },
    "s3": {
        "bucket": "my-large-messages-bucket",
        # Optional toggles
        # "max_inline_bytes": 900_000,     # inline small payloads on Kafka, offload larger ones
        # "max_payload_bytes": 5 * 1024 * 1024 * 1024,  # hard cap on payload size
        # "prefix": "kafka/topic",         # prefix keys for organization/enforcement
        # "deterministic_keys": False,     # use payload hash for idempotent keys
        # "compression": "gzip",           # compress before S3 upload (gzip or None)
        # "ttl_seconds": 86400,            # hint TTL stored in object metadata
        # "server_side_encryption": "aws:kms", # SSE, optionally with KMS key below
        # "sse_kms_key_id": "<kms-key-id>",
    }
}

consumer_config = {
    "kafka": {
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-s3-consumers",
        "auto.offset.reset": "earliest",
        # Optional: send failures to a DLQ topic
        # "dlq_topic": "my-s3-dlq",
    },
    "s3": {
        "bucket": "my-large-messages-bucket",
        # Optional toggles
        # "max_inline_bytes": 900_000,     # inline small payloads on Kafka, offload larger ones
        # "max_payload_bytes": 5 * 1024 * 1024 * 1024,  # hard cap on payload size
        # "delete_after_consume": False,   # delete object only after a successful integrity check
        # "allow_inline_payloads": True,   # allow non-reference payloads to pass through unchanged
        # "prefix": "kafka/topic",         # enforce prefix on incoming references
        # "compression": "gzip",           # decompress automatically on consume
        # "deterministic_keys": False,     # use payload hash for idempotent keys (producer)
        # "ttl_seconds": 86400,            # hint TTL stored in object metadata (producer)
        # "server_side_encryption": "aws:kms", # SSE, optionally with KMS key below (producer)
        # "sse_kms_key_id": "<kms-key-id>",    # (producer)
    }
}
```

## Usage

### Producer

```python
from s3_connector import S3Producer

# Initialize producer with the config
producer = S3Producer(config=producer_config)

# Read a sample file
with open("examples/sample_payload.txt", "rb") as f:
    payload_data = f.read()

# Produce the data to a topic
producer.produce(topic="large-messages-topic", payload=payload_data)
print("Produced message to Kafka via S3.")
```

### Consumer

```python
from s3_connector import S3Consumer

# Initialize consumer with the config
consumer = S3Consumer(config=consumer_config)
consumer.subscribe(["large-messages-topic"])

print("Waiting for messages...")
while True:
    try:
        # Poll for a message
        payload = consumer.poll(timeout=10.0)

        if payload:
            print(f"Received message of size: {len(payload)} bytes")
            # Save the received file
            with open("examples/received_payload.txt", "wb") as f:
                f.write(payload)
            break # Exit after one message for this example

    except KeyboardInterrupt:
        break

consumer.close()

## Testing

Install runtime deps and test tools:

```bash
pip install -r requirements.txt pytest pytest-mock
```

Then run `pytest` from the project root:

```bash
pytest
```

## Docker & Operations

Build locally:

```bash
docker build -t kaf-s3-connector:local .
```

Run as consumer (replace envs as needed):

```bash
docker run --rm -p 8000:8000 \
  -e MODE=consumer \
  -e TOPIC=large-messages-topic \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e KAFKA_GROUP_ID=my-group \
  -e S3_BUCKET=my-large-messages-bucket \
  kaf-s3-connector:local
```

Run as producer reading stdin:

```bash
echo "hello" | docker run --rm -i \
  -e MODE=producer \
  -e TOPIC=large-messages-topic \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e S3_BUCKET=my-large-messages-bucket \
  kaf-s3-connector:local
```

Configuration is driven by env vars:
- Kafka: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID` (consumer), `DLQ_TOPIC`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_SSL_*`, etc.
- S3: `S3_BUCKET`, `S3_PREFIX`, `S3_DELETE_AFTER_CONSUME`, `S3_ALLOW_INLINE_PAYLOADS`, `S3_MAX_INLINE_BYTES`, `S3_MAX_PAYLOAD_BYTES`, `S3_DETERMINISTIC_KEYS`, `S3_COMPRESSION` (`gzip`), `S3_TTL_SECONDS`, `S3_SSE`, `S3_SSE_KMS_KEY_ID`.
- App: `MODE` (`producer`|`consumer`), `TOPIC`, `POLL_TIMEOUT`, `METRICS_PORT` (default 8000).

### Metrics
- `/metrics` exposes Prometheus text format on `METRICS_PORT` (default 8000).
- Sample Prometheus scrape config: `config/prometheus.yml`
- Sample Grafana dashboard JSON: `config/grafana-dashboard.json`

### Helm
- Chart: `charts/kaf-s3-connector`
- Override values (examples):

```bash
helm upgrade --install kaf-s3 charts/kaf-s3-connector \
  --set image.repository=ghcr.io/2pk03/kaf-s3-connector \
  --set image.tag=latest \
  --set env.MODE=consumer \
  --set env.TOPIC=large-messages-topic \
  --set env.KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  --set env.KAFKA_GROUP_ID=my-group \
  --set env.S3_BUCKET=my-large-messages-bucket
```
