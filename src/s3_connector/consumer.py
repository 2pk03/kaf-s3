import boto3
import gzip
import hashlib
import json
import logging
from confluent_kafka import Consumer, KafkaError, Producer
from .exceptions import DataIntegrityError

logger = logging.getLogger(__name__)

class S3Consumer:
    def __init__(self, config):
        """
        Initializes the S3Consumer.

        :param config: A dictionary with 'kafka' and 's3' configurations.
        """
        kafka_config = config.get("kafka", {})
        self.s3_config = config.get("s3", {})
        self.hooks = config.get("hooks", {})

        if "group.id" not in kafka_config:
            raise ValueError("Kafka consumer 'group.id' must be specified.")
        if "bucket" not in self.s3_config:
            raise ValueError("S3 bucket must be specified in the configuration.")

        self.kafka_consumer = Consumer(kafka_config)
        self.s3_client = boto3.client("s3")
        self.s3_bucket = self.s3_config["bucket"]
        self.delete_after_consume = self.s3_config.get("delete_after_consume", False)
        self.allow_inline_payloads = self.s3_config.get("allow_inline_payloads", True)
        self.expected_prefix = self.s3_config.get("prefix", "").rstrip("/")
        self.metric_callback = self.hooks.get("metrics")
        self.dlq_topic = kafka_config.get("dlq_topic") or self.s3_config.get("dlq_topic")
        self.dlq_producer = Producer(kafka_config) if self.dlq_topic else None

    def close(self):
        """
        Closes the Kafka consumer.
        """
        self.kafka_consumer.close()

    def subscribe(self, topics):
        """
        Subscribes the consumer to a list of topics.
        """
        self.kafka_consumer.subscribe(topics)

    def poll(self, timeout=1.0):
        """
        Polls for a message, downloads the payload from S3, and returns it.

        :param timeout: The maximum time to block waiting for a message.
        :return: The downloaded payload (bytes) or None if no message.
        """
        msg = self.kafka_consumer.poll(timeout)

        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            else:
                raise Exception(msg.error())

        raw_value = msg.value()

        try:
            ref_message = json.loads(raw_value.decode('utf-8'))
        except json.JSONDecodeError as e:
            if self.allow_inline_payloads:
                self._emit_metric("consume_inline", topic=msg.topic(), bytes=len(raw_value))
                return raw_value
            logger.warning("Skipping malformed message (not JSON): %s", e)
            self._send_dlq(error="malformed_json", raw=raw_value)
            return None

        try:
            s3_bucket = ref_message["s3_bucket"]
            s3_key = ref_message["s3_key"]
        except KeyError as e:
            logger.warning("Skipping malformed message (missing key %s)", e)
            self._send_dlq(error="missing_key", reference=ref_message)
            return None

        if s3_bucket != self.s3_bucket:
            err = DataIntegrityError(f"Unexpected S3 bucket in message: {s3_bucket}")
            self._send_dlq(error=str(err), reference=ref_message)
            raise err
        if self.expected_prefix and not s3_key.startswith(self.expected_prefix + "/"):
            err = DataIntegrityError(f"S3 key outside allowed prefix: {s3_key}")
            self._send_dlq(error=str(err), reference=ref_message)
            raise err

        expected_etag = ref_message.get("etag")
        expected_sha = ref_message.get("sha256")
        compression = ref_message.get("compression")

        # Download from S3
        response = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        body = response["Body"].read()
        if compression == "gzip":
            body = gzip.decompress(body)

        # Verify ETag for data integrity
        actual_etag = response.get("ETag", "").strip('"')
        if expected_etag and actual_etag and actual_etag != expected_etag:
            err = DataIntegrityError(f"ETag check failed for S3 object {s3_key}")
            self._send_dlq(error=str(err), reference=ref_message)
            raise err

        if expected_sha:
            actual_sha = hashlib.sha256(body).hexdigest()
            if actual_sha != expected_sha:
                err = DataIntegrityError(f"SHA-256 check failed for S3 object {s3_key}")
                self._send_dlq(error=str(err), reference=ref_message)
                raise err

        if self.delete_after_consume:
            try:
                self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                logger.warning("Failed to delete S3 object %s after consume: %s", s3_key, exc)

        self._emit_metric("consume_success", topic=msg.topic(), bytes=len(body), s3_key=s3_key)
        return body

    def _send_dlq(self, error, reference=None, raw=None):
        """
        Best-effort DLQ publication.
        """
        if not self.dlq_topic or not self.dlq_producer:
            return
        payload = {"error": error}
        if reference:
            payload["reference"] = reference
        if raw:
            try:
                payload["raw"] = raw.decode("utf-8", errors="ignore")
            except Exception:
                pass
        try:
            self.dlq_producer.produce(self.dlq_topic, value=json.dumps(payload).encode("utf-8"))
            self.dlq_producer.poll(0)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to publish to DLQ %s: %s", self.dlq_topic, exc)

    def _emit_metric(self, event, **data):
        """
        Send metric events to optional callback.
        """
        if self.metric_callback:
            try:
                self.metric_callback(event, data)
            except Exception:  # pragma: no cover
                logger.debug("Metric callback failed for %s", event)
