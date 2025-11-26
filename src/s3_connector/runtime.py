import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Dict, Any

from .producer import S3Producer
from .consumer import S3Consumer

logger = logging.getLogger(__name__)

class MetricsRegistry:
    """
    Minimal Prometheus text-format metrics registry.
    """
    def __init__(self):
        self._counters = {}
        self._lock = threading.Lock()

    def inc(self, name: str, labels: Dict[str, Any] | None = None, value: int = 1):
        key = (name, tuple(sorted((labels or {}).items())))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def render(self) -> str:
        lines = []
        with self._lock:
            for (name, labels), value in self._counters.items():
                if labels:
                    label_str = ",".join(f'{k}="{v}"' for k, v in labels)
                    lines.append(f"{name}{{{label_str}}} {value}")
                else:
                    lines.append(f"{name} {value}")
        return "\n".join(lines) + "\n"


def metrics_handler(registry: MetricsRegistry):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            body = registry.render().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # pragma: no cover - silence http.server logs
            logger.debug(format, *args)

    return Handler


def start_metrics_server(registry: MetricsRegistry, port: int):
    srv = HTTPServer(("", port), metrics_handler(registry))
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    logger.info("Metrics server listening on :%s", port)
    return srv


def build_config_from_env():
    kafka_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    }
    if os.getenv("KAFKA_GROUP_ID"):
        kafka_config["group.id"] = os.getenv("KAFKA_GROUP_ID")
    # Optional security settings
    for key in [
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "ssl.ca.location",
        "ssl.certificate.location",
        "ssl.key.location",
        "ssl.key.password",
    ]:
        env_key = f"KAFKA_{key.replace('.', '_').upper()}"
        if os.getenv(env_key):
            kafka_config[key] = os.getenv(env_key)

    dlq_topic = os.getenv("DLQ_TOPIC")
    if dlq_topic:
        kafka_config["dlq_topic"] = dlq_topic

    s3_config = {
        "bucket": os.getenv("S3_BUCKET", ""),
        "prefix": os.getenv("S3_PREFIX", ""),
        "region_name": os.getenv("AWS_REGION"),
        "delete_after_consume": os.getenv("S3_DELETE_AFTER_CONSUME", "false").lower() == "true",
        "allow_inline_payloads": os.getenv("S3_ALLOW_INLINE_PAYLOADS", "true").lower() == "true",
        "max_inline_bytes": int(os.getenv("S3_MAX_INLINE_BYTES", "900000")),
        "max_payload_bytes": int(os.getenv("S3_MAX_PAYLOAD_BYTES", str(5 * 1024 * 1024 * 1024))),
        "deterministic_keys": os.getenv("S3_DETERMINISTIC_KEYS", "false").lower() == "true",
        "ttl_seconds": int(os.getenv("S3_TTL_SECONDS")) if os.getenv("S3_TTL_SECONDS") else None,
        "compression": os.getenv("S3_COMPRESSION"),
        "server_side_encryption": os.getenv("S3_SSE"),
        "sse_kms_key_id": os.getenv("S3_SSE_KMS_KEY_ID"),
        "dlq_topic": dlq_topic,
    }

    config = {"kafka": kafka_config, "s3": s3_config, "hooks": {}}
    return config


def metric_hook(registry: MetricsRegistry) -> Callable[[str, Dict[str, Any]], None]:
    def hook(event: str, data: Dict[str, Any]):
        registry.inc(event, data)
    return hook


def run():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    mode = os.getenv("MODE", "consumer")
    topic = os.getenv("TOPIC")
    if not topic:
        raise ValueError("TOPIC is required.")

    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    registry = MetricsRegistry()
    start_metrics_server(registry, metrics_port)

    config = build_config_from_env()
    config["hooks"]["metrics"] = metric_hook(registry)

    if mode == "producer":
        producer = S3Producer(config)
        # Simple stdin producer for demo/prod piping
        for line in iter(os.sys.stdin.buffer.readline, b""):
            payload = line.rstrip(b"\n")
            producer.produce(topic, payload)
            registry.inc("stdin_lines")
    elif mode == "consumer":
        consumer = S3Consumer(config)
        consumer.subscribe([topic])
        timeout = float(os.getenv("POLL_TIMEOUT", "5.0"))
        while True:
            consumer.poll(timeout=timeout)
    else:
        raise ValueError(f"Unknown MODE {mode}")
