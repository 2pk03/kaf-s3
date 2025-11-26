import os
from s3_connector.runtime import MetricsRegistry, build_config_from_env


def test_metrics_registry_render():
    reg = MetricsRegistry()
    reg.inc("produce_offloaded", {"topic": "t"})
    reg.inc("produce_offloaded", {"topic": "t"}, 2)
    reg.inc("consume_success")
    rendered = reg.render()
    assert "produce_offloaded{topic=\"t\"} 3" in rendered
    assert "consume_success 1" in rendered


def test_build_config_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "k:9092")
    monkeypatch.setenv("KAFKA_GROUP_ID", "g1")
    monkeypatch.setenv("S3_BUCKET", "bkt")
    monkeypatch.setenv("S3_PREFIX", "pfx")
    monkeypatch.setenv("S3_TTL_SECONDS", "60")
    monkeypatch.setenv("S3_COMPRESSION", "gzip")
    monkeypatch.setenv("DLQ_TOPIC", "dlq")

    cfg = build_config_from_env()
    assert cfg["kafka"]["bootstrap.servers"] == "k:9092"
    assert cfg["kafka"]["group.id"] == "g1"
    assert cfg["kafka"]["dlq_topic"] == "dlq"
    assert cfg["s3"]["bucket"] == "bkt"
    assert cfg["s3"]["prefix"] == "pfx"
    assert cfg["s3"]["ttl_seconds"] == 60
    assert cfg["s3"]["compression"] == "gzip"
    assert cfg["s3"]["dlq_topic"] == "dlq"
