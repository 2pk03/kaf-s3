import json
import pytest
from unittest.mock import MagicMock
from s3_connector import S3Consumer, DataIntegrityError

@pytest.fixture
def consumer_config():
    return {
        "kafka": {
            "bootstrap.servers": "mock:9092",
            "group.id": "test-group",
            "test.mock.num.brokers": 3
        },
        "s3": {"bucket": "test-bucket", "delete_after_consume": True}
    }

def test_consumer_init(mocker, consumer_config):
    """Tests the initialization of the S3Consumer."""
    mocker.patch("s3_connector.consumer.boto3")
    mocker.patch("s3_connector.consumer.Consumer")
    
    consumer = S3Consumer(consumer_config)
    
    assert consumer.kafka_consumer is not None
    assert consumer.s3_client is not None

def test_poll_message(mocker, consumer_config):
    """Tests polling and successfully retrieving a message."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    # Mock Kafka message
    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "etag": "12345",
        "sha256": "7add08d874756f51da6f92958e09e7596f4bbeea9ea6e8a4e0da23fd76b62917"
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    # Mock S3 response
    s3_payload = b"This is the payload from S3"
    mock_s3_response = {
        "Body": MagicMock(),
        "ETag": '"12345"'
    }
    mock_s3_response["Body"].read.return_value = s3_payload
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    consumer = S3Consumer(consumer_config)
    payload = consumer.poll()

    assert payload == s3_payload
    mock_boto3.client.return_value.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")
    mock_boto3.client.return_value.delete_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")

def test_poll_data_integrity_error(mocker, consumer_config):
    """Tests that DataIntegrityError is raised on ETag mismatch."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    # Mock Kafka message with a different ETag
    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "etag": "expected-etag",
        "sha256": "expected-sha"
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    # Mock S3 response with a mismatched ETag
    mock_s3_response = {"ETag": '"actual-etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = b"payload-mismatch"
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    consumer = S3Consumer(consumer_config)
    
    with pytest.raises(DataIntegrityError):
        consumer.poll()

def test_poll_inline_payload(mocker, consumer_config):
    """Tests returning inline payloads when JSON parsing fails."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    kafka_msg = MagicMock()
    kafka_msg.value.return_value = b"raw-payload"
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    consumer = S3Consumer(consumer_config)
    payload = consumer.poll()

    assert payload == b"raw-payload"


def test_poll_malformed_rejected_when_inline_disabled(mocker, consumer_config):
    """Malformed payload returns None when inline pass-through is disabled."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    kafka_msg = MagicMock()
    kafka_msg.value.return_value = b"raw-payload"
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    cfg = json.loads(json.dumps(consumer_config))  # shallow clone
    cfg["s3"]["allow_inline_payloads"] = False
    consumer = S3Consumer(cfg)
    payload = consumer.poll()

    assert payload is None


def test_poll_unexpected_bucket(mocker, consumer_config):
    """Raises on unexpected bucket."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    ref_message = {
        "s3_bucket": "other-bucket",
        "s3_key": "test-key",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    consumer = S3Consumer(consumer_config)
    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_consumer_config_validation(mocker, consumer_config):
    """Ensures required config keys are validated."""
    mocker.patch("s3_connector.consumer.boto3")
    mocker.patch("s3_connector.consumer.Consumer")

    bad_cfg_missing_group = {
        "kafka": {"bootstrap.servers": "mock:9092"},
        "s3": {"bucket": "test-bucket"},
    }
    with pytest.raises(ValueError):
        S3Consumer(bad_cfg_missing_group)

    bad_cfg_missing_bucket = {
        "kafka": {"bootstrap.servers": "mock:9092", "group.id": "g"},
        "s3": {},
    }
    with pytest.raises(ValueError):
        S3Consumer(bad_cfg_missing_bucket)


def test_consume_gzip_payload(mocker, consumer_config):
    """Compressed payloads are decompressed after download."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "compression": "gzip",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    import gzip
    compressed = gzip.compress(b"test")
    mock_s3_response = {"ETag": '"etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = compressed
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    consumer = S3Consumer(consumer_config)
    payload = consumer.poll()

    assert payload == b"test"


def test_consume_prefix_enforced(mocker, consumer_config):
    """Rejects keys outside configured prefix."""
    mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value

    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "wrong/key",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    cfg = json.loads(json.dumps(consumer_config))
    cfg["s3"]["prefix"] = "allowed"
    consumer = S3Consumer(cfg)

    with pytest.raises(DataIntegrityError):
        consumer.poll()


def test_consume_dlq_on_integrity_error(mocker, consumer_config):
    """Publishes to DLQ on integrity errors."""
    mock_boto3 = mocker.patch("s3_connector.consumer.boto3")
    mock_kafka_consumer_class = mocker.patch("s3_connector.consumer.Consumer")
    mock_kafka_consumer_instance = mock_kafka_consumer_class.return_value
    mock_dlq_producer_class = mocker.patch("s3_connector.consumer.Producer")
    mock_dlq_producer_instance = mock_dlq_producer_class.return_value

    ref_message = {
        "s3_bucket": "test-bucket",
        "s3_key": "test-key",
        "etag": "expected-etag",
    }
    kafka_msg = MagicMock()
    kafka_msg.value.return_value = json.dumps(ref_message).encode('utf-8')
    kafka_msg.error.return_value = None
    mock_kafka_consumer_instance.poll.return_value = kafka_msg

    mock_s3_response = {"ETag": '"actual-etag"', "Body": MagicMock()}
    mock_s3_response["Body"].read.return_value = b"payload"
    mock_boto3.client.return_value.get_object.return_value = mock_s3_response

    cfg = json.loads(json.dumps(consumer_config))
    cfg["kafka"]["dlq_topic"] = "dlq"
    consumer = S3Consumer(cfg)

    with pytest.raises(DataIntegrityError):
        consumer.poll()

    mock_dlq_producer_instance.produce.assert_called_once()
