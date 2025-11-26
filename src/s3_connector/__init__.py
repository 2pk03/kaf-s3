"""
A Python library to seamlessly handle large Kafka messages by offloading them to Amazon S3.
"""

__version__ = "1.1.0"

from .producer import S3Producer
from .consumer import S3Consumer
from .exceptions import DataIntegrityError

__all__ = ["S3Producer", "S3Consumer", "DataIntegrityError"]
