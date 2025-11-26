"""
A Python library to seamlessly handle large Kafka messages by offloading them to Amazon S3.
"""

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

from .producer import S3Producer
from .consumer import S3Consumer
from .exceptions import DataIntegrityError

__all__ = ["S3Producer", "S3Consumer", "DataIntegrityError", "__version__"]
