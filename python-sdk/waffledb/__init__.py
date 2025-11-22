"""WaffleDB Python SDK"""

from .client import WaffleDBClient
from .errors import (
    WaffleDBError,
    ConnectionError,
    TimeoutError,
    ValidationError,
    NotFoundError,
)
from .types import Vector, Metadata, SearchResult, VectorData
from .utils import (
    normalize_vector,
    l2_distance,
    cosine_distance,
    batch_insert,
    batch_search,
)

__version__ = "0.1.0"
__all__ = [
    "WaffleDBClient",
    "WaffleDBError",
    "ConnectionError",
    "TimeoutError",
    "ValidationError",
    "NotFoundError",
    "Vector",
    "Metadata",
    "SearchResult",
    "VectorData",
    "normalize_vector",
    "l2_distance",
    "cosine_distance",
    "batch_insert",
    "batch_search",
]
