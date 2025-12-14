"""WaffleDB Python SDK - Dead Simple Vector Search

Quick start (2 lines):
    from waffledb import client
    
    client.add("docs", ids=["doc1"], embeddings=[[0.1]*384])
    results = client.search("docs", [0.1]*384)

Or use the module-level client:
    from waffledb import db
    db.add("docs", ids=["doc1"], embeddings=[[0.1]*384])

All features work with both approaches.
"""

# Primary simple client
from .client import WaffleClient

# Create default instance
client = WaffleClient()

# Backward compat: also expose as db
db = client

# Utilities
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
    # Simple client (start here!)
    "client",
    "db",
    
    # Errors
    "WaffleDBError",
    "ConnectionError",
    "TimeoutError",
    "ValidationError",
    "NotFoundError",
    
    # Types
    "Vector",
    "Metadata",
    "SearchResult",
    "VectorData",
    
    # Utils
    "normalize_vector",
    "l2_distance",
    "cosine_distance",
    "batch_insert",
    "batch_search",
]
