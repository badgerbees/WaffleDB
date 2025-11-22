# WaffleDB Type Definitions

from typing import List, Dict, Optional, Tuple


Vector = List[float]
Metadata = Dict[str, str]
SearchResult = Tuple[str, float]  # (id, distance)


class VectorData:
    """Represents a vector with metadata."""
    
    def __init__(self, vector_id: str, data: Vector, metadata: Optional[Metadata] = None):
        self.id = vector_id
        self.data = data
        self.metadata = metadata or {}
    
    def __repr__(self) -> str:
        return f"VectorData(id={self.id}, dim={len(self.data)}, metadata={len(self.metadata)})"

