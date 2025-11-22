# Utility functions for WaffleDB Python SDK

import numpy as np
from typing import List


def normalize_vector(vector: List[float]) -> List[float]:
    """Normalize a vector to unit length."""
    arr = np.array(vector, dtype=np.float32)
    norm = np.linalg.norm(arr)
    if norm == 0:
        return vector
    return (arr / norm).tolist()


def l2_distance(a: List[float], b: List[float]) -> float:
    """Compute L2 (Euclidean) distance between two vectors."""
    arr_a = np.array(a, dtype=np.float32)
    arr_b = np.array(b, dtype=np.float32)
    return float(np.linalg.norm(arr_a - arr_b))


def cosine_distance(a: List[float], b: List[float]) -> float:
    """Compute cosine distance between two vectors."""
    arr_a = np.array(a, dtype=np.float32)
    arr_b = np.array(b, dtype=np.float32)
    
    norm_a = np.linalg.norm(arr_a)
    norm_b = np.linalg.norm(arr_b)
    
    if norm_a == 0 or norm_b == 0:
        return 1.0
    
    similarity = np.dot(arr_a, arr_b) / (norm_a * norm_b)
    return float(1.0 - similarity)


def batch_insert(client, vectors: List[List[float]], metadata_list: List[dict] = None) -> List[str]:
    """Batch insert multiple vectors."""
    ids = []
    for i, vector in enumerate(vectors):
        metadata = metadata_list[i] if metadata_list else None
        vec_id = client.insert(vector, metadata=metadata)
        ids.append(vec_id)
    return ids


def batch_search(client, query_vectors: List[List[float]], top_k: int = 10) -> List[List]:
    """Batch search multiple query vectors."""
    results = []
    for query in query_vectors:
        result = client.search(query, top_k=top_k)
        results.append(result)
    return results

