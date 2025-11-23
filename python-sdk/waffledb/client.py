"""WaffleDB Python SDK - Production Ready

Only includes endpoints actually wired in the server.
"""

import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class SearchResult:
    """Search result"""
    id: str
    score: float
    metadata: Optional[Dict[str, Any]] = None


class WaffleClient:
    """Production-ready WaffleDB Python client"""

    def __init__(self, url: str = "http://localhost:8080", timeout: int = 30):
        """Initialize WaffleDB client"""
        self.url = url.rstrip("/")
        self.timeout = timeout

    # ===== COLLECTION MANAGEMENT =====

    def create_collection(self, name: str) -> Dict[str, Any]:
        """Create a collection"""
        resp = requests.post(
            f"{self.url}/collections",
            json={"name": name},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def delete_collection(self, name: str) -> Dict[str, Any]:
        """Delete a collection"""
        resp = requests.delete(f"{self.url}/collections/{name}", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_collections(self) -> Dict[str, Any]:
        """List all collections"""
        resp = requests.get(f"{self.url}/collections", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_stats(self, collection: str) -> Dict[str, Any]:
        """Get collection statistics"""
        resp = requests.get(
            f"{self.url}/collections/{collection}/stats", timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    # ===== DENSE VECTOR OPERATIONS =====

    def insert_dense(self, collection: str, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert dense vectors"""
        resp = requests.post(
            f"{self.url}/collections/{collection}/insert",
            json={"vectors": vectors},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def search_dense(
        self,
        collection: str,
        query_vector: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[SearchResult]:
        """Search with dense vector"""
        payload = {"vector": query_vector, "top_k": top_k}
        if filter:
            payload["filter"] = filter

        resp = requests.post(
            f"{self.url}/collections/{collection}/search",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return [
            SearchResult(id=r["id"], score=r["score"], metadata=r.get("metadata"))
            for r in results
        ]

    def batch_search(self, collection: str, queries: List[Dict[str, Any]]) -> List[List[SearchResult]]:
        """Batch search with multiple vectors"""
        resp = requests.post(
            f"{self.url}/collections/{collection}/batch_search",
            json={"queries": queries},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        all_results = resp.json().get("results", [])
        return [
            [SearchResult(id=r["id"], score=r["score"], metadata=r.get("metadata")) for r in query_results]
            for query_results in all_results
        ]

    def get_vector(self, collection: str, vector_id: str) -> Optional[Dict[str, Any]]:
        """Get a vector by ID"""
        resp = requests.get(
            f"{self.url}/collections/{collection}/vectors/{vector_id}",
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def delete_vector(self, collection: str, vector_id: str) -> Dict[str, Any]:
        """Delete a vector"""
        resp = requests.delete(
            f"{self.url}/collections/{collection}/vectors/{vector_id}",
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def upsert_dense(self, collection: str, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Upsert dense vectors (insert or update)"""
        resp = requests.put(
            f"{self.url}/collections/{collection}/vectors",
            json={"vectors": vectors},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def batch_insert(self, collection: str, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Batch insert vectors"""
        resp = requests.post(
            f"{self.url}/collections/{collection}/batch_insert",
            json={"vectors": vectors},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    # ===== METADATA & UPDATE OPERATIONS =====

    def update_vector(self, collection: str, vector_id: str, vector: List[float]) -> Dict[str, Any]:
        """Update a vector"""
        resp = requests.put(
            f"{self.url}/collections/{collection}/vectors/{vector_id}",
            json={"vector": vector},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def update_metadata(self, collection: str, vector_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Update metadata for a vector"""
        resp = requests.put(
            f"{self.url}/collections/{collection}/vectors/{vector_id}/metadata",
            json=metadata,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def patch_metadata(self, collection: str, vector_id: str, metadata_patch: Dict[str, Any]) -> Dict[str, Any]:
        """Patch metadata for a vector"""
        resp = requests.patch(
            f"{self.url}/collections/{collection}/vectors/{vector_id}/metadata",
            json=metadata_patch,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    # ===== DELETE OPERATIONS =====

    def delete_vectors(self, collection: str, ids: List[str]) -> Dict[str, Any]:
        """Delete multiple vectors"""
        resp = requests.post(
            f"{self.url}/collections/{collection}/delete",
            json={"ids": ids},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    # ===== SNAPSHOTS =====

    def create_snapshot(self, collection: str, name: str = None) -> Dict[str, Any]:
        """Create a snapshot of a collection"""
        payload = {}
        if name:
            payload["name"] = name

        resp = requests.post(
            f"{self.url}/collections/{collection}/snapshot",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    # ===== SERVER HEALTH =====

    def health(self) -> Dict[str, Any]:
        """Check server health"""
        resp = requests.get(f"{self.url}/health", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def is_ready(self) -> bool:
        """Check if server is ready"""
        try:
            resp = requests.get(f"{self.url}/ready", timeout=self.timeout)
            return resp.status_code == 200
        except Exception:
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get server metrics"""
        resp = requests.get(f"{self.url}/metrics", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def liveness(self) -> Dict[str, Any]:
        """Check server liveness"""
        resp = requests.get(f"{self.url}/liveness", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
