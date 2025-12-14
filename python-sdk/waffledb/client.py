"""WaffleDB Python SDK - Dead Simple & Powerful

Simple by default. Auto-creates everything. Handles all use cases.
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
    """Dead simple WaffleDB client - auto-creates collections, handles all use cases"""

    def __init__(self, url: str = "http://localhost:8080", timeout: int = 30):
        """Initialize WaffleDB client"""
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._collections_created = set()

    def _ensure_collection(self, name: str, dimension: Optional[int] = None) -> None:
        """Auto-create collection if doesn't exist"""
        if name in self._collections_created:
            return
        try:
            requests.post(
                f"{self.url}/collections",
                json={"name": name, "dimension": dimension},
                timeout=self.timeout,
            )
            self._collections_created.add(name)
        except Exception:
            # Collection might already exist, that's fine
            self._collections_created.add(name)

    def _check_connection(self) -> None:
        """Verify connection to WaffleDB server"""
        try:
            resp = requests.get(f"{self.url}/health", timeout=self.timeout)
            resp.raise_for_status()
        except requests.ConnectionError:
            raise ConnectionError(
                f"Cannot connect to WaffleDB at {self.url}. "
                f"Make sure server is running: 'docker run -p 8080:8080 waffledb' or check your URL."
            )
        except Exception as e:
            raise ConnectionError(f"WaffleDB server error: {e}")

    def _handle_http_error(self, resp: requests.Response, operation: str) -> None:
        """Handle HTTP errors with helpful messages"""
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            error_msg = str(e)
            if resp.status_code == 400:
                try:
                    details = resp.json().get("error", error_msg)
                    raise ValueError(f"{operation} failed: {details}")
                except:
                    raise ValueError(f"{operation} failed: Bad request (check dimensions, formats)")
            elif resp.status_code == 404:
                raise ValueError(f"{operation} failed: Collection not found")
            elif resp.status_code == 500:
                raise RuntimeError(f"{operation} failed: Server error. Check server logs.")
            else:
                raise ConnectionError(f"{operation} failed: {error_msg}")

    # ===== MAIN OPERATIONS (SIMPLE) =====

    def add(
        self,
        collection: str,
        ids: List[str],
        embeddings: List[List[float]],
        metadata: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Add vectors to collection (auto-creates if needed)
        
        Args:
            collection: Collection name (auto-created if needed)
            ids: Vector IDs (must be strings)
            embeddings: List of embedding vectors
            metadata: Optional metadata for each vector
            
        Raises:
            ValueError: If inputs are invalid
            ConnectionError: If server is unreachable
        """
        if not embeddings:
            raise ValueError("embeddings cannot be empty. Provide at least one embedding.")
        
        if not ids:
            raise ValueError("ids cannot be empty. Provide IDs for each embedding.")
            
        if len(ids) != len(embeddings):
            raise ValueError(f"Mismatch: {len(ids)} IDs but {len(embeddings)} embeddings. They must match.")
        
        if metadata and len(metadata) != len(embeddings):
            raise ValueError(f"Mismatch: {len(metadata)} metadata items but {len(embeddings)} embeddings. They must match.")
        
        # Validate embedding format
        try:
            dimension = len(embeddings[0])
            if dimension == 0:
                raise ValueError("First embedding is empty")
            for i, emb in enumerate(embeddings):
                if not isinstance(emb, (list, tuple)):
                    raise ValueError(f"Embedding {i} is not a list/tuple")
                if len(emb) != dimension:
                    raise ValueError(f"Embedding {i} has {len(emb)} dimensions, expected {dimension}")
        except (TypeError, IndexError) as e:
            raise ValueError(f"Invalid embeddings format: {e}")
        
        self._ensure_collection(collection, dimension)

        vectors = []
        for i, (id_, embedding) in enumerate(zip(ids, embeddings)):
            vec = {"id": str(id_), "vector": embedding}
            if metadata and i < len(metadata):
                vec["metadata"] = metadata[i]
            vectors.append(vec)

        try:
            resp = requests.post(
                f"{self.url}/collections/{collection}/add",
                json={"vectors": vectors},
                timeout=self.timeout,
            )
            self._handle_http_error(resp, f"Add to {collection}")
            return resp.json()
        except requests.ConnectionError:
            self._check_connection()
            raise

    def search(
        self,
        collection: str,
        embedding: List[float],
        limit: int = 10,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[SearchResult]:
        """Search collection for similar vectors
        
        Args:
            collection: Collection to search
            embedding: Query embedding vector
            limit: Max results to return (default 10)
            filter: Optional filter criteria
            
        Returns:
            List of SearchResult objects with id, score, metadata
            
        Raises:
            ValueError: If inputs are invalid
            ConnectionError: If server is unreachable
        """
        if not embedding:
            raise ValueError("embedding cannot be empty")
        
        if not isinstance(embedding, (list, tuple)):
            raise ValueError("embedding must be a list or tuple")
        
        if limit <= 0:
            raise ValueError(f"limit must be > 0, got {limit}")
        
        payload = {"embedding": embedding, "limit": limit}
        if filter:
            payload["filter"] = filter

        try:
            resp = requests.post(
                f"{self.url}/collections/{collection}/search",
                json=payload,
                timeout=self.timeout,
            )
            self._handle_http_error(resp, f"Search in {collection}")
            results = resp.json().get("results", [])
            return [
                SearchResult(id=r["id"], score=r["score"], metadata=r.get("metadata"))
                for r in results
            ]
        except requests.ConnectionError:
            self._check_connection()
            raise

    def delete(self, collection: str, ids: List[str]) -> Dict[str, Any]:
        """Delete vectors from collection
        
        Args:
            collection: Collection name
            ids: Vector IDs to delete
            
        Returns:
            Delete result with count and status
            
        Raises:
            ValueError: If inputs are invalid
            ConnectionError: If server is unreachable
        """
        if not ids:
            raise ValueError("ids cannot be empty. Provide at least one ID to delete.")
        
        try:
            resp = requests.post(
                f"{self.url}/collections/{collection}/delete",
                json={"ids": [str(id_) for id_ in ids]},
                timeout=self.timeout,
            )
            self._handle_http_error(resp, f"Delete from {collection}")
            return resp.json()
        except requests.ConnectionError:
            self._check_connection()
            raise

    def batch_search(self, collection: str, queries: List[Dict[str, Any]]) -> List[List[SearchResult]]:
        """Batch search with multiple vectors"""
        if not queries:
            raise ValueError("queries cannot be empty")
            
        try:
            resp = requests.post(
                f"{self.url}/collections/{collection}/batch_search",
                json={"queries": queries},
                timeout=self.timeout,
            )
            self._handle_http_error(resp, f"Batch search in {collection}")
            all_results = resp.json().get("results", [])
            return [
                [SearchResult(id=r["id"], score=r["score"], metadata=r.get("metadata")) for r in query_results]
                for query_results in all_results
            ]
        except requests.RequestException as e:
            raise ConnectionError(f"Batch search failed: {e}")

    # ===== GET & UPDATE =====

    def get(self, collection: str, vector_id: str) -> Optional[Dict[str, Any]]:
        """Get vector by ID"""
        resp = requests.get(
            f"{self.url}/collections/{collection}/vectors/{vector_id}",
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def update(self, collection: str, vector_id: str, embedding: List[float]) -> Dict[str, Any]:
        """Update vector embedding"""
        resp = requests.put(
            f"{self.url}/collections/{collection}/vectors/{vector_id}",
            json={"vector": embedding},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def update_metadata(self, collection: str, vector_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Update vector metadata"""
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

    # ===== COLLECTION OPERATIONS =====

    def list(self) -> Dict[str, Any]:
        """List all collections"""
        resp = requests.get(f"{self.url}/collections", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def info(self, collection: str) -> Dict[str, Any]:
        """Get collection info/stats"""
        resp = requests.get(
            f"{self.url}/collections/{collection}/stats", timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    def drop(self, collection: str) -> Dict[str, Any]:
        """Delete a collection"""
        resp = requests.delete(f"{self.url}/collections/{collection}", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ===== SNAPSHOTS & BACKUP =====

    def snapshot(self, collection: str, name: Optional[str] = None) -> Dict[str, Any]:
        """Create a snapshot of collection"""
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

    # ===== SERVER HEALTH & METRICS =====

    def health(self) -> Dict[str, Any]:
        """Check server health"""
        resp = requests.get(f"{self.url}/health", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def ready(self) -> bool:
        """Check if server is ready"""
        try:
            resp = requests.get(f"{self.url}/ready", timeout=self.timeout)
            return resp.status_code == 200
        except Exception:
            return False

    def metrics(self) -> Dict[str, Any]:
        """Get server metrics"""
        resp = requests.get(f"{self.url}/metrics", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
