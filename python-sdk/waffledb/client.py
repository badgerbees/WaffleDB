# WaffleDB Python Client

import requests
import json
from typing import List, Dict, Optional, Tuple
from .errors import WaffleDBError, ConnectionError, TimeoutError


class WaffleDBClient:
    """Main client for interacting with WaffleDB."""

    def __init__(self, host: str = "localhost", http_port: int = 8080, timeout: int = 30):
        """
        Initialize WaffleDB client.
        
        Args:
            host: Server hostname
            http_port: HTTP API port
            timeout: Request timeout in seconds
        """
        self.base_url = f"http://{host}:{http_port}/api"
        self.timeout = timeout

    def insert(
        self,
        vector: List[float],
        vector_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Insert a vector.
        
        Args:
            vector: Vector data
            vector_id: Optional ID for the vector
            metadata: Optional metadata dictionary
            
        Returns:
            The ID of the inserted vector
        """
        payload = {"vector": vector, "metadata": metadata}
        if vector_id:
            payload["id"] = vector_id

        try:
            response = requests.post(
                f"{self.base_url}/insert",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            result = response.json()
            return result["id"]
        except requests.exceptions.Timeout:
            raise TimeoutError("Insert request timed out")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Insert failed: {e}")

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
    ) -> List[Tuple[str, float]]:
        """
        Search for similar vectors.
        
        Args:
            query_vector: Query vector
            top_k: Number of top results to return
            
        Returns:
            List of (id, distance) tuples
        """
        payload = {"vector": query_vector, "top_k": top_k}

        try:
            response = requests.post(
                f"{self.base_url}/search",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            result = response.json()
            return [(r["id"], r["distance"]) for r in result["results"]]
        except requests.exceptions.Timeout:
            raise TimeoutError("Search request timed out")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Search failed: {e}")

    def delete(self, vector_id: str) -> None:
        """
        Delete a vector by ID.
        
        Args:
            vector_id: ID of vector to delete
        """
        payload = {"id": vector_id}

        try:
            response = requests.post(
                f"{self.base_url}/delete",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError("Delete request timed out")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Delete failed: {e}")

    def upsert_metadata(self, vector_id: str, metadata: Dict[str, str]) -> None:
        """
        Update or insert metadata for a vector.
        
        Args:
            vector_id: ID of vector
            metadata: Metadata dictionary
        """
        payload = {"id": vector_id, "metadata": metadata}

        try:
            response = requests.post(
                f"{self.base_url}/metadata/update",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError("Metadata update request timed out")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Metadata update failed: {e}")

    def health(self) -> bool:
        """Check server health."""
        try:
            response = requests.get(
                f"{self.base_url.rsplit('/api', 1)[0]}/health",
                timeout=self.timeout,
            )
            return response.status_code == 200
        except Exception:
            return False

