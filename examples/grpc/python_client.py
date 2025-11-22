# gRPC Python client example

import sys
sys.path.insert(0, '../../python-sdk')

from waffledb.client import WaffleDBClient
import random


def main():
    """gRPC client example using WaffleDB Python SDK."""
    
    # Initialize client (using REST transport for now)
    client = WaffleDBClient(host="localhost", http_port=8080)
    
    print("=== WaffleDB gRPC Example (REST Transport) ===")
    
    # Check server health
    if not client.health():
        print("Error: Server not healthy")
        return
    
    print("Server is healthy!")
    
    # Generate random vectors
    print("\nInserting 10 random vectors...")
    vector_ids = []
    for i in range(10):
        vector = [random.random() for _ in range(128)]
        metadata = {
            "index": str(i),
            "type": "example",
            "source": "grpc_example"
        }
        vec_id = client.insert(vector, metadata=metadata)
        vector_ids.append(vec_id)
        print(f"  Inserted {vec_id}")
    
    # Search
    print("\nSearching for similar vectors...")
    query_vector = [random.random() for _ in range(128)]
    results = client.search(query_vector, top_k=5)
    
    print("Top 5 results:")
    for i, (vec_id, distance) in enumerate(results, 1):
        print(f"  {i}. ID: {vec_id}, Distance: {distance:.4f}")
    
    # Update metadata
    print("\nUpdating metadata...")
    if vector_ids:
        client.upsert_metadata(vector_ids[0], {"updated": "true", "reason": "example"})
        print(f"  Updated {vector_ids[0]}")
    
    # Delete
    print("\nDeleting a vector...")
    if vector_ids:
        client.delete(vector_ids[-1])
        print(f"  Deleted {vector_ids[-1]}")
    
    print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()

