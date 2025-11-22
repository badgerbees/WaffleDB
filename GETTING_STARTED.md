# Getting Started with WaffleDB

This guide will help you get WaffleDB up and running in minutes.

## Prerequisites

- Docker and Docker Compose (recommended), OR
- Rust 1.70+ and Cargo for local builds
- Python 3.8+ (for Python SDK)

## Installation

### Option 1: Docker (Easiest)

```bash
# Clone the repository
git clone https://github.com/yourusername/waffledb.git
cd waffledb

# Start WaffleDB
docker-compose -f deployments/docker/docker-compose.yml up

# Server runs on http://localhost:8080
```

### Option 2: Local Build

```bash
# Clone the repository
git clone https://github.com/yourusername/waffledb.git
cd waffledb

# Build
cargo build --release

# Run
./target/release/waffledb-server

# Server runs on http://localhost:8080
```

### Option 3: Python SDK Only

```bash
# Install from source
pip install -e python-sdk/

# Or pip install waffledb (from PyPI when available)
```

## First Steps

### 1. Create a Collection

Collections are where vectors are stored. Each collection has a name and a dimension (embedding size).

```bash
curl -X POST http://localhost:8080/api/collections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "documents",
    "dimension": 384
  }'
```

### 2. Insert a Vector

Let's insert a sample vector with metadata:

```bash
curl -X POST http://localhost:8080/api/collections/documents/insert \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc_001",
    "vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4],
    "metadata": {
      "source": "docs/guide.txt",
      "section": "introduction",
      "chunk": "0"
    }
  }'
```

### 3. Search for Similar Vectors

```bash
curl -X POST http://localhost:8080/api/collections/documents/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.0, 0.0, 0.0, 0.1, 0.2, 0.3, 0.4],
    "top_k": 5,
    "include_metadata": true
  }'
```

Expected response:
```json
{
  "results": [
    {
      "id": "doc_001",
      "distance": 0.0,
      "metadata": {
        "source": "docs/guide.txt",
        "section": "introduction",
        "chunk": "0"
      }
    }
  ]
}
```

## Using Python SDK

### Installation

```bash
# Install the SDK
pip install -e python-sdk/
```

### Basic Usage

```python
from waffledb import WaffleDBClient

# Create client
client = WaffleDBClient(host="localhost", http_port=8080)

# Create a collection
client.create_collection(name="my_docs", dimension=384)

# Insert vectors
client.insert(
    collection_name="my_docs",
    vector_id="doc_1",
    vector=[0.1, 0.2, 0.3, ...],  # 384 dimensions
    metadata={"title": "Introduction to WaffleDB"}
)

# Search
results = client.search(
    collection_name="my_docs",
    vector=[0.1, 0.2, 0.3, ...],
    top_k=5,
    include_metadata=True
)

print(results)
# Output: [{'id': 'doc_1', 'distance': 0.0, 'metadata': {...}}, ...]
```

### Advanced Python Usage

```python
from waffledb import WaffleDBClient

client = WaffleDBClient()

# Batch insert (more efficient)
vectors = [
    {
        "id": f"doc_{i}",
        "vector": [0.1 * i] * 384,
        "metadata": {"index": str(i)}
    }
    for i in range(100)
]

client.batch_insert("my_docs", vectors)

# Update a vector
client.update(
    collection_name="my_docs",
    vector_id="doc_1",
    vector=[0.2, 0.3, 0.4, ...],
    reindex=True  # Re-index after update
)

# Delete a vector
client.delete("my_docs", "doc_1")

# Get collection stats
stats = client.collection_stats("my_docs")
print(stats)
# Output: {'name': 'my_docs', 'vector_count': 99, 'dimension': 384, ...}
```

## Collection Management

### List Collections

```bash
curl http://localhost:8080/api/collections
```

### Delete a Collection

```bash
curl -X DELETE http://localhost:8080/api/collections/documents
```

### Get Collection Stats

```bash
curl http://localhost:8080/api/collections/documents/stats
```

Response:
```json
{
  "name": "documents",
  "dimension": 384,
  "vector_count": 1,
  "created_at": "2025-01-01T12:00:00Z",
  "updated_at": "2025-01-01T12:00:01Z"
}
```

## Duplicate Handling

When creating a collection, you can specify a duplicate policy:

```bash
curl -X POST http://localhost:8080/api/collections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "docs_unique",
    "dimension": 384,
    "duplicate_policy": "overwrite"
  }'
```

- `overwrite`: Replace existing vector with same ID (default)
- `reject`: Reject insertion if ID already exists

## Metadata Filtering

Search with metadata filtering:

```bash
curl -X POST http://localhost:8080/api/collections/documents/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, ...],
    "top_k": 5,
    "include_metadata": true,
    "metadata_filters": {
      "source": "docs/guide.txt"
    }
  }'
```

## Troubleshooting

### Server Won't Start

**Issue**: "Address already in use"
```bash
# Change port in docker-compose.yml or kill existing process
lsof -i :8080
kill <PID>
```

### Connection Refused

**Issue**: "Failed to connect to localhost:8080"
```bash
# Verify server is running
curl http://localhost:8080/api/collections

# Check logs
docker logs waffledb  # If using Docker
```

### Dimension Mismatch

**Issue**: "Vector dimension mismatch"
```python
# Ensure your embedding model outputs the right dimension
# Common dimensions: 384 (MiniLM), 768 (BERT), 1536 (OpenAI)
embeddings = model.encode("text")
print(len(embeddings))  # Should match collection dimension
```

## Next Steps

- Check [README.md](./README.md) for API overview
- Explore `examples/` for more samples
- Read `waffledb-server/` for server architecture
- Check `python-sdk/` for SDK source code

## Performance Tips

1. **Batch Insert**: Use batch operations instead of single inserts
2. **Vector Dimension**: Smaller dimensions = faster search
3. **Top-K**: Limit results to what you need
4. **Metadata Filtering**: Filter in post-processing when possible

## Get Help

- Open an issue on GitHub
- Check existing discussions
- Review examples in the repository

---

Happy indexing with WaffleDB! 🧇
