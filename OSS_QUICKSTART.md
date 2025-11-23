# WaffleDB OSS v1.0 — Quickstart Guide

Welcome to **WaffleDB**, the fastest open-source vector database.

WaffleDB combines **dense vectors, sparse vectors, BM25 full-text search, and hybrid fusion** in a single 5MB binary with sub-10ms search latency.

---

## Installation

### Option 1: Docker (Recommended)

```bash
docker pull waffledb/waffledb:latest
docker run -p 8080:8080 -p 50051:50051 waffledb/waffledb:latest
```

### Option 2: Binary

Download from [GitHub Releases](https://github.com/badgerbees/WaffleDB/releases)

```bash
./waffledb-server
```

### Option 3: From Source

```bash
git clone https://github.com/badgerbees/WaffleDB.git
cd WaffleDB
cargo build --release
./target/release/waffledb-server
```

---

## 60-Second Tutorial

### 1. Install Python SDK

```bash
pip install waffledb
```

### 2. Create and Insert Vectors

```python
from waffledb import WaffleDBClient

client = WaffleDBClient("http://localhost:8080")

# Create collection
client.create_collection(
    "products",
    vectors={
        "embedding": {"vector_type": "dense", "dimension": 384}
    }
)

# Insert vectors
client.insert_dense("products", [
    {"id": "prod_1", "vector": [0.1, 0.2, 0.3, ...]},
    {"id": "prod_2", "vector": [0.15, 0.25, 0.35, ...]},
])
```

### 3. Search

```python
results = client.search_dense(
    "products",
    query_vector=[0.12, 0.22, 0.32, ...],
    top_k=10
)

for result in results:
    print(f"ID: {result.id}, Score: {result.score}")
```

### 4. Hybrid Search (Dense + BM25)

```python
results = client.search_hybrid(
    "products",
    dense_query=[0.1, 0.2, 0.3, ...],
    bm25_query="wireless headphones",
    weights={"dense": 0.6, "bm25": 0.4},
    top_k=10
)
```

---

## Core Features

### ✅ Vector Types

- **Dense**: HNSW-indexed, sub-5ms search
- **Sparse**: BM25-like indexing, TF-IDF scoring
- **Multi-vector**: Multiple vectors per document, automatic fusion
- **Text**: Automatic BM25 indexing

### ✅ Search Modes

- **Dense Search**: Graph-based HNSW
- **Sparse Search**: Inverted index (BM25)
- **Hybrid Search**: Automated fusion of all modalities
- **Metadata Filtering**: Pre-filter before graph traversal
- **Multi-vector Search**: Weighted combination

### ✅ Performance

- **Insert**: 30K+/sec
- **Search P99**: <10ms
- **Compression**: PQ8 (90% size reduction)
- **Binary Size**: 5MB
- **Memory**: ~1-2GB per 1M vectors

### ✅ Durability

- **WAL**: Write-ahead logging
- **Snapshots**: Point-in-time recovery
- **Crash Recovery**: Instant reload on restart
- **RAFT**: Optional clustering

---

## API Reference

### Collections

```python
# Create
client.create_collection("my_collection", {...})

# Delete
client.delete_collection("my_collection")

# List
collections = client.list_collections()

# Stats
stats = client.get_stats("my_collection")

# Schema
schema = client.get_schema("my_collection")
```

### Dense Vectors

```python
# Insert
client.insert_dense("collection", [
    {"id": "vec1", "vector": [...], "metadata": {...}}
])

# Search
results = client.search_dense("collection", [0.1, 0.2, ...], top_k=10)

# Get
vector = client.get_vector("collection", "vec1")

# Delete
client.delete_vector("collection", "vec1")

# Upsert
client.upsert_dense("collection", [...])
```

### Sparse Vectors

```python
# Insert
client.insert_sparse("collection", [
    {"id": "sparse1", "indices": [1, 5, 10], "values": [0.9, 0.7, 0.6]}
])

# Search
results = client.search_sparse("collection", indices=[1, 5, 10], values=[0.9, 0.7, 0.6])
```

### Multi-Vector

```python
# Insert
client.insert_multi_vector("collection", [
    {"id": "multi1", "vectors": {"title_emb": [...], "body_emb": [...]}}
])

# Search
results = client.search_multi_vector("collection", 
    query_vectors={"title_emb": [...], "body_emb": [...]},
    weights={"title_emb": 0.3, "body_emb": 0.7}
)
```

### Hybrid Search

```python
results = client.search_hybrid(
    "collection",
    dense_query=[...],
    sparse_indices=[1, 5, 10],
    sparse_values=[0.9, 0.7, 0.6],
    bm25_query="search terms",
    weights={"dense": 0.5, "sparse": 0.3, "bm25": 0.2},
    top_k=10
)
```

### Metadata Filtering

```python
results = client.search_with_filter(
    "collection",
    query_vector=[...],
    filter={"category": "electronics", "price": {"$lt": 100}},
    top_k=10
)
```

### BM25 Search

```python
results = client.search_bm25("collection", "search query", top_k=10)
```

### Snapshots

```python
# Create
client.create_snapshot("collection", "backup_name")

# List
snapshots = client.list_snapshots()

# Restore
client.restore_snapshot("snapshot_id", "collection")
```

### Server Health

```python
# Health check
health = client.health()

# Ready check
ready = client.is_ready()
```

---

## Configuration

### Environment Variables

```bash
# Server
export WAFFLEDB_PORT=8080
export WAFFLEDB_GRPC_PORT=50051
export WAFFLEDB_DATA_DIR=/data

# Performance
export WAFFLEDB_EF_CONSTRUCTION=50      # HNSW ef_construction
export WAFFLEDB_EF_SEARCH=10            # HNSW ef_search
export WAFFLEDB_BUFFER_SIZE=10000       # Write buffer capacity

# Clustering
export WAFFLEDB_MODE=single             # or "distributed"
export WAFFLEDB_RAFT_ENABLED=false      # RAFT replication

# Logging
export RUST_LOG=info                    # debug, info, warn, error
```

### Docker Compose

```bash
docker-compose -f docker-compose.prod.yml up
```

---

## Performance Tuning

### Dense Vector Search

- **ef_construction**: Higher = better quality, slower build (default: 50)
- **ef_search**: Higher = better recall, slower search (default: 10)

For fastest search: `ef_search=5`
For best quality: `ef_construction=100`

### Compression

Enable PQ8 to reduce memory by 90%:

```python
client.create_collection("collection", {
    "embedding": {
        "vector_type": "dense",
        "dimension": 384,
        "compression": "pq8"
    }
})
```

### Batch Operations

Insert in batches for throughput:

```python
batch = [{"id": f"vec_{i}", "vector": [...]} for i in range(1000)]
client.insert_dense("collection", batch)
```

---

## Monitoring

### Metrics Endpoint

```bash
curl http://localhost:8080/metrics
```

Returns Prometheus metrics:
- `waffledb_insert_latency_ms`
- `waffledb_search_latency_ms`
- `waffledb_vectors_total`
- `waffledb_memory_bytes`

### Health Checks

```bash
# Liveness
curl http://localhost:8080/liveness

# Readiness
curl http://localhost:8080/ready

# Full health
curl http://localhost:8080/health
```

---

## Deployment

### Single Node

```bash
docker run -p 8080:8080 waffledb/waffledb:latest
```

### Multi-Node Cluster

```bash
docker-compose -f deployments/docker/docker-compose.prod.yml up -d
```

Sets up RAFT replication for HA.

### Kubernetes

```bash
kubectl apply -f deployments/k8s/waffledb-deployment.yaml
```

---

## Troubleshooting

### Port Already in Use

```bash
# Change port
export WAFFLEDB_PORT=8081
./waffledb-server
```

### High Memory Usage

Enable compression:

```python
client.create_collection("collection", {
    "embedding": {
        "compression": "pq8"
    }
})
```

### Slow Search

Decrease `ef_search`:

```bash
export WAFFLEDB_EF_SEARCH=5
```

### Connection Refused

Ensure server is running:

```bash
curl http://localhost:8080/health
```

---

## Examples

### Product Search

```python
from waffledb import WaffleDBClient

client = WaffleDBClient()

# Create collection
client.create_collection("products", {
    "description": {"vector_type": "dense", "dimension": 384}
})

# Insert products
products = [
    {
        "id": "product_1",
        "vector": [0.1, 0.2, ...],
        "metadata": {"category": "electronics", "price": 99.99}
    },
]
client.insert_dense("products", products)

# Search with filter
results = client.search_with_filter(
    "products",
    query_vector=[0.1, 0.2, ...],
    filter={"category": "electronics"},
    top_k=5
)
```

### Document Retrieval (RAG)

```python
# Create RAG collection
client.create_collection("documents", {
    "chunk_embedding": {"vector_type": "dense", "dimension": 384},
    "chunk_text": {"vector_type": "text"}
})

# Insert chunks
chunks = [
    {
        "id": "chunk_1",
        "vectors": {
            "chunk_embedding": [...],
        },
        "metadata": {"doc_id": "doc1", "page": 1}
    },
]
client.insert_dense("documents", chunks)

# Hybrid search (dense + BM25)
results = client.search_hybrid(
    "documents",
    dense_query=embed("What is machine learning?"),
    bm25_query="machine learning",
    weights={"dense": 0.7, "bm25": 0.3},
    top_k=5
)
```

---

## Limitations

- **Single shard in OSS mode** (multi-shard in enterprise)
- **No built-in auth** (use reverse proxy)
- **No encryption at rest** (use filesystem encryption)

For enterprise features, see [WaffleDB Cloud](https://cloud.waffledb.io).

---

## Support

- **Issues**: [GitHub Issues](https://github.com/badgerbees/WaffleDB/issues)
- **Discussions**: [GitHub Discussions](https://github.com/badgerbees/WaffleDB/discussions)
- **Slack**: [Join Community Slack](https://slack.waffledb.io)

---

## License

Apache 2.0 — See LICENSE file
