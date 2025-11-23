# WaffleDB: The Fastest, Smallest Vector Database

**Production-ready vector database in a single 5MB binary. Zero external dependencies.**

---

## Why WaffleDB?

| Feature | WaffleDB | Qdrant | Milvus | Chroma |
|---------|----------|--------|--------|---------|
| **Binary Size** | 5MB | 100MB+ | 500MB+ | Requires Python (1GB+) |
| **Startup Time** | <50ms | 2-5s | 10-30s | 5-15s |
| **P99 Latency** | <10ms | 50-100ms | 100-500ms | 200-1000ms |
| **Single Binary** | ✅ | ❌ | ❌ | ❌ |
| **No Dependencies** | ✅ Rust | ❌ C++ | ❌ C++ | ❌ Python |

**Pick WaffleDB if you want simplicity, speed, and zero operational overhead.**

---

## 60 Seconds to Running

```bash
# Start server
docker run -p 8080:8080 waffledb/waffledb:latest

# Insert vectors (Python)
from waffledb import WaffleClient
client = WaffleClient("http://localhost:8080")
client.insert_dense("docs", [{"id": "1", "vector": [0.1, 0.2, ...], "metadata": {"title": "Hello"}}])

# Search
results = client.search_dense("docs", query_vector=[0.1, 0.2, ...], top_k=5)
for r in results:
    print(f"ID: {r.id}, Score: {r.score}")
```

---

## What's Included

✅ Dense vector search (HNSW)  
✅ Metadata filtering (pre-computed indexes)  
✅ Batch insert/search  
✅ Vector updates & deletes  
✅ Snapshots for durability  
✅ REST API + Python SDK  
✅ Multi-collection support  
✅ Health checks & metrics  

---

## APIs

### REST

```bash
# Create collection
POST /collections
{"name": "docs"}

# Insert vectors
POST /collections/{name}/insert
{"vectors": [{"id": "1", "vector": [...], "metadata": {"title": "..."}}]}

# Search
POST /collections/{name}/search
{"vector": [...], "top_k": 5}

# Search with metadata filter
POST /collections/{name}/search
{"vector": [...], "filter": {"title": "hello"}, "top_k": 5}

# Get stats
GET /collections/{name}/stats

# Create snapshot
POST /collections/{name}/snapshot
{"name": "backup_v1"}
```

### Python SDK

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

# Collections
client.create_collection("docs")
client.list_collections()
client.get_stats("docs")

# Insert/Search
client.insert_dense("docs", [{"id": "1", "vector": [...], "metadata": {...}}])
results = client.search_dense("docs", query_vector=[...], top_k=5)
results = client.search_dense("docs", query_vector=[...], top_k=5, filter={"category": "A"})

# Get/Update/Delete
client.get_vector("docs", "1")
client.upsert_dense("docs", [{"id": "1", "vector": [...]}])
client.delete_vector("docs", "1")

# Batch
client.batch_insert("docs", [{"id": "1", "vector": [...]}, ...])
results = client.batch_search("docs", [{"vector": [...]}, ...])

# Snapshots
client.create_snapshot("docs", "backup")
client.restore_snapshot("snapshot_id", "docs")
client.list_snapshots()

# Health
client.health()
client.is_ready()
```

---

## Performance

Benchmarks on t3.xlarge (4 vCPU, 16GB RAM):

- **Dense insert**: 50k+ vectors/sec (768-dim)
- **Dense search**: 10k+ queries/sec (P99 <10ms)
- **Metadata filter**: 5-15ms overhead
- **Memory**: ~50 bytes per vector

Full details: [ARCHITECTURE.md](./ARCHITECTURE.md)

---

## Getting Started

### Docker
```bash
docker run -p 8080:8080 -v waffledb_data:/data waffledb/waffledb:latest
```

### Binary
```bash
./waffledb
# Server on http://localhost:8080
```

### Python SDK
```bash
pip install waffledb
```

### From Source
```bash
cargo build --release
cargo run --release -p waffledb-server
```

---

## Use Cases

**Embeddings Storage** - Simple, fast alternative to Pinecone for small/medium teams  
**Product Search** - Multi-collection with filtered search by category/price/ratings  
**Similarity Detection** - Find duplicates or related items quickly  
**Document Search** - Store docs with dense vectors, search with metadata filters  

---

## Production Ready

✅ 199 tests passing, 14 perf gates validated  
✅ Durability via snapshots  
✅ Health checks & metrics  
✅ Docker support  
✅ Python SDK  
✅ Backward compatible  

---

## Next

- [Full Docs](./OSS_QUICKSTART.md)
- [Architecture](./ARCHITECTURE.md)
- [GitHub](https://github.com/waffledb/waffledb)

---

**Made for developers who want boring reliability over complex features.**

MIT Licensed
