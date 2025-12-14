# WaffleDB

**A high-performance, distributed vector database built in Rust.**

WaffleDB is an open-source vector database optimized for semantic search, RAG systems, and AI applications. Built with async Rust and Actix-web, it delivers sub-10ms latency with HNSW indexing, hybrid search, and enterprise features like multi-tenancy and distributed deployment.

[Website](https://waffledb.io) • [Docs](./docs) • [Discord](#) • [GitHub Issues](https://github.com/waffledb/waffledb)

---

## Features

- **⚡ Ultra-Low Latency** - P99 <10ms vector operations with HNSW indexing
- **🚀 Zero Setup** - Single binary, no external dependencies, auto-collection creation
- **📦 Production Ready** - Multi-tenancy, RAFT-based replication, snapshots, and WAL
- **🔄 Hybrid Search** - Combine vector + keyword search in a single query
- **🐍 Official SDKs** - Python, JavaScript, Rust with type safety
- **📊 Observable** - Built-in metrics, health checks, and observability
- **🔐 Secure by Default** - Multi-tenancy isolation, API keys for cloud

---

## Quick Start

### Start Server

```bash
# Compile from source
cargo build --release
./target/release/waffledb-server

# Or use Docker
docker run -p 8080:8080 waffledb/waffledb:latest
```

### Python SDK

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

# Add vectors (collection auto-creates!)
client.add(
    "my_collection",
    ids=["doc1", "doc2"],
    embeddings=[[0.1]*384, [0.2]*384],
    metadata=[{"title": "Intro"}, {"title": "Advanced"}]
)

# Search
results = client.search("my_collection", [0.15]*384, limit=5)
for r in results:
    print(f"ID: {r.id}, Score: {r.score:.4f}")
```

Install: `pip install waffledb`

### REST API

```bash
# Add vectors
curl -X POST http://localhost:8080/collections/my_collection/add \
  -H "Content-Type: application/json" \
  -d '{
    "ids": ["doc1", "doc2"],
    "embeddings": [[0.1, 0.2, ...], [0.3, 0.4, ...]],
    "metadata": [{"title": "Intro"}, {"title": "Advanced"}]
  }'

# Search
curl -X POST http://localhost:8080/collections/my_collection/search \
  -H "Content-Type: application/json" \
  -d '{
    "embedding": [0.15, 0.25, ...],
    "limit": 5
  }'
```

---

## Architecture

WaffleDB consists of three main components:

- **waffledb-core** - HNSW indexing, vector operations, and storage engine
- **waffledb-server** - Actix-web REST API and multi-tenancy layer
- **waffledb-distributed** - RAFT replication and distributed deployment

See [`/docs/architecture`](./docs) for detailed architecture documentation.

---

## Development

### Build

```bash
cargo build --release
```

### Run Tests

```bash
cargo test --release
```

### Run Benchmarks

```bash
cargo bench
```

---

## Documentation

Full documentation, architecture guides, and benchmarking results are in [`/docs`](./docs).

For a separate docs website, see [waffledb-docs](https://github.com/waffledb/waffledb-docs).

---

## License

AGPL-3.0 License. See [LICENSE](./LICENSE) for details.

---

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to our [GitHub](https://github.com/waffledb/waffledb).

---

## Community

- **Discussions** - GitHub Discussions
- **Issues** - [GitHub Issues](https://github.com/waffledb/waffledb/issues)
- **Discord** - Join our community server
