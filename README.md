# WaffleDB 🧇

**A lightweight, high-performance vector database built in Rust.**

WaffleDB is designed to be **fast**, **efficient**, and **easy to use**. It provides semantic search capabilities for embeddings with zero external dependencies—perfect for RAG applications, semantic search, and vector similarity tasks.

## Why WaffleDB?

### ⚡ Fast
- **HNSW indexing** for O(log n) search performance
- Rust + Actix-web for blazing-fast REST API
- Parallel batch processing (20-50% faster ingestion)
- Zero JVM overhead

### 💪 Better
- **Native metadata support** with JSON flexibility
- **Duplicate handling policies** for deterministic ingestion
- **Collection introspection** with detailed stats
- **Metadata filtering** in search queries
- Automatic type conversion (numbers → strings)
- **Update and patch operations** for embeddings and metadata
- **True delete support** with index cleanup

### 🎯 Lightweight
- Single binary deployment (~50MB)
- No external dependencies (no Redis, no PostgreSQL)
- Docker image under 200MB
- Low memory footprint for production use

## Quick Start

### Docker (Recommended)
```bash
docker-compose -f deployments/docker/docker-compose.yml up
```

Then test it:
```bash
curl -X POST http://localhost:8080/api/collections \
  -H "Content-Type: application/json" \
  -d '{"name": "docs", "dimension": 384}'
```

### Local Development
```bash
# Build
cargo build --release

# Run server
cargo run --release -p waffledb-server

# Server runs on http://localhost:8080
```

## Usage

### Python Client
```bash
pip install -e python-sdk/
```

```python
from waffledb import WaffleDBClient

client = WaffleDBClient(host="localhost", http_port=8080)

# Insert vectors with metadata
client.insert(
    vector=[0.1, 0.2, 0.3, ...],
    vector_id="doc_001",
    metadata={"source": "docs/readme.txt", "section": "intro"}
)

# Search
results = client.search(
    vector=[0.1, 0.2, 0.3, ...],
    top_k=5,
    include_metadata=True
)

for result in results:
    print(f"ID: {result['id']}, Distance: {result['distance']}")
    print(f"Metadata: {result['metadata']}")
```

### REST API
```bash
# Create collection
curl -X POST http://localhost:8080/api/collections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "docs",
    "dimension": 384,
    "duplicate_policy": "overwrite"
  }'

# Insert vector
curl -X POST http://localhost:8080/api/collections/docs/insert \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc_001",
    "vector": [0.1, 0.2, 0.3, ...],
    "metadata": {
      "source": "readme.txt",
      "type": "0"
    }
  }'

# Search
curl -X POST http://localhost:8080/api/collections/docs/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, ...],
    "top_k": 5,
    "include_metadata": true
  }'

# Get collection stats
curl http://localhost:8080/api/collections/docs/stats
```

## Features

### ✅ Core
- **Vector Storage** - HNSW indexing for similarity search
- **Metadata Management** - JSON metadata with automatic type conversion
- **Batch Operations** - Parallel insertion for 20-50% performance gain
- **Collection Management** - Create, list, delete collections
- **Filtering** - Filter results by metadata after search

### ✅ Advanced
- **Duplicate Policies** - Overwrite or reject duplicate IDs
- **Duplicate Detection** - Automatic handling of re-ingestion
- **Update Operations** - Update vector embeddings or metadata
- **Delete Support** - Remove vectors with index cleanup
- **Health Monitoring** - Collection stats and diagnostics

### ✅ APIs
- **REST API** - Full-featured HTTP endpoints
- **Python SDK** - Pythonic client library
- **Rust** - Native Rust bindings

## Project Structure

```
WaffleDB/
├── waffledb-server/       # REST & gRPC server
├── waffledb-core/         # Core indexing engine
├── waffledb-distributed/  # Distributed features (future)
├── python-sdk/            # Python client
├── deployments/docker/    # Docker configuration
├── examples/              # Usage examples
└── README.md              # This file
```

## Performance

Tested with 100K embeddings (384-dim):
- **Insert**: ~35 vectors/sec (sequential), ~100+ vectors/sec (parallel)
- **Search**: <10ms for top-5 results
- **Memory**: ~150MB for 100K vectors
- **Build Time**: ~2.5 minutes (release mode)

## Getting Started

See [GETTING_STARTED.md](./GETTING_STARTED.md) for detailed setup and tutorials.

## Contributing

Contributions welcome! Please submit issues and PRs on GitHub.

## License

MIT - See [LICENSE](./LICENSE) for details.

## Support

- 📖 **Documentation**: See examples/ and GETTING_STARTED.md
- 🐛 **Issues**: GitHub Issues
- 💬 **Discussions**: GitHub Discussions

---

**WaffleDB** - Simple. Fast. Lightweight. Vector Database.
