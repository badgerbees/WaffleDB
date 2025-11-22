# WaffleDB 🧇

**A lightweight, high-performance vector database built in Rust.**

WaffleDB is designed to be **fast**, **efficient**, and **easy to use**. It provides semantic search capabilities for embeddings with zero external dependencies—perfect for RAG applications, semantic search, and vector similarity tasks.

## Why WaffleDB?

### ⚡ Blazing Fast
- **Hybrid Engine** - Buffered inserts (30-35K vectors/sec) + async HNSW building
- **1.25M inserts/sec** on single core (buffer throughput)
- **1-2ms search latency** (P50/P99) with automatic layer fusion
- **O(log n) search** via HNSW indexing with brute-force during HNSW builds
- Rust + Actix-web for zero overhead
- Zero copy operations where possible

### 💪 Better Architecture
- **Dual-layer indexing** - Hot buffer (fast inserts) + Warm HNSW (fast search)
- **Non-blocking builds** - HNSW construction happens async, inserts never block
- **Multi-layer search** - Query both layers in parallel, automatic merging
- **Native metadata support** with JSON flexibility
- **Duplicate handling policies** for deterministic ingestion
- **Metadata filtering** in search queries
- **Update and patch operations** for embeddings and metadata
- **True delete support** with index cleanup

### 🎯 Production-Ready
- Single binary deployment (~6.9MB optimized)
- No external dependencies (no Redis, no PostgreSQL)
- Docker image under 50MB
- Built-in observability with detailed metrics
- 100% backward compatible (can use pure HNSW mode)

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

### ✅ Hybrid Engine (Default)
- **Buffered Inserts** - Append-only write buffer for O(1) inserts
- **Async HNSW Building** - Non-blocking graph construction in background
- **Multi-Layer Search** - Automatic query on both hot and warm layers
- **Smart Capacity Management** - Auto-trigger HNSW build when buffer fills
- **Zero Blocking** - Inserts never wait for index builds

### ✅ Core Storage
- **Vector Storage** - HNSW or Hybrid indexing for similarity search
- **Metadata Management** - JSON metadata with automatic type conversion
- **Batch Operations** - Parallel insertion for performance
- **Collection Management** - Create, list, delete collections
- **Filtering** - Filter results by metadata after search

### ✅ Advanced Features
- **Duplicate Policies** - Overwrite or reject duplicate IDs
- **Duplicate Detection** - Automatic handling of re-ingestion
- **Update Operations** - Update vector embeddings or metadata
- **Delete Support** - Remove vectors with index cleanup
- **Health Monitoring** - Collection stats and diagnostics
- **Multiple Engine Modes** - Switch between Hybrid and pure HNSW

### ✅ APIs
- **REST API** - Full-featured HTTP endpoints
- **Python SDK** - Pythonic client library
- **Rust** - Native Rust bindings

## Performance

### Hybrid Engine (Default)
Tested with 100K embeddings (384-dim) on modern hardware:
- **Insert Throughput**: 1.25M vectors/sec (buffer layer), sustained 30-35K with HNSW builds
- **Search Latency P50**: 1-2ms (multi-layer search)
- **Search Latency P99**: 2-3ms (multi-layer search)
- **Memory**: ~0.78 KB per vector (with compression)
- **HNSW Build Time**: Async, non-blocking (happens in background)
- **Insert Blocking**: Zero - new buffer created while HNSW builds

### Pure HNSW Mode
- **Insert**: ~35 vectors/sec (sequential)
- **Search**: <10ms for top-5 results
- **Memory**: ~150MB for 100K vectors

*The hybrid engine combines the best of both: ultra-fast inserts of the buffer layer with the search performance of HNSW*

## How the Hybrid Engine Works

WaffleDB's hybrid engine uses a three-layer architecture optimized for real-world workloads:

```
┌─────────────────────────────────────────────────────┐
│  Incoming Insert Request                            │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  Layer 1: WriteBuffer (HOT)                         │
│  • Append-only, O(1) inserts                        │
│  • Brute-force search capability                    │
│  • Capacity: 10K vectors (configurable)             │
│  • Speed: 1.25M inserts/sec                         │
└─────────────────────┬───────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
    Buffer fills?           Query arrives?
         │                         │
         ▼                         ▼
   [Auto-trigger]          [Multi-layer search]
   async build                  │
         │                      ├─► Query WriteBuffer (brute-force)
         ▼                      │
┌─────────────────────────────────────────────────────┐
│  Layer 2: HNSW (WARM)                               │
│  • Built asynchronously in background               │
│  • Graph-based O(log n) search                      │
│  • Parallel construction                            │
│  • Search: 1-2ms latency                            │
└──────────────────────────────────────────────────────┘
         │
         └─► Merge results by ID
         │
         └─► Return deduplicated top-k
```

**Key Benefits:**
- **Inserts never block** - writes go to buffer immediately
- **Searches return fast** - queries both layers in parallel
- **Automatic tuning** - HNSW builds when buffer fills
- **Zero configuration** - works out of the box

## Configuration

You can customize the hybrid engine behavior:

```bash
# Use pure HNSW mode (all vectors indexed upfront)
curl -X POST http://localhost:8080/collections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "docs",
    "dimension": 384,
    "engine": "hnsw"  # or "hybrid" (default)
  }'
```

Or programmatically with the Rust library:

```rust
use waffledb::HybridEngine;

// Default: 10K buffer capacity, ef_construction=100, ef_search=50
let engine = HybridEngine::new();

// Custom config
let engine = HybridEngine::with_config(
    5_000,  // buffer_capacity
    200,    // ef_construction (higher = better quality, slower build)
    100,    // ef_search (higher = more accurate search)
);
```

## Project Structure

```
WaffleDB/
├── waffledb-core/         # Core hybrid engine
│   └── src/buffer/        # WriteBuffer + MultiLayerSearcher
├── waffledb-server/       # REST API server
│   ├── src/engines/       # Engine implementations
│   └── src/handlers/      # HTTP request handlers
├── waffledb-distributed/  # Distributed features (future)
├── python-sdk/            # Python client
├── deployments/docker/    # Docker configuration
├── examples/              # Benchmarks and examples
└── README.md              # This file
```

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
