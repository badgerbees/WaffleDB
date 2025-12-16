# WaffleDB

**A lightweight, fast vector database written in Rust. Deploy it, forget about it.**

WaffleDB is a vector database built to be simple and small. Single binary, no dependencies, runs on a laptop. Great for semantic search, retrieval systems, and anything that needs to store and search vectors quickly.

[Website](https://waffledb.ibuildanything.com) • [Docs](https://waffledb.ibuildanything.com/docs) • [GitHub](https://github.com/badgerbees/waffledb)

---

## Why WaffleDB?

- **Tiny** - Single binary, 5MB. No Docker, no cluster, no dependencies.
- **Fast** - Sub-10ms vector search. HNSW indexing built-in.
- **Simple** - Zero config. Collections auto-create. Works immediately.
- **Solid** - RAFT replication, snapshots, and multi-tenancy when you need it.

---

## Get Started

### Run the server

```bash
cargo build --release
./target/release/waffledb-server
```

It listens on `http://localhost:8080`.

### Python SDK

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

# Add some vectors
client.add(
    "products",
    ids=["item1", "item2"],
    embeddings=[[0.1, 0.2, ...], [0.3, 0.4, ...]],
    metadata=[{"name": "Widget"}, {"name": "Gadget"}]
)

# Search
results = client.search("products", [0.15, 0.25, ...], limit=10)
print(results)
```

`pip install waffledb`

### REST API

```bash
curl -X POST http://localhost:8080/collections/products/add \
  -H "Content-Type: application/json" \
  -d '{
    "ids": ["item1", "item2"],
    "embeddings": [[0.1, 0.2], [0.3, 0.4]],
    "metadata": [{"name": "Widget"}, {"name": "Gadget"}]
  }'
```

---

## How it works

WaffleDB is built from three pieces:

- **waffledb-core** - Vector indexing and search (HNSW)
- **waffledb-server** - REST API and multi-tenant support
- **waffledb-distributed** - RAFT replication for clustering (optional)

---

## Build & Test

```bash
# Build
cargo build --release

# Run tests
cargo test --release

# Run benchmarks
cargo bench
```

---

## Documentation

Full documentation is available at [waffledb.ibuildanything.com/docs](https://waffledb.ibuildanything.com/docs)

---

## License

AGPL-3.0. See [LICENSE](./LICENSE).

---

## Questions?

- Open an issue on [GitHub](https://github.com/badgerbees/waffledb/issues)
- Check the [Documentation](https://waffledb.ibuildanything.com/docs)
