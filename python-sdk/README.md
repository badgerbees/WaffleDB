# WaffleDB Python SDK - Dead Simple, Fully Featured

Vector search with 2 lines of code. Auto-creates everything. Handles every use case.

## Installation

```bash
pip install waffledb
```

## 5-Minute Start

### 1. Start Server

```bash
docker run -p 8080:8080 waffledb
```

### 2. Add & Search

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

# Add vectors (collection auto-creates!)
client.add("docs", 
    ids=["doc1", "doc2"],
    embeddings=[[0.1]*384, [0.2]*384],
    metadata=[{"title": "A"}, {"title": "B"}]
)

# Search
results = client.search("docs", [0.15]*384)
for r in results:
    print(f"{r.id}: {r.score:.4f}")
```

Done! No setup, no config, everything auto-created.

---

## Core API

| Method | Purpose |
|--------|---------|
| `add(collection, ids, embeddings, metadata)` | Add/insert vectors |
| `search(collection, embedding, limit)` | Find similar |
| `delete(collection, ids)` | Remove vectors |
| `get(collection, id)` | Get one vector |
| `update(collection, id, embedding)` | Update embedding |
| `update_metadata(collection, id, metadata)` | Update metadata |
| `batch_search(collection, queries)` | Multi-query |
| `list()` | List collections |
| `info(collection)` | Collection stats |
| `drop(collection)` | Delete collection |
| `snapshot(collection, name)` | Backup |
| `health()` | Server health |

---

## Advanced Features (v0.2.0+)

| Method | Purpose |
|--------|---------|
| `delete_by_filter(collection, filter)` | Bulk delete with criteria |
| `update_by_filter(collection, filter, metadata)` | Bulk metadata update |
| `scroll(collection, limit, offset)` | Pagination support |
| `aggregate(collection, operation, field)` | Statistics (count/sum/avg/min/max) |
| `rerank(collection, candidates, query_embedding)` | Re-rank results |
| `stats(collection)` | Detailed collection info |

---

## Real World Examples

### RAG / Semantic Search

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

docs = load_documents()
client.add("kb", ids=[d["id"] for d in docs], embeddings=[d["emb"] for d in docs], metadata=[{"text": d["text"]} for d in docs])

results = client.search("kb", embed("What is Python?"), limit=5)
context = "\n".join(r.metadata["text"] for r in results)
answer = llm.ask(f"Based on: {context}")
```

### Recommendations

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

users = load_users()
client.add("users", ids=[u["id"] for u in users], embeddings=[u["emb"] for u in users], metadata=[{"name": u["name"]} for u in users])

similar = client.search("users", user_embedding, limit=10)
print([r.metadata["name"] for r in similar])
```

### Product Search

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

products = load_products()
client.add("products", ids=[p["id"] for p in products], embeddings=[p["emb"] for p in products], metadata=[{"name": p["name"], "price": p["price"]} for p in products])

results = client.search("products", embed("blue running shoes under 100"), limit=20)
for r in results:
    if r.metadata["price"] < 100:
        print(f"{r.metadata['name']}: ${r.metadata['price']}")
```

### Image Search

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

images = load_images()
client.add("images", ids=[img["id"] for img in images], embeddings=[img["emb"] for img in images], metadata=[{"url": img["url"]} for img in images])

results = client.search("images", image_embedding, limit=20)
for r in results:
    print(r.metadata["url"])
```

### Duplicate Detection

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

docs = load_docs()
client.add("documents", ids=[d["id"] for d in docs], embeddings=[d["emb"] for d in docs], metadata=[{"text": d["text"]} for d in docs])

for doc in docs:
    similar = client.search("documents", doc["emb"], limit=5)
    duplicates = [r for r in similar[1:] if r.score > 0.95]
    if duplicates:
        print(f"Doc {doc['id']} duplicated: {[r.id for r in duplicates]}")
```

### Time Series Patterns

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

windows = extract_time_windows(data)
client.add("patterns", ids=[w["id"] for w in windows], embeddings=[w["emb"] for w in windows], metadata=[{"ts": w["ts"]} for w in windows])

current = extract_window(latest_data)
similar = client.search("patterns", current["emb"], limit=10)
if similar[0].score < 0.8:
    print("Anomaly detected!")
```

### Multi-Tenant

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

for tenant in tenants:
    docs = load_tenant_docs(tenant.id)
    client.add(f"tenant_{tenant.id}", ids=[d["id"] for d in docs], embeddings=[d["emb"] for d in docs])

results = client.search(f"tenant_{tenant_id}", query_emb)
```

### Bulk Operations & Pagination (v0.2.0+)

```python
from waffledb import WaffleClient

client = WaffleClient("http://localhost:8080")

# Bulk delete old documents
client.delete_by_filter("docs", {"created": {"$lt": "2024-01-01"}})

# Bulk update metadata
client.update_by_filter("docs", {"status": "archived"}, {"active": False})

# Scroll through large collections
page = 0
while True:
    results = client.scroll("docs", limit=100, offset=page*100)
    if not results:
        break
    process_batch(results)
    page += 1

# Get collection statistics
stats = client.aggregate("docs", "count")
print(f"Total vectors: {stats['count']}")

avg_score = client.aggregate("docs", "avg", "score")
print(f"Average score: {avg_score}")
```

---

## Configuration

```python
from waffledb import WaffleClient

# Connect to server with custom timeout
client = WaffleClient("http://localhost:8080", timeout=60)
```

---

**Dead simple. Fully featured. 49.5K vectors/sec.**

See the [GitHub repo](https://github.com/waffledb/waffledb) for more.
