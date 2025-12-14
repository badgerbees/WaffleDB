#!/usr/bin/env python3
"""
WaffleDB Python SDK - Simplest Possible Example
Copy-paste this and it just works. No setup, no config.
"""

from waffledb import client

# Add 2 documents (collection auto-creates!)
client.add("docs", 
    ids=["doc1", "doc2"],
    embeddings=[[0.1]*384, [0.2]*384],
    metadata=[{"title": "Python Basics"}, {"title": "Advanced Python"}]
)

# Search for similar
results = client.search("docs", [0.15]*384, limit=5)

# Print results
for r in results:
    print(f"ID: {r.id}, Score: {r.score:.4f}, Title: {r.metadata.get('title') if r.metadata else 'N/A'}")
