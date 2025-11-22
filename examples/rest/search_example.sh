#!/bin/bash

# REST API example script

BASE_URL="http://localhost:8080/api"

echo "=== WaffleDB REST API Example ==="

# Insert vectors
echo "Inserting vectors..."
curl -X POST "$BASE_URL/insert" \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4],
    "metadata": {"name": "vector1", "type": "text"}
  }'

curl -X POST "$BASE_URL/insert" \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.15, 0.25, 0.35, 0.45],
    "metadata": {"name": "vector2", "type": "text"}
  }'

# Search
echo ""
echo "Searching for similar vectors..."
curl -X POST "$BASE_URL/search" \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.12, 0.22, 0.32, 0.42],
    "top_k": 5
  }'

# Health check
echo ""
echo "Health check..."
curl http://localhost:8080/health

