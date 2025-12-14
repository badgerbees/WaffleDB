#!/bin/bash
# WaffleDB Simple REST API Examples
# These shortcuts auto-create collections - zero friction!

set -e

BASE_URL="http://localhost:8080"

echo "🚀 WaffleDB Simple REST API Examples"
echo "===================================="
echo ""
echo "Make sure WaffleDB is running:"
echo "  docker run -p 8080:8080 waffledb"
echo ""

# ============================================================================
# 1. ADD VECTORS (auto-creates collection if needed)
# ============================================================================
echo "1️⃣  Adding vectors (auto-creates collection)..."

curl -s -X POST "$BASE_URL/collections/products/add" \
  -H "Content-Type: application/json" \
  -d '{
    "vectors": [
      {
        "id": "prod_1",
        "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
        "metadata": {"name": "Red Shoes", "price": 59.99, "category": "footwear"}
      },
      {
        "id": "prod_2",
        "vector": [0.15, 0.25, 0.35, 0.45, 0.55],
        "metadata": {"name": "Blue Shoes", "price": 79.99, "category": "footwear"}
      },
      {
        "id": "prod_3",
        "vector": [0.2, 0.3, 0.4, 0.5, 0.6],
        "metadata": {"name": "Black Shirt", "price": 29.99, "category": "clothing"}
      }
    ]
  }' | jq .

echo ""
echo "✅ Vectors added! Collection auto-created."
echo ""

# ============================================================================
# 2. SEARCH (simple query)
# ============================================================================
echo "2️⃣  Searching for similar products..."

curl -s -X POST "$BASE_URL/collections/products/search" \
  -H "Content-Type: application/json" \
  -d '{
    "embedding": [0.12, 0.22, 0.32, 0.42, 0.52],
    "limit": 3
  }' | jq .

echo ""
echo "✅ Search results returned!"
echo ""

# ============================================================================
# 3. BATCH SEARCH (search multiple queries)
# ============================================================================
echo "3️⃣  Batch searching (multiple queries)..."

curl -s -X POST "$BASE_URL/collections/products/batch_search" \
  -H "Content-Type: application/json" \
  -d '{
    "queries": [
      {
        "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
        "top_k": 2
      },
      {
        "embedding": [0.2, 0.3, 0.4, 0.5, 0.6],
        "top_k": 2
      }
    ]
  }' | jq .

echo ""
echo "✅ Batch search completed!"
echo ""

# ============================================================================
# 4. DELETE (multiple vectors)
# ============================================================================
echo "4️⃣  Deleting vectors..."

curl -s -X POST "$BASE_URL/collections/products/delete" \
  -H "Content-Type: application/json" \
  -d '{
    "ids": ["prod_1"]
  }' | jq .

echo ""
echo "✅ Vector deleted!"
echo ""

# ============================================================================
# 5. GET COLLECTION INFO
# ============================================================================
echo "5️⃣  Getting collection stats..."

curl -s -X GET "$BASE_URL/collections/products" | jq .

echo ""
echo "✅ Collection info retrieved!"
echo ""

# ============================================================================
# 6. GET DETAILED STATS
# ============================================================================
echo "6️⃣  Getting detailed statistics..."

curl -s -X GET "$BASE_URL/collections/products/stats" | jq .

echo ""
echo "✅ Stats retrieved!"
echo ""

# ============================================================================
# 7. CREATE SNAPSHOT
# ============================================================================
echo "7️⃣  Creating collection snapshot..."

curl -s -X POST "$BASE_URL/collections/products/snapshot" | jq .

echo ""
echo "✅ Snapshot created!"
echo ""

# ============================================================================
# 8. LIST ALL COLLECTIONS
# ============================================================================
echo "8️⃣  Listing all collections..."

curl -s -X GET "$BASE_URL/collections" | jq .

echo ""
echo "✅ Collections listed!"
echo ""

# ============================================================================
# 9. DELETE COLLECTION
# ============================================================================
echo "9️⃣  Deleting collection..."

curl -s -X DELETE "$BASE_URL/collections/products" | jq .

echo ""
echo "✅ Collection deleted!"
echo ""

echo "🎉 All examples completed!"
echo ""
echo "Key Takeaways:"
echo "  • POST /collections/{name}/add - Auto-creates collection"
echo "  • POST /collections/{name}/simple_search - Simple search"
echo "  • POST /collections/{name}/simple_delete - Delete vectors"
echo "  • GET /collections/{name} - Get collection info"
echo "  • POST /collections/{name}/snapshot - Backup collection"
echo "  • No explicit collection creation needed!"
echo ""
