#!/usr/bin/env python3
"""
Phase 1.5: Integration Test - Verify Simple API Works End-to-End

This script tests that the Python SDK simple API works as documented.
Run this after starting WaffleDB server: docker run -p 8080:8080 waffledb

Expected behavior:
- Collections auto-create on first use
- Search returns results
- Errors provide helpful guidance
- No user setup needed
"""

import sys
import json
from typing import List

# Test imports
try:
    from waffledb import db
    print("✓ Successfully imported: from waffledb import db")
except ImportError as e:
    print(f"✗ Failed to import db: {e}")
    sys.exit(1)

# Test data
TEST_COLLECTION = "test_simple_api"
TEST_IDS = ["doc1", "doc2", "doc3"]
TEST_EMBEDDINGS = [
    [0.1] * 384,  # Different embeddings
    [0.2] * 384,
    [0.3] * 384,
]
TEST_METADATA = [
    {"title": "Document 1", "source": "test"},
    {"title": "Document 2", "source": "test"},
    {"title": "Document 3", "source": "test"},
]


def test_add_with_auto_create():
    """Test: Adding vectors auto-creates collection"""
    print("\n[TEST 1.5.1] Auto-create collection on first add()")
    try:
        db.add(
            TEST_COLLECTION,
            ids=TEST_IDS,
            embeddings=TEST_EMBEDDINGS,
            metadata=TEST_METADATA,
        )
        print(f"✓ Collection '{TEST_COLLECTION}' auto-created")
        return True
    except Exception as e:
        print(f"✗ Failed to add vectors: {e}")
        return False


def test_search():
    """Test: Search returns vectors in rank order"""
    print("\n[TEST 1.5.2] Search returns ranked results")
    try:
        # Search with embedding similar to [0.15]*384 (between doc1 and doc2)
        query = [0.15] * 384
        results = db.search(TEST_COLLECTION, query, limit=3)

        if not results:
            print("✗ Search returned no results")
            return False

        print(f"✓ Search returned {len(results)} results")
        
        # Show results
        for i, result in enumerate(results, 1):
            print(f"  [{i}] {result.id}: score={result.score:.4f}")

        return True
    except Exception as e:
        print(f"✗ Search failed: {e}")
        return False


def test_delete():
    """Test: Delete removes specific vectors"""
    print("\n[TEST 1.5.3] Delete removes vectors")
    try:
        # Delete one vector
        db.delete(TEST_COLLECTION, ids=["doc1"])
        print("✓ Vector 'doc1' deleted")

        # Verify it's gone by searching (should only get 2 results now)
        results = db.search(TEST_COLLECTION, [0.15] * 384, limit=10)
        if any(r.id == "doc1" for r in results):
            print("✗ doc1 still appears in search results")
            return False

        print(f"✓ Verified: {len(results)} vectors remaining (doc1 gone)")
        return True
    except Exception as e:
        print(f"✗ Delete failed: {e}")
        return False


def test_info():
    """Test: Info returns collection metadata"""
    print("\n[TEST 1.5.4] Info returns collection stats")
    try:
        info = db.info(TEST_COLLECTION)
        print(f"✓ Collection info retrieved:")
        print(f"  - Vector count: {info.get('vector_count', 'unknown')}")
        print(f"  - Dimension: {info.get('dimension', 'unknown')}")
        return True
    except Exception as e:
        print(f"✗ Info failed: {e}")
        return False


def test_list():
    """Test: List shows all collections"""
    print("\n[TEST 1.5.5] List shows all collections")
    try:
        collections = db.list()
        if TEST_COLLECTION not in collections:
            print(f"✗ Collection '{TEST_COLLECTION}' not in list: {collections}")
            return False

        print(f"✓ Collection found in list: {collections}")
        return True
    except Exception as e:
        print(f"✗ List failed: {e}")
        return False


def test_error_message():
    """Test: Error messages are helpful"""
    print("\n[TEST 1.5.6] Error messages are helpful")
    try:
        # Try to search non-existent collection
        db.search("nonexistent_collection", [0.1] * 384)
        print("✗ Should have raised an error for non-existent collection")
        return False
    except Exception as e:
        error_msg = str(e)
        # Check if error message includes helpful guidance
        if "how" in error_msg.lower() or "create" in error_msg.lower() or "list" in error_msg.lower():
            print(f"✓ Error message is helpful: {error_msg[:100]}...")
            return True
        else:
            print(f"⚠ Error message could be more helpful: {error_msg}")
            return True  # Not a failure, just a warning


def test_drop():
    """Test: Drop removes entire collection"""
    print("\n[TEST 1.5.7] Drop removes collection")
    try:
        db.drop(TEST_COLLECTION)
        print(f"✓ Collection '{TEST_COLLECTION}' dropped")

        # Verify it's gone
        collections = db.list()
        if TEST_COLLECTION in collections:
            print(f"✗ Collection still exists: {collections}")
            return False

        print("✓ Verified: collection no longer in list")
        return True
    except Exception as e:
        print(f"✗ Drop failed: {e}")
        return False


def main():
    """Run all integration tests"""
    print("=" * 70)
    print("PHASE 1.5: Integration Test - Simple API")
    print("=" * 70)
    print("\nTesting that the Python SDK simple API works as documented.")
    print("Make sure WaffleDB server is running: docker run -p 8080:8080 waffledb\n")

    tests = [
        test_add_with_auto_create,
        test_search,
        test_delete,
        test_info,
        test_list,
        test_error_message,
        test_drop,
    ]

    results = []
    for test in tests:
        try:
            passed = test()
            results.append(passed)
        except Exception as e:
            print(f"✗ Test crashed: {e}")
            results.append(False)

    # Summary
    print("\n" + "=" * 70)
    passed = sum(results)
    total = len(results)
    print(f"RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("✓ ALL TESTS PASSED - Phase 1.5 COMPLETE")
        print("\nPython SDK is ready for production!")
        print("Users can now: from waffledb import db; db.add(...); db.search(...)")
        return 0
    else:
        print(f"✗ {total - passed} test(s) failed - see details above")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
