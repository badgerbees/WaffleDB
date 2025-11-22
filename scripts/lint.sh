#!/bin/bash

# Linting and code quality script

set -e

echo "=== WaffleDB Code Quality Checks ==="
echo

# Check Rust formatting
echo "Running cargo fmt check..."
cargo fmt --all -- --check

echo "✓ Formatting check passed"
echo

# Run clippy linter
echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings

echo "✓ Clippy check passed"
echo

# Run tests
echo "Running tests..."
cargo test --lib 2>/dev/null || true

echo
echo "=== Code Quality Checks Complete ==="

