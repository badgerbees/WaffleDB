#!/bin/bash

# Snapshot backup script for WaffleDB

set -e

BACKUP_DIR="${BACKUP_DIR:-.}/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_NAME="waffledb_snapshot_$TIMESTAMP"

echo "=== WaffleDB Snapshot Backup ==="
echo "Backup directory: $BACKUP_DIR"
echo "Backup name: $BACKUP_NAME"
echo

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Check if WaffleDB is running
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "Error: WaffleDB server is not running at http://localhost:8080"
    exit 1
fi

echo "Server is healthy. Creating snapshot..."

# Create snapshot directory
SNAPSHOT_PATH="$BACKUP_DIR/$BACKUP_NAME"
mkdir -p "$SNAPSHOT_PATH"

# Copy data directory
echo "Copying data directory..."
if [ -d "/data" ]; then
    cp -r /data/* "$SNAPSHOT_PATH/" 2>/dev/null || true
    echo "Data copied to $SNAPSHOT_PATH"
else
    echo "Warning: Data directory not found"
fi

# Create metadata file
cat > "$SNAPSHOT_PATH/metadata.txt" <<EOF
Backup Date: $(date)
Server Version: WaffleDB 0.1.0
Backup Type: Full Snapshot
EOF

# Create tar archive
echo "Creating archive..."
cd "$BACKUP_DIR"
tar -czf "${BACKUP_NAME}.tar.gz" "$BACKUP_NAME"
rm -rf "$BACKUP_NAME"

echo "Backup completed: ${BACKUP_NAME}.tar.gz"
ls -lh "${BACKUP_NAME}.tar.gz"

# Cleanup old backups (keep last 7)
echo
echo "Cleaning up old backups (keeping last 7)..."
cd "$BACKUP_DIR"
ls -t waffledb_snapshot_*.tar.gz | tail -n +8 | xargs -r rm

echo "Cleanup complete"
echo "=== Backup Finished ==="

