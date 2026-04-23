#!/bin/bash
set -e

# Point data directory to HA persistent storage
mkdir -p /data/edec
mkdir -p /share/edec
rm -rf /app/data
ln -sf /data/edec /app/data
export EDEC_SHARED_DATA_ROOT="/share/edec"

echo "Starting EDEC Bot..."
exec python /app/main.py
