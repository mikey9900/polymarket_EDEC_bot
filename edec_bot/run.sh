#!/bin/bash
set -e

# Point data directory to HA persistent storage
mkdir -p /data/edec
rm -rf /app/data
ln -sf /data/edec /app/data

echo "Starting EDEC Bot..."
exec python /app/main.py
