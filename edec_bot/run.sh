#!/bin/bash
set -e

CONFIG_PATH=/data/options.json

# Use Python to read HA options (jq not available in slim image)
PRIVATE_KEY=$(python3 -c "import json; d=json.load(open('$CONFIG_PATH')); print(d['private_key'])")
TELEGRAM_BOT_TOKEN=$(python3 -c "import json; d=json.load(open('$CONFIG_PATH')); print(d['telegram_bot_token'])")
TELEGRAM_CHAT_ID=$(python3 -c "import json; d=json.load(open('$CONFIG_PATH')); print(d['telegram_chat_id'])")

cat > /app/.env << EOF
PRIVATE_KEY=${PRIVATE_KEY}
TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
EOF

# Point data directory to HA persistent storage
mkdir -p /data/edec
rm -rf /app/data
ln -sf /data/edec /app/data

echo "Starting EDEC Bot..."
exec python /app/main.py
