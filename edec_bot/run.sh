#!/bin/bash
set -e

CONFIG_PATH=/data/options.json

# Read credentials from HA add-on options and write .env
PRIVATE_KEY=$(jq --raw-output '.private_key' $CONFIG_PATH)
TELEGRAM_BOT_TOKEN=$(jq --raw-output '.telegram_bot_token' $CONFIG_PATH)
TELEGRAM_CHAT_ID=$(jq --raw-output '.telegram_chat_id' $CONFIG_PATH)

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
