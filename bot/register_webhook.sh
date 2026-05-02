#!/bin/bash
# register_webhook.sh
# Run this ONCE after deploying your Cloudflare Worker.
# It tells Telegram to send all bot messages to your Worker URL.
#
# Usage:
#   BOT_TOKEN="your_token" WORKER_URL="https://your-worker.workers.dev" bash register_webhook.sh

BOT_TOKEN="${BOT_TOKEN:-$1}"
WORKER_URL="${WORKER_URL:-$2}"

if [ -z "$BOT_TOKEN" ] || [ -z "$WORKER_URL" ]; then
  echo "Usage: BOT_TOKEN=xxx WORKER_URL=https://... bash register_webhook.sh"
  exit 1
fi

echo "Registering webhook..."
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
  -d "url=${WORKER_URL}" \
  -d "allowed_updates=[\"message\"]" | python3 -m json.tool

echo ""
echo "Verify webhook:"
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo" | python3 -m json.tool
