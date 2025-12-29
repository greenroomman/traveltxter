#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# ----------------------------
# Google Sheets
# ----------------------------
export SHEET_ID="1HwDKqupTlZngiB12U5YRGyFK0LiN4sN9-CGmjgB7nAs"
export TAB_NAME="RAW_DEALS"

# Use service account JSON file directly (simplest)
export GOOGLE_APPLICATION_CREDENTIALS="service_account.json"

# ----------------------------
# Instagram (Graph API)
# ----------------------------
export IG_USER_ID="17841479115847926"
export 
FB_ACCESS_TOKEN="EAFmuQ1jviZCsBQXWQODhIPrLcMzKE5DekBQttxyijGDqDNceXGk3XLxz58NoZAZCIyR6QAZC9lOa3oRkBOZC42vNQ1ZC1Ckp5b06yeKiTYUc0NCgMn5UgItwwqZAiNCq1IqEihbxNmp16JwUn6pEk2oSH08ZCq0Ql2cYCf2cxkZCBmbAkzEwBRZBdF0RMuDSZBmWQZDZD"

# Optional
export GRAPH_VERSION="v19.0"
export READY_STATUSES="SCORED,READY"
export FB_ACCESS_TOKEN="$IG_ACCESS_TOKEN"

python3 workers/publish_worker.py
