#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

# Sheet config
export SHEET_ID="1HwDKqupTlZngiB12U5YRGyFK0LiN4sN9-CGmjgB7nAs"
export WORKSHEET_NAME="RAW_DEALS"
export GCP_SA_JSON="$(cat gcp_sa_one_line.txt)"

# IMPORTANT: Force the exact working render URL (no guessing)
export RENDER_URL="https://greenroomman.pythonanywhere.com/render"

# Give it more time (image render can take a while)
export RENDER_TIMEOUT="90"

# Debug on
export RENDER_DEBUG="1"

python3 workers/render_worker.py

