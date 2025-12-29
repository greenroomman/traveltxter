#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

export SHEET_ID="PASTE_REAL_SHEET_ID_HERE"
export WORKSHEET_NAME="RAW_DEALS"
export GCP_SA_JSON="$(cat gcp_sa_one_line.txt)"

python3 workers/ai_scorer.py

