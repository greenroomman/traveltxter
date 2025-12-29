#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f "service_account.json" ]]; then
  echo "ERROR: service_account.json not found in repo root."
  echo "Create it by pasting your Google Service Account JSON into service_account.json"
  exit 1
fi

python3 - <<'PY'
import json, pathlib, sys

p = pathlib.Path("service_account.json")
raw = p.read_text(encoding="utf-8")

try:
    data = json.loads(raw)
except Exception as e:
    print("ERROR: service_account.json is not valid JSON.")
    print("Reason:", e)
    sys.exit(1)

one_line = json.dumps(data, separators=(",", ":"), ensure_ascii=False)

out = pathlib.Path("gcp_sa_one_line.txt")
out.write_text(one_line, encoding="utf-8")

print("OK: wrote gcp_sa_one_line.txt")
print("Length:", len(one_line))
PY

