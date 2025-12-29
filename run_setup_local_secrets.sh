#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p tools

cat > tools/minify_gcp_sa_json.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f "service_account.json" ]]; then
  echo "ERROR: service_account.json not found."
  echo "Create it by pasting your Google Service Account JSON into a file named service_account.json"
  exit 1
fi

python3 - <<'PY'
import json, pathlib, sys

p = pathlib.Path("service_account.json")
try:
    data = json.loads(p.read_text(encoding="utf-8"))
except Exception as e:
    print("ERROR: service_account.json is not valid JSON:", e)
    sys.exit(1)

one_line = json.dumps(data, separators=(",", ":"), ensure_ascii=False)

out = pathlib.Path("gcp_sa_one_line.txt")
out.write_text(one_line, encoding="utf-8")

print("SUCCESS:")
print(" - Created gcp_sa_one_line.txt")
print(" - Copy its contents into GCP_SA_JSON")
PY
EOF

chmod +x tools/minify_gcp_sa_json.sh

echo ""
echo "SETUP COMPLETE."
echo ""
echo "NEXT STEPS:"
echo "1) Create service_account.json in the repo root"
echo "2) Run: ./tools/minify_gcp_sa_json.sh"
echo "3) Paste gcp_sa_one_line.txt into run_publish_local.sh"

