#!/usr/bin/env python3
import json
import sys
from pathlib import Path

def main():
    p = Path("service_account.json")
    if not p.exists():
        print("ERROR: service_account.json not found in this folder.")
        sys.exit(1)

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print("ERROR: service_account.json is not valid JSON.")
        print(str(e))
        sys.exit(1)

    print(json.dumps(data, separators=(",", ":")))

if __name__ == "__main__":
    main()

