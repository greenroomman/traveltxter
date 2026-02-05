#!/usr/bin/env python3
"""
TRAVELTXTTER V5 — RENDER CLIENT

Responsibilities:
- Read RAW_DEALS
- Decide AM / PM layout from ingest timestamp
- Resolve theme (RDV → OPS_MASTER fallback)
- Call PythonAnywhere renderer (/api/render)
- Write back graphic_url + rendered_at
- NEVER mutate status

Authoritative endpoint:
https://greenroomman.pythonanywhere.com/api/render
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
import gspread
from typing import Dict, Any

# ============================================================
# ENV / CONSTANTS
# ============================================================

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

# 🔒 HARD DEFAULT — can be overridden, but must include /api/render
RENDER_URL = os.getenv(
    "RENDER_URL",
    "https://greenroomman.pythonanywhere.com/api/render"
)

TIMEZONE_UTC = timezone.utc

# ============================================================
# GOOGLE SHEETS
# ============================================================

def gspread_client():
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

    creds = json.loads(raw.replace("\\n", "\n"))
    return gspread.service_account_from_dict(creds)


def load_headers(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    return {h: i for i, h in enumerate(headers)}


def col(row, hmap, name):
    idx = hmap.get(name)
    if idx is None or idx >= len(row):
        return ""
    return row[idx]


def now_utc():
    return datetime.now(TIMEZONE_UTC)


# ============================================================
# CORE LOGIC
# ============================================================

def infer_layout(ingested_at: str) -> str:
    """
    AM if before 12:00 UTC, else PM.
    """
    try:
        ts = datetime.fromisoformat(ingested_at.replace("Z", "+00:00"))
    except Exception:
        return "PM"

    return "AM" if ts.hour < 12 else "PM"


def main():
    print("============================================================")
    print("🖼️  TRAVELTXTTER V5 — RENDER CLIENT START")
    print(f"🎯 Renderer endpoint: {RENDER_URL}")
    print("============================================================")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws = sh.worksheet(RAW_DEALS_TAB)
    ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)
    ws_ops = sh.worksheet(OPS_MASTER_TAB)

    hmap = load_headers(ws)
    rdv_map = load_headers(ws_rdv)

    values = ws.get_all_values()
    rdv_values = ws_rdv.get_all_values()

    if len(values) < 2:
        print("⚠️ No rows in RAW_DEALS")
        return

    # OPS fallback theme
    ops_theme = ws_ops.acell("B2").value

    # Build RDV lookup by deal_id
    rdv_lookup = {}
    for r in rdv_values[1:]:
        deal_id = col(r, rdv_map, "deal_id")
        if deal_id:
            rdv_lookup[deal_id] = r

    updates = []

    for i, row in enumerate(values[1:], start=2):
        deal_id = col(row, hmap, "deal_id")
        status = col(row, hmap, "status")
        graphic_url = col(row, hmap, "graphic_url")

        if not deal_id:
            continue
        if graphic_url:
            continue
        if status not in ("READY_TO_POST", "VIP_DONE"):
            continue

        ingested_at = col(row, hmap, "ingested_at_utc")
        layout = infer_layout(ingested_at)

        rdv_row = rdv_lookup.get(deal_id)
        theme = col(rdv_row, rdv_map, "dynamic_theme") if rdv_row else ""
        theme = theme or ops_theme or "default"

        payload = {
            "FROM": col(row, hmap, "origin_city"),
            "TO": col(row, hmap, "destination_city"),
            "OUT": datetime.fromisoformat(col(row, hmap, "outbound_date")).strftime("%d%m%y"),
            "IN": datetime.fromisoformat(col(row, hmap, "return_date")).strftime("%d%m%y"),
            "PRICE": f"£{int(float(col(row, hmap, 'price_gbp')))}",
            "layout": layout,
            "theme": theme,
        }

        print(
            f"🎯 Rendering row {i}: "
            f"{payload['FROM']} → {payload['TO']} "
            f"| {layout} | theme={theme}"
        )

        try:
            resp = requests.post(
                RENDER_URL,
                json=payload,
                timeout=25,
            )
        except Exception as e:
            raise RuntimeError(f"Render request failed: {e}")

        if resp.status_code != 200:
            raise RuntimeError(
                f"Render failed ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        url = data.get("graphic_url")
        if not url:
            raise RuntimeError("Renderer returned no graphic_url")

        updates.append({
            "row": i,
            "graphic_url": url,
            "rendered_at": now_utc().isoformat(),
        })

    # ============================================================
    # WRITE BACK
    # ============================================================

    if not updates:
        print("ℹ️ No rows rendered.")
        return

    for u in updates:
        ws.update_cell(u["row"], hmap["graphic_url"] + 1, u["graphic_url"])
        ws.update_cell(u["row"], hmap["rendered_at"] + 1, u["rendered_at"])

    print(f"✅ Rendered {len(updates)} image(s).")


if __name__ == "__main__":
    main()
