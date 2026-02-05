from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
import requests
from google.oauth2.service_account import Credentials

# ============================================================
# TRAVELTXTTER V5 — RENDER CLIENT (FULL FILE)
# PURPOSE:
# - Read candidates from RAW_DEALS_VIEW (read-only projection)
# - Render via PythonAnywhere endpoint /api/render
# - Write graphic_url (+ rendered_timestamp if column exists) back to RAW_DEALS
#
# NON-NEGOTIABLES:
# - Spreadsheet is the single stateful memory.
# - This worker is deterministic and stateless.
# - Do NOT invent columns; only write if header exists.
# ============================================================

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_ID = os.environ.get("SHEET_ID", "").strip()

RAW_TAB = os.environ.get("RAW_DEALS_TAB", os.environ.get("RAW_TAB", "RAW_DEALS")).strip()
RDV_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", os.environ.get("RDV_TAB", "RAW_DEALS_VIEW")).strip()
OPS_MASTER_TAB = os.environ.get("OPS_MASTER_TAB", "OPS_MASTER").strip()

RUN_SLOT = (os.environ.get("RUN_SLOT", "PM").strip().upper() or "PM")  # AM / PM
RENDER_MAX_PER_RUN = int(os.environ.get("RENDER_MAX_ROWS", os.environ.get("RENDER_MAX_PER_RUN", "2")))

# NOTE: user has explicitly standardised to PA endpoint:
#   https://greenroomman.pythonanywhere.com/api/render
RENDER_URL_RAW = os.environ.get("RENDER_URL", "").strip()

# Service account (either JSON string, JSON with escaped newlines, or base64:....)
SERVICE_ACCOUNT_JSON = (
    os.environ.get("GCP_SA_JSON_ONE_LINE")
    or os.environ.get("GCP_SA_JSON")
    or ""
).strip()


# ----------------------------
# Helpers
# ----------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fix_private_key_multiline(raw: str) -> str:
    """If private_key contains literal newlines (invalid JSON), escape them."""
    m = re.search(r'"private_key"\s*:\s*"(.*?)"', raw, flags=re.S)
    if not m:
        return raw
    pk = m.group(1)

    # If already escaped, leave it.
    if "\\n" in pk:
        return raw

    # Replace literal newlines and carriage returns inside the value with \\n
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    return raw[: m.start(1)] + pk_fixed + raw[m.end(1) :]


def load_sa_info() -> dict:
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing service account JSON (set GCP_SA_JSON_ONE_LINE or GCP_SA_JSON).")

    raw = SERVICE_ACCOUNT_JSON.strip()

    if raw.startswith("base64:"):
        raw = base64.b64decode(raw.split("base64:", 1)[1]).decode("utf-8", errors="replace").strip()

    # Attempt 1: as-is
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Attempt 2: common GH secret escaping (\\n inside JSON)
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError:
        pass

    # Attempt 3: private_key contains literal newlines (invalid JSON)
    raw2 = _fix_private_key_multiline(raw)
    return json.loads(raw2)


def gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(load_sa_info(), scopes=GOOGLE_SCOPE)
    return gspread.authorize(creds)


def normalize_render_url(url: str) -> str:
    """Make RENDER_URL robust against common misconfigurations."""
    u = (url or "").strip()
    if not u:
        raise RuntimeError("Missing RENDER_URL env var (expected https://.../api/render).")

    # Fix accidental double /api/api/render
    u = u.replace("/api/api/render", "/api/render")

    # If given base URL, append /api/render
    if re.match(r"^https?://[^/]+/?$", u):
        u = u.rstrip("/") + "/api/render"

    # If ends with /api, append /render
    if re.search(r"/api/?$", u):
        u = u.rstrip("/") + "/render"

    return u


RENDER_URL = normalize_render_url(RENDER_URL_RAW)


def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    name = (name or "").strip()
    if not name:
        raise RuntimeError("Worksheet name is blank (check *_TAB env vars).")
    return sh.worksheet(name)


def header_map(values: List[List[str]]) -> Dict[str, int]:
    if not values:
        return {}
    return {h.strip(): i for i, h in enumerate(values[0]) if h.strip()}


def get_col(row: List[str], h: Dict[str, int], name: str) -> str:
    i = h.get(name)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def safe_float(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def pick_candidates(rdv_values: List[List[str]], limit: int) -> List[int]:
    """Return RDV row indices (1-based sheet row numbers) of render candidates, latest-first."""
    if len(rdv_values) < 2:
        return []

    hdr = header_map(rdv_values)
    deal_id_idx = hdr.get("deal_id")
    ingested_dt_idx = hdr.get("ingested_dt")
    age_idx = hdr.get("age_hours")

    if deal_id_idx is None:
        return []

    candidates: List[tuple] = []
    for r_i, r in enumerate(rdv_values[1:], start=2):
        deal_id = (r[deal_id_idx] if deal_id_idx < len(r) else "").strip()
        if not deal_id:
            continue

        # Sorting key: ingested_dt numeric if present, else -age_hours, else row index
        key = 0.0
        if ingested_dt_idx is not None and ingested_dt_idx < len(r):
            v = safe_float((r[ingested_dt_idx] or "").strip())
            if v is not None:
                key = v
        elif age_idx is not None and age_idx < len(r):
            v = safe_float((r[age_idx] or "").strip())
            if v is not None:
                key = -v
        else:
            key = float(r_i)

        candidates.append((key, r_i))

    candidates.sort(key=lambda t: t[0], reverse=True)
    return [r_i for _, r_i in candidates[: max(0, limit)]]


def post_render(payload: Dict[str, Any]) -> str:
    resp = requests.post(RENDER_URL, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Render failed ({resp.status_code}): {resp.text}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Render returned non-JSON: {resp.text}")

    graphic_url = (data.get("graphic_url") or "").strip()
    if not graphic_url:
        raise RuntimeError(f"Render response missing graphic_url: {data}")
    return graphic_url


def _a1_for(col_idx_0: int, row_1: int) -> str:
    n = col_idx_0 + 1
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_1}"


def main() -> None:
    print("============================================================")
    print("🖼️  TRAVELTXTTER V5 — RENDER CLIENT START")
    print(f"🎯 Renderer endpoint: {RENDER_URL}")
    print(f"🎛️  RUN_SLOT={RUN_SLOT}  RENDER_MAX_PER_RUN={RENDER_MAX_PER_RUN}")
    print("============================================================")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_rdv = open_ws(sh, RDV_TAB)

    raw_values = ws_raw.get_all_values()
    rdv_values = ws_rdv.get_all_values()

    if len(raw_values) < 2:
        print("⚠️ RAW_DEALS is empty. Exiting 0.")
        return

    h_raw = header_map(raw_values)
    if "deal_id" not in h_raw or "graphic_url" not in h_raw:
        raise RuntimeError("RAW_DEALS missing required headers: deal_id, graphic_url")

    # Optional fields
    rendered_ts_col = "rendered_timestamp" if "rendered_timestamp" in h_raw else ("rendered_at" if "rendered_at" in h_raw else "")
    status_col = "status" if "status" in h_raw else ""
    theme_col = "theme" if "theme" in h_raw else ""

    # Build deal_id -> raw row number map
    did_idx = h_raw["deal_id"]
    deal_row_map: Dict[str, int] = {}
    for r_i, r in enumerate(raw_values[1:], start=2):
        did = (r[did_idx] if did_idx < len(r) else "").strip()
        if did:
            deal_row_map[did] = r_i

    rdv_hdr = header_map(rdv_values)
    rdv_did_idx = rdv_hdr.get("deal_id")
    if rdv_did_idx is None:
        raise RuntimeError("RAW_DEALS_VIEW missing required header: deal_id")

    # Oversample then filter
    candidate_rdv_rows = pick_candidates(rdv_values, RENDER_MAX_PER_RUN * 8)

    to_render: List[Dict[str, Any]] = []
    for rdv_row_num in candidate_rdv_rows:
        rdv_row = rdv_values[rdv_row_num - 1]
        deal_id = (rdv_row[rdv_did_idx] if rdv_did_idx < len(rdv_row) else "").strip()
        if not deal_id:
            continue

        raw_row_num = deal_row_map.get(deal_id)
        if not raw_row_num:
            continue

        raw_row = raw_values[raw_row_num - 1]
        if get_col(raw_row, h_raw, "graphic_url"):
            continue

        if status_col:
            st = get_col(raw_row, h_raw, status_col)
            if st not in ("READY_TO_POST", "PUBLISH_READY", "VIP_DONE"):
                continue

        from_city = get_col(raw_row, h_raw, "origin_city") or get_col(raw_row, h_raw, "origin_iata")
        to_city = get_col(raw_row, h_raw, "destination_city") or get_col(raw_row, h_raw, "destination_iata")
        out_date = get_col(raw_row, h_raw, "outbound_date")
        in_date = get_col(raw_row, h_raw, "return_date")

        price_gbp = get_col(raw_row, h_raw, "price_gbp")
        price = f"£{price_gbp}" if price_gbp and not price_gbp.strip().startswith("£") else (price_gbp or "")

        # Theme: prefer RDV dynamic_theme if present, else RAW theme, else OPS theme (B2)
        theme_used = ""
        if "dynamic_theme" in rdv_hdr:
            idx = rdv_hdr["dynamic_theme"]
            theme_used = (rdv_row[idx] if idx < len(rdv_row) else "").strip()
        if not theme_used and theme_col:
            theme_used = get_col(raw_row, h_raw, theme_col)
        if not theme_used:
            try:
                ws_ops = open_ws(sh, OPS_MASTER_TAB)
                theme_used = (ws_ops.acell("B2").value or "").strip()
            except Exception:
                theme_used = ""

        payload = {
            "TO": (to_city or "").upper(),
            "FROM": (from_city or "").upper(),
            "OUT": out_date,
            "IN": in_date,
            "PRICE": price,
            "layout": RUN_SLOT,  # AM/PM
            "theme": theme_used,
        }

        to_render.append(
            {
                "deal_id": deal_id,
                "raw_row_num": raw_row_num,
                "payload": payload,
                "from_city": from_city,
                "to_city": to_city,
                "price": price,
                "theme_used": theme_used,
            }
        )

        if len(to_render) >= RENDER_MAX_PER_RUN:
            break

    if not to_render:
        print("⚠️ No render candidate found (no fresh deals missing graphic_url). Exiting 0.")
        return

    print(f"🎯 Render candidates (latest-first): {[x['raw_row_num'] for x in to_render]}")

    for item in to_render:
        raw_row_num = item["raw_row_num"]
        deal_id = item["deal_id"]
        payload = item["payload"]

        print(
            f"🎯 Render row {raw_row_num}: layout={RUN_SLOT} "
            f"theme_used='{item['theme_used']}' FROM='{item['from_city']}' TO='{item['to_city']}' PRICE='{item['price']}'"
        )

        graphic_url = post_render(payload)

        # Write back: gspread update signature is values first, then range_name (fixes deprecation warning)
        a_graphic = _a1_for(h_raw["graphic_url"], raw_row_num)
        ws_raw.update([[graphic_url]], a_graphic)

        if rendered_ts_col:
            a_ts = _a1_for(h_raw[rendered_ts_col], raw_row_num)
            ws_raw.update([[_utc_now_iso()]], a_ts)

        if "render_error" in h_raw:
            a_err = _a1_for(h_raw["render_error"], raw_row_num)
            ws_raw.update([[""]], a_err)

        print(f"✅ Rendered: {deal_id} -> {graphic_url}")

    print("✅ Render client complete.")


if __name__ == "__main__":
    main()
