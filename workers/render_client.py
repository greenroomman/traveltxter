# workers/render_client.py
from __future__ import annotations

import os
import re
import json
import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# TRAVELTXTER — RENDER CLIENT (V5 COMPLIANT)
#
# LOCKED RULES
# 1) RAW_DEALS is the only writable source of truth.
# 2) RAW_DEALS_VIEW is read-only; theme must be read from RDV.dynamic_theme.
# 3) Layout is derived ONLY from ingested_at_utc (UTC):
#       hour < 12  => AM
#       else       => PM
# 4) OPS_MASTER theme (B5) is fallback ONLY if RDV dynamic_theme blank/unusable.
# 5) This worker does NOT touch status (no promotion / gating).
#    It only writes render outputs + timestamps + errors into RAW_DEALS.
#
# RENDER API CONTRACT
# - Must include these keys correctly formatted:
#     TO, FROM, OUT(ddmmyy), IN(ddmmyy), PRICE(£int rounded up)
# - We also include 'theme' and 'layout' for the renderer to obey palette/layout.
#   (If your PA endpoint ignores extra keys, safe.)
# ============================================================


# ----------------------------
# Environment
# ----------------------------
SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

SERVICE_ACCOUNT_JSON = (
    os.getenv("GCP_SA_JSON_ONE_LINE")
    or os.getenv("GCP_SA_JSON")
    or ""
).strip()

RENDER_MAX_PER_RUN = int(float(os.getenv("RENDER_MAX_PER_RUN", "1") or "1"))

OPS_THEME_CELL = os.getenv("OPS_THEME_CELL", "B5")

# Statuses to render (V5)
RENDER_ELIGIBLE_STATUSES = {
    "PUBLISH_AM",
    "PUBLISH_PM",
    "PUBLISH_BOTH",
    # tolerance (some older flows)
    "READY_TO_POST",
}

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ----------------------------
# Themes (authoritative set)
# ----------------------------
AUTHORITATIVE_THEMES = {
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
}

THEME_ALIASES = {
    "winter sun": "winter_sun",
    "summer sun": "summer_sun",
    "beach break": "beach_break",
    "northern lights": "northern_lights",
    "city breaks": "city_breaks",
    "culture / history": "culture_history",
    "culture & history": "culture_history",
    "culture": "culture_history",
    "history": "culture_history",
    "long haul": "long_haul",
    "luxury": "luxury_value",
    "unexpected": "unexpected_value",
}


def normalize_theme(raw: str | None) -> str:
    if not raw:
        return ""
    # handle "snow|long_haul" or "snow, long_haul"
    parts = re.split(r"[,\|;]+", str(raw).lower())
    for p in parts:
        t = p.strip()
        if not t:
            continue
        t = THEME_ALIASES.get(t, t)
        t = re.sub(r"[^a-z0-9_]+", "_", t).strip("_")
        if t in AUTHORITATIVE_THEMES:
            return t
    return ""


# ----------------------------
# Logging helpers
# ----------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"{_utc_now_iso()} | {msg}", flush=True)


# ----------------------------
# Google Sheets helpers
# ----------------------------
def _sa_creds() -> Credentials:
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")
    try:
        info = json.loads(SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        # tolerate escaped newlines
        info = json.loads(SERVICE_ACCOUNT_JSON.replace("\\n", "\n"))
    return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)


def _col_index(headers: List[str], name: str) -> Optional[int]:
    target = name.strip().lower()
    for i, h in enumerate(headers):
        if (h or "").strip().lower() == target:
            return i
    return None


def _a1(col_1: int, row_1: int) -> str:
    n, s = col_1, ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return f"{s}{row_1}"


def _batch_update(ws: gspread.Worksheet, headers: List[str], row_1: int, updates: Dict[str, str]) -> None:
    idx = {h: i for i, h in enumerate(headers)}
    payload = []
    for k, v in updates.items():
        if k in idx:
            payload.append({"range": _a1(idx[k] + 1, row_1), "values": [[v]]})
    if payload:
        ws.batch_update(payload)


# ----------------------------
# Timestamp / slot inference
# ----------------------------
def _parse_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        ss = s.strip()
        if ss.endswith("Z"):
            ss = ss[:-1] + "+00:00"
        d = datetime.fromisoformat(ss)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def infer_layout_from_ingest(row: Dict[str, str]) -> str:
    ts = _parse_utc(row.get("ingested_at_utc", "") or "")
    if not ts:
        raise RuntimeError("Cannot infer AM/PM — missing/invalid ingested_at_utc")
    return "AM" if ts.hour < 12 else "PM"


# ----------------------------
# Payload normalization
# ----------------------------
def normalize_price(raw: str | None) -> str:
    if not raw:
        return ""
    m = re.search(r"(\d+(?:\.\d+)?)", str(raw).replace(",", ""))
    if not m:
        return ""
    return f"£{int(math.ceil(float(m.group(1))))}"


def normalize_date_ddmmyy(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    # Already ddmmyy
    if re.fullmatch(r"\d{6}", s):
        return s
    # yyyy-mm-dd
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        y, m, d = s.split("-")
        return f"{int(d):02d}{int(m):02d}{int(y)%100:02d}"
    raise ValueError(f"Unsupported date format: {s}")


# ----------------------------
# OPS Master theme fallback
# ----------------------------
def read_ops_theme(sh: gspread.Spreadsheet) -> str:
    ws = sh.worksheet(OPS_MASTER_TAB)
    return normalize_theme((ws.acell(OPS_THEME_CELL).value or "").strip())


# ----------------------------
# RDV dynamic_theme lookup by deal_id
# ----------------------------
def build_rdv_theme_map(rdv_all: List[List[str]]) -> Dict[str, str]:
    if not rdv_all or len(rdv_all) < 2:
        return {}

    headers = rdv_all[0]
    deal_id_i = _col_index(headers, "deal_id")

    # Prefer header match for dynamic_theme. If missing, return empty.
    dyn_i = _col_index(headers, "dynamic_theme")
    if deal_id_i is None or dyn_i is None:
        return {}

    out: Dict[str, str] = {}
    for r in rdv_all[1:]:
        if deal_id_i >= len(r):
            continue
        did = (r[deal_id_i] or "").strip()
        if not did or did in out:
            continue
        dyn = r[dyn_i] if dyn_i < len(r) else ""
        out[did] = dyn
    return out


# ----------------------------
# Candidate selection
# ----------------------------
def is_render_candidate(row: Dict[str, str]) -> Tuple[bool, str]:
    status = (row.get("status") or "").strip()
    if status not in RENDER_ELIGIBLE_STATUSES:
        return (False, "status_not_eligible")

    if (row.get("graphic_url") or "").strip():
        return (False, "already_has_graphic")

    # Render requires city names (enrich should run before)
    if not (row.get("origin_city") or "").strip():
        return (False, "missing_origin_city")
    if not (row.get("destination_city") or "").strip():
        return (False, "missing_destination_city")

    # Must have dates and price
    if not (row.get("outbound_date") or "").strip():
        return (False, "missing_outbound_date")
    if not (row.get("return_date") or "").strip():
        return (False, "missing_return_date")
    if not (row.get("price_gbp") or "").strip():
        return (False, "missing_price_gbp")

    return (True, "ok")


# ----------------------------
# Render one row
# ----------------------------
def render_one(
    sh: gspread.Spreadsheet,
    ws_raw: gspread.Worksheet,
    headers: List[str],
    row_num: int,
    row_values: List[str],
    rdv_theme_map: Dict[str, str],
    ops_theme: str,
) -> bool:
    row = dict(zip(headers, row_values))

    ok, reason = is_render_candidate(row)
    if not ok:
        # only write render_error for candidates that *should* be renderable but are blocked by missing enrichment
        # This helps ops without spamming sheet on every non-eligible row.
        if reason.startswith("missing_"):
            _batch_update(
                ws_raw,
                headers,
                row_num,
                {
                    "render_error": reason,
                    "rendered_timestamp": "",
                    "rendered_at": "",
                },
            )
        return False

    deal_id = (row.get("deal_id") or "").strip()
    rdv_theme_raw = rdv_theme_map.get(deal_id, "")
    theme = normalize_theme(rdv_theme_raw) or ops_theme
    if not theme:
        raise RuntimeError("No valid theme available (RDV dynamic_theme blank + OPS_MASTER blank)")

    layout = infer_layout_from_ingest(row)

    payload = {
        # REQUIRED KEYS (LOCKED FORMATS)
        "FROM": (row.get("origin_city") or "").strip(),
        "TO": (row.get("destination_city") or "").strip(),
        "OUT": normalize_date_ddmmyy(row.get("outbound_date")),
        "IN": normalize_date_ddmmyy(row.get("return_date")),
        "PRICE": normalize_price(row.get("price_gbp")),

        # OPTIONAL (renderer can ignore safely)
        "theme": theme,
        "layout": layout,
        "run_slot": layout,
    }

    resp = requests.post(RENDER_URL, json=payload, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Render failed {resp.status_code}: {resp.text}")

    data = resp.json()
    graphic_url = (data.get("graphic_url") or "").strip()
    if not graphic_url:
        raise RuntimeError("Render response missing graphic_url")

    ts = _utc_now_iso()
    _batch_update(
        ws_raw,
        headers,
        row_num,
        {
            "graphic_url": graphic_url,
            "rendered_timestamp": ts,
            "rendered_at": ts,
            "render_error": "",
        },
    )

    log(f"✅ Rendered row {row_num} | {payload['FROM']} → {payload['TO']} | {theme=} {layout=}")
    return True


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    if not SPREADSHEET_ID or not RENDER_URL:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID or RENDER_URL")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)

    ops_theme = read_ops_theme(sh)

    raw_all = ws_raw.get_all_values()
    if not raw_all or len(raw_all) < 2:
        log("No RAW_DEALS rows")
        return 0

    rdv_all = ws_rdv.get_all_values()
    rdv_theme_map = build_rdv_theme_map(rdv_all)

    headers = raw_all[0]
    status_i = _col_index(headers, "status")
    graphic_i = _col_index(headers, "graphic_url")

    if status_i is None or graphic_i is None:
        raise RuntimeError("RAW_DEALS missing required headers: status and/or graphic_url")

    # Candidate rows: eligible status + no graphic_url
    candidates: List[int] = []
    for i in range(1, len(raw_all)):
        row = raw_all[i]
        status = (row[status_i] if status_i < len(row) else "").strip()
        graphic = (row[graphic_i] if graphic_i < len(row) else "").strip()
        if status in RENDER_ELIGIBLE_STATUSES and not graphic:
            candidates.append(i + 1)
        if len(candidates) >= RENDER_MAX_PER_RUN:
            break

    if not candidates:
        log("No render candidates")
        return 0

    log(f"Render candidates: {candidates} (max {RENDER_MAX_PER_RUN})")

    rendered = 0
    for row_num in candidates:
        idx0 = row_num - 1
        try:
            if render_one(
                sh=sh,
                ws_raw=ws_raw,
                headers=headers,
                row_num=row_num,
                row_values=raw_all[idx0],
                rdv_theme_map=rdv_theme_map,
                ops_theme=ops_theme,
            ):
                rendered += 1
        except Exception as e:
            # Write error on the row (do not touch status)
            ts = _utc_now_iso()
            _batch_update(
                ws_raw,
                headers,
                row_num,
                {
                    "render_error": str(e)[:450],
                    "rendered_timestamp": ts,
                    "rendered_at": ts,
                },
            )
            log(f"⛔ Render failed row {row_num}: {e}")

    log(f"SUMMARY: rendered={rendered} / candidates={len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
