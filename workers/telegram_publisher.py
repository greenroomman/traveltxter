from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

"""TRAVELTXTTER — TELEGRAM PUBLISHER (V5)

VIP-FIRST REVENUE SPINE

STATUS CONTRACT (V5)
- VIP stage:
    - Reads RAW_DEALS rows where:
        status == 'READY_TO_POST'
        posted_vip_at is blank
        publish_window matches the current RUN_SLOT (AM/PM) or 'BOTH'
    - Sends to VIP channel.
    - Writes:
        posted_vip_at = now (ISO 8601 UTC)
        status = 'READY_FREE'

- FREE stage:
    - Reads RAW_DEALS rows where:
        status == 'READY_FREE'
        posted_vip_at is set
        posted_free_at is blank
        VIP delay satisfied
    - Sends to FREE channel.
    - Writes:
        posted_free_at = now
        status = 'PUBLISHED'

FALLBACK (VIP only)
- If no READY_TO_POST candidate exists for this RUN_SLOT, optionally uses RAW_DEALS_VIEW
  to select a fresh (<24h) unposted deal marked fallback_ok, ranked by fallback_rank.

NOTES
- FREE can only ever post deals that VIP already posted.
- This worker is deterministic and sheet-led. It does NOT invent themes.
"""


# ------------------------- CONFIG -------------------------

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", os.getenv("RAW_DEALS_VIEW", "RAW_DEALS_VIEW"))
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

RUN_SLOT = (os.getenv("RUN_SLOT", "PM") or "PM").strip().upper()  # AM or PM

TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_CHANNEL_VIP = os.getenv("TELEGRAM_CHANNEL_VIP", "").strip()
TELEGRAM_BOT_TOKEN_FREE = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_FREE = os.getenv("TELEGRAM_CHANNEL", "").strip()

# Delay for FREE relative to VIP post (hours)
FREE_DELAY_HOURS = float(os.getenv("FREE_DELAY_HOURS", "10"))

# Only consider RAW_DEALS rows newer than this (prevents publishing ancient rows)
MAX_DEAL_AGE_HOURS = float(os.getenv("MAX_DEAL_AGE_HOURS", "24"))

# If you want to completely disable fallback logic, set to false
ENABLE_FALLBACK = (os.getenv("ENABLE_TG_FALLBACK", "true").strip().lower() in ("1", "true", "yes"))

MIN_INGEST_AGE_SECONDS = int(os.getenv("MIN_INGEST_AGE_SECONDS", "90"))

# Google creds
SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
GCP_SA_JSON = os.getenv("GCP_SA_JSON")
GCP_SA_JSON_ONE_LINE = os.getenv("GCP_SA_JSON_ONE_LINE")


# ------------------------- UTIL -------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')}Z | {msg}")


def _fix_private_key_newlines(raw: str) -> str:
    """If the service-account JSON has literal newlines inside the private_key string,
    json.loads will throw 'Invalid control character'. We rewrite those newlines to \\n
    This is deliberately narrow: only touches the private_key value.
    """

    # Only attempt if it looks like service account JSON
    if "\"private_key\"" not in raw:
        return raw

    m = re.search(r'(\"private_key\"\s*:\s*\")(.*?)(\")\s*,', raw, flags=re.DOTALL)
    if not m:
        return raw

    before = raw[: m.start(2)]
    pk = m.group(2)
    after = raw[m.end(2) :]

    # Replace actual newlines and carriage returns inside the private key string
    pk_fixed = pk.replace("\\n", "\n")  # tolerate already-escaped
    pk_fixed = pk_fixed.replace("\r\n", "\n").replace("\r", "\n")
    pk_fixed = pk_fixed.replace("\n", "\\n")

    return before + pk_fixed + after


def load_service_account_info() -> dict:
    raw = (GCP_SA_JSON_ONE_LINE or GCP_SA_JSON or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")

    # Strip accidental surrounding quotes
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1]

    # Common case: JSON contains literal \n sequences
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError:
        pass

    # Next: fix invalid control characters inside private_key
    raw2 = _fix_private_key_newlines(raw)
    try:
        return json.loads(raw2)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Could not parse service account JSON: {e}")


def gspread_client() -> gspread.Client:
    info = load_service_account_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: '{name}'") from e


def build_hmap(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i for i, h in enumerate(headers) if h.strip()}


def get_cell(ws: gspread.Worksheet, a1: str) -> str:
    try:
        return (ws.acell(a1).value or "").strip()
    except Exception:
        return ""


def parse_iso_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # Accept both ...Z and ...+00:00
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def hours_since(dt: Optional[datetime]) -> Optional[float]:
    if not dt:
        return None
    return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0


def publish_window_allows(pw: str, slot: str) -> bool:
    pw_n = (pw or "").strip().upper()
    slot = slot.strip().upper()
    if not pw_n:
        return True  # permissive if blank

    # allow common spellings
    if pw_n in {"BOTH", "PUBLISH_BOTH", "AMPM", "AM_PM"}:
        return True
    if slot == "AM" and ("AM" in pw_n):
        return True
    if slot == "PM" and ("PM" in pw_n):
        return True
    return False


# ------------------------- TELEGRAM -------------------------


def tg_send(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        raise RuntimeError("Missing Telegram token/channel env vars")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": False,
        "parse_mode": "HTML",
    }
    r = requests.post(url, json=payload, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram sendMessage failed: {r.status_code} {r.text[:500]}")


def fmt_date(s: str) -> str:
    return (s or "").strip()


def fmt_money(price_gbp: str) -> str:
    p = (price_gbp or "").strip()
    if not p:
        return ""
    try:
        # normalize
        f = float(p)
        return f"£{int(round(f))}"
    except Exception:
        # already a string like £199
        return p


def build_vip_message(row: Dict[str, str]) -> str:
    """VIP = Product. Shows deal + booking link."""
    to_city = row.get("destination_city") or row.get("destination_iata")
    from_city = row.get("origin_city") or row.get("origin_iata")
    country = row.get("destination_country", "")
    out_date = fmt_date(row.get("outbound_date", ""))
    in_date = fmt_date(row.get("return_date", ""))
    price = fmt_money(row.get("price_gbp", ""))
    
    phrase = (row.get("phrase_used") or "").strip()
    link = (row.get("booking_link_vip") or "").strip()
    
    # Country flag emoji (optional enhancement)
    flag = ""  # Could add flag mapping if desired
    
    lines = [
        f"{price} to {country} {flag}".strip(),
        "",
        f"TO: {to_city}",
        f"FROM: {from_city}",
        f"OUT:  {out_date}",
        f"BACK: {in_date}",
    ]
    
    if phrase:
        lines.append("")
        lines.append(phrase)
    
    if link:
        lines.append("")
        lines.append(f'<a href="{link}">Booking link</a>')
    
    lines.append("")
    lines.append("Shared with VIP first, as always.")
    
    return "\n".join(lines).strip()


def build_free_message(row: Dict[str, str]) -> str:
    """FREE = VIP subscription advertising. No booking link."""
    to_city = row.get("destination_city") or row.get("destination_iata")
    from_city = row.get("origin_city") or row.get("origin_iata")
    country = row.get("destination_country", "")
    out_date = fmt_date(row.get("outbound_date", ""))
    in_date = fmt_date(row.get("return_date", ""))
    price = fmt_money(row.get("price_gbp", ""))
    
    # Get subscription links from env vars
    monthly_link = os.getenv("STRIPE_LINK_MONTHLY", "").strip()
    yearly_link = os.getenv("STRIPE_LINK_YEARLY", "").strip()
    
    # Country flag emoji (optional enhancement)
    flag = ""  # Could add flag mapping if desired
    
    lines = [
        f"{price} to {country} {flag}".strip(),
        "",
        f"TO: {to_city}",
        f"FROM: {from_city}",
        f"OUT:  {out_date}",
        f"BACK: {in_date}",
        "",
        " Our Nomads saw this first. We share deals with them early so they get a bit of breathing room to decide, rather than rushing it.",
        "",
        "If you'd like that early access, to Nomad tier is £3 per month or £30 per year.",
    ]
    
    # Add subscription links if available
    if monthly_link and yearly_link:
        lines.append("")
        lines.append(f'<a href="{monthly_link}">Monthly</a> | <a href="{yearly_link}">Yearly</a>')
    
    lines.append("")
    lines.append("Instagram: @Traveltxter")
    
    return "\n".join(lines).strip()


# ------------------------- DATA MODEL -------------------------


@dataclass
class RawRow:
    idx_1: int  # 1-based sheet row index
    data: Dict[str, str]


def read_raw(ws_raw: gspread.Worksheet) -> Tuple[List[str], List[RawRow]]:
    values = ws_raw.get_all_values()
    if not values:
        return [], []
    headers = values[0]
    hmap = build_hmap(headers)

    out: List[RawRow] = []
    for i, r in enumerate(values[1:], start=2):
        d: Dict[str, str] = {}
        for h, col_i in hmap.items():
            d[h] = (r[col_i] if col_i < len(r) else "").strip()
        out.append(RawRow(i, d))

    return headers, out


def read_rdv_fallback(ws_rdv: gspread.Worksheet) -> Dict[str, Dict[str, str]]:
    """Return mapping deal_id -> rdv fields for fallback selection."""
    try:
        vals = ws_rdv.get_all_values()
    except Exception:
        return {}

    if len(vals) < 2:
        return {}

    headers = vals[0]
    hmap = build_hmap(headers)

    def col(name: str, row: List[str]) -> str:
        i = hmap.get(name)
        return (row[i] if (i is not None and i < len(row)) else "").strip()

    out: Dict[str, Dict[str, str]] = {}
    for r in vals[1:]:
        deal_id = col("deal_id", r)
        if not deal_id:
            continue
        out[deal_id] = {
            "fallback_ok": col("fallback_ok", r),
            "fallback_rank": col("fallback_rank", r),
            "is_fresh_24h": col("is_fresh_24h", r),
            "age_hours": col("age_hours", r),
            "dynamic_theme": col("dynamic_theme", r),
        }
    return out


def is_truthy_cell(v: str) -> bool:
    v = (v or "").strip().lower()
    return v in ("1", "true", "yes", "y", "t")


def to_float(v: str, default: float = 0.0) -> float:
    try:
        return float((v or "").strip())
    except Exception:
        return default


# ------------------------- SELECTION -------------------------


def pick_vip_candidate(
    rows: List[RawRow],
    slot: str,
    theme_today: str,
    rdv_map: Dict[str, Dict[str, str]],
) -> Optional[RawRow]:
    """Pick a VIP candidate.

    Priority:
    1) READY_TO_POST matching publish_window for slot, newest ingested_at_utc first.
    2) If none and ENABLE_FALLBACK: use RDV fallback_ok + is_fresh_24h.
    """

    eligible: List[Tuple[float, RawRow]] = []

    for rr in rows:
        d = rr.data
        if (d.get("status") or "").strip().upper() != "READY_TO_POST":
            continue
        if (d.get("posted_vip_at") or "").strip():
            continue

        # freshness gate
        ing = parse_iso_dt(d.get("ingested_at_utc", ""))
        if not ing:
            continue
        age_h = hours_since(ing)
        if age_h is None:
            continue
        if (datetime.now(timezone.utc) - ing.astimezone(timezone.utc)).total_seconds() < MIN_INGEST_AGE_SECONDS:
            continue
        if age_h > MAX_DEAL_AGE_HOURS:
            continue

        # theme match (soft): if blank theme, allow. If set, must match today's theme.
        theme_row = (d.get("theme") or "").strip().lower()
        if theme_row and theme_today and theme_row != theme_today:
            continue

        if not publish_window_allows(d.get("publish_window", ""), slot):
            continue

        # sort key: newest ingested_at_utc
        eligible.append((ing.timestamp(), rr))

    if eligible:
        eligible.sort(key=lambda t: t[0], reverse=True)
        return eligible[0][1]

    if not ENABLE_FALLBACK:
        return None

    # Fallback lane: must still be unposted by VIP and fresh <24h
    best: Optional[Tuple[float, RawRow]] = None
    for rr in rows:
        d = rr.data
        if (d.get("posted_vip_at") or "").strip():
            continue
        if (d.get("status") or "").strip().upper() == "PUBLISHED":
            continue

        deal_id = (d.get("deal_id") or "").strip()
        if not deal_id:
            continue
        rdv = rdv_map.get(deal_id)
        if not rdv:
            continue

        if not is_truthy_cell(rdv.get("fallback_ok", "")):
            continue
        if not is_truthy_cell(rdv.get("is_fresh_24h", "")):
            continue

        # Also ensure theme matches today (RDV dynamic_theme preferred, else RAW theme)
        rdv_theme = (rdv.get("dynamic_theme") or "").strip().lower()
        theme_row = (d.get("theme") or "").strip().lower()
        if theme_today:
            if rdv_theme and rdv_theme != theme_today:
                continue
            if (not rdv_theme) and theme_row and theme_row != theme_today:
                continue

        rank = to_float(rdv.get("fallback_rank", ""), default=0.0)
        if best is None or rank > best[0]:
            best = (rank, rr)

    return best[1] if best else None


def pick_free_candidate(rows: List[RawRow]) -> Optional[RawRow]:
    """FREE can only post deals VIP already posted, after delay."""

    now = datetime.now(timezone.utc)
    best: Optional[Tuple[float, RawRow]] = None

    for rr in rows:
        d = rr.data
        if (d.get("status") or "").strip().upper() != "READY_FREE":
            continue
        if (d.get("posted_free_at") or "").strip():
            continue

        vip_ts = parse_iso_dt(d.get("posted_vip_at", ""))
        if not vip_ts:
            continue

        # delay gate
        delay_h = (now - vip_ts.astimezone(timezone.utc)).total_seconds() / 3600.0
        if delay_h < FREE_DELAY_HOURS:
            continue

        # age gate (still keep it fresh)
        ing = parse_iso_dt(d.get("ingested_at_utc", ""))
        if not ing:
            continue
        age_h = hours_since(ing)
        if age_h is None or age_h > MAX_DEAL_AGE_HOURS:
            continue

        # Choose oldest VIP-posted first (so FREE catch-up drains backlog)
        key = vip_ts.timestamp()
        if best is None or key < best[0]:
            best = (key, rr)

    return best[1] if best else None


# ------------------------- WRITE BACK -------------------------


def a1(col_idx_0: int, row_idx_1: int) -> str:
    """0-based column, 1-based row -> A1 string."""
    col = col_idx_0 + 1
    s = ""
    while col:
        col, rem = divmod(col - 1, 26)
        s = chr(65 + rem) + s
    return f"{s}{row_idx_1}"


def batch_update_cells(ws: gspread.Worksheet, updates: List[Tuple[int, int, str]]) -> None:
    """updates: list of (row_1, col_0, value)"""
    if not updates:
        return
    data = []
    for r1, c0, v in updates:
        data.append({"range": a1(c0, r1), "values": [[v]]})
    ws.batch_update(data, value_input_option="RAW")


def main() -> int:
    log("======================================================================")
    log("📣 Telegram Publisher V5 — VIP first, FREE mirrors VIP with delay")
    log(f"RAW_DEALS_TAB={RAW_DEALS_TAB} | RAW_DEALS_VIEW_TAB={RAW_DEALS_VIEW_TAB}")
    log(f"RUN_SLOT={RUN_SLOT} | FREE_DELAY_HOURS={FREE_DELAY_HOURS} | MAX_DEAL_AGE_HOURS={MAX_DEAL_AGE_HOURS}")
    log("======================================================================")

    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID env var")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = open_ws(sh, RAW_DEALS_TAB)
    ws_ops = open_ws(sh, OPS_MASTER_TAB)

    # Theme of the day is OPS_MASTER!B2 (per latest contract)
    theme_today = get_cell(ws_ops, "B2").strip().lower()
    log(f"🎯 Theme of day: {theme_today or '(blank)'}")

    raw_headers, raw_rows = read_raw(ws_raw)
    hmap = build_hmap(raw_headers)

    required = [
        "deal_id",
        "origin_iata",
        "destination_iata",
        "outbound_date",
        "return_date",
        "price_gbp",
        "currency",
        "stops",
        "cabin_class",
        "carriers",
        "theme",
        "status",
        "publish_window",
        "graphic_url",
        "booking_link_vip",
        "posted_vip_at",
        "posted_free_at",
        "ingested_at_utc",
    ]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required headers: {missing}")

    rdv_map: Dict[str, Dict[str, str]] = {}
    if ENABLE_FALLBACK:
        try:
            ws_rdv = open_ws(sh, RAW_DEALS_VIEW_TAB)
            rdv_map = read_rdv_fallback(ws_rdv)
        except Exception as e:
            log(f"⚠️ Could not load RDV fallback (non-fatal): {e}")

    # ---------------- VIP stage ----------------
    vip_candidate = pick_vip_candidate(raw_rows, RUN_SLOT, theme_today, rdv_map)

    if vip_candidate:
        msg = build_vip_message(vip_candidate.data)
        log(f"🚀 VIP publish: {vip_candidate.data.get('origin_iata')}→{vip_candidate.data.get('destination_iata')} | {vip_candidate.data.get('deal_id')}")
        tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, msg)

        now_iso = utc_now_iso()
        updates = [
            (vip_candidate.idx_1, hmap["posted_vip_at"], now_iso),
            (vip_candidate.idx_1, hmap["status"], "READY_FREE"),
        ]
        batch_update_cells(ws_raw, updates)
        log("✅ VIP posted + status->READY_FREE")
    else:
        log("ℹ️ No VIP candidate for this slot (READY_TO_POST or fallback).")

    # ---------------- FREE stage ----------------
    free_candidate = pick_free_candidate(raw_rows)

    if free_candidate:
        msg = build_free_message(free_candidate.data)
        log(f"📣 FREE publish: {free_candidate.data.get('origin_iata')}→{free_candidate.data.get('destination_iata')} | {free_candidate.data.get('deal_id')}")
        tg_send(TELEGRAM_BOT_TOKEN_FREE, TELEGRAM_CHANNEL_FREE, msg)

        now_iso = utc_now_iso()
        updates = [
            (free_candidate.idx_1, hmap["posted_free_at"], now_iso),
            (free_candidate.idx_1, hmap["status"], "PUBLISHED"),
        ]
        batch_update_cells(ws_raw, updates)
        log("✅ FREE posted + status->PUBLISHED")
    else:
        log("ℹ️ No FREE candidate ready (needs READY_FREE + VIP delay).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
