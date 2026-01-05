#!/usr/bin/env python3
"""
V4.5x AI Scorer — QUOTA-SAFE (Batch Writes)

Key improvements:
- Batch updates per row (prevents Sheets 429 write-quota errors)
- Retry/backoff on 429
- Phrase bank optional (but logs if missing)
- Deterministic scoring + winner selection
- Produces caption_final in your fixed comms format (IG allows flags only)
"""

import os
import json
import math
import time
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# -------------------------
# Time / logging
# -------------------------
def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


# -------------------------
# Env
# -------------------------
def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    try:
        return int(v) if v else default
    except Exception:
        return default

def env_bool(name: str, default: bool = False) -> bool:
    v = env_str(name, "")
    if not v:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


# -------------------------
# gspread helpers
# -------------------------
def get_gspread_client() -> gspread.Client:
    sa_json = env_str("GCP_SA_JSON_ONE_LINE")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def ensure_columns(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS has no header row.")
    changed = False
    for c in required:
        if c not in headers:
            headers.append(c)
            changed = True
    if changed:
        ws.update([headers], "A1")  # one write
    return {h: i for i, h in enumerate(headers)}

def col_letter(n1: int) -> str:
    """1-indexed col number to A1 letter"""
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1_range(row: int, col_start_0: int, col_end_0: int) -> str:
    """Row is 1-indexed. Col indices are 0-indexed inclusive."""
    start = f"{col_letter(col_start_0 + 1)}{row}"
    end = f"{col_letter(col_end_0 + 1)}{row}"
    return f"{start}:{end}"

def batch_update_with_backoff(ws: gspread.Worksheet, data: List[Dict[str, Any]], max_tries: int = 6) -> None:
    """
    data = [{"range": "A2:D2", "values": [[...]]}, ...]
    Retries on Sheets 429 with exponential backoff.
    """
    delay = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            ws.batch_update(data)
            return
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⚠️ Sheets 429 quota hit. Backoff {delay:.1f}s (attempt {attempt}/{max_tries})")
                time.sleep(delay)
                delay = min(delay * 2, 20.0)
                continue
            raise


# -------------------------
# Phrase bank (optional)
# -------------------------
def load_phrase_bank(sh: gspread.Spreadsheet, tab_name: str) -> List[Dict[str, str]]:
    if not tab_name:
        return []
    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        return []
    rows = ws.get_all_values()
    if len(rows) < 2:
        return []
    headers = [h.strip() for h in rows[0]]
    out: List[Dict[str, str]] = []
    for r in rows[1:]:
        item = {}
        for i, h in enumerate(headers):
            item[h] = r[i].strip() if i < len(r) else ""
        out.append(item)
    return out

def pick_phrase(phrases: List[Dict[str, str]], theme: str, channel: str) -> Optional[Tuple[str, str]]:
    def get(item: Dict[str, str], *keys: str) -> str:
        for k in keys:
            if k in item and item[k]:
                return item[k]
        return ""

    candidates: List[Tuple[str, str]] = []
    for p in phrases:
        approved = get(p, "approved", "Approved").upper()
        if approved not in ("TRUE", "YES", "1", "Y"):
            continue
        ch = (get(p, "channel", "Channel").upper() or "ALL")
        if ch not in ("ALL", channel):
            continue
        th = (get(p, "theme", "Theme").upper() or "ALL")
        if th not in ("ALL", theme.upper()):
            continue
        block = (get(p, "block", "Block").lower() or "benefit_line")
        if block != "benefit_line":
            continue

        pid = get(p, "phrase_id", "id", "phraseId")
        txt = get(p, "text", "phrase", "Text")
        if pid and txt:
            candidates.append((pid, txt))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])  # deterministic
    return candidates[0]


# -------------------------
# Scoring (simple + explainable)
# -------------------------
def parse_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None

def parse_int(x: str) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return 0

def days_until(date_iso: str) -> Optional[int]:
    try:
        d = dt.date.fromisoformat(date_iso)
        return (d - now_utc_dt().date()).days
    except Exception:
        return None

def clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))

def component_scores(price: float, stops: int, depart_in: Optional[int]) -> Dict[str, float]:
    value = 60 * (1.0 - clamp((price - 40.0) / 120.0, 0.0, 1.0))
    route = 20 if stops <= 0 else (12 if stops == 1 else 5)
    if depart_in is None:
        timing = 10
    elif 14 <= depart_in <= 90:
        timing = 20
    elif 7 <= depart_in < 14:
        timing = 14
    elif depart_in < 7:
        timing = 8
    else:
        timing = 12
    return {"value_score": value, "route_score": route, "timing_score": timing}

def score_total(components: Dict[str, float]) -> int:
    total = components["value_score"] + components["route_score"] + components["timing_score"]
    return int(round(clamp(total, 0, 100)))

def grade(total: int) -> Tuple[str, str]:
    # NOTE: Your prior run showed POST at 82/86 and SKIP at 60/64.
    # We'll keep threshold at >=80 for POST.
    if total >= 80:
        return ("POST", "A")
    if total >= 65:
        return ("MAYBE", "B")
    return ("SKIP", "C")


# -------------------------
# Caption / Comms
# -------------------------
COUNTRY_FLAG = {
    "ICELAND": "🇮🇸",
    "THAILAND": "🇹🇭",
    "SPAIN": "🇪🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "JAPAN": "🇯🇵",
    "SWITZERLAND": "🇨🇭",
}

def caption_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def build_caption_fixed(
    price_gbp: float,
    country: str,
    flag: str,
    to_city: str,
    from_city: str,
    out_date: str,
    back_date: str,
    benefit_line: Optional[str],
    monthly_link: str,
    yearly_link: str,
    include_flag: bool,
) -> str:
    first = f"£{price_gbp:.2f} to {country.title()}"
    if include_flag and flag:
        first = f"{first} {flag}"

    parts = [
        first,
        "",
        f"TO: {to_city.upper()}",
        f"FROM: {from_city}",
        f"OUT: {out_date}",
        f"BACK: {back_date}",
    ]
    if benefit_line:
        parts += ["", benefit_line]

    # If Stripe links are missing, we still keep the block but without broken URLs
    upgrade_line = "Upgrade now →"
    if monthly_link and yearly_link:
        upgrade_line = f"Upgrade now → Monthly: {monthly_link}  Yearly: {yearly_link}"

    parts += [
        "",
        "Heads up:",
        "• VIP members saw this 24 hours ago",
        "• Availability is running low",
        "• Best deals go to VIPs first",
        "",
        "Want instant access?",
        "Join TravelTxter Nomad",
        "for £7.99 / month:",
        "",
        "• Deals 24 hours early",
        "• Direct booking links",
        "• Exclusive mistake fares",
        "• Cancel anytime",
        "",
        upgrade_line,
    ]
    return "\n".join(parts).strip()


# -------------------------
# Main
# -------------------------
def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    phrases_tab = env_str("PHRASES_TAB", "")
    run_slot = env_str("RUN_SLOT", "AM").upper()
    max_rows = env_int("SCORER_MAX_ROWS", 20)
    select_winner = env_bool("SELECT_WINNER", True)

    monthly_link = env_str("STRIPE_MONTHLY_LINK", "")
    yearly_link = env_str("STRIPE_YEARLY_LINK", "")
    theme_of_day = env_str("THEME", "DEFAULT").upper()

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    log("============================================================")
    log("🧠 V4.5x Scorer starting (QUOTA-SAFE batch writes)")
    log(f"RUN_SLOT={run_slot} MAX_ROWS={max_rows} SELECT_WINNER={select_winner}")
    if not phrases_tab:
        log("⚠️ PHRASES_TAB is blank (phrase bank will NOT be used).")
    if not (monthly_link and yearly_link):
        log("⚠️ Stripe links missing (caption upsell links will be plain text).")
    if not env_str("THEME", ""):
        log("⚠️ THEME is blank (defaulting to DEFAULT).")
    log("============================================================")

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    required_cols = [
        # inputs
        "deal_id","origin_city","origin_iata","destination_city","destination_iata","destination_country",
        "price_gbp","outbound_date","return_date","stops","airline","theme","status","affiliate_url",
        # outputs
        "ai_score","ai_verdict","ai_grading","scored_timestamp",
        "caption_final","phrase_ids_used","caption_hash",
        "is_instagram_eligible","is_telegram_eligible"
    ]
    hm = ensure_columns(ws, required_cols)

    phrases = load_phrase_bank(sh, phrases_tab)

    rows = ws.get_all_values()
    if len(rows) <= 1:
        log("No rows.")
        return 0

    data = rows[1:]

    def get(row: List[str], col: str) -> str:
        idx = hm.get(col)
        if idx is None or idx >= len(row):
            return ""
        return row[idx].strip()

    candidates: List[Tuple[int, int]] = []  # (sheet_row, score)
    processed = 0

    # For each NEW row: compute outputs, then batch-write row fields in ONE request.
    for sheet_row, row in enumerate(data, start=2):
        if processed >= max_rows:
            break
        if get(row, "status").upper() != "NEW":
            continue

        origin_city = get(row, "origin_city") or get(row, "origin_iata")
        dest_city = get(row, "destination_city") or get(row, "destination_iata")
        country = get(row, "destination_country") or ""
        price = parse_float(get(row, "price_gbp"))
        out_date = get(row, "outbound_date")
        back_date = get(row, "return_date")
        stops = parse_int(get(row, "stops"))
        theme = (get(row, "theme") or theme_of_day).upper()

        # Must have minimum publishable facts; if not, skip touching it.
        if not (origin_city and dest_city and out_date and back_date and price is not None):
            processed += 1
            continue

        depart_in = days_until(out_date)
        comps = component_scores(price, stops, depart_in)
        total = score_total(comps)
        verdict, grading = grade(total)

        is_ig = "TRUE" if verdict == "POST" else "FALSE"
        is_tg = "TRUE" if verdict in ("POST", "MAYBE") else "FALSE"

        picked = pick_phrase(phrases, theme, "IG" if run_slot == "AM" else "TG")
        phrase_id = picked[0] if picked else ""
        benefit_line = picked[1] if picked else None

        flag = COUNTRY_FLAG.get(country.upper(), "")
        include_flag = True  # IG caption can include flags only

        caption = build_caption_fixed(
            price_gbp=price,
            country=(country or dest_city),
            flag=flag,
            to_city=dest_city,
            from_city=origin_city,
            out_date=out_date,
            back_date=back_date,
            benefit_line=benefit_line,
            monthly_link=monthly_link,
            yearly_link=yearly_link,
            include_flag=include_flag,
        )

        next_status = "SCORED" if verdict != "SKIP" else "SKIPPED"

        # Build a single-row write across ALL columns by updating just the specific output cells.
        # We do this by writing each output to its cell, but inside ONE batch_update request.
        updates: List[Dict[str, Any]] = []
        def set_cell(col_name: str, val: Any) -> None:
            c0 = hm[col_name]
            rng = a1_range(sheet_row, c0, c0)
            updates.append({"range": rng, "values": [[val]]})

        set_cell("ai_score", str(total))
        set_cell("ai_verdict", verdict)
        set_cell("ai_grading", grading)
        set_cell("scored_timestamp", now_utc_str())
        set_cell("caption_final", caption)
        set_cell("phrase_ids_used", phrase_id)
        set_cell("caption_hash", caption_hash(caption))
        set_cell("is_instagram_eligible", is_ig)
        set_cell("is_telegram_eligible", is_tg)
        set_cell("status", next_status)

        batch_update_with_backoff(ws, updates)

        log(f"✅ Scored row {sheet_row}: score={total} verdict={verdict} -> {next_status}")

        if verdict == "POST":
            candidates.append((sheet_row, total))

        processed += 1

    # Winner selection: ONE more batch update (single request)
    if select_winner and candidates:
        candidates.sort(key=lambda x: (-x[1], x[0]))
        winner_row, winner_score = candidates[0]
        rng = a1_range(winner_row, hm["status"], hm["status"])
        batch_update_with_backoff(ws, [{"range": rng, "values": [["READY_TO_PUBLISH"]]}])
        log(f"🏁 Winner row {winner_row} score={winner_score} -> READY_TO_PUBLISH")

    log("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
