#!/usr/bin/env python3
"""
TravelTxter V4.5x — AI Scorer (Copy/Paste)

What this file does (and ONLY this file does):
- Finds rows where status == NEW
- Calculates deal_score (0–100)
- Writes scoring fields + caption_final
- Sets status:
    NEW -> SCORED   (publishable candidates)
    NEW -> SKIPPED  (not good enough / missing fields)
- Selects ONE winner from SCORED rows and promotes:
    SCORED -> READY_TO_POST

Critical fix included:
✅ Always promotes at least ONE SCORED row to READY_TO_POST (if any exist),
so render/publish stages can actually run.

Quota-safe:
✅ Uses ONE batch_update per scored row + one batch_update for winner promotion.

Env vars used:
- SPREADSHEET_ID (required)
- RAW_DEALS_TAB (default: RAW_DEALS)
- GCP_SA_JSON_ONE_LINE (required)
- SCORER_MAX_ROWS (default: 25)
- SELECT_WINNER (default: true)
- THEME (optional, default: DEFAULT)
- PHRASES_TAB (optional; if set, pulls phrase bank)
- STRIPE_MONTHLY_LINK (optional)
- STRIPE_YEARLY_LINK (optional)
"""

import os
import json
import time
import math
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# -------------------------
# Time / logging
# -------------------------
def now_utc_str() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


# -------------------------
# Env helpers
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
# Google / Sheets helpers
# -------------------------
def get_gspread_client() -> gspread.Client:
    sa_json = env_str("GCP_SA_JSON_ONE_LINE")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
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
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(row: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{row}"


def batch_update_with_backoff(ws: gspread.Worksheet, data: List[Dict[str, Any]], max_tries: int = 6) -> None:
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
# Parsing helpers
# -------------------------
def parse_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None


def parse_int(x: str, default: int = 0) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def days_until(date_iso: str) -> Optional[int]:
    try:
        d = dt.date.fromisoformat(date_iso)
        return (d - dt.datetime.utcnow().date()).days
    except Exception:
        return None


def clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


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
        item: Dict[str, str] = {}
        for i, h in enumerate(headers):
            item[h] = r[i].strip() if i < len(r) else ""
        out.append(item)
    return out


def pick_phrase(phrases: List[Dict[str, str]], theme: str) -> Optional[Tuple[str, str]]:
    """
    Very strict: only approved TRUE/YES/1.
    Looks for a benefit_line text.
    Supports columns: approved, theme, block, phrase_id, text (case-insensitive-ish).
    """
    def get(item: Dict[str, str], *keys: str) -> str:
        for k in keys:
            if k in item and item[k]:
                return item[k]
        return ""

    candidates: List[Tuple[str, str]] = []
    for p in phrases:
        approved = (get(p, "approved", "Approved") or "").strip().upper()
        if approved not in ("TRUE", "YES", "1", "Y"):
            continue

        th = (get(p, "theme", "Theme") or "ALL").strip().upper()
        if th not in ("ALL", theme.upper()):
            continue

        block = (get(p, "block", "Block") or "benefit_line").strip().lower()
        if block != "benefit_line":
            continue

        pid = (get(p, "phrase_id", "id", "phraseId") or "").strip()
        txt = (get(p, "text", "phrase", "Text") or "").strip()
        if pid and txt:
            candidates.append((pid, txt))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])  # deterministic
    return candidates[0]


# -------------------------
# Flags (IG: flags only)
# -------------------------
COUNTRY_FLAG = {
    "ICELAND": "🇮🇸",
    "SPAIN": "🇪🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "THAILAND": "🇹🇭",
    "JAPAN": "🇯🇵",
    "SWITZERLAND": "🇨🇭",
    "AUSTRIA": "🇦🇹",
    "NORWAY": "🇳🇴",
    "SWEDEN": "🇸🇪",
    "FINLAND": "🇫🇮",
    "MEXICO": "🇲🇽",
}


# -------------------------
# Scoring
# -------------------------
def compute_components(price: float, stops: int, depart_in: Optional[int]) -> Dict[str, float]:
    # Value: cheaper is better; assumes most short-haul sweet spot 40–160
    value = 60 * (1.0 - clamp((price - 40.0) / 140.0, 0.0, 1.0))

    # Route friction: direct best
    route = 20 if stops <= 0 else (12 if stops == 1 else 5)

    # Timing: 2–12 weeks best; too soon or too far less ideal
    if depart_in is None:
        timing = 10
    elif 14 <= depart_in <= 90:
        timing = 20
    elif 7 <= depart_in < 14:
        timing = 14
    elif 1 <= depart_in < 7:
        timing = 9
    elif depart_in <= 0:
        timing = 0
    else:
        timing = 12

    return {"value": value, "route": route, "timing": timing}


def total_score(components: Dict[str, float]) -> int:
    tot = components["value"] + components["route"] + components["timing"]
    return int(round(clamp(tot, 0, 100)))


def verdict_from_score(score: int) -> Tuple[str, str]:
    """
    verdict, grading
    - POST: publishable
    - MAYBE: publishable if you need product / variety
    - SKIP: not publishable
    """
    if score >= 80:
        return "POST", "A"
    if score >= 65:
        return "MAYBE", "B"
    return "SKIP", "C"


# -------------------------
# Caption (locked comms style)
# -------------------------
def caption_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def build_caption_final(
    price: float,
    country: str,
    to_city: str,
    from_city: str,
    out_date: str,
    back_date: str,
    benefit_line: Optional[str],
    monthly_link: str,
    yearly_link: str,
) -> str:
    flag = COUNTRY_FLAG.get((country or "").upper(), "")
    header = f"£{price:.2f} to {country.title()}" if country else f"£{price:.2f} to {to_city}"
    if flag:
        header = f"{header} {flag}"  # IG allowed emojis: flags only

    lines = [
        header,
        f"TO: {to_city.upper()}",
        f"FROM: {from_city}",
        f"OUT:  {out_date}",
        f"BACK: {back_date}",
    ]

    if benefit_line:
        # Must be plain text; no emojis.
        lines += ["", benefit_line]

    # Upsell block (hyperlinks are handled by Telegram/IG platform; here we just include URLs)
    lines += [
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
    ]

    if monthly_link and yearly_link:
        lines += [
            f"Upgrade now (Monthly): {monthly_link}",
            f"Upgrade now (Yearly): {yearly_link}",
        ]
    else:
        lines += ["Upgrade now (Monthly + Yearly links configured in pipeline)"]

    return "\n".join(lines).strip()


# -------------------------
# Main
# -------------------------
def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    phrases_tab = env_str("PHRASES_TAB", "")
    max_rows = env_int("SCORER_MAX_ROWS", 25)
    select_winner = env_bool("SELECT_WINNER", True)
    theme_of_day = env_str("THEME", "DEFAULT").upper()

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK", "")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK", "")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    log("============================================================")
    log("🧠 TravelTxter V4.5x Scorer starting")
    log(f"MAX_ROWS={max_rows} SELECT_WINNER={select_winner} THEME={theme_of_day}")
    if not phrases_tab:
        log("⚠️ PHRASES_TAB is blank (phrase bank disabled).")
    if not (stripe_monthly and stripe_yearly):
        log("⚠️ Stripe links missing (captions will include placeholder upgrade line).")
    log("============================================================")

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    # Support both your newer columns and earlier builds
    required_cols = [
        "status",
        "deal_id",
        "price_gbp",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "destination_country",
        "outbound_date",
        "return_date",
        "stops",
        "deal_theme",     # if present
        "theme",          # if present
        "deal_score",     # canonical score column
        "ai_score",       # legacy score column
        "ai_verdict",
        "ai_grading",
        "caption_final",
        "phrase_ids_used",
        "caption_hash",
        "scored_timestamp",
        "is_instagram_eligible",
        "is_telegram_eligible",
        "last_error",
        "fail_count",
    ]
    hm = ensure_columns(ws, required_cols)

    phrases = load_phrase_bank(sh, phrases_tab)

    values = ws.get_all_values()
    if len(values) <= 1:
        log("No data rows.")
        return 0

    headers = values[0]
    data = values[1:]

    def get(row: List[str], col: str) -> str:
        idx = hm.get(col, -1)
        if idx < 0:
            return ""
        return row[idx].strip() if idx < len(row) else ""

    processed = 0
    scored_candidates: List[Tuple[int, int]] = []  # (sheet_row, score)

    for sheet_row, row in enumerate(data, start=2):
        if processed >= max_rows:
            break
        if get(row, "status").upper() != "NEW":
            continue

        # Required publish facts
        origin_city = get(row, "origin_city") or get(row, "origin_iata")
        dest_city = get(row, "destination_city") or get(row, "destination_iata")
        country = get(row, "destination_country")
        out_date = get(row, "outbound_date")
        back_date = get(row, "return_date")
        price = parse_float(get(row, "price_gbp"))
        stops = parse_int(get(row, "stops"), default=0)

        # Theme source: deal_theme > theme > env THEME
        row_theme = (get(row, "deal_theme") or get(row, "theme") or theme_of_day).strip().upper() or "DEFAULT"

        # If missing key fields, skip safely
        if not (origin_city and dest_city and out_date and back_date and price is not None):
            msg = "Missing required fields for scoring (need origin/destination city, dates, price_gbp)."
            log(f"⚠️ Row {sheet_row}: {msg} -> SKIPPED")
            updates = [
                {"range": a1(sheet_row, hm["status"]), "values": [["SKIPPED"]]},
                {"range": a1(sheet_row, hm["last_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["scored_timestamp"]), "values": [[now_utc_str()]]},
            ]
            batch_update_with_backoff(ws, updates)
            processed += 1
            continue

        depart_in = days_until(out_date)
        comps = compute_components(price, stops, depart_in)
        score = total_score(comps)
        verdict, grading = verdict_from_score(score)

        # Phrase bank: optional benefit line
        picked = pick_phrase(phrases, row_theme)
        phrase_id = picked[0] if picked else ""
        benefit_line = picked[1] if picked else None

        caption = build_caption_final(
            price=price,
            country=country,
            to_city=dest_city,
            from_city=origin_city,
            out_date=out_date,
            back_date=back_date,
            benefit_line=benefit_line,
            monthly_link=stripe_monthly,
            yearly_link=stripe_yearly,
        )

        # Status gating: only publishable candidates remain SCORED
        next_status = "SCORED" if verdict in ("POST", "MAYBE") else "SKIPPED"

        is_ig = "TRUE" if verdict == "POST" else "FALSE"
        is_tg = "TRUE" if verdict in ("POST", "MAYBE") else "FALSE"

        # Batch write ALL outputs for this row in one request
        updates: List[Dict[str, Any]] = []

        def set_cell(col_name: str, val: Any) -> None:
            c0 = hm[col_name]
            updates.append({"range": a1(sheet_row, c0), "values": [[val]]})

        set_cell("deal_score", str(score))
        set_cell("ai_score", str(score))      # keep legacy in sync
        set_cell("ai_verdict", verdict)
        set_cell("ai_grading", grading)
        set_cell("caption_final", caption)
        set_cell("phrase_ids_used", phrase_id)
        set_cell("caption_hash", caption_hash(caption))
        set_cell("scored_timestamp", now_utc_str())
        set_cell("is_instagram_eligible", is_ig)
        set_cell("is_telegram_eligible", is_tg)
        set_cell("last_error", "")
        set_cell("status", next_status)

        batch_update_with_backoff(ws, updates)

        log(f"✅ Scored row {sheet_row}: score={score} verdict={verdict} -> {next_status}")

        if next_status == "SCORED":
            scored_candidates.append((sheet_row, score))

        processed += 1

    # -------------------------
    # WINNER PROMOTION (the missing piece)
    # -------------------------
    if select_winner:
        # If we didn't score any NEW rows this run, still try to pick from existing SCORED rows.
        if not scored_candidates:
            # Scan existing rows for SCORED so pipeline can continue even if feeder ran earlier.
            for sheet_row, row in enumerate(data, start=2):
                if get(row, "status").upper() != "SCORED":
                    continue
                s = parse_int(get(row, "deal_score") or get(row, "ai_score"), default=0)
                scored_candidates.append((sheet_row, s))

        if scored_candidates:
            scored_candidates.sort(key=lambda x: (-x[1], x[0]))  # highest score wins, then earliest row
            winner_row, winner_score = scored_candidates[0]

            # Promote exactly ONE row to READY_TO_POST
            batch_update_with_backoff(ws, [
                {"range": a1(winner_row, hm["status"]), "values": [["READY_TO_POST"]]},
            ])
            log(f"🏆 Winner selected: row {winner_row} score={winner_score} -> READY_TO_POST")
        else:
            log("⚠️ No SCORED rows available to promote. Nothing will publish.")

    log("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
