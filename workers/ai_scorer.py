#!/usr/bin/env python3
import os, json, hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

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
def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    ws.update([[value]], a1)

def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1_for(row: int, col_index_0: int) -> str:
    return f"{col_letter(col_index_0 + 1)}{row}"

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
        ws.update([headers], "A1")
    return {h: i for i, h in enumerate(headers)}

# -------------------------
# Phrase bank
# Expected columns (flexible):
# - phrase_id (or id)
# - approved (TRUE/FALSE)
# - channel (IG/TG/ALL) optional
# - theme optional
# - block optional (benefit_line)
# - text (or phrase)
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
    """
    Returns (phrase_id, text) for a single optional benefit line.
    Deterministic: first match by sorted phrase_id.
    """
    def get(item: Dict[str, str], *keys: str) -> str:
        for k in keys:
            if k in item and item[k]:
                return item[k]
        return ""

    candidates = []
    for p in phrases:
        approved = get(p, "approved", "Approved").upper()
        if approved not in ("TRUE", "YES", "1", "Y"):
            continue
        ch = get(p, "channel", "Channel").upper() or "ALL"
        if ch not in ("ALL", channel):
            continue
        th = get(p, "theme", "Theme").upper() or "ALL"
        if th not in ("ALL", theme.upper()):
            continue
        block = get(p, "block", "Block").lower() or "benefit_line"
        if block != "benefit_line":
            continue
        pid = get(p, "phrase_id", "id", "phraseId") or ""
        txt = get(p, "text", "phrase", "Text") or ""
        if pid and txt:
            candidates.append((pid, txt))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
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
        today = now_utc_dt().date()
        return (d - today).days
    except Exception:
        return None

def clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))

def component_scores(price: float, stops: int, depart_in: Optional[int]) -> Dict[str, float]:
    # Value (0-60): cheaper better
    value = 60 * (1.0 - clamp((price - 40.0) / 120.0, 0.0, 1.0))
    # Route (0-20): direct better
    route = 20 if stops <= 0 else (12 if stops == 1 else 5)
    # Timing (0-20): 14-90 best
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
    if total >= 80:
        return ("POST", "A")
    if total >= 65:
        return ("MAYBE", "B")
    return ("SKIP", "C")

# -------------------------
# Caption formatting contracts
# - IG: only flag emojis allowed (we won't add emojis unless provided by mapping)
# - TG: no emojis
# - All channels: fixed block format
# -------------------------

# Minimal country->flag (extend as needed)
COUNTRY_FLAG = {
    "ICELAND": "🇮🇸",
    "THAILAND": "🇹🇭",
    "SPAIN": "🇪🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "JAPAN": "🇯🇵",
}

def build_primary_block(
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
    include_upsell: bool,
) -> str:
    # First line: £xxx.xx to Country (flag optional)
    first = f"£{price_gbp:.2f} to {country.title()}"
    if include_flag and flag:
        first = f"{first} {flag}"

    lines = [
        first,
        "",
        f"TO: {to_city.upper()}",
        f"FROM: {from_city}",
        f"OUT: {out_date}",
        f"BACK: {back_date}",
    ]

    if benefit_line:
        lines += ["", benefit_line]

    if include_upsell:
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
            f"Upgrade now → Monthly: {monthly_link}  Yearly: {yearly_link}",
        ]

    return "\n".join(lines).strip()

def caption_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

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

    monthly_link = env_str("STRIPE_MONTHLY_LINK")
    yearly_link = env_str("STRIPE_YEARLY_LINK")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    log("============================================================")
    log("🧠 V4.5x Scorer starting (phrase-bank + fixed format)")
    log(f"RUN_SLOT={run_slot} SELECT_WINNER={select_winner}")
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
        "caption_headline","caption_body","caption_footer","caption_final","phrase_ids_used","caption_hash",
        "is_instagram_eligible","is_telegram_eligible"
    ]
    hm = ensure_columns(ws, required_cols)

    phrases = load_phrase_bank(sh, phrases_tab)
    theme_of_day = env_str("THEME", "DEFAULT").upper()

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

    # 1) Score NEW rows → SCORED (or SKIPPED)
    for i, row in enumerate(data, start=2):
        if processed >= max_rows:
            break
        status = get(row, "status").upper()
        if status != "NEW":
            continue

        origin_city = get(row, "origin_city") or get(row, "origin_iata")
        dest_city = get(row, "destination_city") or get(row, "destination_iata")
        country = get(row, "destination_country") or ""
        price = parse_float(get(row, "price_gbp"))
        out_date = get(row, "outbound_date")
        back_date = get(row, "return_date")
        stops = parse_int(get(row, "stops"))
        theme = (get(row, "theme") or theme_of_day).upper()

        # Required facts
        if not (dest_city and origin_city and out_date and back_date and price is not None):
            # leave as NEW (data incomplete)
            processed += 1
            continue

        depart_in = days_until(out_date)
        comps = component_scores(price, stops, depart_in)
        total = score_total(comps)
        verdict, grading = grade(total)

        # Eligibility
        is_ig = "TRUE" if verdict == "POST" else "FALSE"
        is_tg = "TRUE" if verdict in ("POST", "MAYBE") else "FALSE"

        # Benefit line from phrase bank (optional)
        # Channel-specific: IG captions can include a flag emoji but ONLY that
        picked = pick_phrase(phrases, theme, "IG" if run_slot == "AM" else "TG")
        phrase_id = picked[0] if picked else ""
        benefit_line = picked[1] if picked else None

        # Flag emoji only for IG
        flag = COUNTRY_FLAG.get(country.upper(), "")
        include_flag = True  # IG only; TG will not use this caption block
        include_upsell = True  # per your format

        caption = build_primary_block(
            price_gbp=price,
            country=country or dest_city,
            flag=flag,
            to_city=dest_city,
            from_city=origin_city,
            out_date=out_date,
            back_date=back_date,
            benefit_line=benefit_line,
            monthly_link=monthly_link,
            yearly_link=yearly_link,
            include_flag=include_flag,
            include_upsell=include_upsell,
        )

        # Write outputs
        a1_update(ws, a1_for(i, hm["ai_score"]), str(total))
        a1_update(ws, a1_for(i, hm["ai_verdict"]), verdict)
        a1_update(ws, a1_for(i, hm["ai_grading"]), grading)
        a1_update(ws, a1_for(i, hm["scored_timestamp"]), now_utc_str())

        a1_update(ws, a1_for(i, hm["caption_final"]), caption)
        a1_update(ws, a1_for(i, hm["caption_hash"]), caption_hash(caption))
        a1_update(ws, a1_for(i, hm["phrase_ids_used"]), phrase_id)

        a1_update(ws, a1_for(i, hm["is_instagram_eligible"]), is_ig)
        a1_update(ws, a1_for(i, hm["is_telegram_eligible"]), is_tg)

        next_status = "SCORED" if verdict != "SKIP" else "SKIPPED"
        a1_update(ws, a1_for(i, hm["status"]), next_status)

        if verdict == "POST":
            candidates.append((i, total))

        processed += 1
        log(f"✅ Scored row {i}: score={total} verdict={verdict} -> {next_status}")

    # 2) Select a single winner → READY_TO_PUBLISH
    if select_winner and candidates:
        candidates.sort(key=lambda x: (-x[1], x[0]))
        winner_row, winner_score = candidates[0]
        a1_update(ws, a1_for(winner_row, hm["status"]), "READY_TO_PUBLISH")
        log(f"🏁 Winner row {winner_row} score={winner_score} -> READY_TO_PUBLISH")

    log("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
