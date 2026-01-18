# workers/ai_scorer.py
# V4.7.1 — scorer + deterministic phrase selector (hotfix: int-safe _norm)
# Contract:
# - RAW_DEALS is canonical
# - RAW_DEALS_VIEW is read-only
# - Phrase selection happens ONCE at promotion time
# - Publishers never select language
# - Full-file replacement only

import os
import json
import hashlib
from datetime import datetime, timezone

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


RAW_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
PHRASE_TAB = os.getenv("PHRASE_BANK_TAB", "PHRASE_BANK")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
SA_JSON = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")

MIN_INGEST_AGE_SECONDS = int(os.getenv("MIN_INGEST_AGE_SECONDS", "90"))
WINNERS_PER_RUN = int(os.getenv("WINNERS_PER_RUN", "1"))

PHRASE_USED_COL = "phrase_used"
PHRASE_BANK_COL = "phrase_bank"


def _log(msg):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{ts} | {msg}", flush=True)


def _sa_creds():
    if not SA_JSON:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    raw = SA_JSON.strip()
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )


def _norm(s):
    """Return a trimmed string for any input type (int-safe)."""
    if s is None:
        return ""
    if isinstance(s, str):
        return s.strip()
    return str(s).strip()


def _norm_theme(s):
    return _norm(s).lower().replace(" ", "_")


def _norm_iata(s):
    return _norm(s).upper()


def _truthy(v):
    return _norm(v).upper() in ("TRUE", "YES", "Y", "1", "APPROVED")


def _stable_pick(key, items):
    if not items:
        return ""
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(items)
    return items[idx]


def _parse_iso_utc(ts_raw):
    """
    Accepts '2026-01-01T12:34:56Z' or without 'Z'.
    Returns aware UTC datetime or None.
    """
    s = _norm(ts_raw)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        dt = datetime.fromisoformat(s)
        # assume UTC if naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_TAB)
    ws_view = sh.worksheet(VIEW_TAB)

    # Load phrase bank (optional; system must not break if unavailable)
    try:
        ws_phrase = sh.worksheet(PHRASE_TAB)
        phrase_rows = ws_phrase.get_all_records()
    except Exception as e:
        _log(f"PHRASE_BANK not readable: {e}")
        phrase_rows = []

    # Build phrase index: (dest_iata, theme) -> [phrases...]
    phrase_index = []
    for r in phrase_rows:
        theme = _norm_theme(r.get("theme"))
        phrase = _norm(r.get("phrase"))
        approved = _truthy(r.get("approved"))

        dest = ""
        cat = _norm(r.get("category")).lower()
        if cat.startswith("dest:"):
            dest = _norm_iata(cat.split("dest:", 1)[1])

        if dest and theme and phrase and approved:
            phrase_index.append({"dest": dest, "theme": theme, "phrase": phrase})

    view_rows = ws_view.get_all_records()
    raw_rows = ws_raw.get_all_records()

    headers = [h.strip() for h in ws_raw.row_values(1)]
    col = {h: i + 1 for i, h in enumerate(headers)}

    # Hard requirements (fail fast with a clear message)
    for required in ("status", "deal_id", "ingested_at_utc", PHRASE_USED_COL, PHRASE_BANK_COL):
        if required not in col:
            raise RuntimeError(f"Missing required RAW_DEALS column: {required}")

    now = datetime.now(timezone.utc)

    # Map deal_id -> RAW_DEALS row number and ingested_at
    deal_row = {}
    deal_ingested = {}
    for idx, r in enumerate(raw_rows, start=2):
        did = _norm(r.get("deal_id"))
        if did:
            deal_row[did] = idx
            deal_ingested[did] = r.get("ingested_at_utc")

    candidates = []
    too_fresh = 0

    for r in view_rows:
        if _norm(r.get("status")) != "NEW":
            continue

        did = _norm(r.get("deal_id"))
        if not did:
            continue
        if did not in deal_row:
            continue

        ts = _parse_iso_utc(deal_ingested.get(did))
        if not ts:
            continue

        if (now - ts).total_seconds() < MIN_INGEST_AGE_SECONDS:
            too_fresh += 1
            continue

        candidates.append(r)

    _log(f"Eligible NEW candidates: {len(candidates)} | skipped_too_fresh={too_fresh}")

    if not candidates:
        _log("No eligible NEW rows")
        return 0

    promoted = 0
    updates = []

    for r in candidates:
        if promoted >= WINNERS_PER_RUN:
            break

        did = _norm(r.get("deal_id"))
        rownum = deal_row[did]

        dest = _norm_iata(r.get("destination_iata"))
        theme = _norm_theme(r.get("dynamic_theme"))

        phrases = sorted(
            [p["phrase"] for p in phrase_index if p["dest"] == dest and p["theme"] == theme]
        )
        phrase = _stable_pick(f"{did}|{dest}|{theme}", phrases)

        updates.append(Cell(rownum, col["status"], "READY_TO_POST"))
        updates.append(Cell(rownum, col[PHRASE_USED_COL], phrase))
        updates.append(Cell(rownum, col[PHRASE_BANK_COL], phrase))

        promoted += 1
        _log(f"Promoted {did} → READY_TO_POST | dest={dest} theme={theme} phrase={'YES' if phrase else 'NO'}")

    if updates:
        ws_raw.update_cells(updates, value_input_option="RAW")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
