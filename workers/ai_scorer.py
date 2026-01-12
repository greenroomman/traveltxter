# workers/ai_scorer.py
import os
import json
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_SA_JSON_ONE_LINE = os.environ.get("GCP_SA_JSON_ONE_LINE")

# Commercial tuning (safe defaults; can still be overridden by env like before)
MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", 50))
WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", 3))  # 🔥 default now 3
VARIETY_LOOKBACK_HOURS = int(os.environ.get("VARIETY_LOOKBACK_HOURS", 120))
DEST_REPEAT_PENALTY = int(os.environ.get("DEST_REPEAT_PENALTY", 80))
THEME_REPEAT_PENALTY = int(os.environ.get("THEME_REPEAT_PENALTY", 30))
HARD_BLOCK_BAD_DEALS = os.environ.get("HARD_BLOCK_BAD_DEALS", "true").lower() == "true"

NOW = datetime.now(timezone.utc)


def _log(msg: str) -> None:
    print(f"{NOW.strftime('%Y-%m-%dT%H:%M:%SZ')} | {msg}")


def _get_gspread_client():
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE secret/env var")
    sa = json.loads(GCP_SA_JSON_ONE_LINE)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)


def _open_ws(gc, tab_name: str):
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(tab_name)


def _headers(ws):
    # Assumes headers are in row 1 (existing system convention)
    return [h.strip() for h in ws.row_values(1)]


def _col_idx_map(headers):
    # 1-based column index
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _find_first_existing(m: dict, candidates):
    for c in candidates:
        if c in m and m[c] not in (None, ""):
            return c
    return None


def _safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return float(default)
        # strip currency symbols if present
        if isinstance(x, str):
            x = x.replace("£", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return float(default)


def _parse_iso_dt(s):
    if not s:
        return None
    try:
        # tolerate Z
        if isinstance(s, str) and s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _compute_variety_penalties(raw_rows, candidate):
    """
    raw_rows: list of dicts from RAW_DEALS (history)
    candidate: dict from RAW_DEALS_VIEW (current candidate)
    """
    lookback_cutoff = NOW - timedelta(hours=VARIETY_LOOKBACK_HOURS)

    # Try common destination/theme column names (no inference: only if they exist)
    cand_dest = candidate.get("dest_city") or candidate.get("destination_city") or candidate.get("to_city") or candidate.get("destination")
    cand_theme = candidate.get("dynamic_theme") or candidate.get("theme")

    dest_hits = 0
    theme_hits = 0

    for r in raw_rows:
        ts = _parse_iso_dt(r.get("posted_timestamp") or r.get("published_timestamp") or r.get("scored_timestamp") or r.get("created_timestamp"))
        if ts and ts.replace(tzinfo=timezone.utc) < lookback_cutoff:
            continue

        r_status = (r.get("status") or "").strip()
        if not r_status:
            continue

        r_dest = r.get("dest_city") or r.get("destination_city") or r.get("to_city") or r.get("destination")
        r_theme = r.get("dynamic_theme") or r.get("theme")

        if cand_dest and r_dest and str(r_dest).strip().lower() == str(cand_dest).strip().lower():
            dest_hits += 1
        if cand_theme and r_theme and str(r_theme).strip().lower() == str(cand_theme).strip().lower():
            theme_hits += 1

    dest_penalty = DEST_REPEAT_PENALTY if dest_hits >= 2 else 0  # “twice in 7 days” enforced by your lookback window
    theme_penalty = THEME_REPEAT_PENALTY if theme_hits >= 3 else 0

    return dest_penalty, theme_penalty


def _worthiness_from_view(row):
    """
    Prefer sheet brain outputs if present; otherwise fall back to minimal heuristics.
    """
    # If the view already has these, use them
    ws_key = _find_first_existing(row, ["worthiness_score", "worthiness_score_calc", "value_score", "price_value_score"])
    if ws_key:
        worthiness_score = _safe_float(row.get(ws_key), 0)

        # Verdict: trust view if provided
        v_key = _find_first_existing(row, ["worthiness_verdict", "verdict"])
        verdict = (row.get(v_key) if v_key else "").strip()

        # If verdict missing, derive a basic one
        if not verdict:
            if worthiness_score >= 85:
                verdict = "🔥 ELITE"
            elif worthiness_score >= 60:
                verdict = "✅ POST"
            elif worthiness_score < 5:
                verdict = "❌ IGNORE"
            else:
                verdict = "⏸ HOLD"

        why = row.get("why_good") or row.get("why") or row.get("insight") or ""
        return worthiness_score, verdict, str(why)[:500]

    # Fallback heuristic (only if view doesn't provide any signal)
    price_key = _find_first_existing(row, ["price_gbp", "price", "price_value", "total_price"])
    normal_key = _find_first_existing(row, ["normal_price", "benchmark_normal_price", "normal_price_gbp"])

    price = _safe_float(row.get(price_key), 0) if price_key else 0
    normal = _safe_float(row.get(normal_key), 0) if normal_key else 0

    if price <= 0:
        return 0, "❌ IGNORE", "Missing/invalid price"

    if normal > 0:
        discount = (normal - price) / normal
        worthiness_score = max(0, min(100, round(discount * 100)))
        if worthiness_score >= 35:
            verdict = "✅ POST"
        else:
            verdict = "⏸ HOLD"
        return worthiness_score, verdict, f"Vs normal: {int(discount*100)}% under"
    else:
        # no benchmark available, keep conservative
        return 10, "⏸ HOLD", "No benchmark normal price available"


def _batch_update(ws, row_updates, colmap):
    """
    row_updates: list of tuples (row_number:int, dict of {header:value})
    Uses per-row range updates to avoid gspread v6 issues.
    """
    update_calls = 0

    for row_num, data in row_updates:
        for header, value in data.items():
            if header not in colmap:
                continue
            c = colmap[header]
            # update cell
            ws.update_cell(row_num, c, value)
            update_calls += 1

    return update_calls


def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID env var")

    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN}")
    _log(f"VARIETY_LOOKBACK_HOURS={VARIETY_LOOKBACK_HOURS} DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY} THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    _log(f"HARD_BLOCK_BAD_DEALS={HARD_BLOCK_BAD_DEALS}")

    gc = _get_gspread_client()

    # Sheets
    ws_raw = _open_ws(gc, RAW_DEALS_TAB)

    # View/tab names are part of your locked system; do not infer beyond attempting these known names
    # If your export uses different names, set env vars and keep architecture intact.
    view_tab = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
    bench_tab = os.environ.get("ZONE_THEME_BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS")

    ws_view = _open_ws(gc, view_tab)
    ws_bench = _open_ws(gc, bench_tab)

    # Pull intelligence surface
    view_rows = ws_view.get_all_records()
    _log(f"Loaded RAW_DEALS_VIEW intelligence rows: {len(view_rows)}")

    # Pull benchmarks (loaded for continuity/log parity; scorer may rely on view formulas)
    bench_rows = ws_bench.get_all_records()
    _log(f"Loaded ZONE_THEME_BENCHMARKS rows: {len(bench_rows)}")

    # Pull raw history for fatigue/variety
    raw_rows = ws_raw.get_all_records()

    # Identify NEW rows in view
    new = [r for r in view_rows if (r.get("status") or "").strip() == "NEW"]
    _log(f"Found NEW rows: {len(new)}")

    if not new:
        _log("No NEW rows. Exiting.")
        return

    # Cap processing
    new = new[:MAX_ROWS_PER_RUN]

    # Need mapping to actual RAW_DEALS row number
    # Common field names seen in systems like yours.
    rownum_key = _find_first_existing(new[0], ["row_number", "raw_row_number", "raw_row", "sheet_row", "_row"])
    if not rownum_key:
        raise RuntimeError("RAW_DEALS_VIEW is missing a row number column (expected one of: row_number/raw_row_number/raw_row/sheet_row/_row)")

    raw_headers = _headers(ws_raw)
    raw_colmap = _col_idx_map(raw_headers)

    # Score each new candidate
    scored_updates = []
    scored_candidates = []

    for r in new:
        raw_row_number = int(_safe_float(r.get(rownum_key), 0))
        if raw_row_number <= 1:
            continue

        worthiness_score, verdict, why = _worthiness_from_view(r)

        if HARD_BLOCK_BAD_DEALS and worthiness_score < 5:
            verdict = "❌ IGNORE"

        dest_penalty, theme_penalty = _compute_variety_penalties(raw_rows, r)
        deal_score = worthiness_score - dest_penalty - theme_penalty

        scored_updates.append((
            raw_row_number,
            {
                "deal_score": round(deal_score, 2),
                "dest_variety_score": dest_penalty,
                "theme_variety_score": theme_penalty,
                "scored_timestamp": NOW.isoformat().replace("+00:00", "Z"),
                "why_good": why,
                "ai_notes": "Monetisable candidate" if verdict in ("🔥 ELITE", "✅ POST") else "Below publish threshold",
                "worthiness_score": round(worthiness_score, 2),
                "worthiness_verdict": verdict,
                # status written later for winners only
            }
        ))

        scored_candidates.append({
            "raw_row_number": raw_row_number,
            "worthiness_score": worthiness_score,
            "deal_score": deal_score,
            "verdict": verdict,
        })

    _log("Batch writing columns: deal_score, dest_variety_score, theme_variety_score, scored_timestamp, why_good, ai_notes, worthiness_score, worthiness_verdict, status")

    used_calls = _batch_update(ws_raw, scored_updates, raw_colmap)
    _log(f"✅ Batch updates complete. update_cell calls used: {used_calls}")

    # Choose winners: ELITE/POST only, sorted by deal_score
    publishable = [c for c in scored_candidates if c["verdict"] in ("🔥 ELITE", "✅ POST")]
    publishable.sort(key=lambda x: x["deal_score"], reverse=True)

    winners = publishable[:WINNERS_PER_RUN]

    status_updates = []
    for w in winners:
        status_updates.append((w["raw_row_number"], {"status": "READY_TO_POST"}))

    if status_updates:
        _batch_update(ws_raw, status_updates, raw_colmap)

    _log(f"✅ Winners promoted to READY_TO_POST: {len(status_updates)} (WINNERS_PER_RUN={WINNERS_PER_RUN})")


if __name__ == "__main__":
    main()
