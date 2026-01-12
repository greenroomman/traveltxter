# workers/ai_scorer.py
import os
import json
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_SA_JSON_ONE_LINE = os.environ.get("GCP_SA_JSON_ONE_LINE")

# Run controls (your workflow already sets these; defaults are safe)
MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", 50))
WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", 3))  # can be overridden by env
VARIETY_LOOKBACK_HOURS = int(os.environ.get("VARIETY_LOOKBACK_HOURS", 120))
DEST_REPEAT_PENALTY = int(os.environ.get("DEST_REPEAT_PENALTY", 80))
THEME_REPEAT_PENALTY = int(os.environ.get("THEME_REPEAT_PENALTY", 30))
HARD_BLOCK_BAD_DEALS = os.environ.get("HARD_BLOCK_BAD_DEALS", "true").lower() == "true"

RAW_DEALS_VIEW_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
ZONE_THEME_BENCHMARKS_TAB = os.environ.get("ZONE_THEME_BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS")

NOW = datetime.now(timezone.utc)


def _log(msg: str) -> None:
    print(f"{NOW.strftime('%Y-%m-%dT%H:%M:%SZ')} | {msg}")


def _get_gspread_client():
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
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
    return [h.strip() for h in ws.row_values(1)]


def _col_idx_map(headers):
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return float(default)
        if isinstance(x, str):
            x = x.replace("£", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return float(default)


def _parse_iso_dt(s):
    if not s:
        return None
    try:
        if isinstance(s, str) and s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _first_present_key(d: dict, candidates):
    for c in candidates:
        if c in d and d[c] not in (None, ""):
            return c
    return None


def _stringify_id(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _build_deal_id_row_map(raw_records, raw_headers):
    """
    Returns:
      deal_id_key: column header used in RAW_DEALS
      mapping: {deal_id_str: row_number_int}
    """
    # Try common deal id column names (must match your sheet)
    candidates = ["deal_id", "dealid", "id", "offer_id", "duffel_offer_id"]
    deal_id_key = None
    for c in candidates:
        if c in raw_headers:
            deal_id_key = c
            break

    if not deal_id_key:
        raise RuntimeError(
            f"RAW_DEALS is missing a deal id column. Tried: {candidates}"
        )

    mapping = {}
    # raw_records aligns with sheet rows starting at row 2
    for i, r in enumerate(raw_records, start=2):
        did = _stringify_id(r.get(deal_id_key))
        if did:
            mapping[did] = i

    return deal_id_key, mapping


def _compute_variety_penalties(raw_records, candidate_view_row):
    """
    Uses RAW_DEALS history to apply fatigue penalties.
    """
    cutoff = NOW - timedelta(hours=VARIETY_LOOKBACK_HOURS)

    cand_dest = (
        candidate_view_row.get("dest_city")
        or candidate_view_row.get("destination_city")
        or candidate_view_row.get("to_city")
        or candidate_view_row.get("destination")
    )
    cand_theme = candidate_view_row.get("dynamic_theme") or candidate_view_row.get("theme")

    dest_hits = 0
    theme_hits = 0

    for r in raw_records:
        ts = _parse_iso_dt(
            r.get("posted_timestamp")
            or r.get("published_timestamp")
            or r.get("scored_timestamp")
            or r.get("created_timestamp")
        )
        if ts and ts < cutoff:
            continue

        status = (r.get("status") or "").strip()
        if not status:
            continue

        r_dest = (
            r.get("dest_city")
            or r.get("destination_city")
            or r.get("to_city")
            or r.get("destination")
        )
        r_theme = r.get("dynamic_theme") or r.get("theme")

        if cand_dest and r_dest and str(r_dest).strip().lower() == str(cand_dest).strip().lower():
            dest_hits += 1
        if cand_theme and r_theme and str(r_theme).strip().lower() == str(cand_theme).strip().lower():
            theme_hits += 1

    # "Prevent same destination more than twice" -> apply penalty if already >=2 hits
    dest_penalty = DEST_REPEAT_PENALTY if dest_hits >= 2 else 0
    # Theme fatigue softer
    theme_penalty = THEME_REPEAT_PENALTY if theme_hits >= 3 else 0

    return dest_penalty, theme_penalty


def _worthiness_from_view(row):
    """
    Prefer the spreadsheet brain if present:
      - worthiness_score / price_value_score etc.
      - worthiness_verdict / verdict if present
    Otherwise fall back to conservative heuristics.
    """
    score_key = _first_present_key(row, ["worthiness_score", "price_value_score", "value_score"])
    verdict_key = _first_present_key(row, ["worthiness_verdict", "verdict"])

    if score_key:
        score = _safe_float(row.get(score_key), 0.0)
        verdict = (row.get(verdict_key) if verdict_key else "") or ""
        verdict = verdict.strip()

        if not verdict:
            if score >= 85:
                verdict = "🔥 ELITE"
            elif score >= 60:
                verdict = "✅ POST"
            elif score < 5:
                verdict = "❌ IGNORE"
            else:
                verdict = "⏸ HOLD"

        why = row.get("why_good") or row.get("why") or row.get("insight") or ""
        return score, verdict, str(why)[:500]

    # fallback heuristic
    price_key = _first_present_key(row, ["price_gbp", "price", "total_price"])
    normal_key = _first_present_key(row, ["normal_price", "benchmark_normal_price", "normal_price_gbp"])

    price = _safe_float(row.get(price_key), 0.0) if price_key else 0.0
    normal = _safe_float(row.get(normal_key), 0.0) if normal_key else 0.0

    if price <= 0:
        return 0.0, "❌ IGNORE", "Missing/invalid price"

    if normal > 0:
        discount = (normal - price) / normal
        score = max(0.0, min(100.0, round(discount * 100.0, 2)))
        verdict = "✅ POST" if score >= 35 else "⏸ HOLD"
        return score, verdict, f"Vs normal: {int(discount * 100)}% under"

    return 10.0, "⏸ HOLD", "No benchmark normal price available"


def _update_cells(ws, updates, colmap):
    """
    updates: list of (row_number, {header: value})
    Uses update_cell to avoid gspread range quirks.
    """
    calls = 0
    for row_num, data in updates:
        for header, value in data.items():
            if header not in colmap:
                continue
            ws.update_cell(row_num, colmap[header], value)
            calls += 1
    return calls


def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")

    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN}")
    _log(f"VARIETY_LOOKBACK_HOURS={VARIETY_LOOKBACK_HOURS} DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY} THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    _log(f"HARD_BLOCK_BAD_DEALS={HARD_BLOCK_BAD_DEALS}")

    gc = _get_gspread_client()

    ws_raw = _open_ws(gc, RAW_DEALS_TAB)
    ws_view = _open_ws(gc, RAW_DEALS_VIEW_TAB)
    ws_bench = _open_ws(gc, ZONE_THEME_BENCHMARKS_TAB)

    view_rows = ws_view.get_all_records()
    _log(f"Loaded RAW_DEALS_VIEW intelligence rows: {len(view_rows)}")

    bench_rows = ws_bench.get_all_records()
    _log(f"Loaded ZONE_THEME_BENCHMARKS rows: {len(bench_rows)}")

    raw_headers = _headers(ws_raw)
    raw_colmap = _col_idx_map(raw_headers)

    raw_records = ws_raw.get_all_records()

    # Build mapping from RAW_DEALS deal_id -> sheet row number
    deal_id_key_raw, deal_id_to_rownum = _build_deal_id_row_map(raw_records, raw_headers)

    # Find deal id key in view
    if not view_rows:
        _log("RAW_DEALS_VIEW empty. Exiting.")
        return

    deal_id_key_view = None
    for c in ["deal_id", "dealid", "id", "offer_id", "duffel_offer_id"]:
        if c in view_rows[0]:
            deal_id_key_view = c
            break
    if not deal_id_key_view:
        raise RuntimeError("RAW_DEALS_VIEW is missing a deal id column (expected deal_id/id/offer_id variants)")

    new = [r for r in view_rows if (r.get("status") or "").strip() == "NEW"]
    _log(f"Found NEW rows: {len(new)}")
    if not new:
        _log("No NEW rows. Exiting.")
        return

    new = new[:MAX_ROWS_PER_RUN]

    scored_updates = []
    scored_candidates = []

    for r in new:
        deal_id = _stringify_id(r.get(deal_id_key_view))
        if not deal_id:
            continue

        raw_row_number = deal_id_to_rownum.get(deal_id)
        if not raw_row_number:
            # view may contain derived rows; if not present in raw, skip
            continue

        worthiness_score, verdict, why = _worthiness_from_view(r)

        if HARD_BLOCK_BAD_DEALS and worthiness_score < 5:
            verdict = "❌ IGNORE"

        dest_penalty, theme_penalty = _compute_variety_penalties(raw_records, r)
        deal_score = worthiness_score - dest_penalty - theme_penalty

        scored_updates.append(
            (
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
                },
            )
        )

        scored_candidates.append(
            {
                "raw_row_number": raw_row_number,
                "deal_id": deal_id,
                "worthiness_score": worthiness_score,
                "deal_score": deal_score,
                "verdict": verdict,
            }
        )

    _log("Batch writing columns: deal_score, dest_variety_score, theme_variety_score, scored_timestamp, why_good, ai_notes, worthiness_score, worthiness_verdict, status")
    calls = _update_cells(ws_raw, scored_updates, raw_colmap)
    _log(f"✅ Batch updates complete. update_cell calls used: {calls}")

    publishable = [c for c in scored_candidates if c["verdict"] in ("🔥 ELITE", "✅ POST")]
    publishable.sort(key=lambda x: x["deal_score"], reverse=True)

    winners = publishable[:WINNERS_PER_RUN]

    status_updates = []
    for w in winners:
        status_updates.append((w["raw_row_number"], {"status": "READY_TO_POST"}))

    if status_updates:
        _update_cells(ws_raw, status_updates, raw_colmap)

    _log(f"✅ Winners promoted to READY_TO_POST: {len(status_updates)} (WINNERS_PER_RUN={WINNERS_PER_RUN})")


if __name__ == "__main__":
    main()
