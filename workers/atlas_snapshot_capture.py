#!/usr/bin/env python3
"""
workers/atlas_snapshot_capture.py
ATLAS SNAPSHOT CAPTURE - v0
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from uuid import uuid4
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

LCC_IATA_CODES = {
    "FR", "U2", "W6", "VY", "PC", "HV", "LS", "BE", "EN",
    "WX", "ZB", "TOM", "BY", "X3", "4U", "DE", "EW", "HG",
    "DY", "D8", "SK", "FI", "WF", "DX",
    "F9", "G4", "NK", "B6", "WN", "WS", "G3", "VT", "NX",
}


# ── UK School Holiday Windows (England) ───────────────────────────────────────
UK_SCHOOL_HOLIDAYS = [
    ("2025-02-17", "2025-02-21"),  # February half term
    ("2025-04-11", "2025-04-25"),  # Easter
    ("2025-05-26", "2025-05-30"),  # May half term
    ("2025-07-22", "2025-09-03"),  # Summer
    ("2025-10-27", "2025-10-31"),  # Autumn half term
    ("2025-12-20", "2026-01-05"),  # Christmas / New Year
    ("2026-02-16", "2026-02-20"),  # February half term
    ("2026-04-02", "2026-04-17"),  # Easter
    ("2026-05-25", "2026-05-29"),  # May half term
    ("2026-07-21", "2026-09-02"),  # Summer
    ("2026-10-26", "2026-10-30"),  # Autumn half term
    ("2026-12-21", "2027-01-04"),  # Christmas / New Year
]

# ── UK Bank Holidays ───────────────────────────────────────────────────────────
UK_BANK_HOLIDAYS = {
    "2025-04-18", "2025-04-21",  # Good Friday / Easter Monday
    "2025-05-05", "2025-05-26",  # Early May / Spring bank holiday
    "2025-08-25",                # Summer bank holiday
    "2025-12-25", "2025-12-26",  # Christmas
    "2026-01-01",                # New Year
    "2026-04-03", "2026-04-06",  # Good Friday / Easter Monday
    "2026-05-04", "2026-05-25",  # Early May / Spring bank holiday
    "2026-08-31",                # Summer bank holiday
    "2026-12-25", "2026-12-26",  # Christmas
    "2027-01-01",                # New Year
}


def check_school_holiday(departure_date: dt.date) -> bool:
    """Return True if departure_date falls within an England school holiday window."""
    for start_str, end_str in UK_SCHOOL_HOLIDAYS:
        start = dt.date.fromisoformat(start_str)
        end   = dt.date.fromisoformat(end_str)
        if start <= departure_date <= end:
            return True
    return False


def check_bank_holiday_adjacent(departure_date: dt.date) -> bool:
    """Return True if departure_date is a bank holiday or within 1 day of one."""
    for delta in (-1, 0, 1):
        candidate = (departure_date + dt.timedelta(days=delta)).isoformat()
        if candidate in UK_BANK_HOLIDAYS:
            return True
    return False


def env_int(name, default):
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def env_str(name, default=""):
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _sanitize_sa_json(raw):
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE or GCP_SA_JSON.")
    try:
        json.loads(raw)
        return raw
    except Exception:
        pass
    try:
        fixed = raw.replace("\\n", "\n")
        json.loads(fixed)
        return fixed
    except Exception:
        pass
    if '"private_key"' in raw and "BEGIN PRIVATE KEY" in raw:
        try:
            before, rest = raw.split('"private_key"', 1)
            if ':"' in rest:
                k1, krest = rest.split(':"', 1)
                pk_prefix = ':"'
            else:
                k1, krest = rest.split('": "', 1)
                pk_prefix = '": "'
            key_body, after = krest.split("-----END PRIVATE KEY-----", 1)
            key_body = key_body.replace("\r", "").replace("\n", "\\n")
            repaired = (
                before + '"private_key"' + k1 + pk_prefix
                + key_body + "-----END PRIVATE KEY-----" + after
            )
            json.loads(repaired)
            return repaired
        except Exception:
            pass
    raw2 = raw.replace("\r", "")
    json.loads(raw2)
    return raw2


def gspread_client():
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    raw = _sanitize_sa_json(raw)
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _utc_now():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _utc_date():
    return _utc_now().strftime("%Y-%m-%d")


def _utc_time():
    return _utc_now().strftime("%H:%M")


DUFFEL_API = "https://api.duffel.com/air/offer_requests"


def duffel_headers():
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY.")
    return {
        "Authorization": "Bearer " + key,
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def duffel_search(origin, dest, out_date, ret_date, cabin="economy", max_connections=1):
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_connections,
            "return_offers": True,
        }
    }
    try:
        resp = requests.post(DUFFEL_API, headers=duffel_headers(), json=payload, timeout=45)
        if resp.status_code >= 400:
            return None
        data = resp.json().get("data", {})
        offers = data.get("offers") or []
        if not offers:
            return None
        gbp = [o for o in offers if (o.get("total_currency") or "").upper() == "GBP"]
        if not gbp:
            return None
        gbp.sort(key=lambda o: float(o.get("total_amount") or "1e18"))
        return gbp[0]
    except Exception:
        return None


def extract_carriers(offer):
    carriers = []
    try:
        for sl in offer.get("slices") or []:
            for seg in sl.get("segments") or []:
                mc = seg.get("marketing_carrier") or {}
                code = (mc.get("iata_code") or "").upper()
                if code and code not in carriers:
                    carriers.append(code)
    except Exception:
        pass
    return carriers


def extract_stops(offer):
    try:
        stops = 0
        for sl in offer.get("slices") or []:
            segs = sl.get("segments") or []
            stops += max(0, len(segs) - 1)
        return stops
    except Exception:
        return 0


def make_snapshot_key(origin, dest, out_date, ret_date, snapshot_date, capture_time):
    t = capture_time.replace(":", "")
    return origin + "_" + dest + "_" + out_date + "_" + ret_date + "_" + snapshot_date + "_" + t


def load_config(path):
    with open(path, "r") as f:
        return json.load(f)


def generate_target_dates(lookahead_min, lookahead_max, outbound_weekdays, trip_lengths, max_per_dest=2):
    today = _utc_now().date()
    results = []
    weekday_hits = 0
    for delta in range(lookahead_min, lookahead_max + 1):
        candidate = today + dt.timedelta(days=delta)
        if candidate.weekday() not in outbound_weekdays:
            continue
        weekday_hits += 1
        if weekday_hits > max_per_dest:
            break
        for tl in trip_lengths:
            ret = candidate + dt.timedelta(days=tl)
            results.append((
                candidate.strftime("%Y-%m-%d"),
                ret.strftime("%Y-%m-%d"),
                delta,
            ))
    return results


SNAPSHOT_HEADERS = [
    "snapshot_id",
    "snapshot_date", "capture_time_utc",
    "origin_iata", "destination_iata",
    "outbound_date", "return_date",
    "dtd",
    "day_of_week_departure", "day_of_week_snapshot",
    "is_school_holiday_window", "is_bank_holiday_adjacent",
    "price_gbp", "currency",
    "carrier_count", "lcc_present", "direct", "stops", "cabin_class",
    "seats_remaining",
    "price_t7", "price_t14", "rose_10pct", "fell_10pct",
    "snapshot_key", "notes",
    "origin_type", "shi_variance_flag",
]

# Route category lookup — used as a model feature and SHI context.
# LGW skews business/mixed; MAN skews leisure. Extend as routes expand.
ROUTE_CATEGORY: Dict[str, str] = {
    # LGW business-dominant routes
    "LGW-GVA": "business", "LGW-ZRH": "business", "LGW-MXP": "business",
    "LGW-FRA": "business", "LGW-AMS": "business", "LGW-CDG": "business",
    "LGW-MAD": "mixed",    "LGW-FCO": "mixed",    "LGW-BCN": "mixed",
    "LGW-DUB": "mixed",    "LGW-LIS": "mixed",
    # MAN corporate-adjacent
    "MAN-AMS": "mixed",    "MAN-DUB": "mixed",    "MAN-CDG": "mixed",
    "MAN-FRA": "business", "MAN-BRU": "business", "MAN-GVA": "business",
    "MAN-ZRH": "business",
}

def route_category(origin: str, dest: str) -> str:
    return ROUTE_CATEGORY.get(f"{origin}-{dest}", "leisure")


def ensure_snapshot_headers(ws):
    first_row = ws.row_values(1)
    if not first_row or first_row[0] != "snapshot_id":
        ws.update("A1", [SNAPSHOT_HEADERS])
        print("SNAPSHOT_LOG headers written.")


def load_existing_keys(ws):
    values = ws.get_all_values()
    if len(values) < 2:
        return set(), {}
    try:
        hdr = values[0]
        key_col = hdr.index("snapshot_key")
    except (ValueError, IndexError):
        return set(), {}

    # Build 7-day price history per route from the same read — zero extra API calls.
    # Used by shi_variance_flag() to detect fare-class drift during capture.
    today = _utc_now().date()
    cutoff = (today - dt.timedelta(days=7)).isoformat()
    price_history: Dict[str, List[float]] = {}
    try:
        snap_col  = hdr.index("snapshot_date")
        orig_col  = hdr.index("origin_iata")
        dest_col  = hdr.index("destination_iata")
        price_col = hdr.index("price_gbp")
        for row in values[1:]:
            def _get(col):
                return row[col].strip() if col < len(row) else ""
            snap_d = _get(snap_col)
            if snap_d < cutoff:
                continue
            route_k = _get(orig_col) + "-" + _get(dest_col)
            try:
                p = float(_get(price_col))
                if p > 0:
                    price_history.setdefault(route_k, []).append(p)
            except ValueError:
                pass
    except (ValueError, IndexError):
        pass

    keys = {row[key_col] for row in values[1:] if len(row) > key_col and row[key_col]}
    return keys, price_history


def shi_variance_flag(route_key: str, today_price: float, history: Dict[str, List[float]]) -> str:
    """
    Returns 'FLAG' if today's price suggests variance collapse (fare-class drift).
    Returns 'OK' if within normal range. Returns 'INSUFFICIENT_DATA' if <5 baseline points.
    Variance collapse = today's price is an outlier relative to recent distribution,
    suggesting the scraper may be sampling a different fare bucket.
    """
    import statistics
    baseline = history.get(route_key, [])
    if len(baseline) < 5:
        return "INSUFFICIENT_DATA"
    try:
        mean = statistics.mean(baseline)
        stdev = statistics.stdev(baseline)
        if stdev == 0:
            return "FLAG"  # zero variance in baseline = already collapsed
        z = abs(today_price - mean) / stdev
        return "FLAG" if z > 2.5 else "OK"
    except Exception:
        return "INSUFFICIENT_DATA"


def main():
    print("=" * 70)
    print("ATLAS SNAPSHOT CAPTURE v0")
    print("=" * 70)

    config_path = env_str("ATLAS_CONFIG_PATH", "config/atlas_snapshot_config.json")
    snapshot_tab = env_str("SNAPSHOT_LOG_TAB", "SNAPSHOT_LOG")
    sleep_s = float(env_str("FEEDER_SLEEP_SECONDS", "0.5"))
    max_searches = env_int("ATLAS_MAX_SEARCHES", 90)

    cfg = load_config(config_path)
    origins = cfg.get("origins", ["MAN"])
    destinations = cfg.get("destinations", [])
    trip_lengths = cfg.get("trip_length_days", [3, 4, 5])
    outbound_weekdays = cfg.get("outbound_weekdays", [3, 5])
    lookahead_min = cfg.get("lookahead_min_days", 45)
    lookahead_max = cfg.get("lookahead_max_days", 140)
    cabin = cfg.get("cabin_class", "economy")
    max_connections = cfg.get("max_connections", 1)
    max_per_dest = cfg.get("max_date_combos_per_dest", 2)

    if not destinations:
        raise RuntimeError("No destinations in atlas_snapshot_config.json.")

    gc = gspread_client()
    sh = gc.open_by_key(env_str("SPREADSHEET_ID") or env_str("SHEET_ID"))
    ws = sh.worksheet(snapshot_tab)
    ensure_snapshot_headers(ws)
    existing_keys, price_history = load_existing_keys(ws)

    snapshot_date = _utc_date()
    capture_time = _utc_time()

    date_combos = generate_target_dates(
        lookahead_min, lookahead_max, outbound_weekdays, trip_lengths, max_per_dest
    )

    print("Date combos per dest: " + str(len(date_combos)))
    print("Destinations: " + str(destinations))
    print("Origins: " + str(origins))
    print("Max searches: " + str(max_searches))
    print("-" * 70)

    searches = 0
    captured = 0
    skipped = 0
    no_offer = 0
    pending = []

    for origin in origins:
        for dest in destinations:
            for out_date, ret_date, dtd in date_combos:
                if searches >= max_searches:
                    break

                snap_key = make_snapshot_key(
                    origin, dest, out_date, ret_date, snapshot_date, capture_time
                )
                if snap_key in existing_keys:
                    skipped += 1
                    continue

                searches += 1
                print(
                    "[" + str(searches) + "/" + str(max_searches) + "] "
                    + origin + "->" + dest + "  "
                    + out_date + "/" + ret_date + "  DTD=" + str(dtd)
                )

                offer = duffel_search(
                    origin, dest, out_date, ret_date,
                    cabin=cabin, max_connections=max_connections
                )

                row = {h: "" for h in SNAPSHOT_HEADERS}

                out_date_obj = dt.date.fromisoformat(out_date)
                snap_date_obj = dt.date.fromisoformat(snapshot_date)

                row.update({
                    "snapshot_id": str(uuid4()),
                    "snapshot_date": snapshot_date,
                    "capture_time_utc": capture_time,
                    "origin_iata": origin,
                    "destination_iata": dest,
                    "outbound_date": out_date,
                    "return_date": ret_date,
                    "dtd": dtd,
                    "day_of_week_departure": out_date_obj.strftime("%A"),
                    "day_of_week_snapshot": snap_date_obj.strftime("%A"),
                    "is_school_holiday_window": str(check_school_holiday(out_date_obj)).upper(),
                    "is_bank_holiday_adjacent": str(check_bank_holiday_adjacent(out_date_obj)).upper(),
                    "snapshot_key": snap_key,
                })

                if not offer:
                    no_offer += 1
                    row["notes"] = "no_offer"
                    row["origin_type"] = route_category(origin, dest)
                    row["shi_variance_flag"] = ""
                    print("   no offer - logging null row")
                else:
                    carriers = extract_carriers(offer)
                    stops = extract_stops(offer)
                    lcc_present = any(c in LCC_IATA_CODES for c in carriers)
                    price_gbp = round(float(offer.get("total_amount") or 0), 2)
                    seats_remaining = None
                    try:
                        slices = offer.get("slices") or []
                        if slices:
                            first_seg = (slices[0].get("segments") or [{}])[0]
                            seats_remaining = first_seg.get("available_seats")
                    except Exception:
                        seats_remaining = None
                    rkey = origin + "-" + dest
                    shi_flag = shi_variance_flag(rkey, price_gbp, price_history)
                    if shi_flag == "FLAG":
                        print("   ⚠️  SHI variance FLAG — price may be drift/outlier")
                    row.update({
                        "price_gbp": price_gbp,
                        "currency": "GBP",
                        "carrier_count": len(carriers),
                        "lcc_present": str(lcc_present).upper(),
                        "direct": str(stops == 0).upper(),
                        "stops": stops,
                        "cabin_class": cabin,
                        "seats_remaining": seats_remaining if seats_remaining is not None else "",
                        "notes": "",
                        "origin_type": route_category(origin, dest),
                        "shi_variance_flag": shi_flag,
                    })
                    captured += 1
                    print("   GBP " + str(price_gbp) + " | " + ",".join(carriers) + " | direct=" + str(stops == 0))

                pending.append([row[h] for h in SNAPSHOT_HEADERS])
                existing_keys.add(snap_key)
                time.sleep(sleep_s)

    print("-" * 70)

    if pending:
        for attempt in range(1, 4):
            try:
                ws.append_rows(pending, value_input_option="USER_ENTERED")
                print("Written " + str(len(pending)) + " rows to " + snapshot_tab + ".")
                break
            except Exception as e:
                print("append_rows attempt " + str(attempt) + "/3 failed: " + str(e))
                if attempt < 3:
                    time.sleep(10 * attempt)
                else:
                    raise
    else:
        print("No rows written.")

    print(
        "SUMMARY: searches=" + str(searches)
        + " captured=" + str(captured)
        + " no_offer=" + str(no_offer)
        + " skipped=" + str(skipped)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
