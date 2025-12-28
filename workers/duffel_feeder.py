import os
import time
import uuid
import hashlib
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import requests

from lib.sheets import get_env, get_gspread_client, now_iso


DUFFEL_BASE_URL = "https://api.duffel.com/air"
DUFFEL_VERSION = "v2"

RAW_STATUS_NEW = "NEW"


# ----------------------------
# Config / safety defaults
# ----------------------------
DEFAULT_MAX_SEARCHES = 8
DEFAULT_MAX_INSERTS = 20
DEFAULT_SLEEP_SECONDS = 0.4
DEFAULT_MAX_OFFERS_PER_SEARCH = 20
DEFAULT_MAX_RETRIES = 3


def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_env('DUFFEL_API_KEY')}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def fingerprint(origin: str, dest: str, out_date: str, ret_date: str, airline: str, stops: str) -> str:
    raw = f"{origin}|{dest}|{out_date}|{ret_date}|{airline}|{stops}".lower()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def safe_int(x: str, default: int) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def safe_float(x: str, default: float) -> float:
    try:
        return float(str(x).strip())
    except Exception:
        return default


def require_env() -> None:
    # Hard fail fast if secrets missing (prevents silent “empty searches”)
    _ = get_env("DUFFEL_API_KEY")
    _ = get_env("SHEET_ID")


def validate_headers(tab_name: str, headers: List[str]) -> None:
    seen = set()
    dupes = set()
    for h in headers:
        if h in seen:
            dupes.add(h)
        seen.add(h)
    if dupes:
        raise ValueError(f"{tab_name} has duplicate headers: {sorted(dupes)}")


def search_roundtrip(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    cabin_class: str,
    max_connections: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> List[Dict]:

    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, "departure_date": departure_date},
                {"origin": destination, "destination": origin, "departure_date": return_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": max_connections,
        }
    }

    url = f"{DUFFEL_BASE_URL}/offer_requests"
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=duffel_headers(), json=payload, timeout=30)
        except Exception as e:
            print(f"[Duffel] Request exception attempt={attempt}: {e}")
            time.sleep(backoff)
            backoff *= 2
            continue

        # Success
        if r.ok:
            data = (r.json().get("data") or {})
            return data.get("offers") or []

        # Rate limit or transient errors: retry with backoff
        if r.status_code in (429, 500, 502, 503, 504):
            print(f"[Duffel] Retryable error attempt={attempt} status={r.status_code} body={r.text[:400]}")
            time.sleep(backoff)
            backoff *= 2
            continue

        # Non-retryable: log once and stop
        print(f"[Duffel] Non-retryable error status={r.status_code} body={r.text[:400]}")
        return []

    print("[Duffel] Max retries exceeded; returning no offers.")
    return []


def parse_offer(offer: Dict, route: Dict) -> Optional[Dict]:
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return None

    s0 = slices[0] or {}
    s1 = slices[1] or {}
    seg0 = (s0.get("segments") or [])
    seg1 = (s1.get("segments") or [])
    if not seg0 or not seg1:
        return None

    out_seg = seg0[0]
    ret_seg = seg1[0]

    out_date = str(out_seg.get("departing_at", ""))[:10]
    ret_date = str(ret_seg.get("departing_at", ""))[:10]
    if len(out_date) != 10 or len(ret_date) != 10:
        return None

    price = safe_float(str(offer.get("total_amount", "0")), 0.0)
    currency = str(offer.get("total_currency", "")).upper().strip()
    if currency != "GBP":
        return None

    max_price = safe_float(str(route.get("max_price_gbp", "999999")), 999999.0)
    if price <= 0 or price > max_price:
        return None

    airline = str((offer.get("owner") or {}).get("name", "")).strip()
    stops = "0" if int(route["max_connections"]) == 0 else "1"

    fp = fingerprint(
        route["origin_city"],
        route["destination_city"],
        out_date,
        ret_date,
        airline,
        stops,
    )

    return {
        "deal_id": uuid.uuid4().hex[:12],
        "origin_city": route["origin_city"],
        "destination_city": route["destination_city"],
        "destination_country": route["destination_country"],
        "price_gbp": f"{price:.2f}",
        "outbound_date": out_date,
        "return_date": ret_date,
        "trip_length_days": int(route["trip_length_days"]),
        "stops": stops,
        "baggage_included": "",
        "airline": airline,
        "deal_source": "DUFFEL",
        "notes": f'{route["origin_iata"]}->{route["destination_iata"]}',
        "date_added": now_iso(),
        "raw_status": RAW_STATUS_NEW,
        "deal_fingerprint": fp,
    }


def get_required_indices(headers: List[str], required: List[str], tab_name: str) -> Dict[str, int]:
    idx = {h: i for i, h in enumerate(headers)}
    missing = [h for h in required if h not in idx]
    if missing:
        raise ValueError(f"{tab_name} missing required columns: {missing}")
    return idx


def load_existing_fingerprints(raw_ws, raw_headers: List[str]) -> set:
    if "deal_fingerprint" not in raw_headers:
        return set()

    fp_col = raw_headers.index("deal_fingerprint") + 1  # gspread is 1-indexed
    # col_values is cheaper than get_all_values for big sheets
    vals = raw_ws.col_values(fp_col)
    # vals[0] is header
    return set(v.strip() for v in vals[1:] if v and v.strip())


def build_row_for_append(raw_headers: List[str], raw_idx: Dict[str, int], row: Dict) -> List[str]:
    out = [""] * len(raw_headers)
    for k, v in row.items():
        if k in raw_idx:
            out[raw_idx[k]] = str(v)
    return out


def main() -> None:
    require_env()

    max_searches = safe_int(os.getenv("FEEDER_MAX_SEARCHES", str(DEFAULT_MAX_SEARCHES)), DEFAULT_MAX_SEARCHES)
    max_inserts = safe_int(os.getenv("FEEDER_MAX_INSERTS", str(DEFAULT_MAX_INSERTS)), DEFAULT_MAX_INSERTS)
    sleep_seconds = safe_float(os.getenv("FEEDER_SLEEP_SECONDS", str(DEFAULT_SLEEP_SECONDS)), DEFAULT_SLEEP_SECONDS)
    max_offers_per_search = safe_int(os.getenv("FEEDER_MAX_OFFERS_PER_SEARCH", str(DEFAULT_MAX_OFFERS_PER_SEARCH)), DEFAULT_MAX_OFFERS_PER_SEARCH)

    sh = get_gspread_client().open_by_key(get_env("SHEET_ID"))
    raw_ws = sh.worksheet(os.getenv("RAW_DEALS_TAB", "RAW_DEALS"))
    cfg_ws = sh.worksheet(os.getenv("FEEDER_CONFIG_TAB", "CONFIG"))

    raw_headers = raw_ws.row_values(1)
    validate_headers("RAW_DEALS", raw_headers)
    raw_idx = {h: i for i, h in enumerate(raw_headers)}

    cfg_rows = cfg_ws.get_all_values()
    if len(cfg_rows) < 2:
        print("No CONFIG routes.")
        return
    cfg_headers = cfg_rows[0]
    validate_headers("CONFIG", cfg_headers)

    required_cfg = [
        "enabled", "origin_iata", "origin_city",
        "destination_iata", "destination_city", "destination_country",
        "trip_length_days", "max_connections", "cabin_class",
        "max_price_gbp", "step_days", "window_days", "days_ahead",
    ]
    idx = get_required_indices(cfg_headers, required_cfg, "CONFIG")

    existing_fp = load_existing_fingerprints(raw_ws, raw_headers)

    routes: List[Dict] = []
    for r in cfg_rows[1:]:
        if not r:
            continue
        if str(r[idx["enabled"]]).strip().upper() != "TRUE":
            continue

        routes.append({
            "origin_iata": str(r[idx["origin_iata"]]).strip(),
            "origin_city": str(r[idx["origin_city"]]).strip(),
            "destination_iata": str(r[idx["destination_iata"]]).strip(),
            "destination_city": str(r[idx["destination_city"]]).strip(),
            "destination_country": str(r[idx["destination_country"]]).strip(),
            "trip_length_days": safe_int(str(r[idx["trip_length_days"]]), 4),
            "max_connections": safe_int(str(r[idx["max_connections"]]), 1),
            "cabin_class": str(r[idx["cabin_class"]]).strip().lower() or "economy",
            "max_price_gbp": str(r[idx["max_price_gbp"]]).strip(),
            "step_days": safe_int(str(r[idx["step_days"]]), 7),
            "window_days": safe_int(str(r[idx["window_days"]]), 28),
            "days_ahead": safe_int(str(r[idx["days_ahead"]]), 7),
        })

    searches = 0
    inserts = 0
    batch_rows: List[List[str]] = []

    for route in routes:
        if searches >= max_searches or inserts >= max_inserts:
            break

        start = date.today() + timedelta(days=route["days_ahead"])
        end = start + timedelta(days=route["window_days"])
        d = start

        while d <= end:
            if searches >= max_searches or inserts >= max_inserts:
                break

            searches += 1

            offers = search_roundtrip(
                route["origin_iata"],
                route["destination_iata"],
                d.isoformat(),
                (d + timedelta(days=route["trip_length_days"])).isoformat(),
                route["cabin_class"],
                int(route["max_connections"]),
            )

            for offer in offers[:max_offers_per_search]:
                parsed = parse_offer(offer, route)
                if not parsed:
                    continue

                fp = parsed.get("deal_fingerprint", "")
                if fp in existing_fp:
                    continue

                existing_fp.add(fp)
                batch_rows.append(build_row_for_append(raw_headers, raw_idx, parsed))
                inserts += 1

                if inserts >= max_inserts:
                    break

            # Flush batch to Sheets periodically (reduces API calls)
            if batch_rows and (len(batch_rows) >= 10 or inserts >= max_inserts):
                raw_ws.append_rows(batch_rows, value_input_option="USER_ENTERED")
                batch_rows = []

            d += timedelta(days=route["step_days"])
            time.sleep(sleep_seconds)

    # final flush
    if batch_rows:
        raw_ws.append_rows(batch_rows, value_input_option="USER_ENTERED")

    print(f"Duffel feeder done: searches={searches}, inserts={inserts}")


if __name__ == "__main__":
    main()
