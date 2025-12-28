import os
import time
import uuid
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests

from lib.sheets import get_env, get_gspread_client, now_iso
from lib.fingerprints import deal_fingerprint


DUFFEL_BASE_URL = "https://api.duffel.com/air"
DUFFEL_VERSION = "v2"
RAW_STATUS_NEW = "NEW"

FEEDER_SOURCE = os.getenv("FEEDER_SOURCE", "DUFFEL_GHA_FEEDER").strip()


# ----------------------------
# Defaults (can be overridden)
# ----------------------------
DEFAULT_MAX_SEARCHES = 8
DEFAULT_MAX_INSERTS = 20
DEFAULT_SLEEP_SECONDS = 0.6
DEFAULT_MAX_OFFERS_PER_SEARCH = 20
DEFAULT_MAX_RETRIES = 3
DEFAULT_FLUSH_BATCH_SIZE = 10


def safe_int(x: Any, default: int) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def safe_float(x: Any, default: float) -> float:
    try:
        return float(str(x).strip())
    except Exception:
        return default


def require_env() -> None:
    # Hard fail early: if these are missing, we should stop immediately.
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


def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_env('DUFFEL_API_KEY')}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def search_roundtrip(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    cabin_class: str,
    max_connections: int,
    max_retries: int,
) -> List[Dict]:

    url = f"{DUFFEL_BASE_URL}/offer_requests"
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

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=duffel_headers(), json=payload, timeout=30)
        except Exception as e:
            print(f"[Duffel] request exception attempt={attempt}: {e}")
            time.sleep(backoff)
            backoff *= 2
            continue

        if r.ok:
            data = (r.json().get("data") or {})
            return data.get("offers") or []

        if r.status_code in (429, 500, 502, 503, 504):
            print(f"[Duffel] retryable status={r.status_code} attempt={attempt} body={r.text[:300]}")
            time.sleep(backoff)
            backoff *= 2
            continue

        print(f"[Duffel] non-retryable status={r.status_code} body={r.text[:300]}")
        return []

    print("[Duffel] max retries exceeded")
    return []


def parse_offer(offer: Dict, route: Dict) -> Optional[Dict]:
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return None

    seg0 = ((slices[0] or {}).get("segments") or [])
    seg1 = ((slices[1] or {}).get("segments") or [])
    if not seg0 or not seg1:
        return None

    out_seg = seg0[0]
    ret_seg = seg1[0]

    out_date = str(out_seg.get("departing_at", ""))[:10]
    ret_date = str(ret_seg.get("departing_at", ""))[:10]
    if len(out_date) != 10 or len(ret_date) != 10:
        return None

    price = safe_float(offer.get("total_amount", 0), 0.0)
    currency = str(offer.get("total_currency", "")).upper().strip()
    if currency != "GBP":
        return None

    max_price = safe_float(route.get("max_price_gbp", 999999), 999999.0)
    if price <= 0 or price > max_price:
        return None

    airline = str((offer.get("owner") or {}).get("name", "")).strip()
    max_conn = safe_int(route.get("max_connections", 1), 1)
    stops = "0" if max_conn == 0 else "1"

    fp = deal_fingerprint(
        origin_city=route["origin_city"],
        destination_city=route["destination_city"],
        outbound_date=out_date,
        return_date=ret_date,
        airline=airline,
        stops=stops,
    )

    return {
        "deal_id": uuid.uuid4().hex[:12],
        "origin_city": route["origin_city"],
        "destination_city": route["destination_city"],
        "destination_country": route["destination_country"],
        "price_gbp": f"{price:.2f}",
        "outbound_date": out_date,
        "return_date": ret_date,
        "trip_length_days": safe_int(route.get("trip_length_days", 4), 4),
        "stops": stops,
        "baggage_included": "",
        "airline": airline,
        "deal_source": FEEDER_SOURCE,
        "notes": route.get("notes", f'{route["origin_iata"]}->{route["destination_iata"]}'),
        "date_added": now_iso(),
        "raw_status": RAW_STATUS_NEW,
        "deal_fingerprint": fp,
    }


def load_existing_fingerprints(raw_ws, raw_headers: List[str]) -> set:
    if "deal_fingerprint" not in raw_headers:
        return set()

    fp_col = raw_headers.index("deal_fingerprint") + 1
    vals = raw_ws.col_values(fp_col)
    return set(v.strip() for v in vals[1:] if v and v.strip())


def build_row(raw_headers: List[str], raw_idx: Dict[str, int], item: Dict[str, Any]) -> List[str]:
    row = [""] * len(raw_headers)
    for k, v in item.items():
        if k in raw_idx:
            row[raw_idx[k]] = str(v)
    return row


def main() -> None:
    require_env()

    max_searches = safe_int(os.getenv("FEEDER_MAX_SEARCHES", DEFAULT_MAX_SEARCHES), DEFAULT_MAX_SEARCHES)
    max_inserts = safe_int(os.getenv("FEEDER_MAX_INSERTS", DEFAULT_MAX_INSERTS), DEFAULT_MAX_INSERTS)
    sleep_seconds = safe_float(os.getenv("FEEDER_SLEEP_SECONDS", DEFAULT_SLEEP_SECONDS), DEFAULT_SLEEP_SECONDS)
    max_offers = safe_int(os.getenv("FEEDER_MAX_OFFERS_PER_SEARCH", DEFAULT_MAX_OFFERS_PER_SEARCH), DEFAULT_MAX_OFFERS_PER_SEARCH)
    max_retries = safe_int(os.getenv("FEEDER_MAX_RETRIES", DEFAULT_MAX_RETRIES), DEFAULT_MAX_RETRIES)
    flush_batch = safe_int(os.getenv("FEEDER_FLUSH_BATCH_SIZE", DEFAULT_FLUSH_BATCH_SIZE), DEFAULT_FLUSH_BATCH_SIZE)

    sh = get_gspread_client().open_by_key(get_env("SHEET_ID"))
    raw_ws = sh.worksheet(os.getenv("RAW_DEALS_TAB", "RAW_DEALS"))
    cfg_ws = sh.worksheet(os.getenv("FEEDER_CONFIG_TAB", "CONFIG"))

    raw_headers = raw_ws.row_values(1)
    validate_headers("RAW_DEALS", raw_headers)
    raw_idx = {h: i for i, h in enumerate(raw_headers)}

    cfg = cfg_ws.get_all_values()
    if len(cfg) < 2:
        print("No CONFIG routes.")
        return
    cfg_headers = cfg[0]
    validate_headers("CONFIG", cfg_headers)
    cfg_idx = {h: i for i, h in enumerate(cfg_headers)}

    required_cfg = [
        "enabled", "origin_iata", "origin_city",
        "destination_iata", "destination_city", "destination_country",
        "trip_length_days", "max_connections", "cabin_class",
        "max_price_gbp", "step_days", "window_days", "days_ahead",
    ]
    missing = [c for c in required_cfg if c not in cfg_idx]
    if missing:
        raise ValueError(f"CONFIG missing required columns: {missing}")

    existing_fp = load_existing_fingerprints(raw_ws, raw_headers)

    routes: List[Dict[str, Any]] = []
    for r in cfg[1:]:
        if not r:
            continue
        if str(r[cfg_idx["enabled"]]).strip().upper() != "TRUE":
            continue

        routes.append({
            "origin_iata": str(r[cfg_idx["origin_iata"]]).strip(),
            "origin_city": str(r[cfg_idx["origin_city"]]).strip(),
            "destination_iata": str(r[cfg_idx["destination_iata"]]).strip(),
            "destination_city": str(r[cfg_idx["destination_city"]]).strip(),
            "destination_country": str(r[cfg_idx["destination_country"]]).strip(),
            "trip_length_days": safe_int(r[cfg_idx["trip_length_days"]], 4),
            "max_connections": safe_int(r[cfg_idx["max_connections"]], 1),
            "cabin_class": (str(r[cfg_idx["cabin_class"]]).strip().lower() or "economy"),
            "max_price_gbp": safe_float(r[cfg_idx["max_price_gbp"]], 999999.0),
            "step_days": safe_int(r[cfg_idx["step_days"]], 7),
            "window_days": safe_int(r[cfg_idx["window_days"]], 28),
            "days_ahead": safe_int(r[cfg_idx["days_ahead"]], 7),
            "notes": f'{str(r[cfg_idx["origin_iata"]]).strip()}->{str(r[cfg_idx["destination_iata"]]).strip()}',
        })

    searches = 0
    inserts = 0
    batch: List[List[str]] = []

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
                max_retries=max_retries,
            )

            for offer in offers[:max_offers]:
                parsed = parse_offer(offer, route)
                if not parsed:
                    continue

                fp = parsed["deal_fingerprint"]
                if fp in existing_fp:
                    continue

                existing_fp.add(fp)
                batch.append(build_row(raw_headers, raw_idx, parsed))
                inserts += 1

                if inserts >= max_inserts:
                    break

            if batch and (len(batch) >= flush_batch or inserts >= max_inserts):
                raw_ws.append_rows(batch, value_input_option="USER_ENTERED")
                batch = []

            d += timedelta(days=route["step_days"])
            time.sleep(sleep_seconds)

    if batch:
        raw_ws.append_rows(batch, value_input_option="USER_ENTERED")

    print(f"Duffel feeder done: searches={searches}, inserts={inserts}")


if __name__ == "__main__":
    main()
