import os
import time
import uuid
import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from lib.sheets import get_env, get_gspread_client, now_iso


# =============================
# V3.2(A) FEEDER WRITE CONTRACT
# =============================

SAFE_INSERT_FIELDS = [
    "deal_id",
    "origin_city",
    "destination_city",
    "destination_country",
    "price_gbp",
    "outbound_date",
    "return_date",
    "trip_length_days",
    "stops",
    "baggage_included",
    "airline",
    "deal_source",
    "notes",
    "date_added",
    "raw_status",
    "deal_fingerprint",
]

RAW_STATUS_NEW = "NEW"

DUFFEL_BASE_URL = "https://api.duffel.com/air"
DUFFEL_VERSION = "v2"


@dataclass
class RouteConfig:
    enabled: bool
    priority: int
    origin_iata: str
    origin_city: str
    destination_iata: str
    destination_city: str
    destination_country: str
    trip_length_days: int
    max_connections: int
    cabin_class: str
    max_price_gbp: float
    step_days: int
    window_days: int
    days_ahead: int


# -----------------------------
# Utility helpers
# -----------------------------

def _bool(v: Any) -> bool:
    return str(v).strip().upper() in ("TRUE", "1", "YES", "Y")


def _int(v: Any, default: int) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _float(v: Any, default: float) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _dupe_headers(headers: List[str]) -> List[str]:
    seen = set()
    dupes = []
    for h in headers:
        if h in seen:
            dupes.append(h)
        seen.add(h)
    return dupes


def _hm(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers)}


def _fingerprint(
    origin_city: str,
    dest_city: str,
    out_date: str,
    ret_date: str,
    airline: str,
    stops: str,
) -> str:
    raw = 
f"{origin_city}|{dest_city}|{out_date}|{ret_date}|{airline}|{stops}".lower().strip()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _deal_id(fp: str, price_gbp: float) -> str:
    bucket = int(round(price_gbp))
    seed = f"{fp}|{bucket}"
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:10]
    return f"{h}{uuid.uuid4().hex[:6]}"


# -----------------------------
# Duffel helpers
# -----------------------------

def _duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_env('DUFFEL_API_KEY')}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _search_roundtrip(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    cabin_class: str,
    max_connections: int,
) -> List[Dict[str, Any]]:
    url = f"{DUFFEL_BASE_URL}/offer_requests"
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, 
"departure_date": departure_date},
                {"origin": destination, "destination": origin, 
"departure_date": return_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": max_connections,
        }
    }
    r = requests.post(url, headers=_duffel_headers(), json=payload, 
timeout=30)
    if r.status_code not in (200, 201):
        return []
    j = r.json()
    return (j.get("data", {}) or {}).get("offers", []) or []


# -----------------------------
# Offer → RAW_DEALS row mapping
# -----------------------------

def _parse_offer_to_row(offer: Dict[str, Any], route: RouteConfig) -> 
Optional[Dict[str, Any]]:
    try:
        slices = offer.get("slices", [])
        if len(slices) < 2:
            return None

        out_seg = (slices[0].get("segments") or [])[0]
        ret_seg = (slices[1].get("segments") or [])[0]

        outbound_date = str(out_seg.get("departing_at", ""))[:10]
        return_date = str(ret_seg.get("departing_at", ""))[:10]

        total_amount = float(offer.get("total_amount", 0))
        currency = str(offer.get("total_currency", "")).upper().strip()

        # If your Duffel account returns non-GBP, skip to keep pipeline 
simple.
        if currency and currency != "GBP":
            return None

        # Stops: 0 if direct, else 1 (we request max_connections=1, so 
keep it simple).
        stops = "0" if route.max_connections == 0 else "1"

        airline = (offer.get("owner") or {}).get("name", "") or ""

        # Baggage requires deeper parsing; keep conservative default.
        baggage_included = ""

        if total_amount > route.max_price_gbp:
            return None

        fp = _fingerprint(
            origin_city=route.origin_city,
            dest_city=route.destination_city,
            out_date=outbound_date,
            ret_date=return_date,
            airline=airline,
            stops=stops,
        )

        return {
            "deal_fingerprint": fp,
            "deal_id": _deal_id(fp, total_amount),
            "origin_city": route.origin_city,
            "destination_city": route.destination_city,
            "destination_country": route.destination_country,
            "price_gbp": f"{total_amount:.2f}",
            "outbound_date": outbound_date,
            "return_date": return_date,
            "trip_length_days": str(route.trip_length_days),
            "stops": stops,
            "baggage_included": baggage_included,
            "airline": airline,
            "deal_source": "DUFFEL",
            "notes": f"{route.origin_iata}->{route.destination_iata}",
            "date_added": now_iso(),
            "raw_status": RAW_STATUS_NEW,
        }

    except Exception:
        return None


# -----------------------------
# CONFIG loading
# -----------------------------

def _load_routes(sh, tab_name: str) -> List[RouteConfig]:
    ws = sh.worksheet(tab_name)
    values = ws.get_all_values()
    if len(values) < 2:
        return []

    headers = values[0]
    dupes = _dupe_headers(headers)
    if dupes:
        raise ValueError(f"CONFIG has duplicate headers: 
{sorted(set(dupes))}")

    idx = {h: i for i, h in enumerate(headers)}

    def g(row: List[str], key: str, default: str = "") -> str:
        i = idx.get(key)
        return row[i].strip() if i is not None and i < len(row) else 
default

    routes: List[RouteConfig] = []
    for r in values[1:]:
        if not r or not any(c.strip() for c in r):
            continue
        if not _bool(g(r, "enabled", "FALSE")):
            continue

        routes.append(
            RouteConfig(
                enabled=True,
                priority=_int(g(r, "priority", "0"), 0),
                origin_iata=g(r, "origin_iata"),
                origin_city=g(r, "origin_city") or g(r, "origin_iata"),
                destination_iata=g(r, "destination_iata"),
                destination_city=g(r, "destination_city") or g(r, 
"destination_iata"),
                destination_country=g(r, "destination_country"),
                trip_length_days=_int(g(r, "trip_length_days", "4"), 4),
                max_connections=_int(g(r, "max_connections", "1"), 1),
                cabin_class=(g(r, "cabin_class", "economy") or 
"economy").lower(),
                max_price_gbp=_float(g(r, "max_price_gbp", "9999"), 
9999.0),
                step_days=_int(g(r, "step_days", "7"), 7),
                window_days=_int(g(r, "window_days", "28"), 28),
                days_ahead=_int(g(r, "days_ahead", "7"), 7),
            )
        )

    routes.sort(key=lambda x: -x.priority)
    return routes


# -----------------------------
# MAIN
# -----------------------------

def main():
    sheet_id = get_env("SHEET_ID")
    raw_tab = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()
    config_tab = os.getenv("FEEDER_CONFIG_TAB", "CONFIG").strip()

    max_searches = _int(os.getenv("FEEDER_MAX_SEARCHES", "8"), 8)
    max_inserts = _int(os.getenv("FEEDER_MAX_INSERTS", "20"), 20)
    sleep_seconds = _float(os.getenv("FEEDER_SLEEP_SECONDS", "0.4"), 0.4)

    sh = get_gspread_client().open_by_key(sheet_id)
    raw_ws = sh.worksheet(raw_tab)

    raw_headers = raw_ws.row_values(1)
    dupes = _dupe_headers(raw_headers)
    if dupes:
        raise ValueError(f"RAW_DEALS has duplicate headers: 
{sorted(set(dupes))}")

    raw_hm = _hm(raw_headers)

    routes = _load_routes(sh, config_tab)
    if not routes:
        print("No enabled routes in CONFIG.")
        return

    existing_fps = set()
    if "deal_fingerprint" in raw_headers:
        fp_idx = raw_headers.index("deal_fingerprint")
        for r in raw_ws.get_all_values()[1:]:
            if fp_idx < len(r) and r[fp_idx]:
                existing_fps.add(r[fp_idx])

    searches = 0
    inserts = 0

    for route in routes:
        if searches >= max_searches or inserts >= max_inserts:
            break

        start = date.today() + timedelta(days=route.days_ahead)
        end = start + timedelta(days=route.window_days)
        d = start

        while d <= end:
            if searches >= max_searches or inserts >= max_inserts:
                break

            searches += 1
            offers = _search_roundtrip(
                route.origin_iata,
                route.destination_iata,
                d.isoformat(),
                (d + timedelta(days=route.trip_length_days)).isoformat(),
                route.cabin_class,
                route.max_connections,
            )

            for offer in offers[:50]:
                row = _parse_offer_to_row(offer, route)
                if not row:
                    continue

                fp = row.get("deal_fingerprint")
                if fp in existing_fps:
                    continue

                out = [""] * len(raw_headers)
                for k in SAFE_INSERT_FIELDS:
                    if k in raw_hm:
                        out[raw_hm[k] - 1] = str(row.get(k, ""))

                raw_ws.append_row(out, value_input_option="USER_ENTERED")
                existing_fps.add(fp)
                inserts += 1

                if inserts >= max_inserts:
                    break

            time.sleep(sleep_seconds)
            d += timedelta(days=route.step_days)

    print(f"Duffel feeder complete: searches={searches}, 
inserts={inserts}")


if __name__ == "__main__":
    main()

