import os
import time
import uuid
import hashlib
from datetime import date, timedelta
from typing import Dict, List, Optional

import requests

from lib.sheets import get_env, get_gspread_client, now_iso


DUFFEL_BASE_URL = "https://api.duffel.com/air"
DUFFEL_VERSION = "v2"
RAW_STATUS_NEW = "NEW"


def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_env('DUFFEL_API_KEY')}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def fingerprint(origin: str, dest: str, out_date: str, ret_date: str, airline: str) -> str:
    raw = f"{origin}|{dest}|{out_date}|{ret_date}|{airline}".lower()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def search_roundtrip(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    cabin_class: str,
    max_connections: int,
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

    r = requests.post(
        f"{DUFFEL_BASE_URL}/offer_requests",
        headers=duffel_headers(),
        json=payload,
        timeout=30,
    )

    if not r.ok:
        return []

    return (r.json().get("data") or {}).get("offers") or []


def parse_offer(offer: Dict, route: Dict) -> Optional[Dict]:
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return None

    out_seg = slices[0]["segments"][0]
    ret_seg = slices[1]["segments"][0]

    out_date = out_seg["departing_at"][:10]
    ret_date = ret_seg["departing_at"][:10]

    price = float(offer.get("total_amount", 0))
    currency = offer.get("total_currency")

    if currency != "GBP":
        return None

    if price > float(route["max_price_gbp"]):
        return None

    airline = offer.get("owner", {}).get("name", "")

    fp = fingerprint(
        route["origin_city"],
        route["destination_city"],
        out_date,
        ret_date,
        airline,
    )

    stops = "0" if int(route["max_connections"]) == 0 else "1"

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


def main() -> None:
    sh = get_gspread_client().open_by_key(get_env("SHEET_ID"))

    raw_ws = sh.worksheet(os.getenv("RAW_DEALS_TAB", "RAW_DEALS"))
    cfg_ws = sh.worksheet(os.getenv("FEEDER_CONFIG_TAB", "CONFIG"))

    raw_headers = raw_ws.row_values(1)
    raw_idx = {h: i for i, h in enumerate(raw_headers)}

    existing_fp = set()
    if "deal_fingerprint" in raw_idx:
        fp_i = raw_idx["deal_fingerprint"]
        for r in raw_ws.get_all_values()[1:]:
            if fp_i < len(r) and r[fp_i]:
                existing_fp.add(r[fp_i])

    cfg_rows = cfg_ws.get_all_values()
    if len(cfg_rows) < 2:
        print("No CONFIG routes.")
        return

    headers = cfg_rows[0]
    idx = {h: i for i, h in enumerate(headers)}

    routes = []
    for r in cfg_rows[1:]:
        if not r:
            continue
        if r[idx["enabled"]] != "TRUE":
            continue

        routes.append({
            "origin_iata": r[idx["origin_iata"]],
            "origin_city": r[idx["origin_city"]],
            "destination_iata": r[idx["destination_iata"]],
            "destination_city": r[idx["destination_city"]],
            "destination_country": r[idx["destination_country"]],
            "trip_length_days": int(r[idx["trip_length_days"]]),
            "max_connections": int(r[idx["max_connections"]]),
            "cabin_class": r[idx["cabin_class"]],
            "max_price_gbp": r[idx["max_price_gbp"]],
            "step_days": int(r[idx["step_days"]]),
            "window_days": int(r[idx["window_days"]]),
            "days_ahead": int(r[idx["days_ahead"]]),
        })

    inserts = 0

    for route in routes:
        start = date.today() + timedelta(days=route["days_ahead"])
        end = start + timedelta(days=route["window_days"])
        d = start

        while d <= end:
            offers = search_roundtrip(
                route["origin_iata"],
                route["destination_iata"],
                d.isoformat(),
                (d + timedelta(days=route["trip_length_days"])).isoformat(),
                route["cabin_class"],
                int(route["max_connections"]),
            )

            for offer in offers[:20]:
                row = parse_offer(offer, route)
                if not row:
                    continue
                if row["deal_fingerprint"] in existing_fp:
                    continue

                out = [""] * len(raw_headers)
                for k, v in row.items():
                    if k in raw_idx:
                        out[raw_idx[k]] = str(v)

                raw_ws.append_row(out, value_input_option="USER_ENTERED")
                existing_fp.add(row["deal_fingerprint"])
                inserts += 1

            d += timedelta(days=route["step_days"])
            time.sleep(0.4)

    print(f"Duffel feeder complete: {inserts} rows inserted")


if __name__ == "__main__":
    main()
