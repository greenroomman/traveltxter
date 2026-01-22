# ================================================================
# TRAVELTXTTER PIPELINE WORKER — THEME-AWARE, SEASON-AWARE FEEDER
# ================================================================

import os
import math
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List

from workers.utils import (
    load_config,
    log,
    today_utc,
    parse_iata_list,
    append_rows_header_mapped,
)

from duffel import (
    duffel_search,
    offer_connections_safe,
    offer_duration_minutes_safe,
)

# ------------------------------------------------
# THEME PLAYBOOK (CANONICAL)
# ------------------------------------------------

THEME_PLAYBOOK = {
    "winter_sun": {
        "origins": "LGW,LHR,STN,LTN,BRS",
        "months": [11, 12, 1, 2, 3],
        "window": (21, 84),
        "trip": (5, 10),
        "max_stops": 0,
    },
    "summer_sun": {
        "origins": "LGW,LHR,STN,LTN,BRS,MAN",
        "months": [5, 6, 7, 8, 9],
        "window": (21, 90),
        "trip": (5, 10),
        "max_stops": 0,
    },
    "beach_break": {
        "origins": "BRS,LGW,STN,LHR,MAN",
        "months": [4, 5, 6, 7, 8, 9, 10],
        "window": (21, 90),
        "trip": (4, 8),
        "max_stops": 0,
    },
    "city_breaks": {
        "origins": "LGW,LHR,STN,LTN,BRS,MAN",
        "months": list(range(1, 13)),
        "window": (14, 70),
        "trip": (2, 4),
        "max_stops": 0,
    },
    "culture_history": {
        "origins": "LGW,LHR,STN,LTN,BRS,MAN",
        "months": [3, 4, 5, 9, 10, 11],
        "window": (21, 90),
        "trip": (4, 8),
        "max_stops": 0,
    },
    "surf": {
        "origins": "BRS,LGW,STN,LHR",
        "months": [3, 4, 5, 6, 7, 8, 9, 10, 11],
        "window": (21, 90),
        "trip": (5, 10),
        "max_stops": 0,
    },
    "snow": {
        "origins": "LHR,LGW,MAN",
        "months": [12, 1, 2, 3],
        "window": (21, 90),
        "trip": (4, 8),
        "max_stops": 1,
    },
    "northern_lights": {
        "origins": "LGW,LHR,MAN",
        "months": [9, 10, 11, 12, 1, 2, 3],
        "window": (21, 90),
        "trip": (4, 7),
        "max_stops": 1,
    },
    "adventure": {
        "origins": "LGW,LHR,MAN,BRS",
        "months": [3, 4, 5, 6, 7, 8, 9, 10, 11],
        "window": (21, 120),
        "trip": (6, 12),
        "max_stops": 1,
    },
    "long_haul": {
        "origins": "LHR,LGW,MAN",
        "months": list(range(1, 13)),
        "window": (30, 180),
        "trip": (7, 14),
        "max_stops": 1,
    },
    "luxury_value": {
        "origins": "LHR,LGW,MAN",
        "months": list(range(1, 13)),
        "window": (30, 200),
        "trip": (6, 12),
        "max_stops": 1,
    },
    "unexpected_value": {
        "origins": "BRS,LGW,STN,LHR,MAN",
        "months": list(range(1, 13)),
        "window": (14, 120),
        "trip": (3, 10),
        "max_stops": 1,
    },
}

# ------------------------------------------------
# MAIN WORKER
# ------------------------------------------------

def run():
    cfg = load_config()
    theme_today = cfg["THEME_OF_DAY"]

    play = THEME_PLAYBOOK.get(theme_today)
    if not play:
        log(f"⚠️ No playbook for theme {theme_today}, aborting run")
        return

    origins = parse_iata_list(play["origins"])
    months_allowed = play["months"]
    win_min, win_max = play["window"]
    trip_min, trip_max = play["trip"]
    max_stops = play["max_stops"]

    max_searches = int(cfg.get("DUFFEL_MAX_SEARCHES_PER_RUN", 4))
    max_inserts = int(cfg.get("DUFFEL_MAX_INSERTS", 3))

    today = today_utc()
    routes_tried = 0
    inserts = 0

    log(f"🎯 Theme of the day (UTC): {theme_today}")
    log(f"🧭 Origins in play: {origins}")
    log(f"🗓️ Season months allowed: {months_allowed}")
    log(f"📏 Trip length: {trip_min}–{trip_max} days")
    log(f"🔁 Max stops allowed: {max_stops}")

    for origin in origins:
        if routes_tried >= max_searches or inserts >= max_inserts:
            break

        # Choose dates consistent with season
        dep_offset = random.randint(win_min, win_max)
        dep_date = today + timedelta(days=dep_offset)

        if dep_date.month not in months_allowed:
            continue

        trip_len = random.randint(trip_min, trip_max)
        ret_date = dep_date + timedelta(days=trip_len)

        destinations = cfg["THEME_DESTINATIONS"].get(theme_today, [])
        random.shuffle(destinations)

        for dest in destinations:
            if routes_tried >= max_searches or inserts >= max_inserts:
                break

            routes_tried += 1
            log(f"Duffel search: {origin} → {dest} {dep_date}/{ret_date}")

            offers = duffel_search(origin, dest, dep_date, ret_date)
            if not offers:
                continue

            for off in offers:
                stops = offer_connections_safe(off)
                if stops > max_stops:
                    continue

                dur = offer_duration_minutes_safe(off)
                if dur <= 0:
                    continue

                price = off["price_gbp"]
                deal_id = off["id"]

                deal = {
                    "status": "NEW",
                    "deal_id": deal_id,
                    "price_gbp": int(math.ceil(price)),
                    "origin_iata": origin,
                    "destination_iata": dest,
                    "outbound_date": dep_date.strftime("%Y-%m-%d"),
                    "return_date": ret_date.strftime("%Y-%m-%d"),
                    "stops": stops,
                    "deal_theme": theme_today,
                    "created_utc": datetime.utcnow().isoformat(),
                }

                append_rows_header_mapped(
                    sheet_id=cfg["SPREADSHEET_ID"],
                    tab_name=cfg["RAW_DEALS_TAB"],
                    rows=[deal],
                )

                inserts += 1
                log(f"✅ Inserted deal {deal_id} ({origin} → {dest}, £{price})")

                if inserts >= max_inserts:
                    break

    log(f"✓ Searches completed: {routes_tried}")
    log(f"✓ Deals inserted: {inserts}")


if __name__ == "__main__":
    run()
