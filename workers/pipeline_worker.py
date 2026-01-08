#!/usr/bin/env python3
"""
TravelTxter Pipeline Worker (Feeder) — ROUTE_CAPABILITY_MAP ENABLED (LOCKED)

Reads:
- CONFIG_ORIGIN_POOLS (origin_iata, priority, notes)
- THEMES (theme, destination_iata, destination_city, destination_country, priority, notes)
- CONFIG_SIGNALS (iata_hint, destination_city, destination_country, country_code, region, notes)
- ROUTE_CAPABILITY_MAP (origin_city, origin_iata, destination_city, destination_iata)

Writes:
- RAW_DEALS (header-mapped): NEW rows only
"""

import os
import json
import random
import datetime as dt
from typing import Dict, Any, List, Tuple, Set

import requests
import gspread
from google.oauth2.service_account import Credentials


# ==================== LOGGING ====================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==================== ENV / CONSTANTS ====================

# Sheet + auth
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
GCP_SA_JSON_ONE_LINE = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")

# Tabs (LOCKED: do not override with env to avoid silent miswiring)
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
THEMES_TAB = "THEMES"
ORIGINS_TAB = "CONFIG_ORIGIN_POOLS"
SIGNALS_TAB = "CONFIG_SIGNALS"
CAPABILITY_TAB = "ROUTE_CAPABILITY_MAP"

# Duffel
DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")

# Limits (LOCKED ENV VAR NAMES)
MAX_SEARCHES = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "10"))
MAX_INSERTS_TOTAL = int(os.getenv("DUFFEL_MAX_INSERTS", "50"))
MAX_INSERTS_PER_SEARCH = int(os.getenv("DUFFEL_MAX_INSERTS_PER_SEARCH", "5"))

# Variety constraints
MIN_ORIGINS_LOADED = int(os.getenv("MIN_ORIGINS_LOADED", "5"))          # abort if fewer than this loaded
MIN_ORIGIN_VARIETY = int(os.getenv("MIN_ORIGIN_VARIETY", "3"))         # must achieve >= this unique origins in selected routes

# Date window
DAYS_AHEAD_MIN = int(os.getenv("DAYS_AHEAD_MIN", "21"))
DAYS_AHEAD_MAX = int(os.getenv("DAYS_AHEAD_MAX", "90"))
TRIP_LEN_MIN = int(os.getenv("TRIP_LEN_MIN", "3"))
TRIP_LEN_MAX = int(os.getenv("TRIP_LEN_MAX", "7"))

# Canonical theme order (cycle)
MASTER_THEMES = [
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
]

# Optional: strict mode (default TRUE)
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")


# ==================== SHEETS INIT ====================

def init_sheets() -> gspread.Spreadsheet:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)")

    sa = json.loads(GCP_SA_JSON_ONE_LINE)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


# ==================== LOADERS ====================

def _clean_iata(x: Any) -> str:
    s = str(x or "").strip().upper()
    return s

def load_origins(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(ORIGINS_TAB)
    rows = ws.get_all_records()
    origins: List[Dict[str, Any]] = []

    for r in rows:
        iata = _clean_iata(r.get("origin_iata"))
        if not iata:
            continue
        try:
            pr = int(str(r.get("priority", "")).strip())
        except Exception:
            pr = 50
        origins.append({"origin_iata": iata, "priority": pr, "notes": str(r.get("notes", "")).strip()})

    # Dedup by iata keep highest priority
    best: Dict[str, Dict[str, Any]] = {}
    for o in origins:
        cur = best.get(o["origin_iata"])
        if not cur or o["priority"] > cur["priority"]:
            best[o["origin_iata"]] = o

    origins = list(best.values())
    origins.sort(key=lambda x: x["priority"], reverse=True)

    if len(origins) < MIN_ORIGINS_LOADED:
        raise RuntimeError(f"Insufficient origins: {len(origins)} < {MIN_ORIGINS_LOADED}")

    log(f"✓ Loaded {len(origins)} origins from {ORIGINS_TAB}")
    log("  Origins: " + ", ".join([f"{o['origin_iata']}({o['priority']})" for o in origins[:20]]))
    return origins

def load_themes(sheet: gspread.Spreadsheet) -> Dict[str, List[Dict[str, Any]]]:
    ws = sheet.worksheet(THEMES_TAB)
    rows = ws.get_all_records()

    themes: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        theme = str(r.get("theme", "")).strip()
        dest = _clean_iata(r.get("destination_iata"))
        if not theme or not dest:
            continue
        try:
            pr = int(str(r.get("priority", "")).strip())
        except Exception:
            pr = 50

        themes.setdefault(theme, []).append({
            "destination_iata": dest,
            "destination_city": str(r.get("destination_city", "")).strip(),
            "destination_country": str(r.get("destination_country", "")).strip(),
            "priority": pr,
            "notes": str(r.get("notes", "")).strip(),
        })

    for t in themes:
        # dedup dest keep highest priority
        by_dest: Dict[str, Dict[str, Any]] = {}
        for d in themes[t]:
            cur = by_dest.get(d["destination_iata"])
            if not cur or d["priority"] > cur["priority"]:
                by_dest[d["destination_iata"]] = d
        themes[t] = sorted(by_dest.values(), key=lambda x: x["priority"], reverse=True)

    log(f"✓ Loaded THEMES for {len(themes)} themes")
    return themes

def load_config_signals(sheet: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    ws = sheet.worksheet(SIGNALS_TAB)
    rows = ws.get_all_records()

    signals: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        # tolerate legacy column names
        iata = _clean_iata(r.get("iata_hint") or r.get("iata") or r.get("iata_code"))
        if not iata:
            continue
        signals[iata] = {
            "destination_city": str(r.get("destination_city", "")).strip(),
            "destination_country": str(r.get("destination_country", "")).strip(),
            "country_code": str(r.get("country_code", "")).strip().upper(),
            "region": str(r.get("region", "")).strip().lower(),
            "notes": str(r.get("notes", "")).strip(),
        }

    log(f"✓ Loaded {len(signals)} CONFIG_SIGNALS entries")
    return signals

def load_route_capability_map(sheet: gspread.Spreadsheet) -> Tuple[Set[Tuple[str, str]], Dict[str, str]]:
    """
    Returns:
      - allowed_pairs: set of (origin_iata, destination_iata)
      - origin_city_map: origin_iata -> origin_city (best effort)
    """
    ws = sheet.worksheet(CAPABILITY_TAB)
    rows = ws.get_all_records()

    allowed: Set[Tuple[str, str]] = set()
    origin_city_map: Dict[str, str] = {}

    for r in rows:
        o = _clean_iata(r.get("origin_iata"))
        d = _clean_iata(r.get("destination_iata"))
        oc = str(r.get("origin_city", "")).strip()

        if o and d:
            allowed.add((o, d))
            if oc and o not in origin_city_map:
                origin_city_map[o] = oc

    if not allowed:
        msg = f"{CAPABILITY_TAB} is empty or missing required headers"
        if STRICT_CAPABILITY_MAP:
            raise RuntimeError(msg)
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering (not recommended).")

    log(f"✓ Loaded {len(allowed)} allowed route pairs from {CAPABILITY_TAB}")
    return allowed, origin_city_map


# ==================== THEME SELECT ====================

def select_theme_for_today(themes_dict: Dict[str, List[Dict[str, Any]]]) -> Tuple[str, List[Dict[str, Any]]]:
    if not themes_dict:
        raise RuntimeError("No themes available")

    day_of_year = dt.datetime.utcnow().timetuple().tm_yday
    idx = day_of_year % len(MASTER_THEMES)
    selected = MASTER_THEMES[idx]

    # find next theme with destinations if empty
    for _ in range(len(MASTER_THEMES)):
        if selected in themes_dict and themes_dict[selected]:
            log(f"✓ Selected theme: {selected} ({len(themes_dict[selected])} destinations)")
            return selected, themes_dict[selected]
        idx = (idx + 1) % len(MASTER_THEMES)
        selected = MASTER_THEMES[idx]

    raise RuntimeError("No destinations in THEMES for any canonical theme")


# ==================== SAMPLERS ====================

def weighted_sample(items: List[Dict[str, Any]], n: int, key: str, weight_key: str) -> List[str]:
    """Priority-weighted sampling without replacement."""
    if not items:
        return []
    if len(items) <= n:
        return [str(i[key]) for i in items]

    pool = items[:]
    weights = [max(1, int(i.get(weight_key, 1))) for i in pool]
    chosen: List[str] = []

    for _ in range(min(n, len(pool))):
        total = sum(weights)
        r = random.uniform(0, total)
        cum = 0
        pick_idx = None
        for i, w in enumerate(weights):
            cum += w
            if r <= cum:
                pick_idx = i
                break
        if pick_idx is None:
            pick_idx = len(pool) - 1

        chosen.append(str(pool[pick_idx][key]))
        pool.pop(pick_idx)
        weights.pop(pick_idx)

    return chosen

def build_routes_with_variety(
    origin_pool: List[Dict[str, Any]],
    dest_pool: List[Dict[str, Any]],
    allowed_pairs: Set[Tuple[str, str]],
    max_routes: int,
    min_origin_variety: int
) -> List[Tuple[str, str]]:
    """
    Build a candidate set from pools, filter by allowed_pairs,
    then select up to max_routes ensuring >= min_origin_variety unique origins.
    """
    # Sample larger pools to avoid zip-bias
    sampled_origins = weighted_sample(origin_pool, n=max_routes * 4, key="origin_iata", weight_key="priority")
    sampled_dests = weighted_sample(dest_pool, n=max_routes * 6, key="destination_iata", weight_key="priority")

    # Cartesian product -> filter by capability map
    candidates = [(o, d) for o in sampled_origins for d in sampled_dests if o and d and o != d]
    if allowed_pairs:
        candidates = [r for r in candidates if r in allowed_pairs]

    # Dedup + shuffle
    candidates = list(dict.fromkeys(candidates))
    random.shuffle(candidates)

    if not candidates:
        raise RuntimeError("No candidate routes after capability filtering. Check ROUTE_CAPABILITY_MAP vs THEMES/ORIGINS.")

    # Select ensuring origin variety early
    selected: List[Tuple[str, str]] = []
    used_origins: Set[str] = set()

    # First pass: force origin variety
    for o, d in candidates:
        if o in used_origins:
            continue
        selected.append((o, d))
        used_origins.add(o)
        if len(used_origins) >= min_origin_variety:
            break

    if len(used_origins) < min_origin_variety:
        raise RuntimeError(f"Variety guarantee failed: {len(used_origins)} < {min_origin_variety}")

    # Second pass: fill remaining slots
    for o, d in candidates:
        if len(selected) >= max_routes:
            break
        if (o, d) in selected:
            continue
        selected.append((o, d))

    log(f"✓ Built {len(selected)} routes with {len(used_origins)} unique origins (min required {min_origin_variety})")
    return selected


# ==================== DUFFEL SEARCH ====================

def duffel_search(origin: str, destination: str, out_date: str, in_date: str) -> List[Dict[str, Any]]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, "departure_date": out_date},
                {"origin": destination, "destination": origin, "departure_date": in_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 300:
        log(f"❌ Duffel error {r.status_code}: {r.text[:300]}")
        return []

    data = r.json().get("data", {})
    offers = data.get("offers") or []
    return offers


# ==================== ENRICHMENT ====================

def enrich_deal(deal: Dict[str, Any], themes_dict: Dict[str, List[Dict[str, Any]]], signals: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Priority: THEMES > CONFIG_SIGNALS for destination city/country."""
    dest = deal.get("destination_iata", "")
    theme = deal.get("deal_theme") or deal.get("theme") or ""

    # THEMES lookup (theme-specific)
    if theme and theme in themes_dict:
        for d in themes_dict[theme]:
            if d.get("destination_iata") == dest:
                if d.get("destination_city"):
                    deal["destination_city"] = d["destination_city"]
                if d.get("destination_country"):
                    deal["destination_country"] = d["destination_country"]
                break

    # CONFIG_SIGNALS fallback
    if dest in signals:
        if not deal.get("destination_city") and signals[dest].get("destination_city"):
            deal["destination_city"] = signals[dest]["destination_city"]
        if not deal.get("destination_country") and signals[dest].get("destination_country"):
            deal["destination_country"] = signals[dest]["destination_country"]

    return deal


# ==================== WRITE RAW_DEALS ====================

def append_rows_header_mapped(ws, deals: List[Dict[str, Any]]) -> int:
    if not deals:
        return 0

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS header row is empty")

    rows_to_append = []
    for d in deals:
        row = []
        for h in headers:
            row.append(d.get(h, ""))
        rows_to_append.append(row)

    ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    return len(rows_to_append)


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTER PIPELINE WORKER — ROUTE_CAPABILITY_MAP ENABLED")
    log("=" * 80)
    log(f"MAX_SEARCHES={MAX_SEARCHES}, MAX_INSERTS_TOTAL={MAX_INSERTS_TOTAL}, MAX_INSERTS_PER_SEARCH={MAX_INSERTS_PER_SEARCH}")
    log(f"Variety: MIN_ORIGINS_LOADED={MIN_ORIGINS_LOADED}, MIN_ORIGIN_VARIETY={MIN_ORIGIN_VARIETY}")
    log(f"STRICT_CAPABILITY_MAP={STRICT_CAPABILITY_MAP}")

    sheet = init_sheets()
    ws_raw = sheet.worksheet(RAW_DEALS_TAB)

    origins = load_origins(sheet)
    themes_dict = load_themes(sheet)
    signals = load_config_signals(sheet)
    allowed_pairs, origin_city_map = load_route_capability_map(sheet)

    theme, theme_destinations = select_theme_for_today(themes_dict)

    # Build routes (theme-driven + capability filtered)
    routes = build_routes_with_variety(
        origin_pool=origins,
        dest_pool=theme_destinations,
        allowed_pairs=allowed_pairs,
        max_routes=MAX_SEARCHES,
        min_origin_variety=MIN_ORIGIN_VARIETY
    )

    log(f"✓ Routes for theme '{theme}': " + ", ".join([f"{o}->{d}" for o, d in routes[:12]]))

    # Search + collect deals
    all_deals: List[Dict[str, Any]] = []
    searches_done = 0

    for origin, destination in routes:
        if searches_done >= MAX_SEARCHES:
            break
        if len(all_deals) >= MAX_INSERTS_TOTAL:
            break

        # Dates
        depart_offset = random.randint(DAYS_AHEAD_MIN, DAYS_AHEAD_MAX)
        trip_len = random.randint(TRIP_LEN_MIN, TRIP_LEN_MAX)
        out_date = (dt.date.today() + dt.timedelta(days=depart_offset)).isoformat()
        in_date = (dt.date.today() + dt.timedelta(days=depart_offset + trip_len)).isoformat()

        log(f"Duffel: Searching {origin}->{destination} {out_date}/{in_date}")
        offers = duffel_search(origin, destination, out_date, in_date)
        searches_done += 1

        if not offers:
            continue

        # sort cheapest first
        try:
            offers = sorted(offers, key=lambda x: float(x.get("total_amount", "999999")))
        except Exception:
            pass

        offers = offers[:MAX_INSERTS_PER_SEARCH]

        for offer in offers:
            if len(all_deals) >= MAX_INSERTS_TOTAL:
                break

            price = offer.get("total_amount", "")
            deal_id_seed = f"{origin}{destination}{out_date}{offer.get('id','')}"
            deal = {
                # status lifecycle
                "status": "NEW",

                # theme (write both for compatibility)
                "deal_theme": theme,
                "theme": theme,

                # identifiers
                "deal_id": str(abs(hash(deal_id_seed))),

                # route
                "origin_iata": origin,
                "origin_city": origin_city_map.get(origin, ""),  # from ROUTE_CAPABILITY_MAP
                "destination_iata": destination,

                # dates + price
                "outbound_date": out_date,
                "return_date": in_date,
                "price_gbp": price,
                "currency": offer.get("total_currency", "GBP"),

                # friction proxy
                "stops": max(0, len((offer.get("slices") or [{}])[0].get("segments") or []) - 1),

                # timestamps (write both for compatibility)
                "created_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "inserted_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            }

            # destination enrichment (THEMES > CONFIG_SIGNALS)
            deal = enrich_deal(deal, themes_dict, signals)

            all_deals.append(deal)

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {len(all_deals)} (cap {MAX_INSERTS_TOTAL})")

    if not all_deals:
        log("⚠️ No deals found. (Not an error; depends on availability/prices.)")
        return 0

    inserted = append_rows_header_mapped(ws_raw, all_deals)
    log(f"✅ Inserted {inserted} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
