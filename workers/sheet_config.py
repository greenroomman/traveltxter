#!/usr/bin/env python3
"""
TravelTxter V4.5x — lib/sheet_config.py

Single source of truth reader for Google Sheet control-plane tabs:
- CONFIG
- CONFIG_ORIGIN_POOLS
- CONFIG_CARRIER_BIAS
- CONFIG_SIGNALS
- THEMES
- PHRASE_BANK
- MVP_RULES
- DUFFEL_SEARCH_LOG

Rules:
- Safe defaults if a tab is missing/empty
- Header-mapped reads only
- Never writes (writers log to their own tabs)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import datetime as dt
import json

import gspread


def _norm(s: Any) -> str:
    return str(s or "").strip()

def _upper(s: Any) -> str:
    return _norm(s).upper()

def _to_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(_norm(x)))
    except Exception:
        return default

def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(_norm(x))
    except Exception:
        return default

def _truthy(x: Any) -> bool:
    v = _upper(x)
    return v in ("TRUE", "YES", "1", "Y", "ON", "ENABLED")


def _read_tab_as_dicts(sh: gspread.Spreadsheet, tab_name: str) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        return []
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []
    headers = [_norm(h) for h in values[0]]
    out: List[Dict[str, str]] = []
    for r in values[1:]:
        d = {}
        for i, h in enumerate(headers):
            d[h] = _norm(r[i] if i < len(r) else "")
        if any(d.values()):
            out.append(d)
    return out


@dataclass
class ConfigBundle:
    config_routes: List[Dict[str, str]]
    origin_pools: List[Dict[str, str]]
    carrier_bias: List[Dict[str, str]]
    themes: List[Dict[str, str]]
    phrase_bank: List[Dict[str, str]]
    mvp_rules: List[Dict[str, str]]
    signals: List[Dict[str, str]]


def load_config_bundle(sh: gspread.Spreadsheet) -> ConfigBundle:
    return ConfigBundle(
        config_routes=_read_tab_as_dicts(sh, "CONFIG"),
        origin_pools=_read_tab_as_dicts(sh, "CONFIG_ORIGIN_POOLS"),
        carrier_bias=_read_tab_as_dicts(sh, "CONFIG_CARRIER_BIAS"),
        themes=_read_tab_as_dicts(sh, "THEMES"),
        phrase_bank=_read_tab_as_dicts(sh, "PHRASE_BANK"),
        mvp_rules=_read_tab_as_dicts(sh, "MVP_RULES"),
        signals=_read_tab_as_dicts(sh, "CONFIG_SIGNALS"),
    )


# ---------- helpers you’ll reuse everywhere ----------

def pick_theme_for_today(themes_rows: List[Dict[str, str]], override: str = "") -> str:
    """
    Deterministic daily theme rotation if no override.
    """
    if override:
        return _norm(override)

    uniq = sorted({r.get("theme", "").strip() for r in themes_rows if r.get("theme", "").strip()})
    if not uniq:
        return "default"

    doy = int(dt.datetime.utcnow().strftime("%j"))  # 1..366
    return uniq[doy % len(uniq)]


def active_config_routes(config_rows: List[Dict[str, str]], theme: str) -> List[Dict[str, str]]:
    t = _norm(theme)
    out = []
    for r in config_rows:
        if not _truthy(r.get("enabled", "")):
            continue
        if _norm(r.get("theme", "")) != t:
            continue
        out.append(r)
    # sort by priority desc, then origin/dest
    out.sort(key=lambda x: (_to_int(x.get("priority", "0"), 0), x.get("origin_iata",""), x.get("destination_iata","")), reverse=True)
    return out


def origins_for_today(origin_rows: List[Dict[str, str]]) -> List[str]:
    """
    Deterministic origin pool rotation.
    Returns a list of origin_iata ordered by priority.
    """
    if not origin_rows:
        return []

    pools = sorted({r.get("origin_pool","").strip() for r in origin_rows if r.get("origin_pool","").strip()})
    if not pools:
        return []

    doy = int(dt.datetime.utcnow().strftime("%j"))
    pool = pools[doy % len(pools)]

    rows = [r for r in origin_rows if r.get("origin_pool","").strip() == pool and r.get("origin_iata","").strip()]
    rows.sort(key=lambda x: _to_int(x.get("priority","0"), 0), reverse=True)
    return [_upper(r.get("origin_iata")) for r in rows]


def theme_destinations(themes_rows: List[Dict[str, str]], theme: str, limit: int = 20) -> List[str]:
    t = _norm(theme)
    rows = [r for r in themes_rows if _norm(r.get("theme","")) == t and r.get("destination_iata","").strip()]
    rows.sort(key=lambda x: _to_int(x.get("priority","0"), 0), reverse=True)
    out = [_upper(r.get("destination_iata")) for r in rows]
    return out[:limit]


def iata_signal_maps(signals_rows: List[Dict[str, str]]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Builds iata -> city / country maps from CONFIG_SIGNALS.
    Uses: iata_hint + destination_city + destination_country
    """
    city = {}
    country = {}
    for r in signals_rows:
        code = _upper(r.get("iata_hint",""))
        if len(code) != 3:
            continue
        c = _norm(r.get("destination_city",""))
        k = _norm(r.get("destination_country",""))
        if c:
            city[code] = c
        if k:
            country[code] = k
    return city, country


def carrier_bias_weight(bias_rows: List[Dict[str, str]], theme: str, destination_iata: str, carrier_code: str) -> float:
    """
    Returns bias_weight if matches theme+dest+carrier.
    """
    t = _norm(theme)
    d = _upper(destination_iata)
    cc = _upper(carrier_code)
    best = 0.0
    for r in bias_rows:
        if _norm(r.get("theme","")) != t:
            continue
        if _upper(r.get("destination_iata","")) != d:
            continue
        if _upper(r.get("carrier_code","")) != cc:
            continue
        best = max(best, _to_float(r.get("bias_weight","0"), 0.0))
    return best


def mvp_hard_limits(mvp_rows: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Parses only what we can enforce deterministically right now:
    - price range: 10..800
    - stops <= 1
    - outbound_date: tomorrow..+365 days
    """
    return {
        "min_price_gbp": 10.0,
        "max_price_gbp": 800.0,
        "max_stops": 1,
        "max_days_ahead": 365,
        "min_days_ahead": 1,
    }
