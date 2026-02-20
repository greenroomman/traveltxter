#!/usr/bin/env python3
"""
workers/atlas_snapshot_backfill.py
ATLAS SNAPSHOT BACKFILL — v0

Fills price_t7 and price_t14 for mature snapshot rows.
Computes rose_10pct and fell_10pct flags when price_t14 lands.

Run daily AFTER capture (07:20 UTC recommended).
Never touches RAW_DEALS.

MISSING = vanished fare. Real data. Do not impute.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Env helpers
# ----------------------------

def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ----------------------------
# GSpread auth
# ----------------------------

def _sanitize_sa_json(raw: str) -> str:
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

def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    raw = _sanitize_sa_json(raw)
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------
# Time helpers
# ----------------------------

def _utc_date() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


# ----------------------------
# Duffel — price fetch only
# ----------------------------

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

def duffel_headers() -> Dict[str, str]:
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY.")
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def fetch_cheapest_price(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin: str = "economy",
    max_connections: int = 1,
) -> Optional[float]:
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
        return round(float(gbp[0].get("total_amount") or 0), 2)
    except Exception:
        return None


# ----------------------------
# Regret threshold
# ----------------------------

REGRET_THRESHOLD = 0.10


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    print("=" * 70)
    print("ATLAS SNAPSHOT BACKFILL v0")
    print("=" * 70)

    snapshot_tab = env_str("SNAPSHOT_LOG_TAB", "SNAPSHOT_LOG")
    sleep_s = float(env_str("FEEDER_SLEEP_SECONDS", "0.5"))
    max_fills = env_int("ATLAS_BACKFILL_MAX", 50)
    cabin = env_str("ATLAS_CABIN_CLASS", "economy")
    max_connections = env_int("ATLAS_MAX_CONNECTIONS", 1)

    today_str = _utc_date()
    today = dt.date.fromisoformat(today_str)
    t7_target = (today - dt.timedelta(days=7)).isoformat()
    t14_target = (today - dt.timedelta(days=14)).isoformat()

    print(f"📅 Today: {today_str}")
    print(f"🔍 t7  fills for snapshot_date = {t7_target}")
    print(f"🔍 t14 fills for snapshot_date = {t14_target}")
    print("-" * 70)

    gc = gspread_client()
    sh = gc.open_by_key(env_str("SPREADSHEET_ID") or env_str("SHEET_ID"))
    ws = sh.worksheet(snapshot_tab)

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        print("⚠️ SNAPSHOT_LOG has no data rows.")
        return 0

    hdr = all_values[0]
    col: Dict[str, int] = {h: i for i, h in enumerate(hdr)}

    def get(row: List[str], name: str) -> str:
        i = col.get(name)
        return (row[i] if i is not None and i < len(row) else "").strip()

    updates: Dict[tuple, Any] = {}
    fills_t7 = 0
    fills_t14 = 0
    missing_count = 0
    searches = 0

    for row_idx, row in enumerate(all_values[1:], start=2):
        if searches >= max_fills:
            print(f"⚠️ Hit max_fills={max_fills}. Remaining rows deferred to tomorrow.")
            break

        snap_date = get(row, "snapshot_date")
        notes = get(row, "notes")
        price_t0_raw = get(row, "price_gbp")

        if not price_t0_raw or notes == "no_offer":
            continue

        origin = get(row, "origin_iata")
        dest = get(row, "destination_iata")
        out_date = get(row, "outbound_date")
        ret_date = get(row, "return_date")

        if not all([origin, dest, out_date, ret_date]):
            continue

        try:
            if dt.date.fromisoformat(out_date) < today:
                continue
        except Exception:
            pass

        # t7 fill
        if snap_date == t7_target and not get(row, "price_t7"):
            searches += 1
            print(f"🔎 t7  [{searches}] {origin}→{dest}  {out_date}/{ret_date}  (row {row_idx})")

            price = fetch_cheapest_price(
                origin, dest, out_date, ret_date,
                cabin=cabin, max_connections=max_connections
            )

            t7_col = col.get("price_t7")
            if t7_col is not None:
                if price is not None:
                    updates[(row_idx, t7_col + 1)] = price
                    fills_t7 += 1
                    print(f"   ✅ t7 price: £{price}")
                else:
                    updates[(row_idx, t7_col + 1)] = "MISSING"
                    missing_count += 1
                    print(f"   ⚠️ t7: no offer — MISSING logged")

            time.sleep(sleep_s)

        # t14 fill + regret flags
        elif snap_date == t14_target and not get(row, "price_t14"):
            searches += 1
            print(f"🔎 t14 [{searches}] {origin}→{dest}  {out_date}/{ret_date}  (row {row_idx})")

            price = fetch_cheapest_price(
                origin, dest, out_date, ret_date,
                cabin=cabin, max_connections=max_connections
            )

            t14_col = col.get("price_t14")
            rose_col = col.get("rose_10pct")
            fell_col = col.get("fell_10pct")

            if t14_col is not None:
                if price is not None:
                    updates[(row_idx, t14_col + 1)] = price
                    fills_t14 += 1
                    print(f"   ✅ t14 price: £{price}")

                    try:
                        p0 = float(price_t0_raw)
                        rose = price >= p0 * (1 + REGRET_THRESHOLD)
                        fell = price <= p0 * (1 - REGRET_THRESHOLD)
                        if rose_col is not None:
                            updates[(row_idx, rose_col + 1)] = str(rose).upper()
                        if fell_col is not None:
                            updates[(row_idx, fell_col + 1)] = str(fell).upper()
                        direction = "📈 ROSE" if rose else ("📉 FELL" if fell else "➡️  FLAT")
                        print(f"   {direction}  t0=£{p0}  t14=£{price}")
                    except Exception as e:
                        print(f"   ⚠️ Flag computation failed: {e}")
                else:
                    updates[(row_idx, t14_col + 1)] = "MISSING"
                    if rose_col is not None:
                        updates[(row_idx, rose_col + 1)] = "UNKNOWN"
                    if fell_col is not None:
                        updates[(row_idx, fell_col + 1)] = "UNKNOWN"
                    missing_count += 1
                    print(f"   ⚠️ t14: no offer — MISSING/UNKNOWN logged")

            time.sleep(sleep_s)

    if updates:
        print("-" * 70)
        print(f"📝 Applying {len(updates)} cell updates (batch)…")
        # One API call instead of one per cell — critical for Sheets write quota.
        batch = [
            {"range": gspread.utils.rowcol_to_a1(r, c), "values": [[val]]}
            for (r, c), val in updates.items()
        ]
        ws.batch_update(batch, value_input_option="USER_ENTERED")
        print("✅ All updates applied.")
    else:
        print("\n⚠️ No updates to apply.")

    print(
        f"\n📊 SUMMARY: t7_fills={fills_t7}  t14_fills={fills_t14}  "
        f"missing={missing_count}  duffel_calls={searches}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
