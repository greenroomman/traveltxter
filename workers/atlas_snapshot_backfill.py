#!/usr/bin/env python3
"""
workers/atlas_snapshot_backfill.py
ATLAS SNAPSHOT BACKFILL — v1.0 (Crisis Flag)

Fills price_t7 and price_t14 for mature snapshot rows.
Computes rose_10pct and fell_10pct flags when price_t14 lands.
Computes crisis contamination and training_action when labels are set.

Run daily AFTER capture (07:20 UTC recommended).
Never touches RAW_DEALS.

WHAT CHANGED FROM v0:
- Crisis contamination assessment added.
  When a t7 or t14 label is set, the script also computes:
    crisis_contamination_pct_t14, crisis_contamination_pct_t7,
    crisis_label_contaminated, training_action.
  Reads atlas_crisis_config.json. Falls back gracefully if missing.

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
# Crisis detector (embedded)
# ----------------------------

class CrisisDetector:
    """
    Reads crisis events from a JSON config file and evaluates
    whether a given snapshot row falls within a crisis window.
    Falls back to 'no crisis' if the config file is missing.
    """

    def __init__(self, config_path: str = "atlas_crisis_config.json"):
        self.events = []
        self.regions = {}
        self.severity_levels = {}
        try:
            with open(config_path, "r") as f:
                cfg = json.load(f)
            self.events = cfg.get("crisis_events", [])
            self.regions = cfg.get("region_definitions", {})
            self.severity_levels = cfg.get("severity_levels", {})
            active = sum(1 for e in self.events if not e.get("end_date"))
            print(f"✅ Crisis config loaded: {len(self.events)} event(s), {active} active")
        except FileNotFoundError:
            print("⚠️  atlas_crisis_config.json not found — crisis flags disabled")
        except Exception as e:
            print(f"⚠️  Crisis config error: {e} — crisis flags disabled")

    def _parse_date(self, date_str):
        if not date_str:
            return None
        return dt.datetime.strptime(date_str, "%Y-%m-%d")

    def _resolve_affected_iatas(self, event):
        iatas = set(event.get("affected_destinations", []))
        for region_key in event.get("affected_regions", []):
            iatas.update(self.regions.get(region_key, []))
        return iatas

    def _is_date_in_window(self, check_date, start, end):
        d = self._parse_date(check_date)
        s = self._parse_date(start)
        e = self._parse_date(end) if end else dt.datetime(2099, 12, 31)
        return s <= d <= e

    def get_contamination_pct(self, snapshot_date, lookahead_days=14):
        snap_dt = self._parse_date(snapshot_date)
        contaminated_days = 0
        for day_offset in range(1, lookahead_days + 1):
            check_dt = snap_dt + dt.timedelta(days=day_offset)
            check_str = check_dt.strftime("%Y-%m-%d")
            for event in self.events:
                if self._is_date_in_window(check_str, event["start_date"], event.get("end_date")):
                    contaminated_days += 1
                    break
        return contaminated_days / lookahead_days

    def is_label_contaminated(self, snapshot_date, origin, destination, lookahead_days=14):
        snap_dt = self._parse_date(snapshot_date)
        for day_offset in range(1, lookahead_days + 1):
            check_dt = snap_dt + dt.timedelta(days=day_offset)
            check_str = check_dt.strftime("%Y-%m-%d")
            for event in self.events:
                if not self._is_date_in_window(check_str, event["start_date"], event.get("end_date")):
                    continue
                severity = event.get("severity", "low")
                if severity in ("extreme", "high"):
                    return True
                contam_window = self.severity_levels.get(severity, {}).get("label_contamination_window_days", 14)
                if day_offset <= contam_window:
                    return True
        return False

    def get_flags(self, snapshot_date, origin, destination):
        matching = []
        for event in self.events:
            if self._is_date_in_window(snapshot_date, event["start_date"], event.get("end_date")):
                matching.append(event)
        if not matching:
            return {"crisis_flag": "", "crisis_id": "", "crisis_severity": "",
                    "crisis_route_affected": "", "crisis_global_impact": ""}
        route_affected = False
        global_impact = False
        severities = []
        for event in matching:
            affected = self._resolve_affected_iatas(event)
            if origin in affected or destination in affected:
                route_affected = True
            if event.get("global_impact", False):
                global_impact = True
            severities.append(event.get("severity", "low"))
        for s in ["extreme", "high", "moderate", "low"]:
            if s in severities:
                highest = s
                break
        else:
            highest = "low"
        return {
            "crisis_flag": "TRUE",
            "crisis_id": ",".join(e["id"] for e in matching),
            "crisis_severity": highest,
            "crisis_route_affected": str(route_affected).upper(),
            "crisis_global_impact": str(global_impact).upper(),
        }

    def get_training_recommendation(self, snapshot_date, origin, destination):
        flags = self.get_flags(snapshot_date, origin, destination)
        if flags["crisis_flag"] == "TRUE":
            sev = flags["crisis_severity"]
            action = self.severity_levels.get(sev, {}).get("training_action", "flag_only")
            if action == "exclude_all":
                return "exclude_crisis"
            elif action == "exclude_affected" and flags["crisis_route_affected"] == "TRUE":
                return "exclude_crisis"
            else:
                return "flag_review"
        if self.is_label_contaminated(snapshot_date, origin, destination):
            if not self.is_label_contaminated(snapshot_date, origin, destination, lookahead_days=7):
                return "include_t7_only"
            return "exclude_contaminated"
        return "include"


# ----------------------------
# Crisis contamination writer
# ----------------------------

def _write_crisis_contamination(updates, col, row_idx, crisis, snap_date, origin, dest):
    """Write crisis contamination columns for a row during backfill."""
    contam_t14_col = col.get("crisis_contamination_pct_t14")
    contam_t7_col = col.get("crisis_contamination_pct_t7")
    label_contam_col = col.get("crisis_label_contaminated")
    training_col = col.get("training_action")

    if contam_t14_col is not None:
        pct14 = crisis.get_contamination_pct(snap_date, lookahead_days=14)
        updates[(row_idx, contam_t14_col + 1)] = f"{pct14:.2f}"
    if contam_t7_col is not None:
        pct7 = crisis.get_contamination_pct(snap_date, lookahead_days=7)
        updates[(row_idx, contam_t7_col + 1)] = f"{pct7:.2f}"
    if label_contam_col is not None:
        contam = crisis.is_label_contaminated(snap_date, origin, dest)
        updates[(row_idx, label_contam_col + 1)] = str(contam).upper()
    if training_col is not None:
        action = crisis.get_training_recommendation(snap_date, origin, dest)
        updates[(row_idx, training_col + 1)] = action


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    print("=" * 70)
    print("ATLAS SNAPSHOT BACKFILL v1.0 — Crisis Flag")
    print("=" * 70)

    snapshot_tab = env_str("SNAPSHOT_LOG_TAB", "SNAPSHOT_LOG")
    crisis_config_path = env_str("ATLAS_CRISIS_CONFIG_PATH", "atlas_crisis_config.json")
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

    # Crisis detector — reads atlas_crisis_config.json
    crisis = CrisisDetector(crisis_config_path)

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

            # ── Crisis contamination (v1.0) ──
            _write_crisis_contamination(updates, col, row_idx, crisis, snap_date, origin, dest)

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

            # ── Crisis contamination (v1.0) ──
            _write_crisis_contamination(updates, col, row_idx, crisis, snap_date, origin, dest)

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
