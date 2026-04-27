“””
atlas_outcome_verify.py
MIZAR Atlas — Outcome Verification Worker
Runs daily at 10:00 UTC via atlas_outcome_verify.yml

Selects all user_decisions where:

- verification_status = ‘pending’
- decision_timestamp + 7 days <= NOW()

For each, queries Duffel for the same route on the original outbound_date.
Classifies TP/FP/TN/FN, writes to outcome_verification, updates user_decisions.
“””

import os
import sys
import time
import logging
from datetime import datetime, timezone, date
from supabase import create_client

logging.basicConfig(
level=logging.INFO,
format=”%(asctime)s %(levelname)s %(message)s”,
datefmt=”%Y-%m-%dT%H:%M:%SZ”,
)
log = logging.getLogger(**name**)

# —————————————————————————

# Config

# —————————————————————————

SUPABASE_URL = os.environ[“MIZAR_SUPABASE_URL”]
SUPABASE_KEY = os.environ[“MIZAR_SUPABASE_SERVICE_ROLE_KEY”]
DUFFEL_TOKEN = os.environ[“DUFFEL_ACCESS_TOKEN”]

DUFFEL_BASE = “https://api.duffel.com”
DUFFEL_HEADERS = {
“Authorization”: f”Bearer {DUFFEL_TOKEN}”,
“Duffel-Version”: “v2”,
“Content-Type”: “application/json”,
“Accept”: “application/json”,
}

RISE_THRESHOLD_PCT = 10.0   # ground_truth_rose = True if price rose >= this
HIGH_RISK_THRESHOLD = 0.70  # regret_risk_score threshold for TP/FP vs TN/FN
REQUEST_DELAY_S = 1.2       # polite delay between Duffel calls

# —————————————————————————

# Supabase client

# —————————————————————————

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# —————————————————————————

# Duffel helpers

# —————————————————————————

import urllib.request
import urllib.error
import json as _json

def _duffel_post(path: str, payload: dict, retries: int = 3) -> dict | None:
“”“POST to Duffel with retry on 429.”””
url = f”{DUFFEL_BASE}{path}”
data = _json.dumps(payload).encode()
delays = [2, 5, 10]
for attempt in range(retries):
req = urllib.request.Request(url, data=data, headers=DUFFEL_HEADERS, method=“POST”)
try:
with urllib.request.urlopen(req, timeout=20) as resp:
return _json.loads(resp.read())
except urllib.error.HTTPError as e:
if e.code == 429:
wait = delays[min(attempt, len(delays) - 1)]
log.warning(“Duffel 429 — waiting %ss (attempt %d)”, wait, attempt + 1)
time.sleep(wait)
else:
body = e.read().decode(“utf-8”, errors=“replace”)
log.warning(“Duffel HTTP %d: %s”, e.code, body[:300])
return None
except Exception as exc:
log.warning(“Duffel request error: %s”, exc)
return None
log.error(“Duffel exhausted retries for %s”, path)
return None

def cheapest_gbp_price(
origin: str,
destination: str,
outbound_date: date,
cabin_class: str = “economy”,
trip_type: str = “return”,
return_date: date | None = None,
) -> float | None:
“””
Search Duffel for cheapest GBP offer on the given route/date.
Returns float price or None if unavailable.
“””
# Map cabin_class to Duffel cabin_class enum
cabin_map = {
“economy”: “economy”,
“premium_economy”: “premium_economy”,
“business”: “business”,
“first”: “first”,
}
duffel_cabin = cabin_map.get(cabin_class, “economy”)

```
slices = [
    {
        "origin": origin,
        "destination": destination,
        "departure_date": outbound_date.isoformat(),
    }
]
if trip_type == "return" and return_date is not None:
    slices.append(
        {
            "origin": destination,
            "destination": origin,
            "departure_date": return_date.isoformat(),
        }
    )

payload = {
    "data": {
        "slices": slices,
        "passengers": [{"type": "adult"}],
        "cabin_class": duffel_cabin,
    }
}

resp = _duffel_post("/air/offer_requests?return_offers=true", payload)
if resp is None:
    return None

offers = resp.get("data", {}).get("offers", [])
if not offers:
    return None

gbp_prices = []
for offer in offers:
    currency = offer.get("total_currency", "")
    if currency == "GBP":
        try:
            gbp_prices.append(float(offer["total_amount"]))
        except (KeyError, ValueError):
            pass

if not gbp_prices:
    return None

return min(gbp_prices)
```

# —————————————————————————

# Classification

# —————————————————————————

def classify_outcome(regret_risk_score: float, ground_truth_rose: bool) -> str:
predicted_high = regret_risk_score >= HIGH_RISK_THRESHOLD
if predicted_high and ground_truth_rose:
return “TP”
elif predicted_high and not ground_truth_rose:
return “FP”
elif not predicted_high and not ground_truth_rose:
return “TN”
else:
return “FN”

# —————————————————————————

# Main loop

# —————————————————————————

def fetch_pending_decisions() -> list[dict]:
“”“Return decisions eligible for t+7 verification.”””
now = datetime.now(timezone.utc).isoformat()
resp = (
supabase.table(“user_decisions”)
.select(
“decision_id, decision_timestamp, origin_iata, destination_iata, “
“outbound_date, return_date, price_shown_gbp, regret_risk_score, “
“trip_type, cabin_class”
)
.eq(“verification_status”, “pending”)
.lte(“decision_timestamp”, now)  # decision_timestamp + 7d filter applied below
.execute()
)
rows = resp.data or []

```
eligible = []
now_dt = datetime.now(timezone.utc)
for row in rows:
    decision_ts = datetime.fromisoformat(row["decision_timestamp"])
    if (now_dt - decision_ts).days >= 7:
        eligible.append(row)

log.info("Found %d pending decisions, %d eligible for t+7", len(rows), len(eligible))
return eligible
```

def already_verified(decision_id: str) -> bool:
“”“Guard against duplicate key on outcome_verification.”””
resp = (
supabase.table(“outcome_verification”)
.select(“verification_id”)
.eq(“decision_id”, decision_id)
.execute()
)
return len(resp.data or []) > 0

def write_verification(
decision_id: str,
price_t7: float | None,
price_shown: float,
regret_risk_score: float,
failure_reason: str | None,
) -> None:
now_iso = datetime.now(timezone.utc).isoformat()

```
if price_t7 is None:
    row = {
        "decision_id": decision_id,
        "verification_timestamp": now_iso,
        "price_t7_gbp": None,
        "price_change_pct": None,
        "ground_truth_rose": None,
        "prediction_outcome": None,
        "verification_method": "duffel_api",
        "failure_reason": failure_reason or "no_price_returned",
    }
    status = "failed"
else:
    change_pct = ((price_t7 - float(price_shown)) / float(price_shown)) * 100
    ground_truth_rose = change_pct >= RISE_THRESHOLD_PCT
    outcome = classify_outcome(float(regret_risk_score), ground_truth_rose)
    row = {
        "decision_id": decision_id,
        "verification_timestamp": now_iso,
        "price_t7_gbp": round(price_t7, 2),
        "price_change_pct": round(change_pct, 2),
        "ground_truth_rose": ground_truth_rose,
        "prediction_outcome": outcome,
        "verification_method": "duffel_api",
        "failure_reason": None,
    }
    status = "verified"

supabase.table("outcome_verification").insert(row).execute()

supabase.table("user_decisions").update(
    {
        "verification_status": status,
        "updated_at": now_iso,
    }
).eq("decision_id", decision_id).execute()
```

def run():
decisions = fetch_pending_decisions()
if not decisions:
log.info(“No eligible decisions. Exiting.”)
return

```
success = 0
failed = 0
skipped = 0

for d in decisions:
    decision_id = d["decision_id"]

    if already_verified(decision_id):
        log.info("SKIP %s — already in outcome_verification", decision_id)
        skipped += 1
        continue

    origin = d["origin_iata"]
    destination = d["destination_iata"]
    outbound_date_str = d["outbound_date"]
    return_date_str = d.get("return_date")
    trip_type = d.get("trip_type") or "return"
    cabin_class = d.get("cabin_class") or "economy"
    price_shown = d["price_shown_gbp"]
    score = d["regret_risk_score"]

    try:
        outbound_date = date.fromisoformat(outbound_date_str)
    except Exception:
        log.warning("Invalid outbound_date for %s: %s", decision_id, outbound_date_str)
        write_verification(decision_id, None, price_shown, score, "invalid_outbound_date")
        failed += 1
        continue

    # Skip if flight has already departed
    if outbound_date < datetime.now(timezone.utc).date():
        log.info("SKIP %s — outbound_date %s has passed", decision_id, outbound_date)
        write_verification(decision_id, None, price_shown, score, "flight_already_departed")
        failed += 1
        time.sleep(REQUEST_DELAY_S)
        continue

    return_date = None
    if return_date_str:
        try:
            return_date = date.fromisoformat(return_date_str)
        except Exception:
            pass

    log.info(
        "Verifying %s | %s→%s %s | score=%.3f | shown=£%.2f",
        decision_id, origin, destination, outbound_date, float(score), float(price_shown),
    )

    price_t7 = cheapest_gbp_price(
        origin=origin,
        destination=destination,
        outbound_date=outbound_date,
        cabin_class=cabin_class,
        trip_type=trip_type,
        return_date=return_date,
    )

    if price_t7 is not None:
        change_pct = ((price_t7 - float(price_shown)) / float(price_shown)) * 100
        log.info(
            "  → t+7 price=£%.2f | change=%.1f%% | %s",
            price_t7, change_pct,
            classify_outcome(float(score), change_pct >= RISE_THRESHOLD_PCT),
        )
        success += 1
    else:
        log.warning("  → No GBP price returned")
        failed += 1

    write_verification(decision_id, price_t7, price_shown, score, None if price_t7 else "duffel_no_gbp_offer")
    time.sleep(REQUEST_DELAY_S)

log.info(
    "Done. Success=%d Failed=%d Skipped=%d Total=%d",
    success, failed, skipped, len(decisions),
)
```

if **name** == “**main**”:
run()