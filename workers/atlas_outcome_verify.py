#!/usr/bin/env python3
"""
MIZAR Atlas Outcome Verification Worker
Created: March 18, 2026
Updated: April 12, 2026 — fixed Duffel 201 response, return_offers param, rate limit retry
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import requests
from supabase import create_client, Client

SUPABASE_URL = os.environ['MIZAR_SUPABASE_URL']
SUPABASE_KEY = os.environ['MIZAR_SUPABASE_SERVICE_ROLE_KEY']
DUFFEL_TOKEN = os.environ['DUFFEL_ACCESS_TOKEN']

DUFFEL_API_BASE = 'https://api.duffel.com'
DUFFEL_HEADERS = {
    'Authorization': f'Bearer {DUFFEL_TOKEN}',
    'Duffel-Version': 'v2',
    'Content-Type': 'application/json'
}

HIGH_RISK_THRESHOLD = 0.70
PRICE_RISE_THRESHOLD = 0.10
REQUEST_DELAY = 1.0
MAX_RETRIES = 3

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_pending_decisions() -> List[Dict]:
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
    cutoff_str = cutoff_date.isoformat()
    print(f"Cutoff date: {cutoff_str}")

    response = supabase.table('user_decisions').select('*').eq(
        'verification_status', 'pending'
    ).lte(
        'decision_timestamp', cutoff_str
    ).execute()

    print(f"Raw query returned {len(response.data)} rows")
    return response.data


def query_duffel_price(
    origin: str,
    destination: str,
    outbound_date: str,
    return_date: str
) -> Optional[float]:
    payload = {
        'data': {
            'slices': [
                {
                    'origin': origin,
                    'destination': destination,
                    'departure_date': outbound_date
                },
                {
                    'origin': destination,
                    'destination': origin,
                    'departure_date': return_date
                }
            ],
            'passengers': [{'type': 'adult'}],
            'cabin_class': 'economy'
        }
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                f'{DUFFEL_API_BASE}/air/offer_requests?return_offers=true',
                headers=DUFFEL_HEADERS,
                json=payload,
                timeout=30
            )

            if response.status_code == 429:
                reset_after = int(response.headers.get('ratelimit-reset', 10))
                wait = max(reset_after, 5)
                print(f"  Rate limited. Waiting {wait}s before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(wait)
                continue

            if response.status_code not in (200, 201):
                print(f"  Duffel API error {response.status_code}: {response.text[:200]}")
                return None

            data = response.json()
            offers = data.get('data', {}).get('offers', [])

            if not offers:
                return None

            cheapest = min(offers, key=lambda x: float(x['total_amount']))
            return float(cheapest['total_amount'])

        except Exception as e:
            print(f"  Error querying Duffel for {origin}-{destination}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
            else:
                return None

    print(f"  Max retries reached for {origin}-{destination}")
    return None


def classify_prediction(
    regret_risk_score: float,
    price_shown: float,
    price_t7: float
) -> Tuple[bool, str]:
    price_change_pct = ((price_t7 - price_shown) / price_shown) * 100
    ground_truth_rose = price_change_pct >= (PRICE_RISE_THRESHOLD * 100)
    predicted_high_risk = regret_risk_score >= HIGH_RISK_THRESHOLD

    if predicted_high_risk and ground_truth_rose:
        outcome = 'TP'
    elif predicted_high_risk and not ground_truth_rose:
        outcome = 'FP'
    elif not predicted_high_risk and not ground_truth_rose:
        outcome = 'TN'
    else:
        outcome = 'FN'

    return ground_truth_rose, outcome


def verify_decision(decision: Dict) -> bool:
    decision_id = decision['decision_id']
    origin = decision['origin_iata']
    destination = decision['destination_iata']
    outbound_date = decision['outbound_date']
    return_date = decision['return_date']
    price_shown = float(decision['price_shown_gbp'])
    regret_risk_score = float(decision['regret_risk_score'])

    print(f"Verifying {origin}-{destination} on {outbound_date}")

    time.sleep(REQUEST_DELAY)

    price_t7 = query_duffel_price(origin, destination, outbound_date, return_date)

    if price_t7 is None:
        supabase.table('outcome_verification').insert({
            'decision_id': decision_id,
            'verification_method': 'duffel_api',
            'failure_reason': 'Price unavailable at t+7'
        }).execute()

        supabase.table('user_decisions').update({
            'verification_status': 'failed'
        }).eq('decision_id', decision_id).execute()

        print(f"  Failed: Price unavailable")
        return False

    price_change_pct = ((price_t7 - price_shown) / price_shown) * 100
    ground_truth_rose, prediction_outcome = classify_prediction(
        regret_risk_score, price_shown, price_t7
    )

    supabase.table('outcome_verification').insert({
        'decision_id': decision_id,
        'price_t7_gbp': price_t7,
        'price_change_pct': round(price_change_pct, 2),
        'ground_truth_rose': ground_truth_rose,
        'prediction_outcome': prediction_outcome,
        'verification_method': 'duffel_api'
    }).execute()

    supabase.table('user_decisions').update({
        'verification_status': 'verified'
    }).eq('decision_id', decision_id).execute()

    print(f"  {price_shown:.2f} -> {price_t7:.2f} ({price_change_pct:+.1f}%) | {prediction_outcome}")
    return True


def main():
    print("=" * 60)
    print("MIZAR Outcome Verification Worker")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    pending = get_pending_decisions()
    print(f"\nFound {len(pending)} pending decisions ready for verification")

    if not pending:
        print("No decisions to verify. Exiting.")
        return 0

    success_count = 0
    failure_count = 0

    for i, decision in enumerate(pending):
        try:
            if verify_decision(decision):
                success_count += 1
            else:
                failure_count += 1
        except Exception as e:
            print(f"  Error: {e}")
            failure_count += 1

        if (i + 1) % 25 == 0:
            print(f"\n--- Progress: {i+1}/{len(pending)} | Verified: {success_count} | Failed: {failure_count} ---\n")

    print("\n" + "=" * 60)
    print(f"Verification Complete")
    print(f"  Verified: {success_count}")
    print(f"  Failed:   {failure_count}")
    if len(pending) > 0:
        print(f"  Success rate: {success_count / len(pending) * 100:.1f}%")
    print("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
