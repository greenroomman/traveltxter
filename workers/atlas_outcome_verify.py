#!/usr/bin/env python3
"""
MIZAR Atlas Outcome Verification Worker
Created: March 18, 2026
"""

import os
import sys
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

    try:
        response = requests.post(
            f'{DUFFEL_API_BASE}/air/offer_requests',
            headers=DUFFEL_HEADERS,
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            print(f"Duffel API error {response.status_code}: {response.text}")
            return None

        data = response.json()
        offers = data.get('data', {}).get('offers', [])

        if not offers:
            return None

        cheapest = min(offers, key=lambda x: float(x['total_amount']))
        return float(cheapest['total_amount'])

    except Exception as e:
        print(f"Error querying Duffel for {origin}-{destination}: {e}")
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

    print(f"Verifying {decision_id}: {origin}-{destination} on {outbound_date}")

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

        print(f"  ❌ Failed: Price unavailable")
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

    print(f"  ✅ {price_shown:.2f} → {price_t7:.2f} ({price_change_pct:+.1f}%) | {prediction_outcome}")
    return True


def main():
    print("=" * 80)
    print("MIZAR Outcome Verification Worker")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)

    pending = get_pending_decisions()
    print(f"\nFound {len(pending)} pending decisions ready for verification")

    if not pending:
        print("✅ No decisions to verify. Exiting.")
        return 0

    success_count = 0
    failure_count = 0

    for decision in pending:
        try:
            if verify_decision(decision):
                success_count += 1
            else:
                failure_count += 1
        except Exception as e:
            print(f"  ❌ Error verifying {decision['decision_id']}: {e}")
            failure_count += 1

    print("\n" + "=" * 80)
    print(f"Verification Complete")
    print(f"  ✅ Verified: {success_count}")
    print(f"  ❌ Failed: {failure_count}")
    print(f"  Success rate: {success_count / len(pending) * 100:.1f}%")
    print("=" * 80)

    success_rate = success_count / len(pending)
    if success_rate < 0.95:
        print(f"⚠️ Warning: Success rate {success_rate * 100:.1f}% below 95% target")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
