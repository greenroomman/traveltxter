#!/usr/bin/env python3
"""
MIZAR Atlas Outcome Verification Worker
Created: March 18, 2026

This script runs daily to verify RegretRisk predictions 7 days after the original decision.
It queries the Duffel API for actual prices and classifies predictions as TP/FP/TN/FN.

GitHub Actions schedule: Daily at 10:00 UTC (after snapshot capture at 09:00 UTC)
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from supabase import create_client, Client

# ============================================================================
# CONFIGURATION
# ============================================================================

SUPABASE_URL = os.environ['MIZAR_SUPABASE_URL']
SUPABASE_KEY = os.environ['MIZAR_SUPABASE_ANON_KEY']
DUFFEL_TOKEN = os.environ['DUFFEL_ACCESS_TOKEN']

DUFFEL_API_BASE = 'https://api.duffel.com'
DUFFEL_HEADERS = {
    'Authorization': f'Bearer {DUFFEL_TOKEN}',
    'Duffel-Version': 'v2',
    'Content-Type': 'application/json'
}

# Prediction classification threshold
HIGH_RISK_THRESHOLD = 0.70  # RegretRisk >= 0.70 is "high risk"
PRICE_RISE_THRESHOLD = 0.10  # 10% price increase

# ============================================================================
# SUPABASE CLIENT
# ============================================================================

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_pending_decisions() -> List[Dict]:
    """
    Fetch all decisions that are pending verification and have passed t+7 days.
    
    Returns:
        List of decision records ready for verification
    """
    cutoff_date = datetime.utcnow() - timedelta(days=7)
    
    response = supabase.table('user_decisions').select('*').eq(
        'verification_status', 'pending'
    ).lte(
        'decision_timestamp', cutoff_date.isoformat()
    ).execute()
    
    return response.data


def query_duffel_price(
    origin: str,
    destination: str,
    outbound_date: str,
    return_date: str
) -> Optional[float]:
    """
    Query Duffel API for current price on the specified route.
    
    Args:
        origin: IATA code (e.g., 'MAN')
        destination: IATA code (e.g., 'BCN')
        outbound_date: ISO date string (e.g., '2026-07-15')
        return_date: ISO date string (e.g., '2026-07-22')
    
    Returns:
        Price in GBP, or None if route not available
    """
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
        
        # Get cheapest offer
        cheapest = min(offers, key=lambda x: float(x['total_amount']))
        price_gbp = float(cheapest['total_amount'])
        
        return price_gbp
        
    except Exception as e:
        print(f"Error querying Duffel for {origin}-{destination}: {e}")
        return None


def classify_prediction(
    regret_risk_score: float,
    price_shown: float,
    price_t7: float
) -> Tuple[bool, str]:
    """
    Classify prediction as TP/FP/TN/FN based on RegretRisk score vs. actual outcome.
    
    Args:
        regret_risk_score: Predicted probability (0.0-1.0)
        price_shown: Price at query time (GBP)
        price_t7: Price 7 days later (GBP)
    
    Returns:
        Tuple of (ground_truth_rose, prediction_outcome)
        - ground_truth_rose: True if price rose 10%+, False otherwise
        - prediction_outcome: 'TP', 'FP', 'TN', or 'FN'
    """
    price_change_pct = ((price_t7 - price_shown) / price_shown) * 100
    ground_truth_rose = price_change_pct >= (PRICE_RISE_THRESHOLD * 100)
    
    predicted_high_risk = regret_risk_score >= HIGH_RISK_THRESHOLD
    
    if predicted_high_risk and ground_truth_rose:
        outcome = 'TP'  # True Positive
    elif predicted_high_risk and not ground_truth_rose:
        outcome = 'FP'  # False Positive
    elif not predicted_high_risk and not ground_truth_rose:
        outcome = 'TN'  # True Negative
    else:  # not predicted_high_risk and ground_truth_rose
        outcome = 'FN'  # False Negative
    
    return ground_truth_rose, outcome


def verify_decision(decision: Dict) -> bool:
    """
    Verify a single decision by querying Duffel for t+7 price and recording outcome.
    
    Args:
        decision: Decision record from user_decisions table
    
    Returns:
        True if verification succeeded, False if failed
    """
    decision_id = decision['decision_id']
    origin = decision['origin_iata']
    destination = decision['destination_iata']
    outbound_date = decision['outbound_date']
    return_date = decision['return_date']
    price_shown = float(decision['price_shown_gbp'])
    regret_risk_score = float(decision['regret_risk_score'])
    
    print(f"Verifying decision {decision_id}: {origin}-{destination} on {outbound_date}")
    
    # Query Duffel for actual price at t+7
    price_t7 = query_duffel_price(origin, destination, outbound_date, return_date)
    
    if price_t7 is None:
        # Verification failed (route not available)
        supabase.table('outcome_verification').insert({
            'decision_id': decision_id,
            'verification_method': 'duffel_api',
            'failure_reason': 'Route not available at t+7 (sold out or discontinued)'
        }).execute()
        
        supabase.table('user_decisions').update({
            'verification_status': 'failed'
        }).eq('decision_id', decision_id).execute()
        
        print(f"  ❌ Failed: Route not available")
        return False
    
    # Calculate price change and classify prediction
    price_change_pct = ((price_t7 - price_shown) / price_shown) * 100
    ground_truth_rose, prediction_outcome = classify_prediction(
        regret_risk_score, price_shown, price_t7
    )
    
    # Insert verification result
    supabase.table('outcome_verification').insert({
        'decision_id': decision_id,
        'price_t7_gbp': price_t7,
        'price_change_pct': round(price_change_pct, 2),
        'ground_truth_rose': ground_truth_rose,
        'prediction_outcome': prediction_outcome,
        'verification_method': 'duffel_api'
    }).execute()
    
    # Update decision verification status
    supabase.table('user_decisions').update({
        'verification_status': 'verified'
    }).eq('decision_id', decision_id).execute()
    
    print(f"  ✅ Verified: Price {price_shown:.2f} → {price_t7:.2f} ({price_change_pct:+.1f}%) | {prediction_outcome}")
    return True


# ============================================================================
# MAIN VERIFICATION LOOP
# ============================================================================

def main():
    """Main execution function."""
    print("=" * 80)
    print("MIZAR Outcome Verification Worker")
    print(f"Started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    
    # Fetch pending decisions
    pending = get_pending_decisions()
    print(f"\nFound {len(pending)} pending decisions ready for verification")
    
    if not pending:
        print("✅ No decisions to verify. Exiting.")
        return 0
    
    # Verify each decision
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
    
    # Summary
    print("\n" + "=" * 80)
    print(f"Verification Complete")
    print(f"  ✅ Verified: {success_count}")
    print(f"  ❌ Failed: {failure_count}")
    print(f"  Success rate: {success_count / len(pending) * 100:.1f}%")
    print("=" * 80)
    
    # Exit with error code if success rate below 95%
    success_rate = success_count / len(pending)
    if success_rate < 0.95:
        print(f"⚠️ Warning: Success rate {success_rate * 100:.1f}% below 95% target")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
