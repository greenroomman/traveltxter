"""Golden regression: pinned to pre-Step-2 market-only verifier on main.

These expectations are literal, not generated from the future implementation.
Run: python -m unittest discover -s tests -p 'test_atlas_outcome_verify_golden.py'
"""
import importlib.util
import os
import sys
import types
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

# Import without credentials or network.
os.environ.setdefault("MIZAR_SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("MIZAR_SUPABASE_SERVICE_ROLE_KEY", "offline")
os.environ.setdefault("DUFFEL_ACCESS_TOKEN", "offline")
fake_supabase = types.ModuleType("supabase")
fake_supabase.create_client = lambda *_: MagicMock()
sys.modules.setdefault("supabase", fake_supabase)
spec = importlib.util.spec_from_file_location(
    "atlas_outcome_verify_golden", "workers/atlas_outcome_verify.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def offer(amount, currency="GBP"):
    return {"total_amount": amount, "total_currency": currency}


class MarketGoldenRegression(unittest.TestCase):
    def check_search(self, offers, expected):
        response = {"data": {"id": "orq_golden", "offers": offers}}
        with patch.object(verifier, "_duffel_post", return_value=response) as post:
            actual = verifier.cheapest_gbp_price(
                "BRS", "MAD", date(2026, 11, 20), "economy", "one_way"
            )
        self.assertEqual(actual, (expected, "orq_golden"))
        self.assertEqual(post.call_count, 1)

    def test_market_cheapest_ignores_carrier(self):
        self.check_search([offer("100"), offer("80"), offer("92")], 80.0)

    def test_market_filters_non_gbp(self):
        self.check_search([offer("1", "EUR"), offer("92", "GBP")], 92.0)

    def test_market_skips_invalid_prices(self):
        self.check_search([offer("bad"), offer(None), offer("101.25")], 101.25)

    def test_market_no_gbp(self):
        self.check_search([offer("50", "EUR")], None)

    def test_market_empty_offers(self):
        self.check_search([], None)

    def test_market_missing_response(self):
        with patch.object(verifier, "_duffel_post", return_value=None):
            self.assertEqual(
                verifier.cheapest_gbp_price("BRS", "MAD", date(2026, 11, 20)),
                (None, None),
            )

    def test_market_classification_and_threshold(self):
        self.assertEqual(verifier.RISE_THRESHOLD_PCT, 10.0)
        self.assertEqual(verifier.classify_outcome(0.70, True), "TP")
        self.assertEqual(verifier.classify_outcome(0.70, False), "FP")
        self.assertEqual(verifier.classify_outcome(0.69, False), "TN")
        self.assertEqual(verifier.classify_outcome(0.69, True), "FN")


if __name__ == "__main__":
    unittest.main()
