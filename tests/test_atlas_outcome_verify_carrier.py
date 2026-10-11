"""Step 2 contract tests. Expected RED before verifier implementation.

Run: python3 -m unittest discover -s tests -p 'test_atlas_outcome_verify_carrier.py' -v
"""
import importlib.util
import os
import sys
import types
import unittest
from unittest.mock import MagicMock

os.environ.setdefault("MIZAR_SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("MIZAR_SUPABASE_SERVICE_ROLE_KEY", "offline")
os.environ.setdefault("DUFFEL_ACCESS_TOKEN", "offline")
fake_supabase = types.ModuleType("supabase")
fake_supabase.create_client = lambda *_: MagicMock()
sys.modules.setdefault("supabase", fake_supabase)
spec = importlib.util.spec_from_file_location("carrier_contract_worker", "workers/atlas_outcome_verify.py")
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def offer(amount, currency="GBP", marketing="U2", owner="BA", operating="IB"):
    return {
        "total_amount": amount,
        "total_currency": currency,
        "owner": {"iata_code": owner},
        "slices": [{"segments": [{
            "marketing_carrier": {"iata_code": marketing},
            "operating_carrier": {"iata_code": operating},
        }]}],
    }


class CarrierContractTests(unittest.TestCase):
    def measure(self, offers, decision_carrier="U2", shown=80):
        return verifier.measure_same_carrier(
            offers, decision_carrier, shown
        )

    def test_first_segment_marketing_only(self):
        result = self.measure([
            offer("80", marketing="VY", owner="U2"),
            offer("100", marketing="U2", owner="IB", operating="BA"),
            offer("92", marketing="U2", owner="BA", operating="IB"),
        ])
        self.assertEqual(result, ("U2", 92.0, True, "matched"))

    def test_equal_price_order_invariant(self):
        a = [offer("92.00"), offer("92.0"), offer("100")]
        self.assertEqual(self.measure(a), self.measure(list(reversed(a))))

    def test_legacy_null_carrier(self):
        self.assertEqual(self.measure([offer("92")], None),
                         (None, None, None, "decision_carrier_unknown"))

    def test_malformed_gbp_offer_invalidates_carrier_result(self):
        self.assertEqual(
            self.measure([offer("92", marketing="BA"),
                          {"total_amount": "80", "total_currency": "GBP", "slices": []}]),
            (None, None, None, "carrier_extraction_failed"),
        )

    def test_non_gbp_excluded_from_carrier_path(self):
        self.assertEqual(
            self.measure([offer("1", "EUR", "U2"), offer("92", "GBP", "BA")]),
            (None, None, None, "not_present_at_t7"),
        )

    def test_carrier_failure_does_not_block_market_write(self):
        result = verifier.safe_measure_same_carrier(
            [offer("80")], "U2", 80,
            measurement_fn=lambda *_: (_ for _ in ()).throw(RuntimeError("injected"))
        )
        self.assertEqual(result, (None, None, None, "carrier_extraction_failed"))


if __name__ == "__main__":
    unittest.main()
