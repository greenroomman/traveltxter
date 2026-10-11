"""Real Duffel fixture and persistence/integration tests for Step 2."""
import copy
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from test_atlas_outcome_verify_carrier import verifier

FIXTURE = Path(__file__).parent / "fixtures" / "duffel_2026_10_08_sanitised.json"
DATA = json.loads(FIXTURE.read_text())
OFFERS = [o for search in DATA["searches"] for o in search["offers"]]
STATUSES = {"matched", "not_present_at_t7", "decision_carrier_unknown", "carrier_extraction_failed"}


class DbError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class RecordingDb:
    def __init__(self, failures=()):
        self.failures = list(failures)
        self.inserts = []
        self.updates = []
    def table(self, name):
        assert name in ("outcome_verification", "user_decisions")
        self.name = name
        return self
    def insert(self, row):
        self.inserts.append(copy.deepcopy(row))
        return self
    def update(self, row):
        self.updates.append(copy.deepcopy(row))
        return self
    def eq(self, *_):
        return self
    def execute(self):
        if self.name == "outcome_verification" and self.failures:
            raise DbError(self.failures.pop(0))
        return MagicMock(data=[])


class RealFixtureTests(unittest.TestCase):
    def test_fixture_allowlist_and_provenance(self):
        self.assertEqual(len(OFFERS), 48)
        self.assertIn("113218996680", DATA["provenance"]["source"])
        for o in OFFERS:
            self.assertEqual(set(o), {"total_amount", "total_currency", "owner", "slices"})
            self.assertIsInstance(o["total_amount"], str)

    def test_extraction_equivalence_every_real_offer(self):
        for o in OFFERS:
            expected = o["slices"][0]["segments"][0]["marketing_carrier"]["iata_code"]
            with self.subTest(carrier=expected, amount=o["total_amount"]):
                actual = verifier.measure_same_carrier([o], expected, 100)
                self.assertEqual(actual[0], expected)
                self.assertEqual(actual[3], "matched")

    def test_real_fixture_owner_operating_and_return_ignored(self):
        for o in OFFERS:
            first = o["slices"][0]["segments"][0]["marketing_carrier"]["iata_code"]
            self.assertEqual(verifier.measure_same_carrier([o], first, 100)[3], "matched")
            for wrong in set([o["owner"]["iata_code"],
                              o["slices"][0]["segments"][0]["operating_carrier"]["iata_code"],
                              o["slices"][-1]["segments"][0]["marketing_carrier"]["iata_code"]]) - {first}:
                self.assertEqual(verifier.measure_same_carrier([o], wrong, 100)[3], "not_present_at_t7")

    def test_modified_real_structure_non_gbp_not_captured(self):
        modified = copy.deepcopy(OFFERS[0])
        modified["total_currency"] = "EUR"
        first = modified["slices"][0]["segments"][0]["marketing_carrier"]["iata_code"]
        self.assertEqual(verifier.measure_same_carrier([modified], first, 100)[3], "not_present_at_t7")

    def test_malformed_extraction_fails_entire_search(self):
        for broken in ({"total_currency":"GBP","total_amount":"90","slices":[]},
                       {"total_currency":"GBP","total_amount":"90","slices":[{"segments":[{"marketing_carrier":{"iata_code":None}}]}]}):
            self.assertEqual(verifier.measure_same_carrier([OFFERS[0], broken], "ZZ", 100)[3],
                             "carrier_extraction_failed")

    def test_insert_only_and_exact_database_constants(self):
        self.assertEqual(verifier.CARRIER_MATCH_STATUSES, STATUSES)
        self.assertEqual(verifier.SAME_CARRIER_METHOD, "carrier_consistent_v1")
        db = RecordingDb()
        with patch.object(verifier, "supabase", db):
            verifier.write_verification("test", 110, 100, 0.75, None, same_carrier=("BA", 110, True, "matched"))
        self.assertEqual(len(db.inserts), 1)
        self.assertEqual(db.inserts[0]["same_carrier_match_status"], "matched")
        self.assertEqual(db.inserts[0]["same_carrier_verification_method"], "carrier_consistent_v1")
        self.assertEqual(db.inserts[0]["verification_method"], "duffel_api")
        self.assertEqual(len(db.updates), 1)

    def test_integration_exception_persists_market_and_failed_carrier(self):
        db = RecordingDb()
        measurement = verifier.safe_measure_same_carrier(OFFERS, "BA", 100,
            measurement_fn=lambda *_: 1 / 0)
        with patch.object(verifier, "supabase", db):
            verifier.write_verification("test", 115, 100, 0.75, None, same_carrier=measurement)
        self.assertEqual(len(db.inserts), 1)
        self.assertEqual(db.inserts[0]["price_t7_gbp"], 115)
        self.assertEqual(db.inserts[0]["same_carrier_match_status"], "carrier_extraction_failed")
        self.assertEqual(db.inserts[0]["same_carrier_verification_method"], "carrier_consistent_v1")

    def test_fallback_only_for_definitive_codes(self):
        for code in ("23514", "PGRST204", "23505", "TIMEOUT", "OTHER"):
            with self.subTest(code=code):
                db = RecordingDb([code])
                with patch.object(verifier, "supabase", db):
                    verifier.write_verification("test", 110, 100, 0.75, None,
                        same_carrier=("BA", 110, True, "matched"))
                expected = 2 if code in ("23514", "PGRST204") else 1
                self.assertEqual(len(db.inserts), expected)
                if expected == 2:
                    self.assertNotIn("same_carrier_match_status", db.inserts[1])
                self.assertEqual(len(db.updates), 1)

    def test_single_duffel_search_for_both_measurements(self):
        decision = dict(decision_id="test", origin_iata="LHR", destination_iata="MAD",
            outbound_date="2026-12-01", return_date="2026-12-08", trip_type="return",
            cabin_class="economy", price_shown_gbp=100, regret_risk_score=.75,
            decision_carrier_iata="IB")
        mock_response = {"data":{"id":"synthetic-id","offers":OFFERS}}
        with patch.object(verifier, "fetch_pending_decisions", return_value=[decision]), \
             patch.object(verifier, "already_verified", return_value=False), \
             patch.object(verifier, "_duffel_post", return_value=mock_response) as post, \
             patch.object(verifier, "write_verification") as write, \
             patch.object(verifier.time, "sleep"):
            verifier.run()
        self.assertEqual(post.call_count, 1)
        self.assertEqual(write.call_count, 1)
        self.assertEqual(write.call_args.args[1], min(float(o["total_amount"]) for o in OFFERS))


if __name__ == "__main__":
    unittest.main()
