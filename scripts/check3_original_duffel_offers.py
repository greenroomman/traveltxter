"""Check 3: read-only verification of original Duffel offer requests.

No Supabase, no fresh searches, no identifiers or tokens in output.
"""
import json
import os
import urllib.error
import urllib.request
from decimal import Decimal, InvalidOperation

SAMPLES = [
 ("orq_0000BBNaF0czMpJrZAq376", "LHR-PMI", "EW", "308.71", None),
 ("orq_0000BBNaKORWbvu7g6moBk", "LHR-LIS", "TP", "312.87", "397.49"),
 ("orq_0000BBNaMMS6yyrc5G002f", "BHX-FAO", "EI", "421.18", "587.34"),
 ("orq_0000BBNaORBKsiMt79w9wn", "EDI-FCO", "BA", "410.95", "819.77"),
 ("orq_0000BBNaOxTFK1XF85t1Pk", "GLA-FCO", "BA", "648.38", "815.82"),
 ("orq_0000BBNaTX0A9vv0pRLSld", "BHX-PMI", "EW", "191.98", "464.12"),
 ("orq_0000BBNb2TO9aI6ll5ECsD", "LHR-OSL", "BA", "185.65", "185.65"),
 ("orq_0000BBNb3Vpfz29Rcyb5f1", "MAN-MXP", "U2", "89.39", "89.39"),
 ("orq_0000BBNb4fSRK330xzXLqr", "LGW-VCE", "U2", "154.89", "154.89"),
]

def amount(value):
    try:
        number = Decimal(str(value))
        return number if number.is_finite() and number >= 0 else None
    except (InvalidOperation, TypeError, ValueError):
        return None

def fmt(value):
    return None if value is None else str(value.quantize(Decimal("0.01")))

def inspect(sample, token):
    request_id, route, carrier, expected_market, expected_carrier = sample
    result = {"route": route, "carrier": carrier, "verdict": None,
              "market_min": None, "carrier_min": None}
    req = urllib.request.Request(
        "https://api.duffel.com/air/offer_requests/" + request_id,
        headers={"Authorization": "Bearer " + token,
                 "Duffel-Version": "v2", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        result["verdict"] = "NOT_FOUND" if exc.code in (404, 410) else "HTTP_ERROR_" + str(exc.code)
        return result
    except Exception:
        result["verdict"] = "RETRIEVAL_ERROR"
        return result
    data = payload.get("data") if isinstance(payload, dict) else None
    offers = data.get("offers") if isinstance(data, dict) else None
    if offers is None:
        result["verdict"] = "OFFERS_NOT_RETURNED"
        return result
    if not isinstance(offers, list):
        result["verdict"] = "INVALID_OFFERS_STRUCTURE"
        return result
    if not offers:
        result["verdict"] = "NO_OFFERS"
        return result
    market_prices, carrier_prices = [], []
    for offer in offers:
        if not isinstance(offer, dict) or offer.get("total_currency") != "GBP":
            continue
        price = amount(offer.get("total_amount"))
        if price is None:
            continue
        market_prices.append(price)
        try:
            marketing = offer["slices"][0]["segments"][0]["marketing_carrier"]["iata_code"]
        except (KeyError, IndexError, TypeError):
            marketing = None
        if marketing == carrier:
            carrier_prices.append(price)
    market = min(market_prices) if market_prices else None
    same = min(carrier_prices) if carrier_prices else None
    result["market_min"] = fmt(market)
    result["carrier_min"] = fmt(same)
    result["verdict"] = ("PASS" if result["market_min"] == expected_market
                         and result["carrier_min"] == expected_carrier else "FAIL")
    return result

def main():
    token = os.environ.get("DUFFEL_ACCESS_TOKEN")
    if not token:
        raise SystemExit("Missing Duffel credential")
    rows = [inspect(sample, token) for sample in SAMPLES]
    print(json.dumps({"check": "original_offer_request_check3", "results": rows}, indent=2))
    if any(row["verdict"] == "FAIL" for row in rows):
        raise SystemExit(1)

if __name__ == "__main__":
    main()
