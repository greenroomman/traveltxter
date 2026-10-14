"""One-search Duffel offer retention probe. Public fare counts only."""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, timedelta

BASE = "https://api.duffel.com"
CHECKPOINTS = (2, 10, 20, 30, 45, 60, 90, 120)

def call(method, path, token, payload=None):
    headers = {"Authorization": "Bearer " + token, "Duffel-Version": "v2",
               "Accept": "application/json", "Content-Type": "application/json"}
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(BASE + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response), None
    except urllib.error.HTTPError as exc:
        return None, "HTTP_ERROR_" + str(exc.code)
    except Exception:
        return None, "RETRIEVAL_ERROR"

def offer_count(payload):
    data = payload.get("data") if isinstance(payload, dict) else None
    offers = data.get("offers") if isinstance(data, dict) else None
    if not isinstance(offers, list):
        return None
    return len(offers)

def main():
    token = os.environ.get("DUFFEL_ACCESS_TOKEN")
    if not token:
        raise SystemExit("Missing Duffel credential")
    departure = (date.today() + timedelta(days=45)).isoformat()
    request = {"data": {"slices": [{"origin": "BHX", "destination": "MAD",
                 "departure_date": departure}], "passengers": [{"type": "adult"}],
                 "cabin_class": "economy"}}
    start = time.monotonic()
    created, error = call("POST", "/air/offer_requests?return_offers=true", token, request)
    if error:
        print(json.dumps({"phase": "create", "verdict": error}))
        raise SystemExit(1)
    data = created.get("data") if isinstance(created, dict) else None
    request_id = data.get("id") if isinstance(data, dict) else None
    if not isinstance(request_id, str) or not request_id.startswith("orq_"):
        print(json.dumps({"phase": "create", "verdict": "INVALID_RESPONSE"}))
        raise SystemExit(1)
    print(json.dumps({"minute": 0, "offers_present": (offer_count(created) or 0) > 0,
                      "offer_count": offer_count(created), "source": "POST"}), flush=True)
    for minute in CHECKPOINTS:
        wait = minute * 60 - (time.monotonic() - start)
        if wait > 0:
            time.sleep(wait)
        response, error = call("GET", "/air/offer_requests/" + request_id, token)
        count = None if error else offer_count(response)
        verdict = error or ("OFFERS_NOT_RETURNED" if count is None else
                            "OFFERS_PRESENT" if count > 0 else "NO_OFFERS")
        print(json.dumps({"minute": minute, "verdict": verdict,
                          "offers_present": count > 0 if count is not None else None,
                          "offer_count": count}), flush=True)
    print(json.dumps({"probe_complete": True, "checkpoints": len(CHECKPOINTS)}))

if __name__ == "__main__":
    main()
