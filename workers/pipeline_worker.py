#!/usr/bin/env python3
# TravelTxter Feeder — Schema-Safe, Header-Mapped

import os, json, random, datetime as dt, requests, gspread
from google.oauth2.service_account import Credentials

def log(m): 
    print(f"{dt.datetime.utcnow().isoformat()}Z | {m}", flush=True)

# ================= ENV =================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RAW_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")

ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "2"))
SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "2"))
MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "50"))

# ================= AUTH =================
creds = Credentials.from_service_account_info(
    json.loads(os.getenv("GCP_SA_JSON_ONE_LINE")),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(RAW_TAB)

# ================= HEADER MAP =================
headers = ws.row_values(1)
col = {h: i+1 for i, h in enumerate(headers)}

def write_row(data: dict):
    row = [""] * len(headers)
    for k, v in data.items():
        if k in col:
            row[col[k]-1] = v
    ws.append_row(row, value_input_option="USER_ENTERED")

# ================= HELPERS =================
ORIGINS = ["BRS","LGW","STN","MAN","BHX"]
DESTS = ["BCN","PMI","AMS","BUD","PRG","FNC","TFS"]

def pick_dates():
    out = dt.date.today() + dt.timedelta(days=random.randint(30, 90))
    ret = out + dt.timedelta(days=random.randint(3, 7))
    return out.isoformat(), ret.isoformat()

def duffel_search(o, d, od, rd):
    r = requests.post(
        "https://api.duffel.com/air/offer_requests",
        headers={
            "Authorization": f"Bearer {DUFFEL_API_KEY}",
            "Duffel-Version": "v2",
            "Content-Type": "application/json"
        },
        json={
            "data": {
                "slices": [
                    {"origin": o, "destination": d, "departure_date": od},
                    {"origin": d, "destination": o, "departure_date": rd}
                ],
                "passengers": [{"type": "adult"}],
                "cabin_class": "economy"
            }
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["data"]["offers"]

# ================= MAIN =================
random.shuffle(ORIGINS)
random.shuffle(DESTS)

routes = [(o,d) for o in ORIGINS for d in DESTS if o != d][:ROUTES_PER_RUN]

inserted = 0
searched = 0

for o, d in routes:
    if searched >= SEARCHES_PER_RUN or inserted >= MAX_INSERTS:
        break

    od, rd = pick_dates()
    log(f"Searching {o}->{d} {od}/{rd}")

    try:
        offers = duffel_search(o, d, od, rd)
        searched += 1
    except Exception as e:
        log(f"Duffel error {o}->{d}: {e}")
        continue

    offers = sorted(offers, key=lambda x: float(x["total_amount"]))

    for off in offers:
        if inserted >= MAX_INSERTS:
            break

        data = {
            "status": "NEW",
            "deal_id": str(abs(hash(f"{o}{d}{od}{off['total_amount']}"))),
            "origin_iata": o,
            "destination_iata": d,
            "outbound_date": od,
            "return_date": rd,
            "price_gbp": off["total_amount"],
            "currency": "GBP",
            "stops": len(off["slices"][0]["segments"]) - 1,
            "inserted_utc": dt.datetime.utcnow().isoformat() + "Z"
        }

        write_row(data)
        inserted += 1
        log(f"Inserted {o}->{d} £{off['total_amount']}")

log(f"DONE searches={searched} inserted={inserted}")
