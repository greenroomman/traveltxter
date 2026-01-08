#!/usr/bin/env python3
# TravelTxter – Feeder v1 (Theme-Adhered Broad Net)

import os, json, math, random, datetime as dt, requests, gspread
from google.oauth2.service_account import Credentials

def log(m): print(f"{dt.datetime.utcnow().isoformat()}Z | {m}", flush=True)

# ---------- ENV ----------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")
RAW_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "2"))
SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "2"))
MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "6"))

# ---------- AUTH ----------
creds = Credentials.from_service_account_info(
    json.loads(os.getenv("GCP_SA_JSON_ONE_LINE")),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
raw = sh.worksheet(RAW_TAB)

# ---------- HELPERS ----------
def today_theme():
    try:
        ws = sh.worksheet("THEMES")
        rows = ws.get_all_records()
        dow = ["MON","TUE","WED","THU","FRI","SAT","SUN"][dt.datetime.utcnow().weekday()]
        for r in rows:
            if r.get("day") == dow:
                return r.get("theme","CITY")
    except: pass
    return "CITY"

THEME_DESTS = {
    "SNOW": ["GVA","INN","SZG","MUC","ZRH","BGY"],
    "SURF": ["FAO","AGA","FUE","ACE","TFS"],
    "SUMMER": ["PMI","BCN","LIS","OPO","NAP"],
    "CITY": ["BCN","PRG","BUD","KRK","AMS"],
    "WINTER_SUN": ["TFS","LPA","RAK","AGA","FNC"]
}

ORIGINS = ["BRS","LGW","STN","MAN","BHX"]

def pick_dates():
    out = dt.date.today() + dt.timedelta(days=random.randint(30,90))
    ret = out + dt.timedelta(days=random.randint(3,7))
    return out.isoformat(), ret.isoformat()

def duffel_search(o,d,od,rd):
    r = requests.post(
        "https://api.duffel.com/air/offer_requests",
        headers={
            "Authorization": f"Bearer {DUFFEL_API_KEY}",
            "Duffel-Version": "v2",
            "Content-Type": "application/json"
        },
        json={
            "data":{
                "slices":[
                    {"origin":o,"destination":d,"departure_date":od},
                    {"origin":d,"destination":o,"departure_date":rd}
                ],
                "passengers":[{"type":"adult"}],
                "cabin_class":"economy"
            }
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json().get("data",{}).get("offers",[])

# ---------- MAIN ----------
theme = today_theme()
log(f"Theme today: {theme}")

dests = THEME_DESTS.get(theme, THEME_DESTS["CITY"])
random.shuffle(dests)
random.shuffle(ORIGINS)

routes = []
for o in ORIGINS:
    for d in dests:
        if o!=d:
            routes.append((o,d))
routes = routes[:ROUTES_PER_RUN]

inserted = 0
searched = 0

for o,d in routes:
    if searched >= SEARCHES_PER_RUN or inserted >= MAX_INSERTS:
        break

    od, rd = pick_dates()
    log(f"Search {o}->{d} {od}/{rd}")

    try:
        offers = duffel_search(o,d,od,rd)
        searched += 1
    except Exception as e:
        log(f"Duffel error {o}->{d}: {e}")
        continue

    if not offers:
        log(f"No offers {o}->{d}")
        continue

    offers = sorted(offers, key=lambda x: float(x["total_amount"]))[:3]

    for off in offers:
        if inserted >= MAX_INSERTS: break
        price = off["total_amount"]

        raw.append_row([
            "NEW",
            str(abs(hash(f"{o}{d}{od}{price}"))),
            o, d,
            "", "", "",
            od, rd,
            price, "GBP",
            len(off["slices"][0]["segments"])-1,
            theme,
            dt.datetime.utcnow().isoformat()+"Z"
        ], value_input_option="USER_ENTERED")

        inserted += 1
        log(f"Inserted {o}->{d} £{price}")

log(f"DONE searches={searched} inserted={inserted}")
