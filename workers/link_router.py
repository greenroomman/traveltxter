# workers/link_router.py
import os
import requests
from utils.sheets import get_worksheet, batch_update_rows

RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
DUFFEL_API_KEY = os.environ.get("DUFFEL_API_KEY")

DUFFEL_LINKS_ENDPOINT = "https://api.duffel.com/air/links"

HEADERS = {
    "Authorization": f"Bearer {DUFFEL_API_KEY}",
    "Duffel-Version": "v2",
    "Content-Type": "application/json",
}

def create_duffel_demo_link(row):
    """
    Creates a Duffel Links session for demo / VIP booking intent.
    """
    payload = {
        "data": {
            "type": "air_links",
            "slices": [
                {
                    "origin": row["origin_iata"],
                    "destination": row["dest_iata"],
                    "departure_date": row["out_date"],
                },
                {
                    "origin": row["dest_iata"],
                    "destination": row["origin_iata"],
                    "departure_date": row["in_date"],
                }
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }

    r = requests.post(
        DUFFEL_LINKS_ENDPOINT,
        headers=HEADERS,
        json=payload,
        timeout=15,
    )

    if r.status_code != 201:
        return None

    return r.json()["data"]["url"]

def main():
    ws = get_worksheet(RAW_DEALS_TAB)
    rows = ws.get_all_records()

    updates = []

    for idx, row in enumerate(rows, start=2):
        if row.get("status") != "READY_TO_POST":
            continue

        link = create_duffel_demo_link(row)

        if not link:
            continue

        updates.append((
            idx,
            {
                "booking_link_vip": link,
            }
        ))

    if updates:
        batch_update_rows(ws, updates)

    print(f"booking_link_vip populated for {len(updates)} rows.")

if __name__ == "__main__":
    main()
