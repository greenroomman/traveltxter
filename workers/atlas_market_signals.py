import os
import sys
import requests
from datetime import date
from supabase import create_client

def env_str(key):
    val = os.environ.get(key, "").strip()
    if not val:
        print(f"ERROR: {key} not set")
        sys.exit(1)
    return val

def fetch_jet_fuel_price():
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not api_key:
        print("WARNING: EIA_API_KEY not set, skipping jet fuel price")
        return None
    try:
        url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        params = {
            "api_key": api_key, "frequency": "weekly", "data[0]": "value",
            "facets[product][]": "EPD2F", "sort[0][column]": "period",
            "sort[0][direction]": "desc", "offset": 0, "length": 1
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        value = r.json()["response"]["data"][0]["value"]
        print(f"Jet fuel price: ${value}/gal")
        return float(value)
    except Exception as ex:
        print(f"WARNING: Failed to fetch jet fuel price: {ex}")
        return None

def fetch_gbp_fx_rates():
    try:
        r = requests.get(
            "https://api.frankfurter.app/latest",
            params={"from": "GBP", "to": "USD,EUR"},
            timeout=10
        )
        r.raise_for_status()
        rates = r.json()["rates"]
        gbp_usd = float(rates["USD"])
        gbp_eur = float(rates["EUR"])
        print(f"GBP/USD: {gbp_usd}, GBP/EUR: {gbp_eur}")
        return gbp_usd, gbp_eur
    except Exception as ex:
        print(f"WARNING: Failed to fetch FX rates: {ex}")
        return None, None

def main():
    supabase_url = env_str("SUPABASE_URL")
    supabase_key = env_str("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)

    today = date.today().isoformat()

    existing = supabase.table("daily_market_signals").select("signal_date").eq("signal_date", today).execute()
    if existing.data:
        print(f"Signal already recorded for {today}, skipping")
        return

    jet_fuel = fetch_jet_fuel_price()
    gbp_usd, gbp_eur = fetch_gbp_fx_rates()

    if jet_fuel is None and gbp_usd is None:
        print("ERROR: Both data sources failed, nothing to record")
        sys.exit(1)

    record = {
        "signal_date": today,
        "jet_fuel_usd_gal": jet_fuel,
        "gbp_usd_rate": gbp_usd,
        "gbp_eur_rate": gbp_eur,
        "notes": "auto"
    }

    result = supabase.table("daily_market_signals").insert(record).execute()
    if result.data:
        print(f"Market signal recorded for {today}")
    else:
        print(f"ERROR: Insert failed: {result}")
        sys.exit(1)

if __name__ == "__main__":
    main()
