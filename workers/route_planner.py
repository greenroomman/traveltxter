import os
import json
from typing import Any, Dict, List

from openai import OpenAI

from lib.sheets import get_env, get_gspread_client


MANAGED_BY = "ROUTE_PLANNER"

DEFAULT_ORIGINS = [
    {"origin_iata": "LON", "origin_city": "London"},
    {"origin_iata": "MAN", "origin_city": "Manchester"},
    {"origin_iata": "BHX", "origin_city": "Birmingham"},
    {"origin_iata": "BRS", "origin_city": "Bristol"},
    {"origin_iata": "EDI", "origin_city": "Edinburgh"},
    {"origin_iata": "GLA", "origin_city": "Glasgow"},
    {"origin_iata": "NCL", "origin_city": "Newcastle"},
    {"origin_iata": "LBA", "origin_city": "Leeds"},
    {"origin_iata": "EMA", "origin_city": "East Midlands"},
    {"origin_iata": "LPL", "origin_city": "Liverpool"},
]

SYSTEM = "You are a route planner for adventurous backpackers (18-45) based in the UK. Return ONLY valid JSON. No markdown."


def clamp_int(x: Any, lo: int, hi: int, default: int) -> int:
    try:
        v = int(float(str(x).strip()))
        return max(lo, min(hi, v))
    except Exception:
        return default


def validate_headers(tab: str, headers: List[str]) -> None:
    seen = set()
    dupes = set()
    for h in headers:
        if h in seen:
            dupes.add(h)
        seen.add(h)
    if dupes:
        raise ValueError(f"{tab} has duplicate headers: {sorted(dupes)}")


def main() -> None:
    sheet_id = get_env("SHEET_ID")
    themes_tab = os.getenv("THEMES_TAB", "THEMES").strip()
    config_tab = os.getenv("FEEDER_CONFIG_TAB", "CONFIG").strip()

    origins_raw = os.getenv("FEEDER_ORIGINS_JSON", "").strip()
    origins = DEFAULT_ORIGINS if not origins_raw else json.loads(origins_raw)

    model = os.getenv("OPENAI_MODEL_PLANNER", "gpt-4o-mini").strip()
    max_dest_per_theme = clamp_int(os.getenv("PLANNER_MAX_DESTS_PER_THEME", "6"), 2, 12, 6)

    sh = get_gspread_client().open_by_key(sheet_id)
    themes_ws = sh.worksheet(themes_tab)
    config_ws = sh.worksheet(config_tab)

    tvals = themes_ws.get_all_values()
    if len(tvals) < 2:
        print("No THEMES.")
        return
    theaders = tvals[0]
    validate_headers("THEMES", theaders)
    tix = {h: i for i, h in enumerate(theaders)}

    def tg(row: List[str], key: str, default="") -> str:
        i = tix.get(key)
        return row[i].strip() if i is not None and i < len(row) else default

    themes = []
    for r in tvals[1:]:
        if not r or not any(c.strip() for c in r):
            continue
        if tg(r, "enabled", "FALSE").upper() not in ("TRUE", "1", "YES", "Y"):
            continue
        themes.append({
            "theme": tg(r, "theme"),
            "priority": clamp_int(tg(r, "priority", "0"), 0, 100, 0),
            "max_price_gbp": tg(r, "max_price_gbp", "9999"),
            "trip_length_days": clamp_int(tg(r, "trip_length_days", "5"), 2, 30, 5),
            "window_days": clamp_int(tg(r, "window_days", "56"), 14, 180, 56),
            "step_days": clamp_int(tg(r, "step_days", "7"), 1, 30, 7),
            "days_ahead": clamp_int(tg(r, "days_ahead", "14"), 0, 180, 14),
            "max_connections": clamp_int(tg(r, "max_connections", "1"), 0, 2, 1),
            "cabin_class": (tg(r, "cabin_class", "economy") or "economy").lower(),
            "notes": tg(r, "notes", ""),
        })

    themes.sort(key=lambda x: -x["priority"])
    if not themes:
        print("No enabled themes.")
        return

    prompt = {
        "audience": "UK adventurous backpackers & solo travellers age 18-45",
        "themes": [{"theme": t["theme"], "notes": t["notes"]} for t in themes],
        "constraints": {
            "return_iata_hubs": True,
            "max_destinations_per_theme": max_dest_per_theme,
        },
        "output_schema": {
            "destinations_by_theme": {
                "THEME_NAME": [
                    {"destination_iata": "XXX", "destination_city": "City", "destination_country": "Country"}
                ]
            }
        }
    }

    client = OpenAI(api_key=get_env("OPENAI_API_KEY"))
    resp = client.chat.completions.create(
        model=model,
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": json.dumps(prompt)},
        ],
    )
    data = json.loads(resp.choices[0].message.content.strip())
    dbt = data.get("destinations_by_theme") or {}
    if not isinstance(dbt, dict) or not dbt:
        raise ValueError(f"No destinations_by_theme returned: {data}")

    cvals = config_ws.get_all_values()
    if not cvals:
        raise ValueError("CONFIG missing headers row.")
    cheaders = cvals[0]
    validate_headers("CONFIG", cheaders)
    cix = {h: i for i, h in enumerate(cheaders)}

    if "managed_by" not in cix:
        raise ValueError("CONFIG must include 'managed_by' column.")

    # Keep non-planner rows
    kept = [cheaders]
    for r in cvals[1:]:
        mb = r[cix["managed_by"]].strip() if cix["managed_by"] < len(r) else ""
        if mb != MANAGED_BY:
            kept.append(r)

    config_ws.clear()
    config_ws.append_rows(kept, value_input_option="USER_ENTERED")

    required = [
        "enabled", "priority", "origin_iata", "origin_city",
        "destination_iata", "destination_city", "destination_country",
        "trip_length_days", "max_connections", "cabin_class", "max_price_gbp",
        "step_days", "window_days", "days_ahead", "managed_by",
    ]
    missing = [h for h in required if h not in cix]
    if missing:
        raise ValueError(f"CONFIG missing columns: {missing}")

    new_rows = []
    for t in themes:
        theme_name = t["theme"].strip()
        dests = dbt.get(theme_name) or dbt.get(theme_name.upper()) or []
        if not isinstance(dests, list):
            continue
        dests = dests[:max_dest_per_theme]

        for dest in dests:
            diata = (dest.get("destination_iata") or "").strip().upper()
            dcity = (dest.get("destination_city") or "").strip()
            dctry = (dest.get("destination_country") or "").strip()
            if len(diata) != 3:
                continue

            for o in origins:
                row = [""] * len(cheaders)
                row[cix["enabled"]] = "TRUE"
                row[cix["priority"]] = str(int(t["priority"]) + (2 if o["origin_iata"] in ("LON", "MAN") else 0))
                row[cix["origin_iata"]] = o["origin_iata"]
                row[cix["origin_city"]] = o["origin_city"]
                row[cix["destination_iata"]] = diata
                row[cix["destination_city"]] = dcity or diata
                row[cix["destination_country"]] = dctry
                row[cix["trip_length_days"]] = str(t["trip_length_days"])
                row[cix["max_connections"]] = str(t["max_connections"])
                row[cix["cabin_class"]] = t["cabin_class"]
                row[cix["max_price_gbp"]] = str(t["max_price_gbp"])
                row[cix["step_days"]] = str(t["step_days"])
                row[cix["window_days"]] = str(t["window_days"])
                row[cix["days_ahead"]] = str(t["days_ahead"])
                row[cix["managed_by"]] = MANAGED_BY
                new_rows.append(row)

    if new_rows:
        config_ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    print(f"Route planner wrote {len(new_rows)} CONFIG rows.")


if __name__ == "__main__":
    main()
