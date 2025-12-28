import os, json
from typing import Any, Dict, List
from openai import OpenAI

from lib.sheets import get_env, get_gspread_client, now_iso

MANAGED_BY = "ROUTE_PLANNER"

DEFAULT_ORIGINS = [
    {"origin_iata":"LON","origin_city":"London"},
    {"origin_iata":"MAN","origin_city":"Manchester"},
    {"origin_iata":"BHX","origin_city":"Birmingham"},
    {"origin_iata":"BRS","origin_city":"Bristol"},
    {"origin_iata":"EDI","origin_city":"Edinburgh"},
    {"origin_iata":"GLA","origin_city":"Glasgow"},
    {"origin_iata":"NCL","origin_city":"Newcastle"},
    {"origin_iata":"LBA","origin_city":"Leeds"},
    {"origin_iata":"EMA","origin_city":"East Midlands"},
    {"origin_iata":"LPL","origin_city":"Liverpool"},
]

SYSTEM = (
    "You are a travel route planner for adventurous backpackers (18-45) 
based in the UK.\n"
    "Return ONLY valid JSON. No markdown.\n"
)

def clamp_int(x: Any, lo: int, hi: int, default: int) -> int:
    try:
        v = int(float(str(x).strip()))
        return max(lo, min(hi, v))
    except Exception:
        return default

def read_table(ws) -> List[List[str]]:
    return ws.get_all_values()

def headers_map(headers: List[str]) -> Dict[str, int]:
    # 0-based indices
    return {h: i for i, h in enumerate(headers)}

def ensure_no_dupe_headers(headers: List[str], tab_name: str) -> None:
    seen = set()
    dupes = set()
    for h in headers:
        if h in seen:
            dupes.add(h)
        seen.add(h)
    if dupes:
        raise ValueError(f"{tab_name} has duplicate headers: 
{sorted(dupes)}")

def main():
    sheet_id = get_env("SHEET_ID")
    themes_tab = os.getenv("THEMES_TAB", "THEMES").strip()
    config_tab = os.getenv("FEEDER_CONFIG_TAB", "CONFIG").strip()

    client = OpenAI(api_key=get_env("OPENAI_API_KEY"))
    model = os.getenv("OPENAI_MODEL_PLANNER", os.getenv("OPENAI_MODEL", 
"gpt-4o-mini")).strip()

    origins_raw = os.getenv("FEEDER_ORIGINS_JSON", "").strip()
    origins = DEFAULT_ORIGINS if not origins_raw else 
json.loads(origins_raw)

    sh = get_gspread_client().open_by_key(sheet_id)
    themes_ws = sh.worksheet(themes_tab)
    config_ws = sh.worksheet(config_tab)

    # ---- Read THEMES ----
    tvals = read_table(themes_ws)
    if len(tvals) < 2:
        print("No THEMES rows.")
        return
    theaders = tvals[0]
    ensure_no_dupe_headers(theaders, "THEMES")
    tix = headers_map(theaders)

    def tg(row, key, default=""):
        i = tix.get(key)
        return row[i].strip() if i is not None and i < len(row) else 
default

    themes = []
    for r in tvals[1:]:
        if not r or not any(c.strip() for c in r):
            continue
        enabled = tg(r, "enabled", "FALSE").upper() in 
("TRUE","1","YES","Y")
        if not enabled:
            continue
        themes.append({
            "theme": tg(r, "theme"),
            "priority": clamp_int(tg(r, "priority", "0"), 0, 100, 0),
            "max_price_gbp": tg(r, "max_price_gbp", "9999"),
            "trip_length_days": clamp_int(tg(r, "trip_length_days", "5"), 
2, 30, 5),
            "window_days": clamp_int(tg(r, "window_days", "56"), 14, 180, 
56),
            "step_days": clamp_int(tg(r, "step_days", "7"), 1, 30, 7),
            "days_ahead": clamp_int(tg(r, "days_ahead", "14"), 0, 180, 
14),
            "max_connections": clamp_int(tg(r, "max_connections", "1"), 0, 
2, 1),
            "cabin_class": (tg(r, "cabin_class", "economy") or 
"economy").lower(),
            "notes": tg(r, "notes", ""),
        })

    themes.sort(key=lambda x: -x["priority"])
    if not themes:
        print("No enabled themes.")
        return

    # ---- Ask OpenAI for destination hubs (single call, strict JSON) ----
    # Hard caps to keep cost + Duffel load sane
    max_dest_per_theme = 
clamp_int(os.getenv("PLANNER_MAX_DESTS_PER_THEME", "6"), 2, 12, 6)

    prompt = {
        "audience": "UK adventurous backpackers & solo travellers age 
18-45",
        "origins": [{"iata": o["origin_iata"], "city": o["origin_city"]} 
for o in origins],
        "themes": [{"theme": t["theme"], "notes": t["notes"]} for t in 
themes],
        "constraints": {
            "return_iata_hubs": True,
            "max_destinations_per_theme": max_dest_per_theme,
            "prefer": [
                "cheap routes where possible",
                "seasonal relevance (winter sun / snow)",
                "islands and backpacker gateways",
                "Europe + North Africa plus selected long-haul backpacker 
hubs",
            ],
            "avoid": ["tiny airports without consistent service", "overly 
niche towns"],
        },
        "output_schema": {
            "destinations_by_theme": {
                "THEME_NAME": [
                    {"destination_iata": "XXX", "destination_city": 
"City", "destination_country": "Country"}
                ]
            }
        }
    }

    resp = client.chat.completions.create(
        model=model,
        temperature=0.2,
        response_format={"type":"json_object"},
        messages=[
            {"role":"system", "content": SYSTEM},
            {"role":"user", "content": json.dumps(prompt)}
        ]
    )
    data = json.loads(resp.choices[0].message.content.strip())
    dbt = data.get("destinations_by_theme", {}) or {}
    if not isinstance(dbt, dict) or not dbt:
        raise ValueError(f"Planner returned no destinations_by_theme: 
{data}")

    # ---- Read CONFIG + remove old planner rows ----
    cvals = read_table(config_ws)
    if not cvals:
        raise ValueError("CONFIG missing headers row.")
    cheaders = cvals[0]
    ensure_no_dupe_headers(cheaders, "CONFIG")
    cix = headers_map(cheaders)

    if "managed_by" not in cix:
        raise ValueError("CONFIG must include a 'managed_by' column (add 
it to the far right).")

    keep_rows = [cheaders]
    for r in cvals[1:]:
        mb = r[cix["managed_by"]].strip() if cix["managed_by"] < len(r) 
else ""
        if mb != MANAGED_BY:
            keep_rows.append(r)

    # Clear CONFIG and rewrite kept rows (simple, deterministic)
    config_ws.clear()
    config_ws.append_rows(keep_rows, value_input_option="USER_ENTERED")

    # ---- Append new planner rows ----
    # CONFIG required columns (must exist already per your feeder spec)
    required = [
        "enabled","priority","origin_iata","origin_city",
        "destination_iata","destination_city","destination_country",
        
"trip_length_days","max_connections","cabin_class","max_price_gbp",
        "step_days","window_days","days_ahead","managed_by"
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
            diata = (dest.get("destination_iata","") or 
"").strip().upper()
            dcity = (dest.get("destination_city","") or "").strip()
            dctry = (dest.get("destination_country","") or "").strip()
            if not diata or len(diata) != 3:
                continue

            for o in origins:
                row = [""] * len(cheaders)
                def setv(k, v):
                    row[cix[k]] = str(v)

                setv("enabled", "TRUE")
                # Priority: theme priority + slight bias for UK mega hubs
                hub_bias = 2 if o["origin_iata"] in ("LON","MAN") else 0
                setv("priority", int(t["priority"]) + hub_bias)

                setv("origin_iata", o["origin_iata"])
                setv("origin_city", o["origin_city"])

                setv("destination_iata", diata)
                setv("destination_city", dcity or diata)
                setv("destination_country", dctry)

                setv("trip_length_days", t["trip_length_days"])
                setv("max_connections", t["max_connections"])
                setv("cabin_class", t["cabin_class"])
                setv("max_price_gbp", t["max_price_gbp"])
                setv("step_days", t["step_days"])
                setv("window_days", t["window_days"])
                setv("days_ahead", t["days_ahead"])
                setv("managed_by", MANAGED_BY)

                new_rows.append(row)

    if new_rows:
        config_ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    print(f"OK route_planner wrote {len(new_rows)} CONFIG rows at 
{now_iso()}")

if __name__ == "__main__":
    main()

