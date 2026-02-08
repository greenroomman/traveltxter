#!/usr/bin/env python3
"""
Render Client worker for TravelTxter V5 (Pipeline Step 4)

Purpose:
- Generate graphics for deals ready to post

Reads:
- RAW_DEALS (deals with status READY_TO_POST/READY_TO_PUBLISH and blank graphic_url)

Writes (RAW_DEALS only):
- graphic_url

Contract:
- Only renders deals with status = READY_TO_POST or READY_TO_PUBLISH
- Never modifies status, score, or timestamps
- Idempotent: if graphic_url exists, skip
"""

import os
import re
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------ env helpers ------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


# ------------------ robust SA JSON parsing ------------------

def _repair_private_key_newlines(raw: str) -> str:
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    pk_fixed = pk_fixed.replace("\t", "\\t")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end():]

def load_sa_info() -> Dict[str, Any]:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    try:
        return json.loads(raw)
    except Exception:
        pass
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass
    repaired = _repair_private_key_newlines(raw)
    try:
        return json.loads(repaired)
    except Exception:
        pass
    repaired2 = _repair_private_key_newlines(raw).replace("\\n", "\n")
    return json.loads(repaired2)

def gspread_client():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ------------------ sheet helpers ------------------

def idx_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}

def row_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    return {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}

def get_cell(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


# ------------------ date helpers ------------------

def parse_date(s: str) -> Optional[dt.date]:
    """Parse YYYY-MM-DD or similar"""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s).date()
    except Exception:
        pass
    # Try YYYY-MM-DD
    try:
        parts = s.split("-")
        if len(parts) == 3:
            return dt.date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        pass
    return None

def format_ddmmyy(d: dt.date) -> str:
    """Format as ddmmyy e.g. 120326"""
    return d.strftime("%d%m%y")


# ------------------ render API ------------------

def call_render_api(
    base_url: str,
    to_city: str,
    from_city: str,
    out_date: str,
    in_date: str,
    price: str,
    layout: str,
    theme: str,
) -> str:
    """
    Call PythonAnywhere render API
    Returns graphic_url
    """
    endpoint = f"{base_url}/api/render"
    
    payload = {
        "TO": to_city,
        "FROM": from_city,
        "OUT": out_date,
        "IN": in_date,
        "PRICE": price,
        "layout": layout,
        "theme": theme,
    }
    
    response = requests.post(endpoint, json=payload, timeout=30)
    response.raise_for_status()
    result = response.json()
    
    if not result.get("ok"):
        raise RuntimeError(f"Render failed: {result}")
    
    graphic_url = result.get("graphic_url", "").strip()
    if not graphic_url:
        raise RuntimeError(f"No graphic_url in response: {result}")
    
    return graphic_url


# ------------------ main ------------------

def main() -> int:
    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    RENDER_BASE = env("RENDER_BASE_URL", "https://greenroomman.pythonanywhere.com")
    
    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))
    ws = sh.worksheet(RAW_TAB)
    
    values = ws.get_all_values()
    headers = values[0]
    h = idx_map(headers)
    
    # Required columns
    required = [
        "status", "deal_id", "origin_city", "destination_city",
        "outbound_date", "return_date", "price_gbp", "publish_window", "theme"
    ]
    for k in required:
        if k not in h:
            raise RuntimeError(f"RAW_DEALS missing required header: {k}")
    
    if "graphic_url" not in h:
        raise RuntimeError("RAW_DEALS missing graphic_url column")
    
    # Find deals needing rendering
    to_render = []
    for i, row in enumerate(values[1:], start=2):
        rd = row_dict(headers, row)
        
        status = get_cell(rd, "status")
        graphic_url = get_cell(rd, "graphic_url")
        
        # Only render if status is READY_TO_POST or READY_TO_PUBLISH and no graphic yet
        if status not in ("READY_TO_POST", "READY_TO_PUBLISH"):
            continue
        
        if graphic_url:
            continue
        
        to_render.append((i, rd))
    
    if not to_render:
        print("✅ No deals need rendering. All READY_TO_POST/READY_TO_PUBLISH deals already have graphics.")
        return 0
    
    print(f"📸 Render Client — Found {len(to_render)} deals needing graphics")
    print("=" * 60)
    
    rendered_count = 0
    error_count = 0
    
    for row_i, rd in to_render:
        deal_id = get_cell(rd, "deal_id")
        
        try:
            # Extract data
            to_city = get_cell(rd, "destination_city")
            from_city = get_cell(rd, "origin_city")
            
            out_date_raw = get_cell(rd, "outbound_date")
            in_date_raw = get_cell(rd, "return_date")
            
            out_date_obj = parse_date(out_date_raw)
            in_date_obj = parse_date(in_date_raw)
            
            if not out_date_obj or not in_date_obj:
                print(f"⚠️  row={row_i} deal_id={deal_id} — Invalid dates, skipping")
                error_count += 1
                continue
            
            out_date = format_ddmmyy(out_date_obj)
            in_date = format_ddmmyy(in_date_obj)
            
            price_raw = get_cell(rd, "price_gbp")
            try:
                price_val = int(float(price_raw))
                price = f"£{price_val}"
            except Exception:
                print(f"⚠️  row={row_i} deal_id={deal_id} — Invalid price, skipping")
                error_count += 1
                continue
            
            # Determine layout from publish_window
            publish_window = get_cell(rd, "publish_window").upper()
            if "AM" in publish_window:
                layout = "AM"
            elif "PM" in publish_window:
                layout = "PM"
            else:
                layout = "AM"  # Default
            
            theme = get_cell(rd, "theme") or "adventure"
            
            # Call render API
            graphic_url = call_render_api(
                RENDER_BASE, to_city, from_city, out_date, in_date, price, layout, theme
            )
            
            # Write back to sheet
            ws.update_cell(row_i, h["graphic_url"] + 1, graphic_url)
            
            print(f"✅ row={row_i} deal_id={deal_id} layout={layout} theme={theme} → {graphic_url[:80]}...")
            rendered_count += 1
            
        except Exception as e:
            print(f"❌ row={row_i} deal_id={deal_id} — Render failed: {e}")
            error_count += 1
            continue
    
    print("=" * 60)
    print(f"📊 Rendered: {rendered_count} | Errors: {error_count}")
    
    if error_count > 0 and rendered_count == 0:
        return 1  # All failed
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
