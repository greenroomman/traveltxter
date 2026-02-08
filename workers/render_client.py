#!/usr/bin/env python3
"""
Render client test script for TravelTxter V5
Tests PythonAnywhere render endpoint
"""

import requests
import sys
import json


BASE = "https://greenroomman.pythonanywhere.com"
ENDPOINT = f"{BASE}/api/render"


def test_health():
    """Test health endpoint"""
    print("Testing health...")
    try:
        response = requests.get(f"{BASE}/api/health", timeout=10)
        print(response.json())
    except Exception:
        response = requests.get(f"{BASE}/health", timeout=10)
        print(response.json())
    print()
    print("-" * 40)


def test_render(layout, theme, from_city, to_city, out_date, in_date, price):
    """
    Test render endpoint
    
    Args:
        layout: "AM" or "PM"
        theme: e.g. "northern_lights"
        from_city: e.g. "London"
        to_city: e.g. "Keflavik"
        out_date: ddmmyy format e.g. "120326"
        in_date: ddmmyy format e.g. "180326"
        price: e.g. "£159"
    """
    print(f"Rendering layout={layout} theme={theme} {from_city} -> {to_city} {out_date}/{in_date} {price}")
    
    payload = {
        "TO": to_city,
        "FROM": from_city,
        "OUT": out_date,
        "IN": in_date,
        "PRICE": price,
        "layout": layout,
        "theme": theme
    }
    
    try:
        response = requests.post(ENDPOINT, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        print(json.dumps(result, indent=4))
        
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    
    print()
    print("-" * 40)


if __name__ == "__main__":
    test_health()
    test_render("AM", "northern_lights", "London", "Keflavik", "120326", "180326", "£159")
    test_render("PM", "northern_lights", "London", "Keflavik", "120326", "180326", "£159")
    print("\n✅ All render tests passed")
