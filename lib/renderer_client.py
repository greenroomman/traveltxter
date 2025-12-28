import requests

RENDER_URL = "https://greenroomman.pythonanywhere.com/render"

def render_deal_png(deal: dict, timeout_seconds: int = 60) -> str:
    r = requests.post(RENDER_URL, json=deal, timeout=timeout_seconds)
    r.raise_for_status()
    data = r.json()

    if not data.get("ok") or not data.get("graphic_url"):
        raise RuntimeError(f"Render failed: {data}")

    return data["graphic_url"]
