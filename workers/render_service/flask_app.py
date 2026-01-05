from __future__ import annotations

import os
import re
import json
import hashlib
import datetime as dt
from flask import Flask, request, jsonify
from PIL import Image, ImageDraw, ImageFont

app = Flask(__name__)

# ============================================================
# VERSION MARKER (PROVES WHICH CODE IS LIVE)
# ============================================================
RENDER_SERVICE_VERSION = "V4.5x_RENDER_LOCK_2026-01-05_1500Z"

# ============================================================
# CONFIG
# ============================================================
BASE_URL = os.environ.get("BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_RENDER_DIR = os.path.join(BASE_DIR, "static", "renders")
os.makedirs(STATIC_RENDER_DIR, exist_ok=True)

# Optional: a TTF font path (recommended). If missing, default font is used.
FONT_PATH = os.environ.get("RENDER_FONT_PATH", "").strip()


# ============================================================
# Helpers
# ============================================================
def _clean(s: str) -> str:
    return (s or "").strip()

def _safe_slug(text: str) -> str:
    text = _clean(text).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text or "deal"

def _payload_hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:10]

def _validate_payload(p: dict) -> tuple[bool, str]:
    required = ["TO", "FROM", "OUT", "IN", "PRICE"]
    missing = [k for k in required if not _clean(p.get(k, ""))]
    if missing:
        return False, f"Missing required field(s): {', '.join(missing)}"
    return True, ""

def _load_fonts():
    if FONT_PATH and os.path.exists(FONT_PATH):
        return (
            ImageFont.truetype(FONT_PATH, 72),
            ImageFont.truetype(FONT_PATH, 64),
        )
    # Fallback
    return (ImageFont.load_default(), ImageFont.load_default())

def _render_image(payload: dict, out_path: str) -> None:
    """
    LOCKED contract (image text ONLY):
      TO: <City>
      FROM: <City>
      OUT: ddmmyy
      IN: ddmmyy
      PRICE: £xxx
    """
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), "white")
    draw = ImageDraw.Draw(img)

    font_big, font_med = _load_fonts()

    lines = [
        f"TO: {payload['TO']}",
        f"FROM: {payload['FROM']}",
        f"OUT: {payload['OUT']}",
        f"IN: {payload['IN']}",
        f"PRICE: {payload['PRICE']}",
    ]

    y = 220
    for i, line in enumerate(lines):
        font = font_big if i == 0 else font_med
        bbox = draw.textbbox((0, 0), line, font=font)
        tw = bbox[2] - bbox[0]
        x = (W - tw) // 2
        draw.text((x, y), line, fill="black", font=font)
        y += 130

    img.save(out_path, format="PNG")


# ============================================================
# Debug/Health
# ============================================================
@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "version": RENDER_SERVICE_VERSION,
        "ts": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    })

@app.get("/debug/routes")
def debug_routes():
    routes = []
    for r in app.url_map.iter_rules():
        routes.append({"rule": str(r), "methods": sorted([m for m in r.methods if m not in ("HEAD", "OPTIONS")])})
    return jsonify({"version": RENDER_SERVICE_VERSION, "routes": routes})


# ============================================================
# Render handler (used by BOTH /api/render and /render)
# ============================================================
def _handle_render():
    payload = request.get_json(silent=True) or {}
    ok, err = _validate_payload(payload)
    if not ok:
        return jsonify({"success": False, "error": err, "version": RENDER_SERVICE_VERSION}), 400

    # No deal_id required. Always generate unique filename.
    to_slug = _safe_slug(payload.get("TO", "deal"))
    from_slug = _safe_slug(payload.get("FROM", "from"))
    h = _payload_hash(payload)
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{from_slug}_to_{to_slug}_{ts}_{h}.png"
    out_path = os.path.join(STATIC_RENDER_DIR, fname)

    try:
        _render_image(payload, out_path)
    except Exception as e:
        return jsonify({"success": False, "error": f"Render failed: {e}", "version": RENDER_SERVICE_VERSION}), 500

    image_url = f"{BASE_URL}/static/renders/{fname}"

    # Return BOTH keys for backward compatibility
    return jsonify({
        "success": True,
        "image_url": image_url,
        "graphic_url": image_url,
        "version": RENDER_SERVICE_VERSION
    }), 200


# NEW endpoint (current)
@app.post("/api/render")
def api_render():
    return _handle_render()

# OLD endpoint (backwards compatible)
@app.post("/render")
def legacy_render():
    return _handle_render()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
