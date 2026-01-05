from __future__ import annotations

import os
import re
import json
import hashlib
from datetime import datetime

from flask import Flask, request, jsonify

# Pillow exists on PythonAnywhere (NOT on GitHub Actions)
from PIL import Image, ImageDraw, ImageFont

app = Flask(__name__)

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
BASE_URL = os.environ.get("BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_RENDER_DIR = os.path.join(BASE_DIR, "static", "renders")
os.makedirs(STATIC_RENDER_DIR, exist_ok=True)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# Deterministic renderer (NO render_engine required)
# ------------------------------------------------------------
def _render_image(payload: dict, out_path: str) -> None:
    """
    LOCKED contract image text ONLY:
      TO: <City>
      FROM: <City>
      OUT: ddmmyy
      IN: ddmmyy
      PRICE: £xxx
    """
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), "white")
    draw = ImageDraw.Draw(img)

    # Optional: point to a real TTF via env var RENDER_FONT_PATH
    font_path = os.environ.get("RENDER_FONT_PATH", "")
    if font_path and os.path.exists(font_path):
        font_big = ImageFont.truetype(font_path, 72)
        font_med = ImageFont.truetype(font_path, 64)
    else:
        # Fallback to default font if no TTF configured
        font_big = ImageFont.load_default()
        font_med = ImageFont.load_default()

    lines = [
        f"TO: {payload['TO']}",
        f"FROM: {payload['FROM']}",
        f"OUT: {payload['OUT']}",
        f"IN: {payload['IN']}",
        f"PRICE: {payload['PRICE']}",
    ]

    # Centered layout
    y = 220
    for i, line in enumerate(lines):
        font = font_big if i == 0 else font_med
        bbox = draw.textbbox((0, 0), line, font=font)
        tw = bbox[2] - bbox[0]
        x = (W - tw) // 2
        draw.text((x, y), line, fill="black", font=font)
        y += 130

    img.save(out_path, format="PNG")


# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/api/render", methods=["POST"])
def api_render():
    payload = request.get_json(silent=True) or {}

    ok, err = _validate_payload(payload)
    if not ok:
        return jsonify({"success": False, "error": err}), 400

    # Unique filename WITHOUT needing deal_id
    to_slug = _safe_slug(payload.get("TO", "deal"))
    from_slug = _safe_slug(payload.get("FROM", "from"))
    h = _payload_hash(payload)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{from_slug}_to_{to_slug}_{ts}_{h}.png"

    out_path = os.path.join(STATIC_RENDER_DIR, fname)

    try:
        _render_image(payload, out_path)
    except Exception as e:
        return jsonify({"success": False, "error": f"Render failed: {e}"}), 500

    image_url = f"{BASE_URL}/static/renders/{fname}"
    return jsonify({"success": True, "image_url": image_url}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200
