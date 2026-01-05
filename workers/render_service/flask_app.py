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
# Set this on PythonAnywhere as an env var if you want, otherwise defaults
# to your current domain (good enough for your live instance).
BASE_URL = os.environ.get("BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")

# Put renders under ./static/renders relative to this file (works reliably on PA)
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
    # Stable small hash for filenames
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:10]

def _validate_payload(p: dict) -> tuple[bool, str]:
    required = ["TO", "FROM", "OUT", "IN", "PRICE"]
    missing = [k for k in required if not _clean(p.get(k, ""))]
    if missing:
        return False, f"Missing required field(s): {', '.join(missing)}"
    return True, ""


# ------------------------------------------------------------
# Rendering (try render_engine first, then fallback)
# ------------------------------------------------------------
def _render_via_render_engine(payload: dict, out_path: str) -> bool:
    """
    If you have a custom render_engine.py, we use it.
    We support a couple of common function signatures safely.
    """
    try:
        from render_engine import generate_deal_image  # type: ignore
    except Exception:
        return False

    try:
        # Most common signature:
        # generate_deal_image(payload_dict, out_path)
        try:
            generate_deal_image(payload, out_path)
            return True
        except TypeError:
            pass

        # Alternate signature:
        # generate_deal_image(to_city, from_city, out, in, price, out_path)
        try:
            generate_deal_image(
                payload["TO"],
                payload["FROM"],
                payload["OUT"],
                payload["IN"],
                payload["PRICE"],
                out_path,
            )
            return True
        except TypeError:
            pass

        # If their engine returns an Image object
        img = generate_deal_image(payload)
        if hasattr(img, "save"):
            img.save(out_path, format="PNG")
            return True

        return False
    except Exception:
        # If render_engine throws, we fallback to built-in
        return False


def _render_fallback(payload: dict, out_path: str) -> None:
    """
    Deterministic fallback renderer that matches the LOCKED contract:
      TO: <City>
      FROM: <City>
      OUT: ddmmyy
      IN: ddmmyy
      PRICE: £xxx
    """
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), "white")
    draw = ImageDraw.Draw(img)

    # Use a default font available on PA; if you have a TTF, you can point to it via env.
    font_path = os.environ.get("RENDER_FONT_PATH", "")
    if font_path and os.path.exists(font_path):
        font_big = ImageFont.truetype(font_path, 64)
        font_med = ImageFont.truetype(font_path, 56)
    else:
        font_big = ImageFont.load_default()
        font_med = ImageFont.load_default()

    lines = [
        f"TO: {payload['TO']}",
        f"FROM: {payload['FROM']}",
        f"OUT: {payload['OUT']}",
        f"IN: {payload['IN']}",
        f"PRICE: {payload['PRICE']}",
    ]

    # Simple centered block layout
    y = 220
    for i, line in enumerate(lines):
        font = font_big if i == 0 else font_med
        bbox = draw.textbbox((0, 0), line, font=font)
        tw = bbox[2] - bbox[0]
        x = (W - tw) // 2
        draw.text((x, y), line, fill="black", font=font)
        y += 120

    img.save(out_path, format="PNG")


# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/api/render", methods=["POST"])
def api_render():
    # Accept JSON body
    payload = request.get_json(silent=True) or {}

    ok, err = _validate_payload(payload)
    if not ok:
        return jsonify({"success": False, "error": err}), 400

    # Build deterministic unique filename (no deal_id required)
    to_slug = _safe_slug(payload.get("TO", "deal"))
    from_slug = _safe_slug(payload.get("FROM", "from"))
    h = _payload_hash(payload)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{from_slug}_to_{to_slug}_{ts}_{h}.png"

    out_path = os.path.join(STATIC_RENDER_DIR, fname)

    # Try custom engine first, fallback to built-in if needed
    rendered = _render_via_render_engine(payload, out_path)
    if not rendered:
        try:
            _render_fallback(payload, out_path)
        except Exception as e:
            return jsonify({"success": False, "error": f"Render failed: {e}"}), 500

    image_url = f"{BASE_URL}/static/renders/{fname}"
    return jsonify({"success": True, "image_url": image_url}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200
