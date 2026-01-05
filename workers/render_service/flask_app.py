from flask import Flask, request, jsonify
import os
import re
from render_engine import generate_deal_image

app = Flask(__name__)

BASE_URL = "https://yourusername.pythonanywhere.com"
OUT_DIR = "/home/yourusername/mysite/static/images/deals"
os.makedirs(OUT_DIR, exist_ok=True)


def safe_slug(text: str) -> str:
    # ascii-safe filename
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


@app.route("/api/render", methods=["POST"])
def render():
    data = request.json or {}

    required = ["TO", "FROM", "OUT", "IN", "PRICE"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({
            "success": False,
            "error": f"Missing fields: {missing}",
        }), 400

    fname = f"ttx_{data['OUT']}_{safe_slug(data['TO'])}.png"
    path = os.path.join(OUT_DIR, fname)

    ok = generate_deal_image(data, path)
    if not ok or not os.path.exists(path):
        return jsonify({
            "success": False,
            "error": "Image generation failed",
        }), 500

    return jsonify({
        "success": True,
        "image_url": f"{BASE_URL}/static/images/deals/{fname}",
    }), 200


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200
