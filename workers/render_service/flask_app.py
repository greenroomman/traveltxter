from flask import Flask, request, jsonify
import os
from render_engine import generate_deal_image

app = Flask(__name__)

BASE_URL = "https://yourusername.pythonanywhere.com"
OUT_DIR = "/home/yourusername/mysite/static/images/deals"
os.makedirs(OUT_DIR, exist_ok=True)

@app.route("/api/render", methods=["POST"])
def render():
    data = request.json or {}

    required = ["TO", "FROM", "OUT", "IN", "PRICE"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    fname = f"ttx_{data['OUT']}_{data['TO'].lower().replace(' ', '')}.png"
    path = os.path.join(OUT_DIR, fname)

    ok = generate_deal_image(data, path)
    if not ok:
        return jsonify({"error": "Image generation failed"}), 500

    return jsonify({
        "success": True,
        "image_url": f"{BASE_URL}/static/images/deals/{fname}"
    }), 200

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200
