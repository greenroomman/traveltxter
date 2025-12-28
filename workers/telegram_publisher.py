import os
import requests

from lib.sheets import get_ready_deal, mark_posted, mark_error

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHANNEL = os.getenv("TELEGRAM_CHANNEL", "").strip()

MIN_AI_SCORE = os.getenv("TELEGRAM_MIN_AI_SCORE", "").strip()  # e.g. "70"
ALLOW_VERDICTS = os.getenv("TELEGRAM_ALLOW_VERDICTS", "GOOD").strip()  # 
"GOOD" or "GOOD,AVERAGE"


def _send_telegram_message(text: str) -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


def _format_message(deal: dict) -> str:
    origin = (deal.get("origin_city") or "").strip()
    dest = (deal.get("destination_city") or "").strip()
    country = (deal.get("destination_country") or "").strip()
    price = (deal.get("price_gbp") or "").strip()
    out_d = (deal.get("outbound_date") or "").strip()
    ret_d = (deal.get("return_date") or "").strip()
    days = (deal.get("trip_length_days") or "").strip()
    bag = (deal.get("baggage_included") or "").strip()
    stops = (deal.get("stops") or "").strip()
    airline = (deal.get("airline") or "").strip()
    verdict = (deal.get("ai_verdict") or "").strip()
    caption = (deal.get("ai_caption") or "").strip()

    lines = [
        f"✈️ <b>{origin} → {dest}</b> ({country})",
        f"💷 <b>From £{price}</b>",
        f"📅 {out_d} → {ret_d} ({days} days)",
        f"🧳 Bag: {bag} | Stops: {stops}",
        f"🏷 Airline: {airline}",
        "",
        f"🔥 <b>{verdict or 'Deal'}</b>",
    ]

    if caption:
        lines += ["", caption]

    lines += ["", "#traveltxter #cheapflights"]
    return "\n".join(lines)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing env var: TELEGRAM_BOT_TOKEN")
    if not CHANNEL:
        raise RuntimeError("Missing env var: TELEGRAM_CHANNEL (example: 
@traveltxter)")

    allow_verdicts = tuple([v.strip().upper() for v in 
ALLOW_VERDICTS.split(",") if v.strip()])
    min_ai_score = None
    if MIN_AI_SCORE:
        min_ai_score = float(MIN_AI_SCORE)

    deal = get_ready_deal(
        worker_id="telegram_publisher",
        allow_verdicts=allow_verdicts,
        min_ai_score=min_ai_score,
        max_lock_age_minutes=30,
    )

    if not deal:
        print("No deals ready to post.")
        return

    deal_id = (deal.get("deal_id") or "").strip()

    try:
        msg = _format_message(deal)
        _send_telegram_message(msg)
        mark_posted(deal_id)
        print(f"Posted to Telegram: {deal_id}")
    except Exception as e:
        mark_error(deal_id, str(e))
        print(f"ERROR posting {deal_id}: {e}")
        raise


if __name__ == "__main__":
    main()

