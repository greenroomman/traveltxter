import os
import requests

from lib.sheets import get_ready_deal, mark_posted, mark_error

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHANNEL = os.getenv("TELEGRAM_CHANNEL", "").strip()

def send_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()

def format_message(deal: dict) -> str:
    return f"""✈️ <b>{deal['origin_city']} → {deal['destination_city']}</b> ({deal['destination_country']})
💷 <b>From £{deal['price_gbp']}</b>
📅 {deal['outbound_date']} → {deal['return_date']} ({deal['trip_length_days']} days)
🧳 Bag: {deal['baggage_included']} | Stops: {deal['stops']}
🏷 Airline: {deal['airline']}

🔥 <b>{deal['ai_verdict']}</b>

#traveltxter #cheapflights
"""

def main():
    if not BOT_TOKEN or not CHANNEL:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")

    deal = get_ready_deal(worker_id="telegram")
    if not deal:
        print("No deal ready")
        return

    try:
        send_message(format_message(deal))
        mark_posted(deal["deal_id"])
        print("Posted deal:", deal["deal_id"])
    except Exception as e:
        mark_error(deal["deal_id"], str(e))
        raise

if __name__ == "__main__":
    main()
