# workers/telegram_publisher.py
# V4.10 - RUN_SLOT driven publishing (manual-safe) + optional FREE -1 run lag + Instagram proof link
# VIP: AM + PM. FREE: optional AM + PM with lag gate (default 10h), otherwise PM-only with 24h lag (legacy).
# Full-file replacement only. No schema renames.

import os
import json
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------ helpers ------------------

def env(k, d=""):
    return (os.getenv(k, d) or "").strip()

def env_bool(k, default=False):
    v = (os.getenv(k) or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default

def env_int(k, d):
    try:
        return int(env(k, str(d)))
    except Exception:
        return d

def env_float(k, d):
    try:
        return float(env(k, str(d)))
    except Exception:
        return d

def _sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # support one-line secret where newlines were escaped
        info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

def now_utc():
    return dt.datetime.now(dt.timezone.utc)

def parse_iso_utc(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        # supports "2026-01-19T13:15:54Z" and "+00:00"
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)
    except Exception:
        return None

def get_first(row, keys):
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        v = str(v).strip()
        if v != "":
            return v
    return ""

def normalize_price_gbp(x):
    s = str(x or "").strip()
    if not s:
        return ""
    s = s.replace("£", "").replace(",", "").strip()
    try:
        v = float(s)
        # keep 2dp for consistency
        return f"{v:.2f}"
    except Exception:
        # fallback: return original (without leading £)
        return s

def phrase_from_row(row):
    # locked behaviour: phrase_used first, fallback to phrase_bank
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()

def get_country_flag(country_name):
    # keep your existing mapping (extend anytime)
    flag_map = {
        "Iceland": "🇮🇸",
        "Spain": "🇪🇸",
        "Portugal": "🇵🇹",
        "Greece": "🇬🇷",
        "Turkey": "🇹🇷",
        "Morocco": "🇲🇦",
        "Egypt": "🇪🇬",
        "UAE": "🇦🇪",
        "United Arab Emirates": "🇦🇪",
        "Tunisia": "🇹🇳",
        "Cape Verde": "🇨🇻",
        "Gambia": "🇬🇲",
        "Jordan": "🇯🇴",
        "Madeira": "🇵🇹",
        "Canary Islands": "🇪🇸",
        "Tenerife": "🇪🇸",
        "Lanzarote": "🇪🇸",
        "Fuerteventura": "🇪🇸",
        "Gran Canaria": "🇪🇸",
        "Croatia": "🇭🇷",
        "Italy": "🇮🇹",
        "Cyprus": "🇨🇾",
        "Malta": "🇲🇹",
        "Bulgaria": "🇧🇬",
        "Barbados": "🇧🇧",
        "Jamaica": "🇯🇲",
        "Antigua": "🇦🇬",
        "St Lucia": "🇱🇨",
        "Mexico": "🇲🇽",
        "Thailand": "🇹🇭",
        "Indonesia": "🇮🇩",
        "Bali": "🇮🇩",
        "Malaysia": "🇲🇾",
        "Maldives": "🇲🇻",
        "Mauritius": "🇲🇺",
        "Seychelles": "🇸🇨",
        "Azores": "🇵🇹",
        "Switzerland": "🇨🇭",
        "Austria": "🇦🇹",
        "France": "🇫🇷",
        "Norway": "🇳🇴",
        "Sweden": "🇸🇪",
        "Finland": "🇫🇮",
        "Czech Republic": "🇨🇿",
        "Hungary": "🇭🇺",
        "Poland": "🇵🇱",
        "Germany": "🇩🇪",
        "Belgium": "🇧🇪",
        "Netherlands": "🇳🇱",
        "Denmark": "🇩🇰",
        "Estonia": "🇪🇪",
        "Latvia": "🇱🇻",
        "Lithuania": "🇱🇹",
        "Romania": "🇷🇴",
        "Israel": "🇮🇱",
        "USA": "🇺🇸",
        "United States": "🇺🇸",
        "Canada": "🇨🇦",
        "Qatar": "🇶🇦",
        "South Africa": "🇿🇦",
        "Singapore": "🇸🇬",
        "Hong Kong": "🇭🇰",
        "India": "🇮🇳",
        "Japan": "🇯🇵",
        "South Korea": "🇰🇷",
        "China": "🇨🇳",
        "Australia": "🇦🇺",
        "New Zealand": "🇦🇺",
        "Brazil": "🇧🇷",
        "Argentina": "🇦🇷",
        "Colombia": "🇨🇴",
        "Slovakia": "🇸🇰",
        "Bosnia": "🇧🇦",
        "North Macedonia": "🇲🇰",
        "Armenia": "🇦🇲",
        "Georgia": "🇬🇪",
    }
    return flag_map.get(country_name, "🌍")

def tg_send(token, chat_id, text, disable_preview=True):
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": str(disable_preview).lower(),
        },
        timeout=30,
    )
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram send failed: {r.text}")


# ------------------ publish windows (legacy fallback) ------------------

def in_vip_window(now):
    """
    VIP twice daily.
    Defaults (UTC): AM 06:00–11:59, PM 15:00–20:59
    Override with:
      VIP_WINDOW_AM_START, VIP_WINDOW_AM_END (hours 0-23)
      VIP_WINDOW_PM_START, VIP_WINDOW_PM_END
    """
    am_start = env_int("VIP_WINDOW_AM_START", 6)
    am_end = env_int("VIP_WINDOW_AM_END", 11)
    pm_start = env_int("VIP_WINDOW_PM_START", 15)
    pm_end = env_int("VIP_WINDOW_PM_END", 20)
    h = now.hour
    return (am_start <= h <= am_end) or (pm_start <= h <= pm_end)

def in_free_window(now):
    """
    FREE once daily (PM only).
    Defaults (UTC): 15:00–20:59
    Override with:
      FREE_WINDOW_PM_START, FREE_WINDOW_PM_END
    """
    pm_start = env_int("FREE_WINDOW_PM_START", 15)
    pm_end = env_int("FREE_WINDOW_PM_END", 20)
    h = now.hour
    return (pm_start <= h <= pm_end)


# ------------------ message builders ------------------
# DO NOT MODIFY CORE SCHEMATIC (fields/order). Copy tweaks are allowed.

def build_vip_message(row):
    country = get_first(row, ["destination_country"])
    city = get_first(row, ["destination_city"])
    origin = get_first(row, ["origin_city"])
    price = normalize_price_gbp(get_first(row, ["price_gbp", "price"]))
    outbound = get_first(row, ["outbound_date", "dep_date", "out_date"])
    back = get_first(row, ["inbound_date", "return_date", "ret_date", "back_date"])
    phrase = phrase_from_row(row)
    booking_link = get_first(row, ["booking_link_vip"])

    flag = get_country_flag(country)

    msg = "\n".join([
        f"£{price} to {country} {flag}",
        f"TO: {city}",
        f"FROM: {origin}",
        f"OUT:  {outbound}",
        f"BACK: {back}",
        phrase,
        "If you’ve had this place on your radar, this is one of those prices that’s worth a proper look. Routing and dates were both sensible when we checked.",
        f'<a href="{booking_link}">Booking link</a>',
        "Shared with VIP first, as always.",
    ]).strip()

    return msg

def build_free_message(row):
    country = get_first(row, ["destination_country"])
    city = get_first(row, ["destination_city"])
    origin = get_first(row, ["origin_city"])
    price = normalize_price_gbp(get_first(row, ["price_gbp", "price"]))
    outbound = get_first(row, ["outbound_date", "dep_date", "out_date"])
    back = get_first(row, ["inbound_date", "return_date", "ret_date", "back_date"])
    phrase = phrase_from_row(row)

    flag = get_country_flag(country)

    monthly = env("STRIPE_LINK_MONTHLY") or env("SUBSCRIPTION_LINK_MONTHLY")
    yearly = env("STRIPE_LINK_YEARLY") or env("SUBSCRIPTION_LINK_YEARLY")

    if not monthly or not yearly:
        raise RuntimeError("Missing STRIPE_LINK_MONTHLY / STRIPE_LINK_YEARLY (or legacy SUBSCRIPTION_LINK_*)")

    # Optional Instagram proof link for external distribution (Reddit etc.)
    ig_url = env("INSTAGRAM_PROFILE_URL") or env("IG_PROFILE_URL") or env("INSTAGRAM_URL")
    ig_handle = env("INSTAGRAM_HANDLE", "Traveltxter").lstrip("@").strip()

    footer = ""
    if ig_url:
        footer = f'Want to sanity-check us first? We post the same finds on Instagram: <a href="{ig_url}">@{ig_handle}</a>'

    lines = [
        f"£{price} to {country} {flag}",
        f"TO: {city}",
        f"FROM: {origin}",
        f"OUT:  {outbound}",
        f"BACK: {back}",
        phrase,
        "VIP members saw this first. We share deals with them early so they get a bit of breathing room to decide, rather than rushing it.",
        "If you’d like that early access, VIP is £3 per month or £30 per year.",
        f'<a href="{monthly}">Monthly</a> | <a href="{yearly}">Yearly</a>',
    ]
    if footer:
        lines.append(footer)

    return "\n".join(lines).strip()


# ------------------ sheet updates ------------------

def idx_map(headers):
    return {k: i for i, k in enumerate(headers)}

def must_have(h, name):
    if name not in h:
        raise RuntimeError(f"RAW_DEALS missing required header: {name}")

def set_cell(ws, row_i_1, col_i_0, value):
    ws.update_cell(row_i_1, col_i_0 + 1, value)


# ------------------ main ------------------

def _run_slot():
    s = env("RUN_SLOT", "").upper()
    return s if s in ("AM", "PM") else ""

def main():
    run_slot = _run_slot()

    print("============================================================")
    print(f"📣 Telegram Publisher starting | RUN_SLOT={run_slot or '(missing)'}")
    print("============================================================")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))
    ws = sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))

    values = ws.get_all_values()
    headers = values[0]
    h = idx_map(headers)

    must_have(h, "status")
    # optional fields handled best-effort:
    # posted_telegram_vip_at, posted_telegram_free_at

    now = now_utc()

    # FREE options:
    # - FREE_BOTH_SLOTS=true enables FREE eligibility on AM and PM runs (manual mode).
    # - FREE_LAG_HOURS is the minimum hours after VIP before FREE can publish (default 10h for "-1 run" feel).
    # If RUN_SLOT is missing, we fall back to legacy windows and 24h lag unless overridden.
    free_both_slots = env_bool("FREE_BOTH_SLOTS", True)
    free_lag_hours = env_float("FREE_LAG_HOURS", 10.0)

    # ---------------- STAGE 1: VIP ----------------
    vip_allowed = True
    if not run_slot:
        # legacy fallback when running on schedules without RUN_SLOT
        vip_allowed = in_vip_window(now)

    if vip_allowed:
        for i, r in enumerate(values[1:], start=2):
            if r[h["status"]] == "READY_TO_POST":
                row = {headers[j]: r[j] for j in range(len(headers))}
                msg = build_vip_message(row)

                tg_send(env("TELEGRAM_BOT_TOKEN_VIP"), env("TELEGRAM_CHANNEL_VIP"), msg, disable_preview=True)

                set_cell(ws, i, h["status"], "POSTED_TELEGRAM_VIP")
                if "posted_telegram_vip_at" in h:
                    set_cell(ws, i, h["posted_telegram_vip_at"], now.isoformat().replace("+00:00", "Z"))

                print("✅ Published to Telegram VIP")
                return 0
    else:
        print("⏱️ VIP window closed — skipping VIP stage for this run")

    print("No VIP deals ready (status=READY_TO_POST)")

    # ---------------- STAGE 2: FREE ----------------
    # If RUN_SLOT is present (manual mode), FREE can run AM+PM when FREE_BOTH_SLOTS=true.
    # If FREE_BOTH_SLOTS=false, FREE runs only on PM slot.
    # If RUN_SLOT is missing, fall back to legacy window and 24h lag (unless overridden).
    free_allowed = True

    if run_slot:
        if (not free_both_slots) and (run_slot != "PM"):
            free_allowed = False
    else:
        free_allowed = in_free_window(now)
        # legacy default was 24h; respect that unless user explicitly sets FREE_LAG_HOURS
        if env("FREE_LAG_HOURS", "").strip() == "":
            free_lag_hours = 24.0

    if free_allowed:
        for i, r in enumerate(values[1:], start=2):
            if r[h["status"]] != "POSTED_TELEGRAM_VIP":
                continue

            row = {headers[j]: r[j] for j in range(len(headers))}

            vip_ts = row.get("posted_telegram_vip_at", "")
            vip_time = parse_iso_utc(vip_ts)
            if not vip_time:
                # Do NOT violate lag logic; hold until fixed.
                print("⏳ FREE blocked: missing/invalid posted_telegram_vip_at timestamp")
                continue

            hours = (now - vip_time).total_seconds() / 3600.0
            if hours < free_lag_hours:
                print(f"⏳ FREE not ready: {hours:.1f}h elapsed (need {free_lag_hours:.1f}h)")
                continue

            msg = build_free_message(row)
            tg_send(env("TELEGRAM_BOT_TOKEN"), env("TELEGRAM_CHANNEL"), msg, disable_preview=True)

            set_cell(ws, i, h["status"], "POSTED_ALL")
            if "posted_telegram_free_at" in h:
                set_cell(ws, i, h["posted_telegram_free_at"], now.isoformat().replace("+00:00", "Z"))

            print("✅ Published to Telegram FREE")
            return 0
    else:
        print("⏱️ FREE not enabled for this slot — skipping FREE stage for this run")

    print("No deals ready to publish")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
