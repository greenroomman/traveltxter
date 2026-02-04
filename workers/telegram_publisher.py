# workers/telegram_publisher.py
# V5 — Telegram Publisher (VIP + FREE) with:
# - NEW status protocol:
#     IG sets: PUBLISH_*  -> READY_TO_POST
#     TG VIP sets: READY_TO_POST -> VIP_DONE
#     TG FREE sets: VIP_DONE -> PUBLISHED
# - Theme/ethos preserved (AM=long-haul feel, PM=short-haul feel) via heuristic
# - Fallback selection if no READY_TO_POST (uses RAW_DEALS_VIEW tg_fallback/tg_rank + <24h freshness)
# - TRUE “-1 run lag” for FREE when RUN_SLOT exists (canonical 07:30/16:30 UTC), else legacy lag hours
#
# Authority rules:
# - Writes ONLY to RAW_DEALS (status + posted timestamps + publish_error fields if present)
# - Reads RAW_DEALS_VIEW for fallback only (never writes RDV)

from __future__ import annotations

import os
import json
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------ helpers ------------------

def env(k, d=""):
    return (os.getenv(k, d) or "").strip()

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

def env_bool(k, d=False):
    v = env(k, "")
    if not v:
        return d
    return v.lower() in ("1", "true", "yes", "y", "on")

def _sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
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

def iso_z(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_iso_utc(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
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

def safe_float(x, default=None):
    try:
        s = str(x or "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def safe_int(x, default=None):
    try:
        s = str(x or "").strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def normalize_price_gbp(x):
    s = str(x or "").strip()
    if not s:
        return ""
    s = s.replace("£", "").replace(",", "").strip()
    try:
        v = float(s)
        # keep 2dp (publisher copy uses "£{price}" already)
        return f"{v:.2f}"
    except Exception:
        return s

def phrase_from_row(row):
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()

def get_country_flag(country_name):
    flag_map = {
        "Iceland": "🇮🇸", "Spain": "🇪🇸", "Portugal": "🇵🇹", "Greece": "🇬🇷", "Turkey": "🇹🇷",
        "Morocco": "🇲🇦", "Egypt": "🇪🇬", "UAE": "🇦🇪", "United Arab Emirates": "🇦🇪",
        "Tunisia": "🇹🇳", "Cape Verde": "🇨🇻", "Gambia": "🇬🇲", "Jordan": "🇯🇴",
        "Croatia": "🇭🇷", "Italy": "🇮🇹", "Cyprus": "🇨🇾", "Malta": "🇲🇹", "Bulgaria": "🇧🇬",
        "Mexico": "🇲🇽", "Thailand": "🇹🇭", "Indonesia": "🇮🇩", "Malaysia": "🇲🇾",
        "Maldives": "🇲🇻", "Mauritius": "🇲🇺", "Seychelles": "🇸🇨",
        "Switzerland": "🇨🇭", "Austria": "🇦🇹", "France": "🇫🇷",
        "Norway": "🇳🇴", "Sweden": "🇸🇪", "Finland": "🇫🇮",
        "Czech Republic": "🇨🇿", "Hungary": "🇭🇺", "Poland": "🇵🇱", "Germany": "🇩🇪",
        "Belgium": "🇧🇪", "Netherlands": "🇳🇱", "Denmark": "🇩🇰",
        "Romania": "🇷🇴",
        "USA": "🇺🇸", "United States": "🇺🇸", "Canada": "🇨🇦",
        "Qatar": "🇶🇦", "South Africa": "🇿🇦", "Singapore": "🇸🇬", "Hong Kong": "🇭🇰",
        "India": "🇮🇳", "Japan": "🇯🇵", "South Korea": "🇰🇷", "China": "🇨🇳",
        "Australia": "🇦🇺", "Brazil": "🇧🇷", "Argentina": "🇦🇷", "Colombia": "🇨🇴",
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


# ------------------ run schedule windows (true -1 run lag) ------------------

def parse_hhmm(s, default_hhmm):
    s = (s or "").strip()
    if not s:
        s = default_hhmm
    if ":" not in s:
        raise ValueError(f"Bad HH:MM value: {s}")
    hh, mm = s.split(":", 1)
    return int(hh), int(mm)

def slot_windows(now):
    """
    Returns (am_run_dt, pm_run_dt) for today in UTC.
    Defaults to canonical schedule: 07:30 and 16:30 UTC.
    Override with AM_RUN_TIME_UTC, PM_RUN_TIME_UTC (HH:MM).
    """
    am_h, am_m = parse_hhmm(env("AM_RUN_TIME_UTC", "07:30"), "07:30")
    pm_h, pm_m = parse_hhmm(env("PM_RUN_TIME_UTC", "16:30"), "16:30")
    d = now.date()
    am_dt = dt.datetime(d.year, d.month, d.day, am_h, am_m, tzinfo=dt.timezone.utc)
    pm_dt = dt.datetime(d.year, d.month, d.day, pm_h, pm_m, tzinfo=dt.timezone.utc)
    return am_dt, pm_dt

def previous_vip_window(now, run_slot):
    """
    True -1 run lag:
      - If current run_slot=PM: publish FREE for deals VIP-posted in [today AM run, today PM run)
      - If current run_slot=AM: publish FREE for deals VIP-posted in [yesterday PM run, today AM run)
    """
    am_dt, pm_dt = slot_windows(now)
    if run_slot == "PM":
        return am_dt, pm_dt
    if run_slot == "AM":
        y = now.date() - dt.timedelta(days=1)
        pm_h, pm_m = parse_hhmm(env("PM_RUN_TIME_UTC", "16:30"), "16:30")
        y_pm = dt.datetime(y.year, y.month, y.day, pm_h, pm_m, tzinfo=dt.timezone.utc)
        return y_pm, am_dt
    return None, None


# ------------------ message builders ------------------

def build_vip_message(row):
    country = get_first(row, ["destination_country"])
    city = get_first(row, ["destination_city"])
    origin = get_first(row, ["origin_city"])
    price = normalize_price_gbp(get_first(row, ["price_gbp", "price"]))
    outbound = get_first(row, ["outbound_date"])
    back = get_first(row, ["return_date"])
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
        f'<a href="{booking_link}">Booking link</a>' if booking_link else "",
        "Shared with VIP first, as always.",
    ]).strip()

    # remove accidental blank lines from missing booking link
    msg = "\n".join([ln for ln in msg.split("\n") if ln.strip() != ""])
    return msg

def build_free_message(row):
    country = get_first(row, ["destination_country"])
    city = get_first(row, ["destination_city"])
    origin = get_first(row, ["origin_city"])
    price = normalize_price_gbp(get_first(row, ["price_gbp", "price"]))
    outbound = get_first(row, ["outbound_date"])
    back = get_first(row, ["return_date"])
    phrase = phrase_from_row(row)
    flag = get_country_flag(country)

    monthly = env("STRIPE_LINK_MONTHLY")
    yearly = env("STRIPE_LINK_YEARLY")
    if not monthly or not yearly:
        raise RuntimeError("Missing STRIPE_LINK_MONTHLY / STRIPE_LINK_YEARLY")

    ig_handle = env("INSTAGRAM_HANDLE", "Traveltxter").lstrip("@").strip()
    ig_url = env("INSTAGRAM_PROFILE_URL") or env("IG_PROFILE_URL") or env("INSTAGRAM_URL")
    ig_line = f'Instagram: <a href="{ig_url}">@{ig_handle}</a>' if ig_url else f"Instagram: @{ig_handle}"

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
        ig_line,
    ]
    return "\n".join([ln for ln in lines if (ln or "").strip()]).strip()


# ------------------ selection (AM long-haul / PM short-haul) ------------------

def _is_longhaul(row: dict) -> bool:
    """
    Deterministic heuristic using existing columns:
      - outbound_duration_minutes >= 360 (6h) OR
      - total_duration_hours >= 9 OR
      - via_hub truthy AND outbound_duration_minutes >= 300
    Soft hint:
      - (theme or deal_theme) contains 'long_haul'
    """
    out_min = safe_int(row.get("outbound_duration_minutes"), None)
    tot_hr = safe_float(row.get("total_duration_hours"), None)
    via_hub = str(row.get("via_hub") or "").strip().lower() in ("true", "1", "yes", "y")
    connection_type = str(row.get("connection_type") or "").strip().lower()

    theme = (row.get("theme") or row.get("deal_theme") or "").strip().lower()
    if "long_haul" in theme:
        return True

    if out_min is not None and out_min >= 360:
        return True
    if tot_hr is not None and tot_hr >= 9:
        return True
    if via_hub and out_min is not None and out_min >= 300:
        return True
    if connection_type in ("multi_stop", "connecting", "connection"):
        if out_min is not None and out_min >= 300:
            return True
    return False


# ------------------ sheet utils ------------------

def idx_map(headers):
    return {k: i for i, k in enumerate(headers)}

def must_have(h, name):
    if name not in h:
        raise RuntimeError(f"RAW_DEALS missing required header: {name}")

def set_cell(ws, row_i_1, col_i_0, value):
    ws.update_cell(row_i_1, col_i_0 + 1, value)

def get_row_dict(headers, row_values):
    return {headers[j]: (row_values[j] if j < len(row_values) else "") for j in range(len(headers))}


# ------------------ RUN_SLOT ------------------

def _run_slot():
    s = env("RUN_SLOT", "").upper()
    # accept legacy values
    if s in ("AM", "PM"):
        return s
    return ""


# ------------------ primary (READY_TO_POST) picker ------------------

def _pick_ready_to_post(values, headers, h, run_slot: str):
    """
    Picks ONE candidate with status READY_TO_POST.
    Prefers newest by ingested_at_utc if available.
    Applies ethos preference (AM long, PM short) but will fall back to any.
    Returns (row_index_1based, row_dict) or (None, None).
    """
    candidates = []
    for i, r in enumerate(values[1:], start=2):
        if (r[h["status"]] if h["status"] < len(r) else "") != "READY_TO_POST":
            continue
        row = get_row_dict(headers, r)
        # must have basics for TG message
        if not (row.get("destination_city") and row.get("origin_city") and row.get("outbound_date") and row.get("return_date")):
            continue
        candidates.append((i, row))

    if not candidates:
        return None, None

    has_ingested = "ingested_at_utc" in h

    def ingested_key(item):
        _, row = item
        if not has_ingested:
            return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        t = parse_iso_utc(row.get("ingested_at_utc", ""))
        return t or dt.datetime.min.replace(tzinfo=dt.timezone.utc)

    if has_ingested:
        candidates.sort(key=ingested_key, reverse=True)

    preferred = []
    fallback = []
    for item in candidates:
        _, row = item
        is_long = _is_longhaul(row)
        if run_slot == "AM":
            (preferred if is_long else fallback).append(item)
        elif run_slot == "PM":
            (preferred if (not is_long) else fallback).append(item)
        else:
            preferred.append(item)

    pick = preferred[0] if preferred else fallback[0]
    return pick


# ------------------ fallback (RDV tg_fallback/tg_rank + freshness) ------------------

def _safe_bool(v) -> bool:
    s = str(v or "").strip().lower()
    return s in ("true", "1", "yes", "y")

def _safe_rank(v) -> float:
    try:
        return float(str(v or "").strip())
    except Exception:
        return -1.0

def _build_rdv_fallback_list(rdv_values: list[list[str]]) -> list[dict]:
    """
    Returns list of dicts with:
      deal_id, tg_rank(float), ingested_at_utc(str), is_fresh_24h(bool), destination_city, origin_city
    Only rows where tg_fallback TRUE.
    """
    if not rdv_values or len(rdv_values) < 2:
        return []
    hdr = rdv_values[0]
    hi = {str(h).strip(): idx for idx, h in enumerate(hdr)}

    # Required
    if "deal_id" not in hi:
        return []

    def gv(r, key):
        idx = hi.get(key)
        return r[idx] if idx is not None and idx < len(r) else ""

    out = []
    for r in rdv_values[1:]:
        did = str(gv(r, "deal_id")).strip()
        if not did:
            continue
        tg_fb = _safe_bool(gv(r, "tg_fallback"))
        if not tg_fb:
            continue

        fresh = True
        if "is_fresh_24h" in hi:
            fresh = _safe_bool(gv(r, "is_fresh_24h"))
        elif "age_hours" in hi:
            ah = safe_float(gv(r, "age_hours"), None)
            fresh = (ah is not None and ah <= 24.0)
        if not fresh:
            continue

        out.append({
            "deal_id": did,
            "tg_rank": _safe_rank(gv(r, "tg_rank")),
            "ingested_at_utc": str(gv(r, "ingested_at_utc")).strip(),
            "destination_city": str(gv(r, "destination_city")).strip(),
            "origin_city": str(gv(r, "origin_city")).strip(),
            "destination_country": str(gv(r, "destination_country")).strip(),
            "origin_country": str(gv(r, "origin_country")).strip(),
            "price_gbp": str(gv(r, "price_gbp")).strip(),
            "outbound_date": str(gv(r, "outbound_date")).strip(),
            "return_date": str(gv(r, "return_date")).strip(),
        })
    return out

def _pick_fallback_from_rdv(raw_values, raw_headers, raw_h, rdv_values, run_slot: str):
    """
    Pick ONE RDV fallback item, then map it back to RAW_DEALS by deal_id.
    Only selects if:
      - RAW_DEALS row exists for deal_id
      - posted_telegram_vip_at is blank (not already VIP-posted)
      - status is NOT PUBLISHED (avoid resurfacing)
    Returns (row_index_1based, raw_row_dict, chosen_reason) or (None, None, reason)
    """
    items = _build_rdv_fallback_list(rdv_values)
    if not items:
        return None, None, "no_rdv_fallback_items"

    # sort by tg_rank desc then newest ingested
    def ing_dt(it):
        t = parse_iso_utc(it.get("ingested_at_utc", ""))
        return t or dt.datetime.min.replace(tzinfo=dt.timezone.utc)

    items.sort(key=lambda it: (it["tg_rank"], ing_dt(it)), reverse=True)

    # Map raw deal_id -> row index
    did_col = raw_h.get("deal_id")
    status_col = raw_h.get("status")
    vip_ts_col = raw_h.get("posted_telegram_vip_at")
    if did_col is None or status_col is None:
        return None, None, "raw_missing_deal_id_or_status"

    for it in items:
        did = it["deal_id"]
        # find raw row
        for i, r in enumerate(raw_values[1:], start=2):
            did_raw = (r[did_col] if did_col < len(r) else "").strip()
            if did_raw != did:
                continue

            raw_row = get_row_dict(raw_headers, r)

            # already VIP-posted?
            if vip_ts_col is not None:
                if (raw_row.get("posted_telegram_vip_at") or "").strip():
                    break

            # avoid resurfacing completed
            st = (raw_row.get("status") or "").strip()
            if st in ("PUBLISHED", "POSTED_ALL"):
                break

            # ethos preference (soft): AM prefers longhaul, PM prefers shorthaul
            is_long = _is_longhaul(raw_row)
            if run_slot == "AM" and not is_long:
                # keep looking for a better match, but allow fallback
                pass
            if run_slot == "PM" and is_long:
                pass

            return i, raw_row, f"rdv_fallback tg_rank={it['tg_rank']}"
        # continue scanning next fallback item

    return None, None, "no_mappable_raw_row"


# ------------------ main ------------------

def main():
    run_slot = _run_slot()
    now = now_utc()

    print("============================================================")
    print(f"📣 Telegram Publisher — V5 | RUN_SLOT={run_slot or '(missing)'}")
    print("STATUS FLOW: READY_TO_POST -> VIP_DONE -> PUBLISHED")
    print("FALLBACK: RDV tg_fallback/tg_rank + <24h freshness")
    print("============================================================")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))

    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    RDV_TAB = env("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")

    ws = sh.worksheet(RAW_TAB)
    values = ws.get_all_values()
    headers = values[0]
    h = idx_map(headers)

    must_have(h, "status")
    must_have(h, "deal_id")

    # optional columns (won't crash if missing)
    has_vip_ts = "posted_telegram_vip_at" in h
    has_free_ts = "posted_telegram_free_at" in h
    has_pub_err = "publish_error" in h
    has_pub_err_at = "publish_error_at" in h

    # Load RDV only once (for fallback)
    rdv_values = []
    try:
        ws_rdv = sh.worksheet(RDV_TAB)
        rdv_values = ws_rdv.get_all_values()
    except Exception:
        rdv_values = []

    # ---------------- STAGE 1: VIP ----------------
    vip_token = env("TELEGRAM_BOT_TOKEN_VIP")
    vip_chat = env("TELEGRAM_CHANNEL_VIP")
    if not vip_token or not vip_chat:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP / TELEGRAM_CHANNEL_VIP")

    row_i, row = _pick_ready_to_post(values, headers, h, run_slot)
    chosen_reason = "ready_to_post"

    if not row_i:
        # Use fallback if no READY_TO_POST
        row_i, row, chosen_reason = _pick_fallback_from_rdv(values, headers, h, rdv_values, run_slot)

    if row_i and row:
        try:
            msg = build_vip_message(row)
            tg_send(vip_token, vip_chat, msg, disable_preview=True)

            # Update RAW_DEALS status + vip timestamp
            set_cell(ws, row_i, h["status"], "VIP_DONE")
            if has_vip_ts:
                set_cell(ws, row_i, h["posted_telegram_vip_at"], iso_z(now))

            # clear publish_error on success (optional but useful)
            if has_pub_err:
                set_cell(ws, row_i, h["publish_error"], "")
            if has_pub_err_at:
                set_cell(ws, row_i, h["publish_error_at"], "")

            print(f"✅ VIP published | row={row_i} | reason={chosen_reason} | ethos={'longhaul' if run_slot=='AM' else 'shorthaul' if run_slot=='PM' else 'auto'}")
        except Exception as e:
            if has_pub_err:
                set_cell(ws, row_i, h["publish_error"], str(e)[:450])
            if has_pub_err_at:
                set_cell(ws, row_i, h["publish_error_at"], iso_z(now))
            raise
    else:
        print("⚠️ No VIP candidate found (READY_TO_POST empty and fallback empty).")

    # ---------------- STAGE 2: FREE (dictated by VIP; -1 run lag) ----------------
    free_token = env("TELEGRAM_BOT_TOKEN")
    free_chat = env("TELEGRAM_CHANNEL")
    if not free_token or not free_chat:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHANNEL")

    monthly = env("STRIPE_LINK_MONTHLY")
    yearly = env("STRIPE_LINK_YEARLY")
    if not monthly or not yearly:
        raise RuntimeError("Missing STRIPE_LINK_MONTHLY / STRIPE_LINK_YEARLY")

    # Legacy lag if RUN_SLOT missing
    legacy_lag_hours = env_float("FREE_LAG_HOURS", 10.0)
    if not run_slot and env("FREE_LAG_HOURS", "").strip() == "":
        # older default
        legacy_lag_hours = 24.0

    # Decide window
    win_start, win_end = (None, None)
    if run_slot:
        win_start, win_end = previous_vip_window(now, run_slot)
        print(f"🧭 FREE window (-1 run): {win_start.isoformat()} → {win_end.isoformat()}")

    # Find a FREE candidate:
    free_pick = None
    for i, r in enumerate(values[1:], start=2):
        st = (r[h["status"]] if h["status"] < len(r) else "").strip()
        if st != "VIP_DONE":
            continue
        row2 = get_row_dict(headers, r)

        # already free-posted?
        if has_free_ts and (row2.get("posted_telegram_free_at") or "").strip():
            continue

        vip_time = parse_iso_utc(row2.get("posted_telegram_vip_at", "")) if has_vip_ts else None
        if run_slot:
            # require vip_time and within prev run window
            if not vip_time or not (win_start <= vip_time < win_end):
                continue
        else:
            # legacy lag
            if not vip_time:
                continue
            hours = (now - vip_time).total_seconds() / 3600.0
            if hours < legacy_lag_hours:
                continue

        free_pick = (i, row2)
        break

    # Soft fallback for FREE (still “dictated by VIP”):
    # If none matched the strict window, publish the most recent VIP_DONE not yet free-posted,
    # but ONLY if it’s still fresh (<24h) when is_fresh_24h exists.
    if not free_pick:
        for i, r in enumerate(values[1:], start=2):
            st = (r[h["status"]] if h["status"] < len(r) else "").strip()
            if st != "VIP_DONE":
                continue
            row2 = get_row_dict(headers, r)
            if has_free_ts and (row2.get("posted_telegram_free_at") or "").strip():
                continue
            # freshness gate if column exists
            if "is_fresh_24h" in h:
                if not _safe_bool(row2.get("is_fresh_24h")):
                    continue
            free_pick = (i, row2)
            break

    if free_pick:
        i, row2 = free_pick
        try:
            msg = build_free_message(row2)
            tg_send(free_token, free_chat, msg, disable_preview=True)

            # status VIP_DONE -> PUBLISHED
            set_cell(ws, i, h["status"], "PUBLISHED")
            if has_free_ts:
                set_cell(ws, i, h["posted_telegram_free_at"], iso_z(now))

            # clear publish_error on success (optional)
            if has_pub_err:
                set_cell(ws, i, h["publish_error"], "")
            if has_pub_err_at:
                set_cell(ws, i, h["publish_error_at"], "")

            print(f"✅ FREE published | row={i}")
        except Exception as e:
            if has_pub_err:
                set_cell(ws, i, h["publish_error"], str(e)[:450])
            if has_pub_err_at:
                set_cell(ws, i, h["publish_error_at"], iso_z(now))
            raise
    else:
        print("⚠️ No FREE candidate ready (VIP_DONE not found / not in window).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
