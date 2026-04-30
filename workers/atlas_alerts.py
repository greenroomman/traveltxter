#!/usr/bin/env python3
"""
atlas_alerts.py — MIZAR pipeline health checks + weekly precision digest

Runs daily at 08:31 UTC via GitHub Actions.
Weekly digest fires on Mondays only.

Env vars required:
MIZAR_SUPABASE_URL
MIZAR_SUPABASE_SERVICE_ROLE_KEY
SLACK_WEBHOOK_URL
"""

import os
import datetime
import requests
from supabase import create_client


SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def post_slack(message: str) -> None:
    resp = requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": message},
        timeout=10,
    )

    if resp.status_code != 200:
        print(f"Slack post failed: {resp.status_code} {resp.text}")
    else:
        print("Slack posted OK")


def already_alerted(alert_type: str) -> bool:
    today = datetime.date.today().isoformat()

    result = (
        supabase.table("alert_log")
        .select("id")
        .eq("alert_type", alert_type)
        .eq("alert_date", today)
        .execute()
    )

    return len(result.data or []) > 0


def log_alert(alert_type: str) -> None:
    today = datetime.date.today().isoformat()

    supabase.table("alert_log").insert(
        {"alert_type": alert_type, "alert_date": today}
    ).execute()


def run_health_check() -> None:
    print("Running daily health check...")

    today = datetime.date.today().isoformat()

    result = (
        supabase.table("system_health_daily")
        .select("pipeline_status,snapshots_today")
        .eq("health_date", today)
        .execute()
    )

    if not result.data:
        print(f"No system_health_daily row for {today} — skipping health check.")
        return

    row = result.data[0]
    pipeline_status = row.get("pipeline_status", "unknown")
    snapshots_today = row.get("snapshots_today", 0)

    alerts_fired = []

    if pipeline_status != "HEALTHY":
        if not already_alerted("pipeline_unhealthy"):
            alerts_fired.append(
                f":rotating_light: *Pipeline unhealthy* — status: `{pipeline_status}`"
            )
            log_alert("pipeline_unhealthy")

    if snapshots_today is not None and snapshots_today < 100:
        if not already_alerted("snapshots_low"):
            alerts_fired.append(
                f":warning: *Snapshot count low* — {snapshots_today} rows today "
                f"(expected 153)"
            )
            log_alert("snapshots_low")

    if alerts_fired:
        post_slack("*MIZAR Pipeline Alert*\n" + "\n".join(alerts_fired))
        print(f"Fired {len(alerts_fired)} alert(s).")
    else:
        print("Health check passed. No alerts.")


def run_weekly_digest() -> None:
    print("Running weekly precision digest...")

    cutoff = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    ).isoformat()

    ov_result = (
        supabase.table("outcome_verification")
        .select("decision_id,prediction_outcome,price_t7_gbp,price_change_pct")
        .gte("verification_timestamp", cutoff)
        .not_.is_("prediction_outcome", "null")
        .execute()
    )

    ov_rows = ov_result.data or []

    if not ov_rows:
        post_slack(
            "*MIZAR Weekly Precision — API*\n"
            "No verified decisions this week (n=0)\n"
            "No action recommended."
        )
        print("No verified rows — digest sent.")
        return

    ov_by_id = {r["decision_id"]: r for r in ov_rows if r.get("decision_id")}
    decision_ids = list(ov_by_id.keys())

    ud_result = (
        supabase.table("user_decisions")
        .select(
            "decision_id,regret_risk_score,signal_shown,user_action,"
            "price_shown_gbp,origin_iata,destination_iata,client_platform"
        )
        .in_("decision_id", decision_ids)
        .eq("client_platform", "api")
        .execute()
    )

    ud_rows = ud_result.data or []

    joined = []

    for ud in ud_rows:
        ov = ov_by_id.get(ud.get("decision_id"))
        if ov:
            joined.append({**ud, **ov})

    n_total = len(joined)

    if n_total < 30:
        post_slack(
            "*MIZAR Weekly Precision — API*\n"
            f"Insufficient data this week (n={n_total})\n"
            "No action recommended."
        )
        print(f"Digest sent: insufficient data, n={n_total}.")
        return

    tp = fp = tn = fn = 0

    for r in joined:
        outcome = r.get("prediction_outcome")

        if outcome == "TP":
            tp += 1
        elif outcome == "FP":
            fp += 1
        elif outcome == "TN":
            tn += 1
        elif outcome == "FN":
            fn += 1

    high_risk_n = tp + fp
    precision_pct = (tp / high_risk_n * 100) if high_risk_n > 0 else 0.0

    bk_result = (
        supabase.table("user_decisions")
        .select("signal_shown,user_action")
        .eq("client_platform", "api")
        .gte("decision_timestamp", cutoff)
        .not_.is_("signal_shown", "null")
        .execute()
    )

    bk_rows = bk_result.data or []

    shown_total = 0
    shown_booked = 0
    hidden_total = 0
    hidden_booked = 0

    for r in bk_rows:
        if r.get("signal_shown") is True:
            shown_total += 1
            if r.get("user_action") == "booked_now":
                shown_booked += 1
        else:
            hidden_total += 1
            if r.get("user_action") == "booked_now":
                hidden_booked += 1

    rate_shown = (shown_booked / shown_total * 100) if shown_total > 0 else 0.0
    rate_hidden = (hidden_booked / hidden_total * 100) if hidden_total > 0 else 0.0
    lift = rate_shown - rate_hidden

    tp_rises = []
    fn_rises = []

    for r in joined:
        price_shown = r.get("price_shown_gbp")
        price_t7 = r.get("price_t7_gbp")

        if price_shown is None or price_t7 is None:
            continue

        rise = float(price_t7) - float(price_shown)

        if rise <= 0:
            continue

        if r.get("prediction_outcome") == "TP":
            tp_rises.append(rise)
        elif r.get("prediction_outcome") == "FN":
            fn_rises.append(rise)

    avg_saved = sum(tp_rises) / len(tp_rises) if tp_rises else 0.0
    total_saved = sum(tp_rises)
    wrong_wait_count = len(fn_rises)
    avg_loss = sum(fn_rises) / len(fn_rises) if fn_rises else 0.0

    route_tp = {}

    for r in joined:
        if r.get("prediction_outcome") != "TP":
            continue

        price_shown = r.get("price_shown_gbp")
        price_t7 = r.get("price_t7_gbp")

        if price_shown is None or price_t7 is None:
            continue

        rise = float(price_t7) - float(price_shown)

        if rise <= 0:
            continue

        route = f"{r.get('origin_iata', '?')} → {r.get('destination_iata', '?')}"
        route_tp.setdefault(route, []).append(rise)

    top_route_line = ""

    eligible_routes = {k: v for k, v in route_tp.items() if len(v) >= 2}

    if eligible_routes:
        best_route = max(
            eligible_routes,
            key=lambda k: sum(eligible_routes[k]) / len(eligible_routes[k]),
        )
        best_avg = sum(eligible_routes[best_route]) / len(eligible_routes[best_route])
        top_route_line = f"\nTop route this week: {best_route} (avg +£{best_avg:.0f} rise)"

    n_for_gate = high_risk_n if high_risk_n >= 10 else n_total

    if precision_pct >= 70 and n_for_gate >= 100:
        status = "✅ USE IN OUTREACH"
    elif precision_pct >= 60 and n_for_gate >= 50:
        status = "⚠️ DIRECTIONAL ONLY"
    else:
        status = "❌ HOLD"

    message = (
        "*MIZAR Weekly Precision — API*\n"
        f"Precision (≥0.70): {precision_pct:.1f}%\n"
        f"Sample size: n={n_total} verified | {high_risk_n} high-risk calls\n"
        "\n"
        "*Immediate booking rate:*\n"
        f"• Signal shown: {rate_shown:.1f}% (n={shown_total})\n"
        f"• Signal hidden: {rate_hidden:.1f}% (n={hidden_total})\n"
        f"• Lift: +{lift:.1f}pp\n"
        "\n"
        "*Financial impact:*\n"
        f"• Avg £ saved (correct interventions): £{avg_saved:.0f}\n"
        f"• Total £ saved: £{total_saved:.0f}\n"
        f"• “Wrong wait” cases: {wrong_wait_count} (avg loss £{avg_loss:.0f})"
        f"{top_route_line}\n"
        "\n"
        f"Status: *{status}*"
    )

    post_slack(message)

    print(
        f"Weekly digest posted. Precision={precision_pct:.1f}% "
        f"n={n_total} high_risk_n={high_risk_n} status={status}"
    )


if __name__ == "__main__":
    run_health_check()

    today = datetime.date.today()

    if today.weekday() == 0:
        run_weekly_digest()
    else:
        print(f"Not Monday (weekday={today.weekday()}) — skipping weekly digest.")