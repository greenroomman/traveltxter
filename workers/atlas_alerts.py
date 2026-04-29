import os
import json
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
alerts = []

print("Running alert check for", today)


def set_github_output(has_alerts: bool) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"has_alerts={'true' if has_alerts else 'false'}\n")


def already_alerted(alert_type: str) -> bool:
    result = (
        sb.table("alert_log")
        .select("id")
        .eq("alert_type", alert_type)
        .eq("report_date", today)
        .execute()
    )
    return bool(result.data)


def record_alert(alert_type: str, metric_value: str) -> None:
    if already_alerted(alert_type):
        print(f"Already alerted today for {alert_type}, skipping")
        return

    alerts.append({"type": alert_type, "value": metric_value})

    try:
        sb.table("alert_log").upsert(
            {
                "alert_type": alert_type,
                "report_date": today,
                "metric_value": metric_value,
            },
            on_conflict="alert_type,report_date",
        ).execute()
    except Exception as e:
        print(f"alert_log write failed for {alert_type}: {e}")


# Check system_health_daily

health = (
    sb.table("system_health_daily")
    .select("pipeline_status,snapshots_today")
    .eq("report_date", today)
    .execute()
    .data
)

if health:
    row = health[0]

    pipeline_status = row.get("pipeline_status")
    if pipeline_status != "HEALTHY":
        record_alert("pipeline_unhealthy", f"pipeline_status={pipeline_status}")

    snapshots = row.get("snapshots_today")
    if snapshots is not None and int(snapshots) < 150:
        record_alert("snapshots_low", f"snapshots_today={snapshots}/153")
else:
    record_alert("health_row_missing", "No system_health_daily row for today")


# Check model_performance_daily

perf = (
    sb.table("model_performance_daily")
    .select("precision_high_risk,high_risk_verified")
    .eq("report_date", today)
    .execute()
    .data
)

if perf:
    row = perf[0]

    precision = row.get("precision_high_risk")
    high_risk_verified = int(row.get("high_risk_verified") or 0)

    if precision is not None and high_risk_verified >= 10 and float(precision) < 50:
        record_alert(
            "precision_low",
            f"precision_high_risk={precision}% (n={high_risk_verified})",
        )
else:
    print("No model_performance_daily row for today")


# Write Slack payload

if alerts:
    lines = [f"• `{a['type']}` — {a['value']}" for a in alerts]

    payload = {
        "text": f"*MIZAR Alert* — {today}\n" + "\n".join(lines)
    }

    with open("/tmp/slack_payload.json", "w") as f:
        json.dump(payload, f)

    set_github_output(True)
    print(f"{len(alerts)} alert(s) queued for Slack delivery")
else:
    set_github_output(False)
    print("No alerts to send")