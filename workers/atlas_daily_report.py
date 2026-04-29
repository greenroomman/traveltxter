import os
import sys
import json
import httpx
from datetime import datetime, timezone
from supabase import create_client


SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_PARENT_ID = "327c2a68314581a2a1aaf8a2d609b425"

EXPECTED_SNAPSHOTS = 153
MIN_HEALTHY_SNAPSHOTS = 150


sb = create_client(SUPABASE_URL, SUPABASE_KEY)

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

print("Running daily report for", today)


# ─────────────────────────────────────────────────────────────
# Decisions
# ─────────────────────────────────────────────────────────────

decisions = sb.table("user_decisions").select("*").execute().data or []

total_d = len(decisions)
today_d = sum(1 for r in decisions if (r.get("decision_timestamp") or "")[:10] == today)
pending = sum(1 for r in decisions if r.get("verification_status") == "pending")
verified = sum(1 for r in decisions if r.get("verification_status") == "verified")
failed_v = sum(1 for r in decisions if r.get("verification_status") == "failed")
v2_d = sum(1 for r in decisions if (r.get("model_version") or "").startswith("v2"))
v1_d = sum(1 for r in decisions if (r.get("model_version") or "").startswith("v1"))


# ─────────────────────────────────────────────────────────────
# Outcome verification
# ─────────────────────────────────────────────────────────────

ov = sb.table("outcome_verification").select("*").execute().data or []

tp = sum(1 for r in ov if r.get("prediction_outcome") == "TP")
fp = sum(1 for r in ov if r.get("prediction_outcome") == "FP")
tn = sum(1 for r in ov if r.get("prediction_outcome") == "TN")
fn_c = sum(1 for r in ov if r.get("prediction_outcome") == "FN")

verified_today = sum(
    1 for r in ov if (r.get("verification_timestamp") or "")[:10] == today
)

precision = round(tp / (tp + fp) * 100, 1) if (tp + fp) > 0 else 0.0

price_changes = [
    float(r["price_change_pct"])
    for r in ov
    if r.get("price_change_pct") is not None
]
avg_change = round(sum(price_changes) / len(price_changes), 2) if price_changes else 0.0


# ─────────────────────────────────────────────────────────────
# Snapshots
# ─────────────────────────────────────────────────────────────

snaps = (
    sb.table("snapshots")
    .select("snapshot_date,price_gbp,origin_iata")
    .execute()
    .data
    or []
)

total_s = len(snaps)
today_s = sum(1 for r in snaps if r.get("snapshot_date") == today)

prices = [
    float(r["price_gbp"])
    for r in snaps
    if r.get("price_gbp") is not None
]
avg_price = round(sum(prices) / len(prices), 2) if prices else 0.0

origins = len(set(r["origin_iata"] for r in snaps if r.get("origin_iata")))


# ─────────────────────────────────────────────────────────────
# API keys, users, usage
# ─────────────────────────────────────────────────────────────

keys = (
    sb.table("api_keys_v2")
    .select("*")
    .eq("is_active", True)
    .execute()
    .data
    or []
)

users = sb.table("user_tiers").select("*").execute().data or []

total_users = len(users)
trialing = sum(1 for r in users if r.get("subscription_status") == "trialing")
active_subs = sum(1 for r in users if r.get("subscription_status") == "active")

usage = sb.table("api_usage").select("timestamp").execute().data or []
today_calls = sum(1 for r in usage if (r.get("timestamp") or "")[:10] == today)


# ─────────────────────────────────────────────────────────────
# Market signals
# ─────────────────────────────────────────────────────────────

fuel_data = (
    sb.table("daily_market_signals")
    .select("*")
    .order("signal_date", desc=True)
    .limit(1)
    .execute()
    .data
    or []
)

fuel_price = float(fuel_data[0]["jet_fuel_usd_gal"]) if fuel_data else "N/A"
fuel_date = fuel_data[0]["signal_date"] if fuel_data else "N/A"
gbp_usd = float(fuel_data[0]["gbp_usd_rate"]) if fuel_data else "N/A"


# ─────────────────────────────────────────────────────────────
# Health flags
# ─────────────────────────────────────────────────────────────

if today_s >= MIN_HEALTHY_SNAPSHOTS:
    pipeline_status = "HEALTHY"
elif today_s > 0:
    pipeline_status = "LOW"
else:
    pipeline_status = "MISSING"

flags = []

if today_s < MIN_HEALTHY_SNAPSHOTS:
    flags.append(
        "CRITICAL: Snapshot coverage below threshold: "
        + str(today_s)
        + " / "
        + str(EXPECTED_SNAPSHOTS)
        + " expected"
    )

if failed_v > 30:
    flags.append("WARNING: Verification failures: " + str(failed_v) + " total")

if precision < 20 and verified > 100:
    flags.append(
        "WARNING: Precision low: "
        + str(precision)
        + "% on "
        + str(verified)
        + " verified decisions"
    )

if not flags:
    flags.append("OK: All systems nominal.")


# ─────────────────────────────────────────────────────────────
# Markdown report
# ─────────────────────────────────────────────────────────────

lines = [
    "# MIZAR Daily Health - " + today,
    "",
    "Generated: " + now_str,
    "",
    "---",
    "",
    "## Pipeline",
    "",
    "| Metric | Value | Status |",
    "|---|---|---|",
    "| Snapshots today | "
    + str(today_s)
    + " / "
    + str(EXPECTED_SNAPSHOTS)
    + " expected | "
    + pipeline_status
    + " |",
    "| Total snapshots | " + str(total_s) + " | -- |",
    "| Active origins | " + str(origins) + " / 9 | -- |",
    "| Avg network price | GBP " + str(avg_price) + " | -- |",
    "| Fuel price | USD "
    + str(fuel_price)
    + "/gal ("
    + str(fuel_date)
    + ") | -- |",
    "| GBP/USD | " + str(gbp_usd) + " | -- |",
    "",
    "## Decisions and Verification",
    "",
    "| Metric | Value |",
    "|---|---|",
    "| Total decisions | " + str(total_d) + " |",
    "| Decisions today | " + str(today_d) + " |",
    "| Pending t+7 | " + str(pending) + " |",
    "| Verified | " + str(verified) + " |",
    "| Failed | " + str(failed_v) + " |",
    "| v2_0_0 decisions | " + str(v2_d) + " |",
    "| v1 decisions | " + str(v1_d) + " |",
    "| Verified today | " + str(verified_today) + " |",
    "| Live precision (all) | " + str(precision) + "% |",
    "| TP / FP / TN / FN | "
    + str(tp)
    + " / "
    + str(fp)
    + " / "
    + str(tn)
    + " / "
    + str(fn_c)
    + " |",
    "| Avg price change (verified) | " + str(avg_change) + "% |",
    "",
    "## API and Users",
    "",
    "| Metric | Value |",
    "|---|---|",
    "| Active API keys | " + str(len(keys)) + " |",
    "| Total users | " + str(total_users) + " |",
    "| Trialing | " + str(trialing) + " |",
    "| Active subscriptions | " + str(active_subs) + " |",
    "| API calls today | " + str(today_calls) + " |",
    "",
    "## Flags",
    "",
]

for flag in flags:
    lines.append("- " + flag)

report = "\n".join(lines)
print(report)


# ─────────────────────────────────────────────────────────────
# Step 1: Create Notion page
# Always runs, even on unhealthy days
# ─────────────────────────────────────────────────────────────

headers = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

new_page = {
    "parent": {"page_id": NOTION_PARENT_ID},
    "icon": {"emoji": "📊"},
    "properties": {
        "title": [{"text": {"content": "Daily Health - " + today}}],
    },
    "children": [
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "Auto-generated at " + now_str},
                    }
                ]
            },
        },
        {
            "object": "block",
            "type": "code",
            "code": {
                "language": "markdown",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": report[:1900]},
                    }
                ],
            },
        },
    ],
}

res = httpx.post(
    "https://api.notion.com/v1/pages",
    headers=headers,
    json=new_page,
)

if res.status_code == 200:
    page_url = res.json().get("url", "")
    print("Notion page created: " + page_url)
else:
    print("Notion create failed: " + str(res.status_code))
    print(res.text)
    sys.exit(1)


# ─────────────────────────────────────────────────────────────
# Step 2: Upsert into system_health_daily
# Always runs after Notion creation
# ─────────────────────────────────────────────────────────────

health_row = {
    "report_date": today,
    "snapshots_today": today_s,
    "expected_snapshots": EXPECTED_SNAPSHOTS,
    "pipeline_status": pipeline_status,
    "total_decisions": total_d,
    "verified_decisions": verified,
    "pending_decisions": pending,
    "failed_verifications": failed_v,
    "live_precision_all": float(precision),
    "api_calls_today": today_calls,
    "flags": flags,
}

try:
    sb.table("system_health_daily").upsert(
        health_row,
        on_conflict="report_date",
    ).execute()
    print("system_health_daily upserted for", today)
except Exception as exc:
    print("system_health_daily upsert failed:", exc)
    sys.exit(1)


# ─────────────────────────────────────────────────────────────
# Step 3: Enforce snapshot threshold
# Exit 1 makes GitHub Action fail
# ─────────────────────────────────────────────────────────────

if today_s < MIN_HEALTHY_SNAPSHOTS:
    print(
        "CRITICAL: Snapshot coverage "
        + str(today_s)
        + " < "
        + str(MIN_HEALTHY_SNAPSHOTS)
        + ". Pipeline unhealthy. Exiting with status 1."
    )
    sys.exit(1)

print("All checks passed.")