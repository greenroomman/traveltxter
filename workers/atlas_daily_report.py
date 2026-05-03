import os
import sys
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

# ============================================================
# DATA LOAD
# ============================================================

decisions = sb.table("user_decisions").select("*").execute().data or []
ov = sb.table("outcome_verification").select("*").execute().data or []
usage = sb.table("api_usage").select("timestamp").execute().data or []

snap_count_res = (
    sb.table("snapshots")
    .select("snapshot_date", count="exact")
    .eq("snapshot_date", today)
    .execute()
)

today_s = snap_count_res.count or 0

fuel_data = (
    sb.table("daily_market_signals")
    .select("*")
    .order("signal_date", desc=True)
    .limit(1)
    .execute()
    .data or []
)

# ============================================================
# METRICS
# ============================================================

total_d = len(decisions)
today_d = sum(1 for r in decisions if (r.get("decision_timestamp") or "")[:10] == today)
pending = sum(1 for r in decisions if r.get("verification_status") == "pending")
verified = sum(1 for r in decisions if r.get("verification_status") == "verified")
failed_v = sum(1 for r in decisions if r.get("verification_status") == "failed")

tp = sum(1 for r in ov if r.get("prediction_outcome") == "TP")
fp = sum(1 for r in ov if r.get("prediction_outcome") == "FP")
tn = sum(1 for r in ov if r.get("prediction_outcome") == "TN")
fn_c = sum(1 for r in ov if r.get("prediction_outcome") == "FN")

precision = round(tp / (tp + fp) * 100, 1) if (tp + fp) > 0 else 0.0

today_calls = sum(1 for r in usage if (r.get("timestamp") or "")[:10] == today)

fuel_price = float(fuel_data[0]["jet_fuel_usd_gal"]) if fuel_data else "N/A"
fuel_date = fuel_data[0]["signal_date"] if fuel_data else "N/A"

# ============================================================
# HEALTH STATUS
# ============================================================

if today_s >= MIN_HEALTHY_SNAPSHOTS:
    pipeline_status = "HEALTHY"
elif today_s > 0:
    pipeline_status = "LOW"
else:
    pipeline_status = "MISSING"

flags = []

if today_s < MIN_HEALTHY_SNAPSHOTS:
    flags.append(f"CRITICAL: Snapshot coverage {today_s}/{EXPECTED_SNAPSHOTS}")

if not flags:
    flags.append("OK")

# ============================================================
# NOTION REPORT
# ============================================================

report = f"""
MIZAR Daily Health - {today}

Generated: {now_str}

Snapshots today: {today_s}/{EXPECTED_SNAPSHOTS} ({pipeline_status})

Decisions today: {today_d}
Total decisions: {total_d}
Pending verifications: {pending}
Verified decisions: {verified}
Failed verifications: {failed_v}

Outcome counts:
TP: {tp}
FP: {fp}
TN: {tn}
FN: {fn_c}

Live precision all: {precision}%

API calls today: {today_calls}

Fuel: {fuel_price} ({fuel_date})

Flags:
{chr(10).join(flags)}
""".strip()

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

try:
    notion_response = httpx.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json={
            "parent": {"page_id": NOTION_PARENT_ID},
            "properties": {
                "title": [{"text": {"content": f"Daily Health - {today}"}}]
            },
            "children": [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": report[:1900]},
                            }
                        ]
                    },
                }
            ],
        },
        timeout=30,
    )

    if notion_response.status_code >= 400:
        print("Notion report write failed:", notion_response.status_code, notion_response.text)
    else:
        print("Notion report written")

except Exception as e:
    print("Notion report write failed:", e)

# ============================================================
# SYSTEM HEALTH TABLE
# ============================================================

sb.table("system_health_daily").upsert(
    {
        "report_date": today,
        "snapshots_today": today_s,
        "pipeline_status": pipeline_status,
        "total_decisions": total_d,
        "verified_decisions": verified,
        "failed_verifications": failed_v,
        "live_precision_all": precision,
        "api_calls_today": today_calls,
        "flags": flags,
    },
    on_conflict="report_date",
).execute()

print(
    f"system_health_daily updated: "
    f"report_date={today}, "
    f"snapshots_today={today_s}, "
    f"pipeline_status={pipeline_status}"
)

# ============================================================
# MODEL PERFORMANCE TABLE
# ============================================================

try:
    v2 = {
        r["decision_id"]: r
        for r in decisions
        if r.get("model_version") == "v2_0_0" and r.get("decision_id")
    }

    v2_ov = [
        r for r in ov
        if r.get("decision_id") in v2
        and r.get("prediction_outcome") is not None
    ]

    high = [
        r for r in v2_ov
        if float(v2[r["decision_id"]].get("regret_risk_score") or 0) >= 0.70
    ]

    tp_all = sum(1 for r in v2_ov if r.get("prediction_outcome") == "TP")
    fp_all = sum(1 for r in v2_ov if r.get("prediction_outcome") == "FP")
    tn_all = sum(1 for r in v2_ov if r.get("prediction_outcome") == "TN")
    fn_all = sum(1 for r in v2_ov if r.get("prediction_outcome") == "FN")

    tp_h = sum(1 for r in high if r.get("prediction_outcome") == "TP")
    fp_h = sum(1 for r in high if r.get("prediction_outcome") == "FP")

    precision_h = round(tp_h / (tp_h + fp_h) * 100, 2) if (tp_h + fp_h) else None
    recall_all = round(tp_all / (tp_all + fn_all) * 100, 2) if (tp_all + fn_all) else None

    v2_changes = [
        float(r["price_change_pct"])
        for r in v2_ov
        if r.get("price_change_pct") is not None
    ]
    avg_change = round(sum(v2_changes) / len(v2_changes), 2) if v2_changes else None

    sb.table("model_performance_daily").upsert(
        {
            "report_date": today,
            "model_version": "v2_0_0",
            "total_verified": len(v2_ov),
            "high_risk_verified": len(high),
            "tp": tp_all,
            "fp": fp_all,
            "tn": tn_all,
            "fn": fn_all,
            "precision_high_risk": precision_h,
            "recall_global": recall_all,
            "avg_price_change": avg_change,
        },
        on_conflict="report_date",
    ).execute()

    print(
        f"model_performance_daily updated: "
        f"total_verified={len(v2_ov)}, "
        f"high_risk_verified={len(high)}, "
        f"precision_high_risk={precision_h}"
    )

except Exception as e:
    print("model performance failed:", e)

# ============================================================
# ENFORCEMENT
# ============================================================

if today_s < MIN_HEALTHY_SNAPSHOTS:
    print(f"Pipeline unhealthy - snapshot coverage {today_s}/{EXPECTED_SNAPSHOTS}")
    sys.exit(1)

print("All checks passed")