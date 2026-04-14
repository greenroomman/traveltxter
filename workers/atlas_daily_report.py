
import os, sys, httpx
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_PAGE_ID = "341c2a683145815b9af0cb836bd90f4a"

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
print("Running daily report for", today)

decisions = sb.table("user_decisions").select("*").execute().data
total_d = len(decisions)
today_d = sum(1 for r in decisions if r["decision_timestamp"][:10] == today)
pending = sum(1 for r in decisions if r["verification_status"] == "pending")
verified = sum(1 for r in decisions if r["verification_status"] == "verified")
failed_v = sum(1 for r in decisions if r["verification_status"] == "failed")
v2_d = sum(1 for r in decisions if (r.get("model_version") or "").startswith("v2"))
v1_d = sum(1 for r in decisions if (r.get("model_version") or "").startswith("v1"))

ov = sb.table("outcome_verification").select("*").execute().data
tp = sum(1 for r in ov if r.get("prediction_outcome") == "TP")
fp = sum(1 for r in ov if r.get("prediction_outcome") == "FP")
tn = sum(1 for r in ov if r.get("prediction_outcome") == "TN")
fn_c = sum(1 for r in ov if r.get("prediction_outcome") == "FN")
verified_today = sum(1 for r in ov if (r.get("verification_timestamp") or "")[:10] == today)
precision = round(tp / (tp + fp) * 100, 1) if (tp + fp) > 0 else 0.0
price_changes = [float(r["price_change_pct"]) for r in ov if r.get("price_change_pct") is not None]
avg_change = round(sum(price_changes) / len(price_changes), 2) if price_changes else 0.0

snaps = sb.table("snapshots").select("snapshot_date,price_gbp,origin_iata").execute().data
total_s = len(snaps)
today_s = sum(1 for r in snaps if r["snapshot_date"] == today)
prices = [float(r["price_gbp"]) for r in snaps if r.get("price_gbp")]
avg_price = round(sum(prices) / len(prices), 2) if prices else 0.0
origins = len(set(r["origin_iata"] for r in snaps if r.get("origin_iata")))

keys = sb.table("api_keys_v2").select("*").eq("is_active", True).execute().data
users = sb.table("user_tiers").select("*").execute().data
total_users = len(users)
trialing = sum(1 for r in users if r.get("subscription_status") == "trialing")
active_subs = sum(1 for r in users if r.get("subscription_status") == "active")

usage = sb.table("api_usage").select("timestamp").execute().data
today_calls = sum(1 for r in usage if (r.get("timestamp") or "")[:10] == today)

fuel_data = sb.table("daily_market_signals").select("*").order("signal_date", desc=True).limit(1).execute().data
fuel_price = float(fuel_data[0]["jet_fuel_usd_gal"]) if fuel_data else "N/A"
fuel_date = fuel_data[0]["signal_date"] if fuel_data else "N/A"
gbp_usd = float(fuel_data[0]["gbp_usd_rate"]) if fuel_data else "N/A"

pipeline_status = "HEALTHY" if today_s >= 150 else "LOW" if today_s > 0 else "MISSING"

flags = []
if today_s < 150:
    flags.append("[WARNING] Pipeline: only " + str(today_s) + " snapshots today (expected 153)")
if failed_v > 30:
    flags.append("[WARNING] Verification failures: " + str(failed_v) + " total")
if precision < 20 and verified > 100:
    flags.append("[WARNING] Precision low: " + str(precision) + "% on " + str(verified) + " verified decisions")
if not flags:
    flags.append("[OK] All systems nominal.")

lines = [
    "# MIZAR Daily Health -- " + today,
    "",
    "Generated: " + now_str,
    "",
    "---",
    "",
    "## Pipeline",
    "",
    "| Metric | Value | Status |",
    "|---|---|---|",
    "| Snapshots today | " + str(today_s) + " / 153 expected | " + pipeline_status + " |",
    "| Total snapshots | " + str(total_s) + " | -- |",
    "| Active origins | " + str(origins) + " / 9 | -- |",
    "| Avg network price | GBP " + str(avg_price) + " | -- |",
    "| Fuel price | USD " + str(fuel_price) + "/gal (" + str(fuel_date) + ") | -- |",
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
    "| TP / FP / TN / FN | " + str(tp) + " / " + str(fp) + " / " + str(tn) + " / " + str(fn_c) + " |",
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
for f in flags:
    lines.append("- " + f)

report = "\n".join(lines)
print(report)

headers = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

page_res = httpx.get(
    "https://api.notion.com/v1/blocks/" + NOTION_PAGE_ID + "/children",
    headers=headers
)

new_block = {
    "children": [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "Report -- " + today}}]
            }
        },
        {
            "object": "block",
            "type": "code",
            "code": {
                "language": "markdown",
                "rich_text": [{"type": "text", "text": {"content": report[:1900]}}]
            }
        }
    ]
}

res = httpx.patch(
    "https://api.notion.com/v1/blocks/" + NOTION_PAGE_ID + "/children",
    headers=headers,
    json=new_block
)

if res.status_code == 200:
    print("Notion updated successfully")
else:
    print("Notion update failed: " + str(res.status_code))
    print(res.text)
    sys.exit(1)
