#!/usr/bin/env python3
"""
Ops Alert — Telegram notifier for GitHub Actions failures.

Usage:
  python workers/ops_alert.py

Env:
  OPS_TELEGRAM_BOT_TOKEN
  OPS_TELEGRAM_CHAT_ID
  OPS_MESSAGE (optional)  - message body
  GITHUB_RUN_URL (optional)
"""

import os
import sys
import requests
import datetime as dt


def env(k: str, default: str = "") -> str:
    return (os.getenv(k) or default).strip()


def send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(str(data))


def main() -> int:
    bot = env("OPS_TELEGRAM_BOT_TOKEN")
    chat = env("OPS_TELEGRAM_CHAT_ID")
    if not bot or not chat:
        print("Missing OPS_TELEGRAM_BOT_TOKEN or OPS_TELEGRAM_CHAT_ID", flush=True)
        return 0  # don't fail pipeline for missing ops secrets

    repo = env("GITHUB_REPOSITORY", "unknown/repo")
    run_id = env("GITHUB_RUN_ID", "")
    attempt = env("GITHUB_RUN_ATTEMPT", "")
    actor = env("GITHUB_ACTOR", "")
    workflow = env("GITHUB_WORKFLOW", "")
    job = env("OPS_JOB_NAME", "")
    step = env("OPS_STEP_NAME", "")
    status = env("OPS_STATUS", "FAILED")
    msg = env("OPS_MESSAGE", "A workflow step failed.")
    run_url = env("GITHUB_RUN_URL")

    if not run_url and run_id:
        run_url = f"https://github.com/{repo}/actions/runs/{run_id}"

    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    lines = []
    lines.append("🚨 <b>TravelTxter Ops Alert</b>")
    lines.append(f"<b>Status:</b> {status}")
    if workflow:
        lines.append(f"<b>Workflow:</b> {workflow}")
    if job:
        lines.append(f"<b>Job:</b> {job}")
    if step:
        lines.append(f"<b>Step:</b> {step}")
    lines.append(f"<b>Repo:</b> {repo}")
    if actor:
        lines.append(f"<b>Actor:</b> {actor}")
    if attempt:
        lines.append(f"<b>Attempt:</b> {attempt}")
    lines.append(f"<b>Time:</b> {ts}")
    lines.append("")
    lines.append(f"<b>Message:</b> {msg}")
    if run_url:
        lines.append("")
        lines.append(f"🔗 {run_url}")

    text = "\n".join(lines)

    try:
        send(bot, chat, text)
        print("Ops alert sent.", flush=True)
        return 0
    except Exception as e:
        print(f"Ops alert failed: {e}", flush=True)
        return 0  # never break pipeline due to alert


if __name__ == "__main__":
    raise SystemExit(main())
