#!/usr/bin/env python3
"""
Send court hearing reminders to clients via Telegram.

Flow:
1. Query Notion ⚖️ Засідання for hearings scheduled TOMORROW
2. For each hearing → get linked Кейс → get linked Клієнт
3. If client has Telegram Chat ID → send reminder
4. Also notify the lawyer (owner)

Runs daily via GitHub Actions (recommended: 18:00 Kyiv time).
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
OWNER_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Notion database IDs
HEARINGS_DB_ID = os.environ.get("NOTION_DATABASE_ID", "")  # ⚖️ Засідання
BRANDING_NAME = os.environ.get("BRANDING_NAME", "Адвокатське бюро")
BRANDING_URL = os.environ.get("BRANDING_URL", "")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
NOTION_API = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


def send_message(chat_id, text, parse_mode="HTML"):
    """Send a Telegram message."""
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode
    }
    resp = requests.post(f"{TELEGRAM_API}/sendMessage", json=data)
    if not resp.ok:
        print(f"  Telegram error: {resp.text}")
    return resp.json()


def get_tomorrow_date():
    """Get tomorrow's date in YYYY-MM-DD format (Kyiv time approximation)."""
    # GitHub Actions runs in UTC; Kyiv is UTC+2/+3
    # If script runs at 18:00 Kyiv (15:00/16:00 UTC), tomorrow is correct
    utc_now = datetime.utcnow()
    kyiv_offset = timedelta(hours=3)  # EEST (summer) — adjust if needed
    kyiv_now = utc_now + kyiv_offset
    tomorrow = kyiv_now + timedelta(days=1)
    return tomorrow.strftime("%Y-%m-%d")


def query_tomorrow_hearings(tomorrow_str):
    """Query Notion for hearings scheduled tomorrow."""
    data = {
        "filter": {
            "property": "Дата засідання",
            "date": {
                "equals": tomorrow_str
            }
        },
        "page_size": 100
    }
    resp = requests.post(
        f"{NOTION_API}/databases/{HEARINGS_DB_ID}/query",
        headers=NOTION_HEADERS,
        json=data
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def get_page(page_id):
    """Fetch a single Notion page by ID."""
    resp = requests.get(
        f"{NOTION_API}/pages/{page_id}",
        headers=NOTION_HEADERS
    )
    resp.raise_for_status()
    return resp.json()


def get_property_text(page, prop_name):
    """Extract plain text from a title/rich_text property."""
    prop = page.get("properties", {}).get(prop_name, {})
    prop_type = prop.get("type", "")

    if prop_type == "title":
        items = prop.get("title", [])
    elif prop_type == "rich_text":
        items = prop.get("rich_text", [])
    else:
        return ""

    return "".join(item.get("plain_text", "") for item in items)


def get_property_date(page, prop_name):
    """Extract date string from a date property."""
    prop = page.get("properties", {}).get(prop_name, {})
    date_obj = prop.get("date", {})
    if date_obj:
        return date_obj.get("start", "")
    return ""


def get_property_number(page, prop_name):
    """Extract number from a number property."""
    prop = page.get("properties", {}).get(prop_name, {})
    return prop.get("number")


def get_relation_ids(page, prop_name):
    """Extract related page IDs from a relation property."""
    prop = page.get("properties", {}).get(prop_name, {})
    if prop.get("type") == "relation":
        return [r["id"] for r in prop.get("relation", [])]
    return []


def extract_hearing_info(hearing_page):
    """Extract key info from a hearing page."""
    return {
        "title": get_property_text(hearing_page, "Подія"),
        "case_number": get_property_text(hearing_page, "Номер справи"),
        "date": get_property_date(hearing_page, "Дата засідання"),
        "time": get_property_text(hearing_page, "Час"),
        "court": get_property_text(hearing_page, "Суд"),
        "judge": get_property_text(hearing_page, "Суддя"),
        "room": get_property_text(hearing_page, "Зал"),
        "subject": get_property_text(hearing_page, "Предмет"),
        "case_ids": get_relation_ids(hearing_page, "Кейс"),
    }


def format_date_ua(date_str):
    """Format date to Ukrainian format: 15 квітня 2026."""
    months_ua = {
        1: "січня", 2: "лютого", 3: "березня", 4: "квітня",
        5: "травня", 6: "червня", 7: "липня", 8: "серпня",
        9: "вересня", 10: "жовтня", 11: "листопада", 12: "грудня"
    }
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.day} {months_ua[dt.month]} {dt.year}"
    except (ValueError, KeyError):
        return date_str


def build_client_message(info):
    """Build a reminder message for the client."""
    date_ua = format_date_ua(info["date"])
    time_str = f" о {info['time']}" if info["time"] else ""

    lines = [
        f"📋 <b>Нагадування про судове засідання</b>",
        "",
        f"Завтра, <b>{date_ua}{time_str}</b>,",
        f"відбудеться судове засідання по Вашій справі",
        f"№ <b>{info['case_number']}</b>",
    ]

    if info["court"]:
        lines.append(f"")
        lines.append(f"🏛 Суд: {info['court']}")
    if info["room"]:
        lines.append(f"📍 Зал: {info['room']}")
    if info["judge"]:
        lines.append(f"👤 Суддя: {info['judge']}")

    lines.append("")
    branding = BRANDING_NAME
    if BRANDING_URL:
        branding += f" | {BRANDING_URL}"
    lines.append(branding)

    return "\n".join(lines)


def build_owner_summary(reminders_sent, reminders_failed, no_chatid):
    """Build a summary message for the lawyer."""
    lines = ["📊 <b>Нагадування клієнтам — звіт</b>", ""]

    if reminders_sent:
        lines.append(f"✅ Надіслано: {len(reminders_sent)}")
        for r in reminders_sent:
            lines.append(f"  • {r['client_name']} — справа {r['case_number']}")

    if no_chatid:
        lines.append(f"")
        lines.append(f"⚠️ Без Telegram (не підключені): {len(no_chatid)}")
        for r in no_chatid:
            lines.append(f"  • {r['client_name']} — справа {r['case_number']}")

    if reminders_failed:
        lines.append(f"")
        lines.append(f"❌ Помилки: {len(reminders_failed)}")
        for r in reminders_failed:
            lines.append(f"  • {r['client_name']} — {r['error']}")

    if not reminders_sent and not no_chatid and not reminders_failed:
        lines.append("Завтра засідань немає.")

    return "\n".join(lines)


def main():
    if not BOT_TOKEN or not NOTION_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN and NOTION_TOKEN are required")
        sys.exit(1)

    if not HEARINGS_DB_ID:
        print("ERROR: NOTION_DATABASE_ID (Засідання) is required")
        sys.exit(1)

    tomorrow = get_tomorrow_date()
    print(f"Checking hearings for: {tomorrow}")

    # 1. Query tomorrow's hearings
    hearings = query_tomorrow_hearings(tomorrow)
    print(f"Found {len(hearings)} hearing(s) tomorrow")

    if not hearings:
        # Notify owner: no hearings tomorrow
        if OWNER_CHAT_ID:
            send_message(int(OWNER_CHAT_ID), f"📅 Завтра ({tomorrow}) засідань немає.")
        return

    reminders_sent = []
    reminders_failed = []
    no_chatid = []

    for hearing_page in hearings:
        info = extract_hearing_info(hearing_page)
        print(f"\nHearing: {info['case_number']} at {info['court']}")

        # 2. Get linked case(s)
        for case_id in info["case_ids"]:
            try:
                case_page = get_page(case_id)
            except Exception as e:
                print(f"  Error fetching case {case_id}: {e}")
                continue

            # 3. Get linked clients
            client_ids = get_relation_ids(case_page, "👤 Клієнти АБ")
            if not client_ids:
                print(f"  No clients linked to case")
                continue

            for client_id in client_ids:
                try:
                    client_page = get_page(client_id)
                except Exception as e:
                    print(f"  Error fetching client {client_id}: {e}")
                    continue

                client_name = get_property_text(client_page, "ПІБ / назва юрособи")
                chat_id = get_property_number(client_page, "Telegram Chat ID")

                # Check role — only notify clients, not opponents
                role_prop = client_page.get("properties", {}).get("Роль", {})
                role = role_prop.get("select", {})
                if role and role.get("name") == "опонент":
                    print(f"  Skipping opponent: {client_name}")
                    continue

                if not chat_id:
                    print(f"  No Telegram Chat ID for {client_name}")
                    no_chatid.append({
                        "client_name": client_name,
                        "case_number": info["case_number"]
                    })
                    continue

                # 4. Send reminder
                message = build_client_message(info)
                try:
                    result = send_message(int(chat_id), message)
                    if result.get("ok"):
                        print(f"  ✅ Sent to {client_name} (chat_id: {int(chat_id)})")
                        reminders_sent.append({
                            "client_name": client_name,
                            "case_number": info["case_number"]
                        })
                    else:
                        error = result.get("description", "Unknown error")
                        print(f"  ❌ Failed for {client_name}: {error}")
                        reminders_failed.append({
                            "client_name": client_name,
                            "case_number": info["case_number"],
                            "error": error
                        })
                except Exception as e:
                    print(f"  ❌ Error sending to {client_name}: {e}")
                    reminders_failed.append({
                        "client_name": client_name,
                        "case_number": info["case_number"],
                        "error": str(e)
                    })

    # 5. Send summary to owner
    if OWNER_CHAT_ID:
        summary = build_owner_summary(reminders_sent, reminders_failed, no_chatid)
        send_message(int(OWNER_CHAT_ID), summary)

    print(f"\nDone: {len(reminders_sent)} sent, {len(no_chatid)} no chat_id, {len(reminders_failed)} failed")


if __name__ == "__main__":
    main()
