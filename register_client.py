#!/usr/bin/env python3
"""
Register client Telegram Chat IDs via deep link.

Flow:
1. Client clicks t.me/BOT_USERNAME?start=199_1234_24
2. This script polls getUpdates, finds /start messages
3. Looks up case number in Notion (Кейси АБ)
4. Finds linked client (Клієнти АБ)
5. Writes client's chat_id to Notion

Deep link format: t.me/BOT_USERNAME?start=CASE_NUMBER
  - Slashes in case number replaced with underscores
  - e.g., 199/1234/24 → 199_1234_24
"""

import os
import sys
import json
import requests

# ── Config ────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
OWNER_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")  # your personal chat for notifications

# Notion database IDs
CASES_DB_ID = os.environ.get("NOTION_CASES_DB_ID", "")
CLIENTS_DB_ID = os.environ.get("NOTION_CLIENTS_DB_ID", "")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
NOTION_API = "https://api.notion.com/v1"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

STATE_FILE = "register_state.json"


def load_state():
    """Load last processed update_id."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"last_update_id": 0}


def save_state(state):
    """Save last processed update_id."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_updates(offset=0):
    """Poll Telegram for new messages."""
    params = {"timeout": 0}
    if offset > 0:
        params["offset"] = offset
    resp = requests.get(f"{TELEGRAM_API}/getUpdates", params=params)
    resp.raise_for_status()
    return resp.json().get("result", [])


def send_message(chat_id, text, parse_mode="HTML"):
    """Send a Telegram message."""
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode
    }
    resp = requests.post(f"{TELEGRAM_API}/sendMessage", json=data)
    return resp.json()


def decode_case_number(param):
    """Convert deep link parameter back to case number.
    199_1234_24 → 199/1234/24
    """
    return param.replace("_", "/")


def find_case_by_number(case_number):
    """Search Notion Кейси АБ for a case by case number."""
    data = {
        "filter": {
            "property": "Номер справи",
            "rich_text": {
                "equals": case_number
            }
        },
        "page_size": 1
    }
    resp = requests.post(
        f"{NOTION_API}/databases/{CASES_DB_ID}/query",
        headers=NOTION_HEADERS,
        json=data
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return results[0] if results else None


def get_linked_clients(case_page):
    """Get client page IDs linked to a case via 👤 Клієнти АБ relation."""
    # Try to find the relation property
    props = case_page.get("properties", {})
    client_rel = props.get("👤 Клієнти АБ", {})
    if client_rel.get("type") == "relation":
        return [r["id"] for r in client_rel.get("relation", [])]
    return []


def update_client_chat_id(client_page_id, chat_id):
    """Write Telegram Chat ID to a client record in Notion."""
    data = {
        "properties": {
            "Telegram Chat ID": {
                "number": chat_id
            }
        }
    }
    resp = requests.patch(
        f"{NOTION_API}/pages/{client_page_id}",
        headers=NOTION_HEADERS,
        json=data
    )
    resp.raise_for_status()
    return resp.json()


def get_client_name(client_page_id):
    """Get client name from Notion."""
    resp = requests.get(
        f"{NOTION_API}/pages/{client_page_id}",
        headers=NOTION_HEADERS
    )
    resp.raise_for_status()
    page = resp.json()
    title_prop = page.get("properties", {}).get("ПІБ / назва юрособи", {})
    titles = title_prop.get("title", [])
    return titles[0]["plain_text"] if titles else "Клієнт"


def get_case_title(case_page):
    """Get case title from a case page object."""
    title_prop = case_page.get("properties", {}).get("справа", {})
    titles = title_prop.get("title", [])
    return titles[0]["plain_text"] if titles else "—"


def process_start_command(update):
    """Process a /start command with deep link parameter."""
    message = update.get("message", {})
    text = message.get("text", "")
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    first_name = chat.get("first_name", "")

    if not text.startswith("/start "):
        # /start without parameter — just a greeting
        send_message(
            chat_id,
            "👋 Вітаю! Це бот для нагадувань про судові засідання.\n\n"
            "Якщо ваш адвокат надав вам посилання — натисніть на нього, "
            "щоб підключити нагадування."
        )
        return

    # Extract case number from deep link parameter
    param = text.split("/start ", 1)[1].strip()
    case_number = decode_case_number(param)
    print(f"  Deep link: {param} → case number: {case_number}")

    # Find case in Notion
    case_page = find_case_by_number(case_number)
    if not case_page:
        send_message(
            chat_id,
            f"❌ Справу з номером {case_number} не знайдено.\n"
            "Перевірте посилання або зверніться до вашого адвоката."
        )
        print(f"  Case not found: {case_number}")
        return

    case_title = get_case_title(case_page)

    # Find linked clients
    client_ids = get_linked_clients(case_page)
    if not client_ids:
        send_message(
            chat_id,
            "⚠️ Справу знайдено, але клієнт ще не прив'язаний.\n"
            "Зверніться до вашого адвоката."
        )
        # Notify owner
        if OWNER_CHAT_ID:
            send_message(
                int(OWNER_CHAT_ID),
                f"⚠️ Клієнт {first_name} (chat_id: {chat_id}) натиснув deep link "
                f"для справи {case_number}, але клієнт не прив'язаний до кейсу в Notion."
            )
        print(f"  No clients linked to case {case_number}")
        return

    # Update first linked client with chat_id
    client_id = client_ids[0]
    update_client_chat_id(client_id, chat_id)
    client_name = get_client_name(client_id)

    # Confirm to client
    send_message(
        chat_id,
        f"✅ Нагадування про засідання підключено!\n\n"
        f"📋 Справа: {case_number}\n"
        f"Ви будете отримувати повідомлення за день до кожного засідання.\n\n"
        f"Адвокатське бюро advhelp.online"
    )

    # Notify owner
    if OWNER_CHAT_ID:
        send_message(
            int(OWNER_CHAT_ID),
            f"✅ Клієнт <b>{client_name}</b> підключив нагадування\n"
            f"📋 Справа: {case_number}\n"
            f"💬 Chat ID: {chat_id}"
        )

    print(f"  Registered: {client_name} → chat_id {chat_id} for case {case_number}")


def main():
    if not BOT_TOKEN or not NOTION_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN and NOTION_TOKEN are required")
        sys.exit(1)

    if not CASES_DB_ID:
        print("ERROR: NOTION_CASES_DB_ID is required")
        sys.exit(1)

    state = load_state()
    last_id = state.get("last_update_id", 0)

    print(f"Polling updates (offset: {last_id + 1})...")
    updates = get_updates(offset=last_id + 1 if last_id else 0)
    print(f"Found {len(updates)} new update(s)")

    for update in updates:
        update_id = update.get("update_id", 0)
        message = update.get("message", {})
        text = message.get("text", "")

        if text.startswith("/start"):
            print(f"Processing /start from {message.get('chat', {}).get('first_name', '?')}")
            try:
                process_start_command(update)
            except Exception as e:
                print(f"  ERROR: {e}")

        # Always update last_id
        if update_id > last_id:
            last_id = update_id

    state["last_update_id"] = last_id
    save_state(state)
    print(f"Done. Last update_id: {last_id}")


if __name__ == "__main__":
    main()
