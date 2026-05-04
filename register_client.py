#!/usr/bin/env python3
"""
Register client Telegram Chat IDs via deep link.

Flow:
1. Client clicks t.me/BOT_USERNAME?start=199_1234_24
2. This script polls getUpdates, finds /start messages
3. Looks up case number in Notion (Кейси АБ)
4. Finds linked client in Контакти (filtered by role = "клієнт")
5. Writes client's chat_id to Notion
"""

import os
import sys
import json
import requests

# ── Config ────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
OWNER_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"last_update_id": 0}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_updates(offset=0):
    params = {"timeout": 0}
    if offset > 0:
        params["offset"] = offset
    resp = requests.get(f"{TELEGRAM_API}/getUpdates", params=params)
    resp.raise_for_status()
    return resp.json().get("result", [])


def send_message(chat_id, text, parse_mode="HTML"):
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    resp = requests.post(f"{TELEGRAM_API}/sendMessage", json=data)
    return resp.json()


def decode_case_number(param):
    return param.replace("_", "/")


def find_case_by_number(case_number):
    data = {
        "filter": {
            "property": "Номер справи",
            "rich_text": {"equals": case_number}
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


def get_linked_contacts(case_page):
    """Get all contact IDs linked to a case via 'Учасники' relation."""
    props = case_page.get("properties", {})
    client_rel = props.get("Учасники", {})
    if client_rel.get("type") == "relation":
        return [r["id"] for r in client_rel.get("relation", [])]
    return []


def get_contact_info(contact_page_id):
    """Fetch contact page and extract name + role."""
    resp = requests.get(
        f"{NOTION_API}/pages/{contact_page_id}",
        headers=NOTION_HEADERS
    )
    resp.raise_for_status()
    page = resp.json()
    props = page.get("properties", {})

    # Name (title)
    title_prop = props.get("ПІБ / назва юрособи", {})
    titles = title_prop.get("title", [])
    name = titles[0]["plain_text"] if titles else "—"

    # Role (select, NOT multi_select)
    role_prop = props.get("Роль у справі", {})
    role_obj = role_prop.get("select")
    role = role_obj.get("name", "") if role_obj else ""

    return {"name": name, "role": role}


def find_client_among_contacts(contact_ids):
    """Find the contact with role='клієнт' among the linked contacts."""
    for cid in contact_ids:
        info = get_contact_info(cid)
        if info["role"] == "клієнт":
            return {"id": cid, "name": info["name"]}
    return None


def update_client_chat_id(client_page_id, chat_id):
    data = {
        "properties": {
            "Telegram Chat ID": {"number": chat_id}
        }
    }
    resp = requests.patch(
        f"{NOTION_API}/pages/{client_page_id}",
        headers=NOTION_HEADERS,
        json=data
    )
    resp.raise_for_status()
    return resp.json()


def get_case_title(case_page):
    title_prop = case_page.get("properties", {}).get("справа", {})
    titles = title_prop.get("title", [])
    return titles[0]["plain_text"] if titles else "—"


def process_start_command(update):
    message = update.get("message", {})
    text = message.get("text", "")
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    first_name = chat.get("first_name", "")

    if not text.startswith("/start "):
        send_message(
            chat_id,
            "👋 Вітаю! Це бот для нагадувань про судові засідання.\n\n"
            "Якщо ваш адвокат надав вам посилання — натисніть на нього, "
            "щоб підключити нагадування."
        )
        return

    param = text.split("/start ", 1)[1].strip()
    case_number = decode_case_number(param)
    print(f"  Deep link: {param} -> case number: {case_number}")

    # 1. Find case
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

    # 2. Get all contacts linked to this case (field 'Учасники')
    contact_ids = get_linked_contacts(case_page)
    if not contact_ids:
        send_message(
            chat_id,
            "⚠️ Справу знайдено, але учасники ще не прив'язані.\n"
            "Зверніться до вашого адвоката."
        )
        if OWNER_CHAT_ID:
            send_message(
                int(OWNER_CHAT_ID),
                f"⚠️ Клієнт {first_name} (chat_id: {chat_id}) натиснув deep link "
                f"для справи {case_number}, але в полі 'Учасники' немає записів."
            )
        print(f"  No contacts linked to case {case_number}")
        return

    # 3. Find the contact with role='клієнт' (NOT суд, NOT опонент, etc.)
    client = find_client_among_contacts(contact_ids)
    if not client:
        send_message(
            chat_id,
            "⚠️ Справу знайдено, але клієнта з потрібною роллю не знайдено.\n"
            "Зверніться до вашого адвоката."
        )
        if OWNER_CHAT_ID:
            send_message(
                int(OWNER_CHAT_ID),
                f"⚠️ Клієнт {first_name} (chat_id: {chat_id}) натиснув deep link "
                f"для справи {case_number}. В 'Учасники' є контакти, але жоден не має "
                f"роль 'клієнт'. Перевір ролі в Контактах."
            )
        print(f"  No contact with role='клієнт' for case {case_number}")
        return

    # 4. Write chat_id to the correct contact
    update_client_chat_id(client["id"], chat_id)

    send_message(
        chat_id,
        f"✅ Нагадування про засідання підключено!\n\n"
        f"📋 Справа: {case_number}\n"
        f"Ви будете отримувати повідомлення за день до кожного засідання.\n\n"
        f"Адвокатське бюро advhelp.online"
    )

    if OWNER_CHAT_ID:
        send_message(
            int(OWNER_CHAT_ID),
            f"✅ Клієнт <b>{client['name']}</b> підключив нагадування\n"
            f"📋 Справа: {case_number}\n"
            f"💬 Chat ID: {chat_id}"
        )

    print(f"  Registered: {client['name']} -> chat_id {chat_id} for case {case_number}")


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

        if update_id > last_id:
            last_id = update_id

    state["last_update_id"] = last_id
    save_state(state)
    print(f"Done. Last update_id: {last_id}")


if __name__ == "__main__":
    main()
