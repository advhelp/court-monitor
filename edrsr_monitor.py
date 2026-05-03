"""
ЄДРСР Monitor — Моніторинг судових рішень
reyestr.court.gov.ua → Notion (📜 Рішення) + Telegram

Окремий скрипт від court-monitor (CSV ДСА засідання).
Запускається 2 рази на день через GitHub Actions.
"""

import json
import os
import re
import sys
import time
import hashlib
import logging
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlencode

import requests

# ─── Config ───────────────────────────────────────────────────────

EDRSR_URL = "https://reyestr.court.gov.ua/"
EDRSR_REVIEW_URL = "https://reyestr.court.gov.ua/Review/"

# Notion databases
NOTION_DECISIONS_DB = "cd409a96a60849f8b5d864ad6f252ad1"
NOTION_CASES_DB = "272cfd318d33495494243620503615e7"

STATE_FILE = Path(__file__).parent / "edrsr_state.json"

# Delay between requests to avoid rate limiting (seconds)
REQUEST_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("edrsr-monitor")


# ─── HTML Parser for ЄДРСР results ───────────────────────────────

class EDRSRResultParser(HTMLParser):
    """
    Parses ЄДРСР search results HTML table.

    Each result row has cells with CSS classes:
    - RegNumber: registration number + link to /Review/{id}
    - VRType: type of decision (вирок, ухвала, рішення, постанова)
    - RegDate: registration date in registry
    - LawDate: date of decision
    - CSType: form of proceedings (цивільне, кримінальне, etc.)
    - CaseNumber: case number
    - CourtName: court name
    - ChairmenName: judge name
    """

    def __init__(self):
        super().__init__()
        self.results = []
        self._current_row = {}
        self._current_cell_class = None
        self._current_text = ""
        self._current_href = None
        self._in_td = False
        self._in_a = False
        self._known_classes = {
            "RegNumber", "VRType", "RegDate", "LawDate",
            "CSType", "CaseNumber", "CourtName", "ChairmenName",
        }

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)

        if tag == "td":
            css_class = attrs_dict.get("class", "").strip()
            # Classes may have suffixes like "RegNumber tr1"
            matched = None
            for part in css_class.split():
                if part in self._known_classes:
                    matched = part
                    break
            if matched:
                self._in_td = True
                self._current_cell_class = matched
                self._current_text = ""
                self._current_href = None

        elif tag == "a" and self._in_td:
            self._in_a = True
            href = attrs_dict.get("href", "")
            if href:
                self._current_href = href

    def handle_endtag(self, tag):
        if tag == "td" and self._in_td:
            text = self._current_text.strip()
            cls = self._current_cell_class

            if cls:
                self._current_row[cls] = text
                if cls == "RegNumber" and self._current_href:
                    self._current_row["_href"] = self._current_href

            self._in_td = False
            self._current_cell_class = None

        elif tag == "a" and self._in_a:
            self._in_a = False

        elif tag == "tr" and self._current_row:
            if (self._current_row.get("CaseNumber") and
                    self._current_row.get("RegNumber")):
                self.results.append(dict(self._current_row))
            self._current_row = {}

    def handle_data(self, data):
        if self._in_td:
            self._current_text += data


def parse_edrsr_html(html: str) -> list[dict]:
    """Parse ЄДРСР HTML response and return list of decisions."""
    parser = EDRSRResultParser()
    parser.feed(html)
    return parser.results


# ─── ЄДРСР API ───────────────────────────────────────────────────

def warm_session(session: requests.Session):
    """
    GET the main ЄДРСР page to obtain session cookies.
    This helps avoid CAPTCHA triggers on the first POST.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    }
    try:
        r = session.get(EDRSR_URL, headers=headers, timeout=15)
        r.raise_for_status()
        log.info(f"Session warmed, cookies: {list(session.cookies.keys())}")
    except requests.RequestException as e:
        log.warning(f"Session warm-up failed: {e}")


# Counter for debug HTML saves (save first 2 responses for analysis)
_debug_count = 0


def search_edrsr(case_number: str, session: requests.Session) -> list[dict]:
    """
    Search ЄДРСР for decisions by case number.
    Returns list of decision dicts.
    
    Logic: parse results FIRST, then determine why there are none.
    The CAPTCHA modal HTML is always present in the page template,
    so we cannot use its presence as a CAPTCHA indicator.
    """
    global _debug_count

    form_data = {
        "CaseNumber": case_number,
        "Sort": "1",
        "PagingInfo.ItemsPerPage": "100",
        "Liga": "true",
        "ProviderItem": "1",
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": EDRSR_URL,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    }

    try:
        resp = session.post(
            EDRSR_URL,
            data=urlencode(form_data),
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"ЄДРСР request failed for {case_number}: {e}")
        return []

    html = resp.text

    # Save first 2 responses for debugging
    if _debug_count < 2:
        _debug_count += 1
        debug_path = Path(__file__).parent / f"debug_edrsr_{_debug_count}.html"
        try:
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(html)
            log.info(f"Debug HTML saved: {debug_path.name} ({len(html)} chars)")
        except Exception:
            pass

    # Step 1: Try to parse results FIRST
    decisions = parse_edrsr_html(html)

    if decisions:
        # Enrich with full URL and review_id
        for d in decisions:
            href = d.pop("_href", "")
            if href and "/Review/" in href:
                doc_id = href.split("/Review/")[-1].strip().rstrip("/")
                d["review_id"] = doc_id
                d["url"] = f"{EDRSR_REVIEW_URL}{doc_id}"
            elif href:
                d["review_id"] = href.strip("/").split("/")[-1]
                d["url"] = f"{EDRSR_URL.rstrip('/')}{href}"
            else:
                d["review_id"] = ""
                d["url"] = ""
        return decisions

    # Step 2: No results parsed — determine why

    # Legitimate "nothing found"
    if "За заданими параметрами пошуку нічого не знайдено" in html:
        log.info(f"  {case_number}: no decisions")
        return []

    # Check for real CAPTCHA activation:
    # The CAPTCHA modal is ALWAYS in the page template HTML.
    # Real CAPTCHA block = no result rows AND no "nothing found" message
    # AND the page shows the CAPTCHA challenge actively.
    # Best indicator: response is very short (just the page shell without results)
    # or contains a specific CAPTCHA activation JS call.
    if len(html) < 5000:
        log.warning(
            f"CAPTCHA likely active for {case_number} "
            f"(response too short: {len(html)} chars)"
        )
        return []

    # Page returned but no results and no "not found" — could be
    # empty results area or other issue
    log.info(f"  {case_number}: no decisions found (response {len(html)} chars)")
    return []


def decision_uid(decision: dict) -> str:
    """Generate unique ID for a decision."""
    review_id = decision.get("review_id", "")
    if review_id:
        return f"edrsr_{review_id}"
    # Fallback: hash key fields
    key = "|".join([
        decision.get("CaseNumber", ""),
        decision.get("RegNumber", ""),
        decision.get("LawDate", ""),
        decision.get("VRType", ""),
    ])
    return f"edrsr_{hashlib.md5(key.encode()).hexdigest()}"


# ─── Notion: Fetch cases ─────────────────────────────────────────

def fetch_cases_from_notion(token: str) -> dict[str, dict]:
    """
    Query Кейси АБ and return {case_number: {"page_id": ..., "name": ...}}.
    """
    if not token:
        log.warning("Notion token not configured")
        return {}

    cases = {}
    url = f"https://api.notion.com/v1/databases/{NOTION_CASES_DB}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    has_more = True
    start_cursor = None

    while has_more:
        payload = {
            "page_size": 100,
            "filter": {
                "and": [
                    {"property": "Етап", "status": {"does_not_equal": "архів"}},
                    {"property": "Етап", "status": {"does_not_equal": "успіх"}},
                ]
            },
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            if not r.ok:
                log.error(f"Notion API {r.status_code}: {r.text[:500]}")
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.error(f"Failed to query Кейси АБ: {e}")
            return cases

        for page in data.get("results", []):
            page_id = page["id"]
            props = page.get("properties", {})

            # Extract case number
            case_prop = props.get("Номер справи", {})
            rich_text = case_prop.get("rich_text", [])
            if not rich_text:
                continue
            case_num = "".join(
                rt.get("plain_text", "") for rt in rich_text
            ).strip()
            if not case_num:
                continue

            # Extract case name from title property
            case_name = ""
            for prop_name, prop_data in props.items():
                if prop_data.get("type") == "title":
                    title_arr = prop_data.get("title", [])
                    case_name = "".join(
                        t.get("plain_text", "") for t in title_arr
                    ).strip()
                    break

            cases[case_num] = {
                "page_id": page_id,
                "name": case_name or case_num,
            }

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    log.info(f"Fetched {len(cases)} cases from Кейси АБ")
    return cases


# ─── Notion: Create decision ─────────────────────────────────────

def parse_date_ua(date_str: str) -> str | None:
    """Parse Ukrainian date format (dd.mm.yyyy) to ISO."""
    date_str = date_str.strip()
    for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def map_decision_type(vr_type: str) -> str | None:
    """Map ЄДРСР VRType to Notion select value."""
    vr_lower = vr_type.lower().strip()
    mapping = {
        "вирок": "вирок",
        "ухвала": "ухвала",
        "рішення": "рішення",
        "постанова": "постанова",
        "судовий наказ": "судовий наказ",
        "окрема ухвала": "окрема ухвала",
        "окрема думка": "окрема думка",
        "додаткове рішення": "додаткове рішення",
    }
    for key, val in mapping.items():
        if key in vr_lower:
            return val
    return vr_type.strip() if vr_type.strip() else None


def map_form(cs_type: str) -> str | None:
    """Map ЄДРСР CSType to Notion 'Форма судочинства' select."""
    cs_lower = cs_type.lower().strip()
    mapping = {
        "цивільн": "цивільне",
        "кримінал": "кримінальне",
        "адмін": "адміністративне",
        "господар": "господарське",
    }
    for key, val in mapping.items():
        if key in cs_lower:
            return val
    return None


def create_notion_decision(
    token: str,
    decision: dict,
    case_page_id: str | None = None,
    case_name: str | None = None,
) -> bool:
    """Create a page in 📜 Рішення database, linked to Кейси АБ."""
    if not token:
        return False

    case_num = decision.get("CaseNumber", "").strip()
    vr_type = decision.get("VRType", "").strip()
    law_date = decision.get("LawDate", "").strip()
    cs_type = decision.get("CSType", "").strip()
    court = decision.get("CourtName", "").strip()
    judge = decision.get("ChairmenName", "").strip()
    url = decision.get("url", "")

    # Build title
    type_display = vr_type if vr_type else "Документ"
    display_name = case_name if case_name else case_num
    title = f"{type_display} — {display_name}"
    if law_date:
        title += f" ({law_date})"

    # Properties matching 📜 Рішення schema
    props = {
        "Документ": {
            "title": [{"text": {"content": title[:2000]}}]
        },
        "Номер справи": {
            "rich_text": [{"text": {"content": case_num}}]
        },
        "Джерело": {
            "select": {"name": "auto: ЄДРСР"}
        },
    }

    # Relation to Кейси АБ
    if case_page_id:
        props["Кейс"] = {"relation": [{"id": case_page_id}]}

    # Date of decision
    iso_date = parse_date_ua(law_date) if law_date else None
    if iso_date:
        props["Дата документа"] = {"date": {"start": iso_date}}

    # Type of decision
    decision_type = map_decision_type(vr_type)
    if decision_type:
        props["Тип документа"] = {"select": {"name": decision_type}}

    # Form of proceedings
    form = map_form(cs_type)
    if form:
        props["Форма судочинства"] = {"select": {"name": form}}

    # Text fields
    for prop, val in [("Суд", court), ("Суддя", judge)]:
        if val:
            props[prop] = {"rich_text": [{"text": {"content": val[:2000]}}]}

    # URL to ЄДРСР
    if url:
        props["Посилання ЄДРСР"] = {"url": url}

    try:
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28",
            },
            json={
                "parent": {"database_id": NOTION_DECISIONS_DB},
                "icon": {"type": "emoji", "emoji": "📜"},
                "properties": props,
            },
            timeout=15,
        )
        if r.status_code == 200:
            log.info(f"Notion: created decision for {case_num} ({vr_type})")
            return True
        else:
            log.warning(f"Notion error {r.status_code}: {r.text[:300]}")
            return False
    except requests.RequestException as e:
        log.error(f"Notion failed: {e}")
        return False


# ─── Telegram ────────────────────────────────────────────────────

def send_telegram(token: str, chat_id: str, msg: str):
    if not token or not chat_id:
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        r.raise_for_status()
        log.info("Telegram: sent")
    except requests.RequestException as e:
        log.error(f"Telegram failed: {e}")


def format_decision_tg(decision: dict, case_name: str | None = None) -> str:
    """Format decision for Telegram notification."""
    case_num = decision.get("CaseNumber", "—")
    vr_type = decision.get("VRType", "—")
    law_date = decision.get("LawDate", "—")
    court = decision.get("CourtName", "—")
    judge = decision.get("ChairmenName", "—")
    url = decision.get("url", "")

    case_display = case_name if case_name else case_num

    msg = f"📜 <b>Нове рішення</b>\n"
    msg += f"📋 <b>{case_display}</b>\n"
    if case_name:
        msg += f"📁 Справа: <code>{case_num}</code>\n"
    msg += f"📄 Тип: {vr_type}\n"
    msg += f"📅 Дата: {law_date}\n"
    msg += f"🏛 Суд: {court}\n"
    msg += f"👨‍⚖️ Суддя: {judge}\n"
    if url:
        msg += f'🔗 <a href="{url}">Переглянути на ЄДРСР</a>'

    return msg


# ─── State management ────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"decisions": {}, "last_run": None}


def save_state(state: dict):
    state["last_run"] = datetime.now().isoformat()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ─── Main ────────────────────────────────────────────────────────
def decision_exists_in_notion(token: str, review_id: str) -> bool:
    try:
        r = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_DECISIONS_DB}/query",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28",
            },
            json={
                "filter": {
                    "property": "Посилання ЄДРСР",
                    "url": {"equals": f"https://reyestr.court.gov.ua/Review/{review_id}"}
                },
                "page_size": 1,
            },
            timeout=10,
        )
        return len(r.json().get("results", [])) > 0
    except Exception:
        return False
def main():
    log.info("=" * 50)
    log.info("ЄДРСР Decision Monitor v3")
    log.info("=" * 50)

    # Load config from environment (GitHub Secrets)
    notion_token = os.environ.get("NOTION_TOKEN", "")
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")

    if not notion_token:
        log.error("NOTION_TOKEN not set!")
        sys.exit(1)

    state = load_state()
    known_decisions = state.get("decisions", {})

    # Fetch cases from Notion
    cases_map = fetch_cases_from_notion(notion_token)
    if not cases_map:
        log.error("No cases found in Кейси АБ!")
        sys.exit(1)

    log.info(f"Checking {len(cases_map)} cases against ЄДРСР...")

    session = requests.Session()
    warm_session(session)
    time.sleep(2)

    total_found = 0
    total_new = 0
    captcha_cases = []

    for case_num, case_info in cases_map.items():
        case_page_id = case_info.get("page_id")
        case_name = case_info.get("name")

        log.info(f"Searching ЄДРСР: {case_num}")

        decisions = search_edrsr(case_num, session)

        if not decisions:
            time.sleep(REQUEST_DELAY)
            continue

        total_found += len(decisions)
        log.info(f"  {case_num}: found {len(decisions)} decisions")

        for d in decisions:
            uid = decision_uid(d)

            if uid in known_decisions:
                continue

            total_new += 1
            log.info(f"  NEW: {d.get('VRType', '?')} from {d.get('LawDate', '?')}")

            # Save to state
            known_decisions[uid] = {
                "case": case_num,
                "type": d.get("VRType", ""),
                "date": d.get("LawDate", ""),
                "seen": datetime.now().isoformat(),
            }
            state["decisions"] = known_decisions
            save_state(state)
            # Перевірка дубля в Notion
            review_id = d.get("review_id", "")
            if review_id and decision_exists_in_notion(notion_token, review_id):
                log.info(f"  вже існує в Notion, пропускаємо")
                continue
            # Write to Notion
            create_notion_decision(
                notion_token, d,
                case_page_id=case_page_id,
                case_name=case_name,
            )

            # Telegram notification
            send_telegram(
                tg_token, tg_chat,
                format_decision_tg(d, case_name=case_name),
            )

            time.sleep(0.5)

        # Respectful delay between ЄДРСР requests
        time.sleep(REQUEST_DELAY)

    session.close()

    # Summary
    log.info("=" * 50)
    log.info(f"Summary: {total_found} decisions found, {total_new} new")
    log.info(f"Known decisions in state: {len(known_decisions)}")
    log.info("=" * 50)

    if total_new > 0:
        summary = (
            f"📊 <b>ЄДРСР Monitor:</b> перевірено {len(cases_map)} справ\n"
            f"📜 Знайдено {total_found} рішень, з них <b>{total_new} нових</b>"
        )
        send_telegram(tg_token, tg_chat, summary)

    # Save state
    state["decisions"] = known_decisions
    save_state(state)


if __name__ == "__main__":
    main()
