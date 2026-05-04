"""
Court Hearing Monitor v10 — Моніторинг судових засідань
CSV ДСА → фільтрація → Notion (⚖️ Засідання) + Telegram
"""

import csv
import json
import os
import sys
import hashlib
import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import requests

# ─── Config ───────────────────────────────────────────────────────

CSV_URL_DSA = (
    "https://dsa.court.gov.ua/storage/portal/open_data_files/"
    "91509/513/8faabdb91244be394947eb26f2153a1f.csv"
)
CSV_URL_DATA_GOV = (
    "https://data.gov.ua/dataset/42eaff6e-45da-4426-b4a1-f30989bfd36f/"
    "resource/98d6ba0d-1c18-4835-ae68-bfc0af724bfa/download/"
    "spisok-sprav-priznachenih-do-rozglyadu.csv"
)

CONFIG_FILE = Path(__file__).parent / "config.json"
STATE_FILE = Path(__file__).parent / "state.json"

# Notion database ID for ⚖️ Засідання
NOTION_HEARINGS_DB = "1cb005324ce44910b3a31d7599ba7505"

# Кейси АБ database + data source IDs
NOTION_CASES_DB = "272cfd318d33495494243620503615e7"
NOTION_CASES_DS = "5a8d544a-1fef-432e-b086-06ece834653d"

# Column name candidates (CSV format may vary)
CASE_NUMBER_COLS = [
    "case", "Номер справи", "номер справи", "case_number",
    "Номер_справи", "НОМЕР СПРАВИ", "№ справи",
]
DATE_COLS = [
    "date", "Дата засідання", "дата засідання", "Дата", "дата",
    "hearing_date", "Дата/Час", "Дата_засідання",
]
TIME_COLS = [
    "time", "Час засідання", "час засідання", "Час", "час",
    "hearing_time", "Час_засідання",
]
JUDGE_COLS = [
    "judges", "judge", "Суддя", "суддя", "Суддя-доповідач",
    "Головуючий суддя", "Склад суду",
]
COURT_COLS = [
    "court_name", "Суд", "суд", "Назва суду", "court",
    "Найменування суду",
]
SUBJECT_COLS = [
    "case_description", "Предмет позову", "предмет позову",
    "Предмет", "Опис", "subject", "Обвинувачення",
]
HALL_COLS = [
    "court_room", "Зал", "зал", "Номер залу", "hall",
    "Зал судового засідання",
]
INVOLVED_COLS = [
    "case_involved", "Сторони", "сторони", "parties",
]
FORM_COLS = [
    "Форма судочинства", "форма судочинства",
    "Форма", "form", "jurisdiction_form",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("court-monitor")


# ─── Helpers ──────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_FILE.exists():
        log.error(f"Config not found: {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
    # Env vars override config (for GitHub Secrets)
    for key in [
        "telegram_bot_token", "telegram_chat_id",
        "notion_token", "notion_database_id",
    ]:
        env_key = key.upper()
        if os.environ.get(env_key):
            config[key] = os.environ[env_key]
    # Default Notion DB to hardcoded value
    if not config.get("notion_database_id"):
        config["notion_database_id"] = NOTION_HEARINGS_DB
    return config


def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"hearings": {}, "last_run": None}


def save_state(state: dict):
    state["last_run"] = datetime.now().isoformat()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def find_column(headers: list[str], candidates: list[str]) -> str | None:
    headers_lower = [h.strip().lower() for h in headers]
    for c in candidates:
        if c.lower() in headers_lower:
            return headers[headers_lower.index(c.lower())].strip()
    return None


def hearing_id(row: dict, cm: dict) -> str:
    key = f"{row.get(cm['case'], '')}|{row.get(cm['date'], '')}"
    if cm["time"]:
        key += f"|{row.get(cm['time'], '')}"
    return hashlib.md5(key.encode()).hexdigest()


# ─── CSV Download & Parse ────────────────────────────────────────

def download_and_filter(case_numbers: list[str]) -> tuple[list[dict], dict]:
    case_set = set(cn.strip() for cn in case_numbers)
    log.info(f"Monitoring {len(case_set)} cases")

    for url in [CSV_URL_DSA, CSV_URL_DATA_GOV]:
        try:
            log.info(f"Downloading: {url[:60]}...")
            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            log.warning(f"Failed: {e}")
            continue
    else:
        log.error("All CSV sources failed!")
        return [], {}

    # Detect encoding
    enc = "utf-8"
    ct = resp.headers.get("content-type", "")
    if "1251" in ct or "windows" in ct:
        enc = "windows-1251"

    # Read header
    lines = resp.iter_lines(decode_unicode=False)
    raw_header = next(lines)
    for try_enc in [enc, "utf-8", "windows-1251", "cp1251"]:
        try:
            header_text = raw_header.decode(try_enc)
            enc = try_enc
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        header_text = raw_header.decode("utf-8", errors="replace")

    delim = "\t" if "\t" in header_text else (";" if ";" in header_text else ",")
    log.info(f"Detected delimiter: {'TAB' if delim == chr(9) else delim}")
    headers = [
        h.strip().strip("\ufeff").strip('"').strip("'")
        for h in next(csv.reader(StringIO(header_text), delimiter=delim))
    ]
    log.info(f"Columns ({len(headers)}): {headers[:8]}...")

    # Save headers for debug
    with open(Path(__file__).parent / "debug_headers.json", "w", encoding="utf-8") as f:
        json.dump(headers, f, ensure_ascii=False, indent=2)

    cm = {
        "case": find_column(headers, CASE_NUMBER_COLS),
        "date": find_column(headers, DATE_COLS),
        "time": find_column(headers, TIME_COLS),
        "judge": find_column(headers, JUDGE_COLS),
        "court": find_column(headers, COURT_COLS),
        "subject": find_column(headers, SUBJECT_COLS),
        "hall": find_column(headers, HALL_COLS),
        "form": find_column(headers, FORM_COLS),
    }

    if not cm["case"]:
        log.error(f"Case number column not found! Headers: {headers}")
        return [], {}
    log.info(f"Mapping: {cm}")

    # Today (for filtering past hearings)
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Stream & filter
    matches = []
    skipped_past = 0
    total = 0
    for raw in lines:
        total += 1
        if total % 500_000 == 0:
            log.info(f"  {total:,} rows, {len(matches)} matches...")
        try:
            line = raw.decode(enc, errors="replace")
        except Exception:
            continue
        if not any(cn in line for cn in case_set):
            continue
        try:
            vals = next(csv.reader(StringIO(line), delimiter=delim))
            row = dict(zip(headers, [v.strip().strip('"') for v in vals]))
        except Exception:
            continue
        if row.get(cm["case"], "").strip() not in case_set:
            continue

        # Filter: skip hearings in the past
        date_val = row.get(cm["date"], "").strip() if cm["date"] else ""
        iso_date, _ = parse_date(date_val) if date_val else (None, None)
        if iso_date and iso_date < today_str:
            skipped_past += 1
            continue

        matches.append(row)

    log.info(f"Done: {total:,} rows, {len(matches)} matches, {skipped_past} past skipped")
    return matches, cm


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


def format_tg_message(row: dict, cm: dict, case_name: str | None = None) -> str:
    g = lambda k: row.get(cm[k], "").strip() if cm[k] else "—"
    case_display = case_name if case_name else g('case')
    msg = f"⚖️ <b>Нове засідання</b>\n📋 <b>{case_display}</b>"
    if case_name:
        msg += f"\n📁 Справа: <code>{g('case')}</code>"
    msg += f"\n📅 Дата: <b>{g('date')}</b>"
    if cm["time"] and g("time"):
        msg += f" о <b>{g('time')}</b>"
    msg += f"\n🏛 Суд: {g('court')}\n👨‍⚖️ Суддя: {g('judge')}"
    if cm["hall"] and g("hall"):
        msg += f"\n🚪 Зал: {g('hall')}"
    if cm["subject"] and g("subject"):
        msg += f"\n📝 {g('subject')[:200]}"
    return msg


# ─── Notion ──────────────────────────────────────────────────────

# Global cache: page_id -> case title (populated by fetch_cases_from_notion)
_case_title_cache: dict[str, str] = {}


def fetch_cases_from_notion(token: str) -> dict[str, dict] | None:
    """
    Query Кейси АБ database and return dict: {case_number: {"page_id": ..., "name": ...}}.
    Also populates _case_title_cache as a side effect for fast title lookups.
    Only cases with non-empty 'Номер справи' field are included.
    """
    global _case_title_cache
    _case_title_cache.clear()

    if not token:
        log.warning("Notion token not configured, no cases fetched")
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
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.error(f"Failed to query Кейси АБ: {e}")
            return None

        for page in data.get("results", []):
            page_id = page["id"]
            # Normalized version (no dashes) for cache lookup
            page_id_norm = page_id.replace("-", "")
            props = page.get("properties", {})

            # Extract case number
            case_prop = props.get("Номер справи", {})
            rich_text = case_prop.get("rich_text", [])
            if not rich_text:
                continue
            case_num = "".join(rt.get("plain_text", "") for rt in rich_text).strip()
            if not case_num:
                continue

            # Extract case name from title property "справа"
            case_name = ""
            for prop_name, prop_data in props.items():
                if prop_data.get("type") == "title":
                    title_arr = prop_data.get("title", [])
                    case_name = "".join(t.get("plain_text", "") for t in title_arr).strip()
                    break

            cases[case_num] = {
                "page_id": page_id,
                "name": case_name or case_num,
            }

            # Populate global cache (used by ICS feed generator)
            # Use normalized id (no dashes) as key
            if case_name:
                _case_title_cache[page_id_norm] = case_name

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    log.info(f"Fetched {len(cases)} cases from Кейси АБ ({len(_case_title_cache)} with names)")
    return cases


def parse_date(date_str: str) -> tuple[str | None, str | None]:
    """Parse date string to ISO format. Returns (date, time)."""
    date_str = date_str.strip()
    # Try datetime formats first (date + time in one field)
    for fmt in ["%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")
        except ValueError:
            continue
    # Date only
    for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d"), None
        except ValueError:
            continue
    return None, None


def map_form(form_str: str) -> str | None:
    """Map CSV form value to Notion select option."""
    form_lower = form_str.lower().strip()
    mapping = {
        "цивільн": "цивільне",
        "кримінал": "кримінальне",
        "адмін": "адміністративне",
        "господар": "господарське",
        "купап": "купап",
        "адміністративн": "адміністративне",
    }
    for key, val in mapping.items():
        if key in form_lower:
            return val
    return None

def hearing_exists_in_notion(token: str, db_id: str, case_num: str, date_str: str) -> bool:
    """Перевіряє чи засідання вже є в Notion по справі і даті."""
    try:
        iso_date, _ = parse_date(date_str) if date_str else (None, None)
        if not iso_date:
            return False
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28",
            },
            json={
                "filter": {
                    "and": [
                        {"property": "Номер справи", "rich_text": {"equals": case_num}},
                        {"property": "Дата засідання", "date": {"equals": iso_date}},
                    ]
                },
                "page_size": 1,
            },
            timeout=10,
        )
        return len(r.json().get("results", [])) > 0
    except Exception:
        return False


def create_notion_hearing(token: str, db_id: str, row: dict, cm: dict, case_page_id: str | None = None, case_name: str | None = None):
    """Create a page in ⚖️ Засідання database, linked to Кейси АБ via relation."""
    if not token or not db_id:
        log.warning("Notion not configured")
        return

    g = lambda k: row.get(cm[k], "").strip() if cm[k] else ""

    case_num = g("case")
    date_str = g("date")
    time_str = g("time")
    court = g("court")
    judge = g("judge")
    hall = g("hall")
    subject = g("subject")
    form = g("form")

    # Build title — use case name if available, fallback to case number
    display_name = case_name if case_name else f"Засідання {case_num}"
    title = display_name

    # Properties matching ⚖️ Засідання schema
    props = {
        "Подія": {
            "title": [{"text": {"content": title}}]
        },
        "Номер справи": {
            "rich_text": [{"text": {"content": case_num}}]
        },
        "Статус": {
            "select": {"name": "заплановано"}
        },
        "Джерело": {
            "select": {"name": "auto: CSV ДСА"}
        },
    }

    # Link to case in Кейси АБ via relation
    if case_page_id:
        props["Кейс"] = {"relation": [{"id": case_page_id}]}

    # Date
    iso_date, parsed_time = parse_date(date_str) if date_str else (None, None)
    if iso_date:
        props["Дата засідання"] = {"date": {"start": iso_date}}
    # Use parsed time from date field if no separate time column
    if parsed_time and not time_str:
        time_str = parsed_time

    # Text fields
    for prop, val in [
        ("Суд", court),
        ("Суддя", judge),
        ("Зал", hall),
        ("Предмет", subject[:2000] if subject else ""),
        ("Час", time_str),
    ]:
        if val:
            props[prop] = {"rich_text": [{"text": {"content": val}}]}

    # Form of proceedings

    # Create page
    try:
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28",
            },
            json={
                "parent": {"database_id": db_id},
                "icon": {"type": "emoji", "emoji": "⚖️"},
                "properties": props,
            },
            timeout=15,
        )
        if r.status_code == 200:
            log.info(f"Notion: created hearing for {case_num}")
        else:
            log.warning(f"Notion error {r.status_code}: {r.text[:300]}")
    except requests.RequestException as e:
        log.error(f"Notion failed: {e}")


# ─── ICS Calendar Feed ────────────────────────────────────────────

def ics_escape(text: str) -> str:
    """Escape special characters in iCalendar text fields."""
    if not text:
        return ""
    # Order matters: backslash first
    text = text.replace("\\", "\\\\")
    text = text.replace(";", "\\;")
    text = text.replace(",", "\\,")
    text = text.replace("\n", "\\n")
    text = text.replace("\r", "")
    return text


def ics_fold_line(line: str) -> str:
    """Fold long lines per RFC 5545 (max 75 octets per line)."""
    if len(line.encode("utf-8")) <= 75:
        return line
    # Split into 73-char chunks (leaving room for CRLF + space)
    result = []
    current = ""
    for char in line:
        test = current + char
        if len(test.encode("utf-8")) > 73:
            result.append(current)
            current = " " + char  # Continuation lines start with space
        else:
            current = test
    if current:
        result.append(current)
    return "\r\n".join(result)


def fetch_case_title(token: str, page_id: str) -> str:
    """Fetch a single case page and extract its title."""
    if not token or not page_id:
        return ""
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
    }
    try:
        r = requests.get(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=headers,
            timeout=15,
        )
        if r.status_code != 200:
            return ""
        data = r.json()
        props = data.get("properties", {})
        # Find the title property (type=title)
        for prop_name, prop_data in props.items():
            if prop_data.get("type") == "title":
                title_arr = prop_data.get("title", [])
                return "".join(t.get("plain_text", "") for t in title_arr).strip()
    except requests.RequestException:
        pass
    return ""


def delete_past_hearings(token: str, db_id: str) -> int:
    """Delete (archive) hearings with dates before today."""
    if not token or not db_id:
        return 0

    deleted = 0
    has_more = True
    start_cursor = None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    # Find all past hearings
    while has_more:
        payload = {
            "page_size": 100,
            "filter": {
                "property": "Дата засідання",
                "date": {"before": datetime.now().strftime("%Y-%m-%d")},
            },
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            r = requests.post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                headers=headers,
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.error(f"Failed to query past hearings: {e}")
            return deleted

        for page in data.get("results", []):
            page_id = page["id"]
            # Archive the page
            try:
                ar = requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=headers,
                    json={"archived": True},
                    timeout=10,
                )
                if ar.status_code == 200:
                    deleted += 1
                else:
                    log.warning(f"Failed to archive hearing {page_id}: {ar.status_code}")
            except requests.RequestException as e:
                log.error(f"Error archiving hearing {page_id}: {e}")

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    if deleted:
        log.info(f"Deleted {deleted} past hearings from Notion")
    else:
        log.info("No past hearings to delete")
    return deleted


def delete_archived_case_hearings(
    token: str,
    db_id: str,
    active_case_numbers: set[str],
    state: dict,
) -> int:
    """Delete hearings linked to archived/completed cases.

    Compares all hearings in Notion against the set of active case numbers.
    If a hearing's case number is NOT in the active set, it means the case
    has been archived/completed — so the hearing is deleted.

    SAFETY: uses dynamic threshold based on previous successful run.
    Skips cleanup if active cases dropped >50% since last run.
    """
    if not token or not db_id or not active_case_numbers:
        return 0

    # SAFETY: dynamic threshold based on previous run.
    # Protects against API errors, partial pagination, accidental mass-archive.
    current_count = len(active_case_numbers)
    last_count = state.get("last_active_cases_count")

    if last_count is None:
        # First run ever — record baseline, skip cleanup just to be safe
        log.info(
            f"First run: recording baseline of {current_count} active cases. "
            f"Cleanup will start from next run."
        )
        state["last_active_cases_count"] = current_count
        return 0

    drop_ratio = (last_count - current_count) / last_count if last_count > 0 else 0

    if drop_ratio > 0.5:
        log.warning(
            f"Active cases dropped sharply: {last_count} -> {current_count} "
            f"({drop_ratio*100:.0f}% decrease). Skipping cleanup to prevent "
            f"mass deletion. If this is intentional, run will normalize next time."
        )
        # Update baseline so next run can resume normally
        state["last_active_cases_count"] = current_count
        return 0

    # Normal operation — update baseline and proceed
    state["last_active_cases_count"] = current_count

    deleted = 0
    has_more = True
    start_cursor = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            r = requests.post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                headers=headers,
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.error(f"Failed to query hearings for archive cleanup: {e}")
            return deleted

        for page in data.get("results", []):
            page_id = page["id"]
            props = page.get("properties", {})

            # Get case number from hearing
            case_prop = props.get("Номер справи", {})
            rich_text = case_prop.get("rich_text", [])
            case_num = "".join(rt.get("plain_text", "") for rt in rich_text).strip()

            if not case_num:
                continue

            # If case number is NOT among active cases — delete hearing
            if case_num not in active_case_numbers:
                try:
                    ar = requests.patch(
                        f"https://api.notion.com/v1/pages/{page_id}",
                        headers=headers,
                        json={"archived": True},
                        timeout=10,
                    )
                    if ar.status_code == 200:
                        deleted += 1
                        log.info(f"  Archived hearing for inactive case {case_num}")
                    else:
                        log.warning(f"Failed to archive hearing {page_id}: {ar.status_code}")
                except requests.RequestException as e:
                    log.error(f"Error archiving hearing {page_id}: {e}")

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    if deleted:
        log.info(f"Deleted {deleted} hearings for archived/completed cases")
    else:
        log.info("No hearings for archived cases to delete")
    return deleted


def fetch_future_hearings_from_notion(token: str, db_id: str) -> list[dict]:
    """Query ⚖️ Засідання database for all future hearings."""
    if not token or not db_id:
        return []

    today_iso = datetime.now().strftime("%Y-%m-%d")
    hearings = []
    has_more = True
    start_cursor = None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    while has_more:
        payload = {
            "page_size": 100,
            "filter": {
                "property": "Дата засідання",
                "date": {"on_or_after": today_iso},
            },
            "sorts": [{"property": "Дата засідання", "direction": "ascending"}],
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            r = requests.post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                headers=headers,
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.error(f"Failed to fetch hearings: {e}")
            return hearings

        for page in data.get("results", []):
            props = page.get("properties", {})

            def get_text(prop_name: str) -> str:
                p = props.get(prop_name, {})
                rt = p.get("rich_text") or p.get("title") or []
                return "".join(t.get("plain_text", "") for t in rt).strip()

            def get_select(prop_name: str) -> str:
                p = props.get(prop_name, {})
                sel = p.get("select")
                return sel.get("name", "") if sel else ""

            def get_date(prop_name: str) -> str:
                p = props.get(prop_name, {})
                d = p.get("date")
                return d.get("start", "") if d else ""

            def get_relation(prop_name: str) -> list[str]:
                p = props.get(prop_name, {})
                rel = p.get("relation", [])
                # Normalize: strip dashes to match cache keys
                return [r["id"].replace("-", "") for r in rel if "id" in r]

            # Resolve case title from cache (populated by fetch_cases_from_notion)
            case_ids = get_relation("Кейс")
            case_title = ""
            if case_ids:
                case_title = _case_title_cache.get(case_ids[0], "")
                if not case_title:
                    case_title = fetch_case_title(token, case_ids[0])

            hearings.append({
                "id": page["id"],
                "title": get_text("Подія"),
                "case": get_text("Номер справи"),
                "case_title": case_title,
                "date": get_date("Дата засідання"),
                "time": get_text("Час"),
                "court": get_text("Суд"),
                "judge": get_text("Суддя"),
                "hall": get_text("Зал"),
                "subject": get_text("Предмет"),
                "status": get_select("Статус"),
                "url": page.get("url", ""),
            })

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    log.info(f"Fetched {len(hearings)} future hearings from Notion")
    return hearings


def generate_ics_feed(token: str, db_id: str) -> bool:
    """Generate docs/hearings.ics file with all future hearings."""
    hearings = fetch_future_hearings_from_notion(token, db_id)
    if not hearings:
        log.info("No hearings to export to ICS")
        # Still create empty calendar so file exists
        hearings = []

    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Court Monitor//AdvHelp//UA",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:Судові засідання",
        "X-WR-CALDESC:Автоматичний моніторинг засідань ДСА",
        "X-WR-TIMEZONE:Europe/Kiev",
        "REFRESH-INTERVAL;VALUE=DURATION:PT1H",
        "X-PUBLISHED-TTL:PT1H",
    ]

    for h in hearings:
        if not h["date"]:
            continue

        # Parse date
        try:
            date_obj = datetime.strptime(h["date"], "%Y-%m-%d")
        except ValueError:
            continue

        # Parse time if present
        start_dt = None
        end_dt = None
        if h["time"]:
            try:
                # Try HH:MM
                hour, minute = h["time"].split(":")[:2]
                start_dt = date_obj.replace(hour=int(hour), minute=int(minute))
                end_dt = start_dt + timedelta(hours=1)
            except (ValueError, IndexError):
                pass

        # Build UID stable across runs
        uid = f"hearing-{h['id']}@court-monitor.advhelp"

        # Build SUMMARY: prefer case title from Кейси АБ (linked case name)
        case_short = h["case"] or "—"
        case_title = h.get("case_title", "").strip()
        if case_title and case_short and case_short != "—":
            summary = f"⚖️ {case_title} — {case_short}"
        elif case_title:
            summary = f"⚖️ {case_title}"
        else:
            summary = f"⚖️ Засідання {case_short}"

        # Build DESCRIPTION
        desc_parts = []
        if h["case"]:
            desc_parts.append(f"Справа: {h['case']}")
        if h["judge"]:
            desc_parts.append(f"Суддя: {h['judge']}")
        if h["hall"]:
            desc_parts.append(f"Зал: {h['hall']}")
        if h["subject"]:
            desc_parts.append(f"Предмет: {h['subject']}")
        if h["url"]:
            desc_parts.append(f"Notion: {h['url']}")
        description = "\\n".join(ics_escape(p) for p in desc_parts)

        # Location = court name
        location = ics_escape(h["court"]) if h["court"] else ""

        lines.append("BEGIN:VEVENT")
        lines.append(f"UID:{uid}")
        lines.append(f"DTSTAMP:{now_utc}")

        if start_dt and end_dt:
            # Floating time (no timezone) — calendar app uses local
            lines.append(f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}")
            lines.append(f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%S')}")
        else:
            # All-day event
            lines.append(f"DTSTART;VALUE=DATE:{date_obj.strftime('%Y%m%d')}")
            next_day = date_obj + timedelta(days=1)
            lines.append(f"DTEND;VALUE=DATE:{next_day.strftime('%Y%m%d')}")

        lines.append(ics_fold_line(f"SUMMARY:{ics_escape(summary)}"))
        if location:
            lines.append(ics_fold_line(f"LOCATION:{location}"))
        if description:
            lines.append(ics_fold_line(f"DESCRIPTION:{description}"))

        lines.append("STATUS:CONFIRMED")
        lines.append("TRANSP:OPAQUE")

        # Reminder 1 day before
        lines.append("BEGIN:VALARM")
        lines.append("ACTION:DISPLAY")
        lines.append("DESCRIPTION:Завтра судове засідання")
        lines.append("TRIGGER:-P1D")
        lines.append("END:VALARM")

        # Reminder 1 hour before
        lines.append("BEGIN:VALARM")
        lines.append("ACTION:DISPLAY")
        lines.append("DESCRIPTION:Засідання за годину")
        lines.append("TRIGGER:-PT1H")
        lines.append("END:VALARM")

        lines.append("END:VEVENT")

    lines.append("END:VCALENDAR")

    # Write to docs/hearings.ics
    docs_dir = Path(__file__).parent / "docs"
    docs_dir.mkdir(exist_ok=True)
    ics_path = docs_dir / "hearings.ics"

    # Use CRLF as required by RFC 5545
    content = "\r\n".join(lines) + "\r\n"

    with open(ics_path, "w", encoding="utf-8", newline="") as f:
        f.write(content)

    log.info(f"ICS feed written: {ics_path} ({len(hearings)} events)")
    return True


def notify_calendar_url_once(config: dict, state: dict) -> bool:
    """Send Telegram message with calendar URL — only once per repo."""
    if state.get("calendar_url_sent"):
        return False

    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if "/" not in repo:
        log.info("GITHUB_REPOSITORY not set, skipping calendar URL notification")
        return False

    owner, repo_name = repo.split("/", 1)
    calendar_url = f"https://{owner}.github.io/{repo_name}/hearings.ics"

    msg = (
        "🎉 <b>Календар засідань готовий!</b>\n\n"
        "Підпишись на нього у своєму календарі — і всі засідання автоматично там з'являться:\n\n"
        f"<code>{calendar_url}</code>\n\n"
        "📱 <b>iPhone:</b> відкрий це посилання в Safari → "
        "Підписатись на календар\n\n"
        "🌐 <b>Google Calendar:</b> calendar.google.com → "
        "ліворуч «+» → «З URL» → встав посилання\n\n"
        "💡 Засідання оновлюються 2 рази на день автоматично."
    )

    send_telegram(
        config.get("telegram_bot_token", ""),
        config.get("telegram_chat_id", ""),
        msg,
    )

    state["calendar_url_sent"] = True
    log.info(f"Calendar URL sent to Telegram: {calendar_url}")
    return True


# ─── Main ────────────────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info("Court Hearing Monitor v10 (cleanup archived cases)")
    log.info("=" * 50)

    config = load_config()
    state = load_state()

    # Fetch cases from Notion Кейси АБ (primary source)
    notion_token = config.get("notion_token", "")
    cases_map = fetch_cases_from_notion(notion_token)

    # SAFETY: API failure must abort, not fall through
    if cases_map is None:
        log.error("Notion API failed — aborting run to prevent data corruption")
        send_telegram(
            config.get("telegram_bot_token", ""),
            config.get("telegram_chat_id", ""),
            "🚨 <b>Court Monitor:</b> Notion API недоступний. "
            "Запуск зупинено щоб не видалити дані. Перевір токен і назви полів.",
        )
        sys.exit(1)

    # SAFETY: empty result is suspicious — skip cleanup, don't process CSV
    if not cases_map:
        log.warning("Notion returned 0 cases — skipping run, no cleanup performed")
        save_state(state)
        return

    case_list = list(cases_map.keys())
    active_case_numbers = set(case_list)

    # Clean up past hearings from Notion (keeps rollup accurate)
    hearings_db = config.get("notion_database_id", NOTION_HEARINGS_DB)
    delete_past_hearings(notion_token, hearings_db)

    # Clean up hearings for archived/completed cases
    delete_archived_case_hearings(notion_token, hearings_db, active_case_numbers, state)

    rows, cm = download_and_filter(case_list)

    if not cm:
        send_telegram(
            config.get("telegram_bot_token", ""),
            config.get("telegram_chat_id", ""),
            "⚠️ <b>Court Monitor:</b> Не вдалося розпізнати колонки CSV. "
            "Перевірте debug_headers.json",
        )
        sys.exit(1)

    if not rows:
        log.info("No hearings found in CSV")
        # Still regenerate ICS in case Notion has manually added hearings
        log.info("Generating ICS calendar feed...")
        generate_ics_feed(
            notion_token,
            config.get("notion_database_id", NOTION_HEARINGS_DB),
        )
        notify_calendar_url_once(config, state)
        save_state(state)
        return

    # Filter out past hearings - keep only today and future
    today = datetime.now().date()
    future_rows = []
    skipped_past = 0
    for row in rows:
        date_str = row.get(cm["date"], "") if cm["date"] else ""
        iso_date, _ = parse_date(date_str) if date_str else (None, None)
        if iso_date:
            try:
                row_date = datetime.strptime(iso_date, "%Y-%m-%d").date()
                if row_date < today:
                    skipped_past += 1
                    continue
            except ValueError:
                pass
        future_rows.append(row)

    log.info(f"Filtered: {len(rows)} total, {skipped_past} past skipped, {len(future_rows)} future kept")
    rows = future_rows

    if not rows:
        log.info("No future hearings found")
        # Still regenerate ICS in case Notion has manually added hearings
        log.info("Generating ICS calendar feed...")
        generate_ics_feed(
            notion_token,
            config.get("notion_database_id", NOTION_HEARINGS_DB),
        )
        notify_calendar_url_once(config, state)
        save_state(state)
        return

    # Find new hearings
    known = state.get("hearings", {})
    new_items = []

    for row in rows:
        hid = hearing_id(row, cm)
        if hid not in known:
            new_items.append(row)
            known[hid] = {
                "case": row.get(cm["case"], ""),
                "date": row.get(cm["date"], "") if cm["date"] else "",
                "seen": datetime.now().isoformat(),
            }

    log.info(f"Total: {len(rows)}, New: {len(new_items)}")

    if new_items:
        for row in new_items:
            case_num = row.get(cm["case"], "").strip()
            case_info = cases_map.get(case_num) or {}
            case_page_id = case_info.get("page_id")
            case_name = case_info.get("name")

            # Telegram notification
            send_telegram(
                config.get("telegram_bot_token", ""),
                config.get("telegram_chat_id", ""),
                format_tg_message(row, cm, case_name=case_name),
            )
            # Перевірка дубля в Notion
            date_val = row.get(cm["date"], "") if cm["date"] else ""
            if hearing_exists_in_notion(notion_token, config.get("notion_database_id", NOTION_HEARINGS_DB), case_num, date_val):
                log.info(f"  вже існує в Notion {case_num}, пропускаємо")
            else:
                # Notion: create hearing page with relation to case
                create_notion_hearing(
                    notion_token,
                    config.get("notion_database_id", NOTION_HEARINGS_DB),
                    row,
                    cm,
                    case_page_id=case_page_id,
                    case_name=case_name,
                )

    # Clean old entries (90 days)
    cutoff = (datetime.now() - timedelta(days=90)).isoformat()
    state["hearings"] = {
        k: v for k, v in known.items()
        if v.get("seen", "9999") > cutoff
    }

    # Generate ICS calendar feed for all future hearings in Notion
    log.info("Generating ICS calendar feed...")
    generate_ics_feed(
        notion_token,
        config.get("notion_database_id", NOTION_HEARINGS_DB),
    )

    # Notify about calendar URL (only once)
    notify_calendar_url_once(config, state)
    save_state(state)

    log.info("Done!")


if __name__ == "__main__":
    main()
