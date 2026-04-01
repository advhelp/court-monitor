# ⚖️ Court Hearing Monitor v2

Автоматичний моніторинг судових засідань → Notion + Telegram.

## Що робить

- Двічі на день завантажує CSV з dsa.court.gov.ua (~300 МБ, потоково)
- Фільтрує по номерах справ з config.json
- Нові засідання → Telegram повідомлення + запис у Notion (⚖️ Засідання)
- Старі засідання не дублюються (state.json)

## GitHub Secrets

| Secret | Значення |
|--------|----------|
| TELEGRAM_BOT_TOKEN | Токен бота |
| TELEGRAM_CHAT_ID | ID чату |
| NOTION_TOKEN | ntn_... (Internal integration token) |
| NOTION_DATABASE_ID | 1cb005324ce44910b3a31d7599ba7505 |

## Settings

Settings → Actions → General → Workflow permissions → Read and write permissions
