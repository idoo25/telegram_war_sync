# Telegram War Sync

A hardened Telegram translation and sync pipeline built with Telethon and OpenAI.

## What it does
- Live listening from a source Telegram channel to a target Telegram channel
- Hebrew translation with `gpt-5-mini`
- Source-to-target message registry to prevent duplicate publishing
- Edit handling within a configurable time window
- Periodic source reconciliation for missed posts
- Periodic duplicate cleanup in the target channel
- JSON import / repair flows for Telegram export files
- Catch-up republishing for missed posts in chronological order

## Main files
- `telegram_news_pipeline.py`: primary live/import/repair pipeline
- `catchup_republish.py`: catch-up script for missed posts from recent exports
- `tests/`: unit tests with mocks for Telegram/OpenAI flows
- `.env.example`: example configuration

## Setup
```powershell
python -m pip install -r requirements.txt
copy .env.example .env
```

Fill the required values in `.env`.

## Run live mode
```powershell
python telegram_news_pipeline.py live
```

## Run tests
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Notes
- `.env`, Telegram session files, registries, logs, and other local runtime artifacts are intentionally excluded from version control.
- `public_mapping_history_snapshot.json` is a public snapshot of source-to-target message history for archival/reference purposes.
- The separate portable/GUI build is kept local and is not part of this repository.
