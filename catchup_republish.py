#!/usr/bin/env python3
"""Catch-up republisher for Telegram export JSON (parallel translate + ordered publish)."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from telegram_news_pipeline import (
    DEFAULT_ENV_FILE,
    DEFAULT_MESSAGE_REGISTRY_FILE,
    OpenAITranslator,
    StatePersistenceError,
    WorkItem,
    apply_env_file,
    ensure_channel_access,
    ensure_sent_message_id,
    extract_text,
    get_media_path,
    is_mostly_english,
    load_message_registry,
    load_progress,
    lookup_registered_target_message,
    persist_message_registry,
    persist_registry_and_progress,
    publish_message,
    register_target_message,
    save_progress,
    start_telegram_client,
)

DEFAULT_JSON_PATH = r"C:\Users\Gary\Downloads\Telegram Desktop\ChatExport_2026-03-10 (1)\result.json"
DEFAULT_MIN_DATE = "2026-03-08"
WHITESPACE_RE = re.compile(r"\s+")


def parse_min_date(raw_value: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except Exception as exc:
        raise ValueError(f"Invalid --min-date format: {raw_value}. Use YYYY-MM-DD") from exc


def extract_message_date(msg: Dict) -> Optional[date]:
    raw = str(msg.get("date", "")).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        return None


def marker_for_dedupe(text: str) -> str:
    normalized = WHITESPACE_RE.sub(" ", text).strip().casefold()
    return normalized[:120]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Catch-up publisher from Telegram export JSON (parallel translation mode)."
    )
    parser.add_argument("--json-file", default=DEFAULT_JSON_PATH)
    parser.add_argument("--target-channel", type=int, default=0)
    parser.add_argument("--source-channel", type=int, default=0)
    parser.add_argument("--session", default="")
    parser.add_argument("--existing-limit", type=int, default=1500)
    parser.add_argument("--min-date", default=DEFAULT_MIN_DATE, help="Format: YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=30)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--progress-file", default="progress_catchup.txt")
    parser.add_argument("--registry-file", default=DEFAULT_MESSAGE_REGISTRY_FILE)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    apply_env_file(Path(DEFAULT_ENV_FILE))

    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"Error: JSON file not found at {json_path}")
        return

    try:
        min_date = parse_min_date(args.min_date)
    except ValueError as exc:
        print(str(exc))
        return

    api_id_raw = os.getenv("TG_API_ID", "").strip()
    api_hash = os.getenv("TG_API_HASH", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
    session_name = (args.session or os.getenv("TG_SESSION", "user_live_session")).strip()
    auth_mode = os.getenv("TG_AUTH_MODE", "user").strip() or "user"
    bot_token = os.getenv("TG_BOT_TOKEN", "").strip() or None

    target_channel = args.target_channel
    if target_channel == 0:
        target_channel = int(os.getenv("TARGET_CHANNEL", "0") or "0")

    source_channel = args.source_channel
    if source_channel == 0:
        source_channel = int(os.getenv("SOURCE_CHANNEL", "0") or "0")
    source_channel_id = source_channel if source_channel != 0 else None

    if not api_id_raw or not api_hash or not openai_key or target_channel == 0:
        print("Missing required settings: TG_API_ID, TG_API_HASH, OPENAI_API_KEY, TARGET_CHANNEL")
        return

    api_id = int(api_id_raw)
    progress_file = Path(args.progress_file) if args.progress_file else None
    registry_file = Path(args.registry_file) if args.registry_file else None
    last_id = load_progress(progress_file)
    registry_map = load_message_registry(registry_file, source_channel_id=source_channel_id)

    with json_path.open(encoding="utf-8") as f:
        data = json.load(f)

    messages = data.get("messages", [])
    messages.sort(key=lambda m: int(m.get("id", 0)))
    print(f"Loaded {len(messages)} messages from JSON.")

    client = await start_telegram_client(
        session_name=session_name,
        api_id=api_id,
        api_hash=api_hash,
        bot_token=bot_token,
        auth_mode=auth_mode,
    )

    try:
        await ensure_channel_access(client, target_channel, "target")
        print(
            f"Fetching last {args.existing_limit} messages from target channel ({target_channel}) "
            "to check for duplicates..."
        )
        existing_marker_targets: Dict[str, int] = {}
        async for msg in client.iter_messages(target_channel, limit=args.existing_limit):
            if msg.message:
                marker = marker_for_dedupe(msg.message)
                if len(marker) > 5 and getattr(msg, "id", None) is not None:
                    existing_marker_targets.setdefault(marker, int(msg.id))

        print(f"Fetched {len(existing_marker_targets)} duplicate markers from target channel.")

        translator = OpenAITranslator(api_key=openai_key, model=model)

        pending = []
        skipped_old = 0
        skipped_not_eng = 0
        skipped_empty = 0
        skipped_progress = 0
        skipped_registry = 0

        for msg in messages:
            if msg.get("type") != "message":
                continue

            msg_id = int(msg.get("id", 0))
            if msg_id <= 0:
                continue

            if last_id > 0 and msg_id <= last_id:
                skipped_progress += 1
                continue

            if lookup_registered_target_message(registry_map, msg_id, source_channel_id) is not None:
                skipped_registry += 1
                continue

            msg_date = extract_message_date(msg)
            if not msg_date or msg_date < min_date:
                skipped_old += 1
                continue

            orig_text = extract_text(msg).strip()
            if orig_text and not is_mostly_english(orig_text):
                skipped_not_eng += 1
                continue

            media = get_media_path(msg, json_path.parent)
            if not orig_text and not media:
                skipped_empty += 1
                continue

            pending.append(
                WorkItem(
                    id=msg_id,
                    original_text=orig_text,
                    media_path=media,
                    source_published_at=str(msg.get("date", "")).strip() or None,
                )
            )

        if not pending:
            print("No pending messages to process.")
            print("\n--- Catch-up Summary ---")
            print(f"Total Published (>={min_date.isoformat()}): 0")
            print(f"Skipped (Old < {min_date.isoformat()}): {skipped_old}")
            print("Skipped (Duplicates): 0")
            print(f"Skipped (Non-English): {skipped_not_eng}")
            print(f"Skipped (Empty): {skipped_empty}")
            print(f"Skipped (Already in progress file): {skipped_progress}")
            print(f"Skipped (Already in registry): {skipped_registry}")
            print("Errors: 0")
            return

        print(f"Found {len(pending)} pending messages after filters.")

        translated_buffer: Dict[int, str] = {}
        queue: asyncio.Queue[Optional[WorkItem]] = asyncio.Queue()
        worker_count = max(1, min(args.workers, len(pending)))

        for item in pending:
            queue.put_nowait(item)
        for _ in range(worker_count):
            queue.put_nowait(None)

        async def translator_worker(worker_id: int) -> None:
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return
                try:
                    final_text = item.original_text
                    if item.original_text:
                        print(f"[Worker {worker_id}] Translating {item.id}...")
                        translated = await translator.translate(item.original_text)
                        final_text = (translated if translated else item.original_text).strip()
                    translated_buffer[item.id] = final_text
                except Exception as exc:
                    print(f"[Worker {worker_id}] Error translating {item.id}: {exc}")
                    translated_buffer[item.id] = item.original_text
                finally:
                    queue.task_done()

        async def publisher_worker() -> None:
            published = 0
            skipped_dupes = 0
            error_cnt = 0

            for item in pending:
                while item.id not in translated_buffer:
                    await asyncio.sleep(0.2)

                final_text = translated_buffer.pop(item.id)
                marker = marker_for_dedupe(final_text) if final_text else ""
                target_message_id = existing_marker_targets.get(marker) if len(marker) > 5 else None
                is_duplicate = target_message_id is not None

                if is_duplicate:
                    print(f"[{item.id}] DUPLICATE detected. Skipping.")
                    skipped_dupes += 1
                    if not args.dry_run and register_target_message(
                        registry_map,
                        item.id,
                        target_message_id,
                        source_channel_id=source_channel_id,
                        published_at=item.source_published_at,
                    ) is not None:
                        persist_registry_and_progress(
                            registry_file=registry_file,
                            registry_map=registry_map,
                            dry_run=False,
                            context=f"Persisting duplicate mapping for source message {item.id}",
                        )
                    continue

                try:
                    sent_result = await publish_message(
                        client,
                        target_channel,
                        text=final_text,
                        media=item.media_path,
                        dry_run=args.dry_run,
                    )
                    if args.dry_run:
                        print(f"[{item.id}] Dry run publish simulated.")
                        continue

                    sent_message_id = ensure_sent_message_id(
                        sent_result,
                        f"Catch-up published source message {item.id}",
                    )
                    register_target_message(
                        registry_map,
                        item.id,
                        sent_message_id,
                        source_channel_id=source_channel_id,
                        published_at=item.source_published_at,
                    )
                    persist_registry_and_progress(
                        registry_file=registry_file,
                        registry_map=registry_map,
                        progress_file=progress_file,
                        progress_message_id=item.id,
                        dry_run=False,
                        context=f"Persisting catch-up state for source message {item.id}",
                    )
                    print(f"[{item.id}] Published successfully.")
                    published += 1
                    if len(marker) > 5:
                        existing_marker_targets.setdefault(marker, sent_message_id)
                    if args.delay > 0:
                        await asyncio.sleep(args.delay)
                except StatePersistenceError:
                    raise
                except Exception as exc:
                    print(f"[{item.id}] Error publishing: {exc}")
                    error_cnt += 1

            print("\n--- Catch-up Summary ---")
            print(f"Total Published (>={min_date.isoformat()}): {published}")
            print(f"Skipped (Old < {min_date.isoformat()}): {skipped_old}")
            print(f"Skipped (Duplicates): {skipped_dupes}")
            print(f"Skipped (Non-English): {skipped_not_eng}")
            print(f"Skipped (Empty): {skipped_empty}")
            print(f"Skipped (Already in progress file): {skipped_progress}")
            print(f"Skipped (Already in registry): {skipped_registry}")
            print(f"Errors: {error_cnt}")

        workers = [asyncio.create_task(translator_worker(i + 1)) for i in range(worker_count)]
        publisher = asyncio.create_task(publisher_worker())

        await asyncio.gather(*workers, publisher)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())






