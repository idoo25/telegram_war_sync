#!/usr/bin/env python3
"""
Unified Telegram translation pipeline.

Combines:
1) Live relay from source channel to target channel.
2) JSON export import (full or English-only repair mode) with resume support.
3) Fixing already-published English messages in the target channel.
4) Quick export stats utility.

Use environment variables (recommended):
- TG_API_ID / API_ID
- TG_API_HASH / API_HASH
- TG_BOT_TOKEN / BOT_TOKEN
- TG_AUTH_MODE (auto|bot|user)
- OPENAI_API_KEY
- TARGET_CHANNEL
- SOURCE_CHANNEL
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI
from telethon import TelegramClient, events

SYSTEM_PROMPT = (
    "You are a professional Hebrew news editor. "
    "Translate the news report from English to Hebrew. "
    "Maintain the exact format (dashes, emojis, line breaks). "
    "Keep all URLs and @mentions as they are. "
    "Use natural, professional Hebrew (e.g., 'interview' -> 'ריאיון'). "
    "Perform proofreading to ensure no grammar errors. "
    "Output ONLY the translated text, nothing else."
)

ENG_RE = re.compile(r"[a-zA-Z]")
HEB_RE = re.compile(r"[א-ת]")
DUPLICATE_TEXT_CLEAN_RE = re.compile(r"[\u200b-\u200f\ufeff]")
LOGGER = logging.getLogger("telegram-news-pipeline")
DEFAULT_ENV_FILE = ".env"
DEFAULT_MESSAGE_REGISTRY_FILE = "message_registry.json"
LEGACY_LIVE_STATE_FILE = "live_message_map.json"
DEFAULT_MESSAGE_REGISTRY_BACKUP_SUFFIX = ".bak"
DEFAULT_LIVE_STATE_FILE = DEFAULT_MESSAGE_REGISTRY_FILE
DEFAULT_HEARTBEAT_FILE = "live_heartbeat.json"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
DEFAULT_EDIT_WINDOW_HOURS = 24
DEFAULT_SOURCE_RECONCILE_INTERVAL_MINUTES = 5.0
DEFAULT_SOURCE_RECONCILE_LOOKBACK_HOURS = 24.0
DEFAULT_TARGET_DEDUPE_SCAN_INTERVAL_MINUTES = 5.0
DEFAULT_TARGET_DEDUPE_LOOKBACK_HOURS = 24.0
DEFAULT_TARGET_DEDUPE_CLUSTER_GAP_MINUTES = 60.0
MIN_TARGET_DEDUPE_TEXT_LENGTH = 24


class StatePersistenceError(RuntimeError):
    pass


@dataclass
class WorkItem:
    id: int
    original_text: str
    media_path: Optional[Path]
    source_published_at: Optional[str] = None

def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def apply_env_file(path: Path, overwrite: bool = False) -> Dict[str, str]:
    values = load_env_file(path)
    for key, value in values.items():
        if overwrite or os.getenv(key) in (None, ""):
            os.environ[key] = value
    return values


def load_json_file(path: Path, strict: bool = False) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if strict:
            raise ValueError(f"Failed parsing JSON file {path}: {exc}") from exc
        return {}
    if not isinstance(data, dict):
        if strict:
            raise ValueError(f"JSON file {path} must contain an object at the top level.")
        return {}
    return data


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding=encoding,
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_file.write(content)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def build_backup_snapshot_path(
    path: Path, suffix: str = DEFAULT_MESSAGE_REGISTRY_BACKUP_SUFFIX
) -> Path:
    return path.with_name(f"{path.name}{suffix}")


def create_backup_snapshot(
    path: Path, suffix: str = DEFAULT_MESSAGE_REGISTRY_BACKUP_SUFFIX
) -> Optional[Path]:
    if not path.exists():
        return None

    backup_path = build_backup_snapshot_path(path, suffix=suffix)
    atomic_write_text(backup_path, path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def load_json_file_with_backup(
    path: Path, backup_path: Optional[Path] = None
) -> Dict[str, Any]:
    resolved_backup_path = backup_path or build_backup_snapshot_path(path)

    if not path.exists():
        if not resolved_backup_path.exists():
            return {}
        LOGGER.warning("Primary registry file %s is missing. Loading backup %s instead.", path, resolved_backup_path)
        data = load_json_file(resolved_backup_path, strict=True)
    else:
        try:
            return load_json_file(path, strict=True)
        except ValueError as primary_exc:
            if not resolved_backup_path.exists():
                raise
            LOGGER.warning(
                "Failed parsing primary registry %s (%s). Loading backup %s instead.",
                path,
                primary_exc,
                resolved_backup_path,
            )
            data = load_json_file(resolved_backup_path, strict=True)

    try:
        save_json_file(path, data, create_backup=False)
        LOGGER.info("Restored primary registry %s from backup %s", path, resolved_backup_path)
    except Exception as restore_exc:
        LOGGER.warning(
            "Loaded registry from backup %s but failed restoring primary %s: %s",
            resolved_backup_path,
            path,
            restore_exc,
        )
    return data


def save_json_file(path: Path, data: Dict[str, Any], create_backup: bool = False) -> None:
    if create_backup:
        try:
            create_backup_snapshot(path)
        except Exception as exc:
            LOGGER.warning("Failed creating backup snapshot for %s: %s", path, exc)
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def persist_live_heartbeat(
    path: Optional[Path], payload: Dict[str, Any], dry_run: bool = False
) -> None:
    if not path or dry_run:
        return
    try:
        save_json_file(path, payload, create_backup=False)
    except Exception as exc:
        LOGGER.warning("Failed writing live heartbeat %s: %s", path, exc)


def ensure_sent_message_id(sent_result: Any, context: str) -> int:
    sent_message_id = extract_sent_message_id(sent_result)
    if sent_message_id is None:
        raise StatePersistenceError(f"{context}: Telegram did not return a target message id.")
    return sent_message_id


def persist_registry_and_progress(
    registry_file: Optional[Path],
    registry_map: Dict[str, Dict[str, Any]],
    progress_file: Optional[Path] = None,
    progress_message_id: Optional[int] = None,
    dry_run: bool = False,
    context: str = "state persistence",
) -> None:
    if dry_run:
        return
    try:
        persist_message_registry(registry_file, registry_map, dry_run=False)
        if progress_file is not None and progress_message_id is not None:
            save_progress(progress_file, progress_message_id)
    except Exception as exc:
        raise StatePersistenceError(f"{context}: {exc}") from exc


def extract_sent_message_id(sent_result: Any) -> Optional[int]:
    if sent_result is None:
        return None
    if isinstance(sent_result, list):
        for item in sent_result:
            message_id = getattr(item, "id", None)
            if message_id is not None:
                return int(message_id)
        return None
    message_id = getattr(sent_result, "id", None)
    return int(message_id) if message_id is not None else None


def build_source_message_key(
    source_message_id: int, source_channel_id: Optional[int] = None
) -> str:
    message_text = str(int(source_message_id))
    if source_channel_id is None:
        return message_text
    return f"{int(source_channel_id)}:{message_text}"


def parse_datetime_value(raw_value: Any) -> Optional[datetime]:
    if raw_value in (None, ""):
        return None

    if isinstance(raw_value, datetime):
        dt_value = raw_value
    elif isinstance(raw_value, (int, float)):
        dt_value = datetime.fromtimestamp(float(raw_value), tz=timezone.utc)
    else:
        text_value = str(raw_value).strip()
        if not text_value:
            return None
        if text_value.endswith("Z"):
            text_value = text_value[:-1] + "+00:00"
        try:
            dt_value = datetime.fromisoformat(text_value)
        except ValueError:
            return None

    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc)


def format_datetime_value(raw_value: Any) -> Optional[str]:
    dt_value = parse_datetime_value(raw_value)
    if dt_value is None:
        return None
    return dt_value.isoformat()


def normalize_message_id_list(raw_value: Any) -> List[int]:
    if raw_value is None:
        return []
    if not isinstance(raw_value, (list, tuple, set)):
        raw_items = [raw_value]
    else:
        raw_items = list(raw_value)

    message_ids: List[int] = []
    seen_ids = set()
    for item in raw_items:
        text_value = str(item).strip()
        if not text_value.lstrip("-").isdigit():
            continue
        message_id = int(text_value)
        if message_id in seen_ids:
            continue
        seen_ids.add(message_id)
        message_ids.append(message_id)
    return message_ids


def extract_registered_target_message_ids(record: Optional[Dict[str, Any]]) -> List[int]:
    if not isinstance(record, dict):
        return []
    target_ids = normalize_message_id_list(record.get("known_target_message_ids"))
    primary_target_id = record.get("target_message_id")
    if primary_target_id is not None:
        primary_list = normalize_message_id_list([primary_target_id])
        for message_id in primary_list:
            if message_id not in target_ids:
                target_ids.append(message_id)
    return target_ids


def build_target_to_source_index(
    message_map: Dict[str, Dict[str, Any]]
) -> Dict[str, str]:
    target_to_source: Dict[str, str] = {}
    for source_key, record in message_map.items():
        for target_message_id in extract_registered_target_message_ids(record):
            target_to_source[str(int(target_message_id))] = source_key
    return target_to_source


def normalize_registry_record(raw_value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_value, dict):
        target_value = (
            raw_value.get("target_message_id")
            or raw_value.get("target_id")
            or raw_value.get("message_id")
        )
    else:
        target_value = raw_value

    target_text = str(target_value).strip()
    if not target_text.lstrip("-").isdigit():
        return None

    primary_target_id = int(target_text)
    record: Dict[str, Any] = {"target_message_id": primary_target_id}
    published_at = None
    if isinstance(raw_value, dict):
        published_at = raw_value.get("published_at") or raw_value.get("source_published_at")
        known_target_ids = extract_registered_target_message_ids(raw_value)
    else:
        known_target_ids = [primary_target_id]

    if primary_target_id not in known_target_ids:
        known_target_ids.append(primary_target_id)
    if known_target_ids:
        record["known_target_message_ids"] = known_target_ids

    published_value = format_datetime_value(published_at)
    if published_value is not None:
        record["published_at"] = published_value
    return record


def load_message_registry(
    path: Optional[Path], source_channel_id: Optional[int] = None
) -> Dict[str, Dict[str, Any]]:
    if not path:
        return {}

    registry_path = path
    if not registry_path.exists() and registry_path.name == DEFAULT_MESSAGE_REGISTRY_FILE:
        legacy_path = registry_path.with_name(LEGACY_LIVE_STATE_FILE)
        if legacy_path.exists():
            registry_path = legacy_path

    data = load_json_file_with_backup(registry_path)
    raw_map = data.get("source_to_target", {}) if isinstance(data, dict) else {}
    message_map: Dict[str, Dict[str, Any]] = {}

    for raw_source_key, raw_target_value in raw_map.items():
        source_key = str(raw_source_key).strip()
        record = normalize_registry_record(raw_target_value)
        if record is None:
            continue
        if (
            source_channel_id is not None
            and ":" not in source_key
            and source_key.lstrip("-").isdigit()
        ):
            source_key = build_source_message_key(int(source_key), source_channel_id)
        message_map[source_key] = record

    return message_map


def persist_message_registry(
    path: Optional[Path], message_map: Dict[str, Dict[str, Any]], dry_run: bool = False
) -> None:
    if not path or dry_run:
        return
    save_json_file(
        path,
        {
            "source_to_target": message_map,
            "target_to_source": build_target_to_source_index(message_map),
        },
        create_backup=True,
    )


def lookup_registered_target_record(
    message_map: Dict[str, Dict[str, Any]],
    source_message_id: int,
    source_channel_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    return message_map.get(build_source_message_key(source_message_id, source_channel_id))


def lookup_registered_target_message(
    message_map: Dict[str, Dict[str, Any]],
    source_message_id: int,
    source_channel_id: Optional[int] = None,
) -> Optional[int]:
    record = lookup_registered_target_record(message_map, source_message_id, source_channel_id)
    if not record:
        return None
    target_value = record.get("target_message_id")
    return int(target_value) if target_value is not None else None


def is_edit_within_window(
    record: Optional[Dict[str, Any]],
    edit_timestamp: Any,
    max_age_hours: float,
) -> bool:
    if record is None:
        return False
    if max_age_hours <= 0:
        return True

    published_at = parse_datetime_value(record.get("published_at"))
    if published_at is None:
        return True

    edit_time = parse_datetime_value(edit_timestamp) or datetime.now(timezone.utc)
    return edit_time <= published_at + timedelta(hours=max_age_hours)


def register_target_message(
    message_map: Dict[str, Dict[str, Any]],
    source_message_id: int,
    target_message_id: Optional[int],
    source_channel_id: Optional[int] = None,
    published_at: Any = None,
) -> Optional[str]:
    if target_message_id is None:
        return None
    source_key = build_source_message_key(source_message_id, source_channel_id)
    existing_record = message_map.get(source_key, {})
    known_target_ids = extract_registered_target_message_ids(existing_record)
    if int(target_message_id) not in known_target_ids:
        known_target_ids.append(int(target_message_id))

    record: Dict[str, Any] = {
        "target_message_id": int(target_message_id),
        "known_target_message_ids": known_target_ids,
    }
    published_value = format_datetime_value(published_at)
    if published_value is None and isinstance(existing_record, dict):
        published_value = format_datetime_value(existing_record.get("published_at"))
    if published_value is not None:
        record["published_at"] = published_value
    message_map[source_key] = record
    return source_key


def set_registry_primary_target_message(
    message_map: Dict[str, Dict[str, Any]],
    source_key: str,
    target_message_id: int,
) -> bool:
    record = message_map.get(source_key)
    if not isinstance(record, dict):
        return False

    known_target_ids = [message_id for message_id in extract_registered_target_message_ids(record) if message_id != int(target_message_id)]
    known_target_ids.append(int(target_message_id))
    record["target_message_id"] = int(target_message_id)
    record["known_target_message_ids"] = [int(target_message_id)]
    return True


def repoint_registry_target_message(
    message_map: Dict[str, Dict[str, Any]],
    old_target_message_id: int,
    new_target_message_id: int,
) -> int:
    updated = 0
    for record in message_map.values():
        if not isinstance(record, dict):
            continue
        known_target_ids = extract_registered_target_message_ids(record)
        if int(old_target_message_id) not in known_target_ids:
            continue
        rebuilt_target_ids = [
            int(new_target_message_id) if message_id == int(old_target_message_id) else message_id
            for message_id in known_target_ids
        ]
        deduped_target_ids: List[int] = []
        for message_id in rebuilt_target_ids:
            if message_id not in deduped_target_ids:
                deduped_target_ids.append(message_id)
        record["known_target_message_ids"] = deduped_target_ids
        if int(record.get("target_message_id", 0)) == int(old_target_message_id):
            record["target_message_id"] = int(new_target_message_id)
        updated += 1
    return updated


def normalize_duplicate_text(text: str) -> str:
    cleaned_text = DUPLICATE_TEXT_CLEAN_RE.sub("", text or "")
    return re.sub(r"\s+", " ", cleaned_text).strip().casefold()


def extract_message_text(message: Any) -> str:
    for attr_name in ("message", "raw_text", "text"):
        value = getattr(message, attr_name, None)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def extract_message_media_key(message: Any) -> Optional[str]:
    direct_candidates = (
        ("photo", getattr(message, "photo", None)),
        ("document", getattr(message, "document", None)),
    )
    for prefix, candidate in direct_candidates:
        candidate_id = getattr(candidate, "id", None)
        if candidate_id is not None:
            return f"{prefix}:{candidate_id}"

    media_obj = getattr(message, "media", None)
    media_candidates = (
        ("photo", getattr(media_obj, "photo", None)),
        ("document", getattr(media_obj, "document", None)),
    )
    for prefix, candidate in media_candidates:
        candidate_id = getattr(candidate, "id", None)
        if candidate_id is not None:
            return f"{prefix}:{candidate_id}"

    return None


def build_target_dedupe_signature(message: Any) -> Optional[str]:
    normalized_text = normalize_duplicate_text(extract_message_text(message))
    media_key = extract_message_media_key(message)
    if not normalized_text and not media_key:
        return None
    if media_key is None and len(normalized_text) < MIN_TARGET_DEDUPE_TEXT_LENGTH:
        return None
    signature_payload = f"{normalized_text}|{media_key or ''}"
    return hashlib.sha256(signature_payload.encode("utf-8")).hexdigest()


def plan_registry_duplicate_deletions(
    messages: List[Any],
    message_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    recent_by_id = {
        int(getattr(message, "id", 0)): message
        for message in messages
        if getattr(message, "id", None) is not None
    }
    plans: List[Dict[str, Any]] = []

    for source_key, record in message_map.items():
        target_ids = [
            target_id
            for target_id in extract_registered_target_message_ids(record)
            if target_id in recent_by_id
        ]
        if len(target_ids) < 2:
            continue
        target_messages = [recent_by_id[target_id] for target_id in target_ids]
        sorted_messages = sorted(
            target_messages,
            key=lambda item: (
                parse_datetime_value(getattr(item, "date", None)) or datetime.min.replace(tzinfo=timezone.utc),
                int(getattr(item, "id", 0) or 0),
            ),
        )
        plans.append(
            {
                "mode": "registry",
                "source_key": source_key,
                "keep": sorted_messages[-1],
                "delete": sorted_messages[:-1],
            }
        )

    return plans


def plan_recent_duplicate_deletions(
    messages: List[Any],
    lookback_hours: float,
    cluster_gap_minutes: float,
) -> List[Dict[str, Any]]:
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max(lookback_hours, 0.0))
    grouped_messages: Dict[str, List[Any]] = {}

    for message in messages:
        message_time = parse_datetime_value(getattr(message, "date", None))
        if message_time is None or message_time < cutoff_time:
            continue
        signature = build_target_dedupe_signature(message)
        if signature is None:
            continue
        grouped_messages.setdefault(signature, []).append(message)

    plans: List[Dict[str, Any]] = []
    cluster_gap = timedelta(minutes=max(cluster_gap_minutes, 0.0))

    for signature, group in grouped_messages.items():
        if len(group) < 2:
            continue
        sorted_group = sorted(
            group,
            key=lambda item: (
                parse_datetime_value(getattr(item, "date", None)) or cutoff_time,
                int(getattr(item, "id", 0) or 0),
            ),
        )
        current_cluster: List[Any] = []

        for item in sorted_group:
            item_time = parse_datetime_value(getattr(item, "date", None)) or cutoff_time
            if not current_cluster:
                current_cluster = [item]
                continue

            previous_time = (
                parse_datetime_value(getattr(current_cluster[-1], "date", None)) or cutoff_time
            )
            if cluster_gap_minutes <= 0 or item_time - previous_time <= cluster_gap:
                current_cluster.append(item)
                continue

            if len(current_cluster) > 1:
                plans.append(
                    {
                        "mode": "content_fallback",
                        "signature": signature,
                        "keep": current_cluster[-1],
                        "delete": current_cluster[:-1],
                    }
                )
            current_cluster = [item]

        if len(current_cluster) > 1:
            plans.append(
                {
                    "mode": "content_fallback",
                    "signature": signature,
                    "keep": current_cluster[-1],
                    "delete": current_cluster[:-1],
                }
            )

    return plans


async def reconcile_recent_source_messages(
    client: TelegramClient,
    source_channel: int,
    lookback_hours: float,
    process_message: Any,
) -> Dict[str, int]:
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max(lookback_hours, 0.0))
    recent_messages: List[Any] = []

    async for message in client.iter_messages(source_channel, limit=None):
        message_time = parse_datetime_value(getattr(message, "date", None))
        if message_time is None:
            continue
        if message_time < cutoff_time:
            break
        recent_messages.append(message)

    processed = 0
    skipped_empty = 0
    for message in reversed(recent_messages):
        if not extract_message_text(message) and not getattr(message, "media", None):
            skipped_empty += 1
            continue
        await process_message(message)
        processed += 1

    summary = {
        "scanned": len(recent_messages),
        "processed": processed,
        "skipped_empty": skipped_empty,
    }
    LOGGER.info(
        "Source reconciliation summary: scanned=%s | processed=%s | skipped_empty=%s",
        summary["scanned"],
        summary["processed"],
        summary["skipped_empty"],
    )
    return summary


async def delete_target_message(
    client: TelegramClient,
    target_channel: int,
    message_id: int,
    dry_run: bool,
    max_retries: int = 3,
) -> None:
    if dry_run:
        LOGGER.info("[DRY RUN] Would delete message %s from %s", message_id, target_channel)
        return

    for attempt in range(1, max_retries + 1):
        try:
            await client.delete_messages(target_channel, [message_id])
            return
        except Exception as exc:
            wait_s = 2 * attempt
            if hasattr(exc, "seconds"):
                wait_s = max(wait_s, int(getattr(exc, "seconds")) + 1)
            if attempt == max_retries:
                raise
            LOGGER.warning(
                "Delete error (attempt %s/%s) for message %s: %s. Retrying in %ss",
                attempt,
                max_retries,
                message_id,
                exc,
                wait_s,
            )
            await asyncio.sleep(wait_s)


async def sweep_recent_target_duplicates(
    client: TelegramClient,
    target_channel: int,
    message_map: Dict[str, Dict[str, Any]],
    registry_file: Optional[Path],
    lookback_hours: float,
    cluster_gap_minutes: float,
    dry_run: bool,
    action_lock: Optional[asyncio.Lock] = None,
) -> Dict[str, int]:
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max(lookback_hours, 0.0))
    recent_messages: List[Any] = []

    async for message in client.iter_messages(target_channel, limit=None):
        message_time = parse_datetime_value(getattr(message, "date", None))
        if message_time is None:
            continue
        if message_time < cutoff_time:
            break
        recent_messages.append(message)

    registry_plans = plan_registry_duplicate_deletions(recent_messages, message_map)
    covered_message_ids = set()
    for plan in registry_plans:
        keep_message_id = getattr(plan["keep"], "id", None)
        if keep_message_id is not None:
            covered_message_ids.add(int(keep_message_id))
        for delete_message in plan["delete"]:
            delete_message_id = getattr(delete_message, "id", None)
            if delete_message_id is not None:
                covered_message_ids.add(int(delete_message_id))

    orphan_messages = [
        message
        for message in recent_messages
        if int(getattr(message, "id", 0) or 0) not in covered_message_ids
    ]
    content_plans = plan_recent_duplicate_deletions(
        orphan_messages,
        lookback_hours,
        cluster_gap_minutes,
    )
    plans = registry_plans + content_plans
    deleted_count = 0
    updated_registry_entries = 0

    for plan in plans:
        keep_message = plan["keep"]
        keep_message_id = getattr(keep_message, "id", None)
        if keep_message_id is None:
            continue

        for delete_message in plan["delete"]:
            delete_message_id = getattr(delete_message, "id", None)
            if delete_message_id is None:
                continue

            async def perform_delete() -> None:
                nonlocal deleted_count, updated_registry_entries
                await delete_target_message(
                    client=client,
                    target_channel=target_channel,
                    message_id=int(delete_message_id),
                    dry_run=dry_run,
                )
                if plan.get("mode") == "registry" and plan.get("source_key"):
                    if set_registry_primary_target_message(
                        message_map,
                        str(plan["source_key"]),
                        int(keep_message_id),
                    ):
                        updated_registry_entries += 1
                else:
                    updated_registry_entries += repoint_registry_target_message(
                        message_map,
                        int(delete_message_id),
                        int(keep_message_id),
                    )
                persist_message_registry(registry_file, message_map, dry_run=dry_run)
                deleted_count += 1
                LOGGER.info(
                    "Deleted duplicate target message %s and kept %s (mode=%s)",
                    delete_message_id,
                    keep_message_id,
                    plan.get("mode", "unknown"),
                )

            if action_lock is None:
                await perform_delete()
            else:
                async with action_lock:
                    await perform_delete()

    summary = {
        "scanned": len(recent_messages),
        "registry_clusters": len(registry_plans),
        "fallback_clusters": len(content_plans),
        "clusters": len(plans),
        "deleted": deleted_count,
        "registry_updates": updated_registry_entries,
    }
    LOGGER.info(
        "Duplicate sweep summary: scanned=%s | registry_clusters=%s | fallback_clusters=%s | deleted=%s | registry_updates=%s",
        summary["scanned"],
        summary["registry_clusters"],
        summary["fallback_clusters"],
        summary["deleted"],
        summary["registry_updates"],
    )
    return summary


def env_first(names: List[str], default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return value
    return default


def env_first_int(names: List[str], default: Optional[int] = None) -> Optional[int]:
    raw = env_first(names, None)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_first_float(names: List[str], default: Optional[float] = None) -> Optional[float]:
    raw = env_first(names, None)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def configure_stdout_utf8() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def extract_text(export_msg: Dict[str, Any]) -> str:
    text_obj = export_msg.get("text", "")
    if isinstance(text_obj, str):
        return text_obj
    if isinstance(text_obj, list):
        parts: List[str] = []
        for part in text_obj:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict) and "text" in part:
                parts.append(str(part["text"]))
        return "".join(parts)
    return ""


def is_mostly_english(text: str, min_eng: int = 15, margin: int = 10) -> bool:
    if not text:
        return False
    eng_chars = len(ENG_RE.findall(text))
    heb_chars = len(HEB_RE.findall(text))
    return eng_chars > min_eng and eng_chars > heb_chars + margin


def get_media_path(export_msg: Dict[str, Any], base_dir: Path) -> Optional[Path]:
    relative = export_msg.get("photo") or export_msg.get("file")
    if not relative:
        return None
    path = base_dir / str(relative)
    return path if path.exists() else None


def load_progress(progress_file: Optional[Path]) -> int:
    if not progress_file or not progress_file.exists():
        return 0
    try:
        return int(progress_file.read_text(encoding="utf-8").strip())
    except Exception:
        return 0


def save_progress(progress_file: Optional[Path], message_id: int) -> None:
    if not progress_file:
        return
    atomic_write_text(progress_file, str(message_id), encoding="utf-8")


def normalize_channel(value: Optional[int], name: str) -> int:
    if value is None:
        raise ValueError(f"Missing required channel value: {name}")
    return int(value)


def validate_common_args(args: argparse.Namespace, require_source: bool = False) -> None:
    missing: List[str] = []
    if not args.api_id:
        missing.append("--api-id or env TG_API_ID/API_ID")
    if not args.api_hash:
        missing.append("--api-hash or env TG_API_HASH/API_HASH")
    if not args.openai_api_key:
        missing.append("--openai-api-key or env OPENAI_API_KEY")
    if getattr(args, "target_channel", None) is None and not getattr(args, "command", "") == "lookup-mapping":
        missing.append("--target-channel or env TARGET_CHANNEL")
    if require_source and getattr(args, "source_channel", None) is None:
        missing.append("--source-channel or env SOURCE_CHANNEL")
    if getattr(args, "auth_mode", "auto") == "bot" and not args.bot_token:
        missing.append("--bot-token or env TG_BOT_TOKEN/BOT_TOKEN when --auth-mode=bot")
    if missing:
        raise ValueError("Missing required settings:\n- " + "\n- ".join(missing))


class OpenAITranslator:
    def __init__(self, api_key: str, model: str, retries: int = 3):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self.retries = max(1, retries)

    async def translate(self, text: str) -> Optional[str]:
        if not text:
            return ""

        for attempt in range(1, self.retries + 1):
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": text},
                    ],
                )
                content = response.choices[0].message.content or ""
                return content.strip()
            except Exception as exc:
                if attempt == self.retries:
                    LOGGER.error("Translation failed after %s attempts: %s", attempt, exc)
                    return None
                wait_s = 2 * attempt
                LOGGER.warning(
                    "Translation error (attempt %s/%s): %s. Retrying in %ss",
                    attempt,
                    self.retries,
                    exc,
                    wait_s,
                )
                await asyncio.sleep(wait_s)
        return None


async def start_telegram_client(
    session_name: str,
    api_id: int,
    api_hash: str,
    bot_token: Optional[str],
    auth_mode: str,
) -> TelegramClient:
    client = TelegramClient(session_name, api_id, api_hash)
    mode = (auth_mode or "auto").lower()

    if mode not in {"auto", "bot", "user"}:
        raise ValueError("auth_mode must be one of: auto, bot, user")

    use_bot = mode == "bot" or (mode == "auto" and bool(bot_token))
    if use_bot:
        if not bot_token:
            raise ValueError("Bot authentication requested but bot token is missing.")
        await client.start(bot_token=bot_token)
        LOGGER.info("Telegram auth mode: bot")
    else:
        await client.start()
        LOGGER.info("Telegram auth mode: user")

    return client


async def ensure_channel_access(
    client: TelegramClient, channel_id: int, channel_label: str
) -> None:
    try:
        await client.get_entity(channel_id)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot access {channel_label} channel {channel_id}. "
            "If you are using a bot, make sure it is admin in that channel; "
            "otherwise run with --auth-mode user."
        ) from exc


async def publish_message(
    client: TelegramClient,
    target_channel: int,
    text: str,
    media: Optional[Any],
    dry_run: bool,
    max_retries: int = 3,
) -> Any:
    if dry_run:
        LOGGER.info(
            "[DRY RUN] Would publish to %s | media=%s | text_len=%s",
            target_channel,
            bool(media),
            len(text or ""),
        )
        return None

    for attempt in range(1, max_retries + 1):
        try:
            if media is not None:
                return await client.send_file(
                    target_channel,
                    media,
                    caption=text or None,
                    link_preview=False,
                )
            if text:
                return await client.send_message(target_channel, text, link_preview=False)
            return None
        except Exception as exc:
            message = str(exc).lower()
            if media is not None and text and "caption" in message and "too long" in message:
                LOGGER.warning("Caption too long. Sending media and text as separate posts.")
                media_result = await client.send_file(
                    target_channel,
                    media,
                    caption=None,
                    link_preview=False,
                )
                text_result = await client.send_message(target_channel, text, link_preview=False)
                return text_result or media_result

            wait_s = 2 * attempt
            if hasattr(exc, "seconds"):
                wait_s = max(wait_s, int(getattr(exc, "seconds")) + 1)

            if attempt == max_retries:
                raise

            LOGGER.warning(
                "Publish error (attempt %s/%s): %s. Retrying in %ss",
                attempt,
                max_retries,
                exc,
                wait_s,
            )
            await asyncio.sleep(wait_s)


async def edit_message(
    client: TelegramClient,
    target_channel: int,
    message_id: int,
    translated_text: str,
    dry_run: bool,
    max_retries: int = 3,
) -> None:
    if dry_run:
        LOGGER.info("[DRY RUN] Would edit message %s in %s", message_id, target_channel)
        return

    for attempt in range(1, max_retries + 1):
        try:
            await client.edit_message(target_channel, message_id, translated_text)
            return
        except Exception as exc:
            wait_s = 2 * attempt
            if hasattr(exc, "seconds"):
                wait_s = max(wait_s, int(getattr(exc, "seconds")) + 1)
            if attempt == max_retries:
                raise
            LOGGER.warning(
                "Edit error (attempt %s/%s) for message %s: %s. Retrying in %ss",
                attempt,
                max_retries,
                message_id,
                exc,
                wait_s,
            )
            await asyncio.sleep(wait_s)


def read_export_messages(json_file: Path) -> List[Dict[str, Any]]:
    with json_file.open("r", encoding="utf-8") as f:
        data = json.load(f)
    messages = data.get("messages", [])
    messages.sort(key=lambda x: x.get("id", 0))
    return messages


async def run_export_pipeline(
    client: TelegramClient,
    translator: OpenAITranslator,
    json_file: Path,
    target_channel: int,
    worker_count: int,
    publish_delay: float,
    progress_file: Optional[Path],
    registry_file: Optional[Path],
    source_channel_id: Optional[int],
    min_id: Optional[int],
    max_id: Optional[int],
    filter_only_english: bool,
    translate_only_english: bool,
    dry_run: bool,
) -> None:
    if not json_file.exists():
        raise FileNotFoundError(f"JSON file not found: {json_file}")

    messages = read_export_messages(json_file)
    base_dir = json_file.parent
    last_id = load_progress(progress_file)
    registry_map = load_message_registry(registry_file, source_channel_id=source_channel_id)
    pending: List[WorkItem] = []
    skipped_registry = 0

    for msg in messages:
        if msg.get("type") != "message":
            continue

        msg_id = int(msg.get("id", 0))
        if msg_id <= 0:
            continue
        if last_id > 0 and msg_id <= last_id:
            continue
        if min_id is not None and msg_id < min_id:
            continue
        if max_id is not None and msg_id > max_id:
            continue
        if lookup_registered_target_message(registry_map, msg_id, source_channel_id) is not None:
            skipped_registry += 1
            continue

        original_text = extract_text(msg)
        media_path = get_media_path(msg, base_dir)

        if filter_only_english:
            if not original_text or not is_mostly_english(original_text):
                continue
        else:
            if not original_text and not media_path:
                continue

        pending.append(
            WorkItem(
                id=msg_id,
                original_text=original_text,
                media_path=media_path,
                source_published_at=str(msg.get("date", "")).strip() or None,
            )
        )

    if not pending:
        LOGGER.info("No pending messages found. SkippedRegistry=%s", skipped_registry)
        return

    LOGGER.info(
        "Found %s pending messages. SkippedRegistry=%s",
        len(pending),
        skipped_registry,
    )

    translated_buffer: Dict[int, str] = {}
    queue: asyncio.Queue[Optional[WorkItem]] = asyncio.Queue()
    worker_count = max(1, min(worker_count, len(pending)))

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
                    should_translate = True
                    if translate_only_english and not is_mostly_english(item.original_text):
                        should_translate = False
                    if should_translate:
                        LOGGER.info("[Worker %s] Translating message %s", worker_id, item.id)
                        translated = await translator.translate(item.original_text)
                        final_text = translated if translated else item.original_text
                translated_buffer[item.id] = final_text
            except Exception as exc:
                LOGGER.exception(
                    "[Worker %s] Unexpected error on message %s: %s",
                    worker_id,
                    item.id,
                    exc,
                )
                translated_buffer[item.id] = item.original_text
            finally:
                queue.task_done()

    async def publisher_worker() -> None:
        published = 0
        for item in pending:
            while item.id not in translated_buffer:
                await asyncio.sleep(0.2)
            final_text = translated_buffer.pop(item.id)
            try:
                sent_result = await publish_message(
                    client=client,
                    target_channel=target_channel,
                    text=final_text,
                    media=item.media_path,
                    dry_run=dry_run,
                )
                if dry_run:
                    LOGGER.info("[DRY RUN] Simulated publish for message %s", item.id)
                    continue

                sent_message_id = ensure_sent_message_id(
                    sent_result,
                    f"Published source message {item.id}",
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
                    context=f"Persisting state for published message {item.id}",
                )
                published += 1
                LOGGER.info("Published message %s (%s/%s)", item.id, published, len(pending))
            except StatePersistenceError as exc:
                LOGGER.critical("State persistence failed after publishing message %s: %s", item.id, exc)
                raise
            except Exception as exc:
                LOGGER.error("Publish failed for message %s: %s", item.id, exc)
            if publish_delay > 0:
                await asyncio.sleep(publish_delay)
        LOGGER.info(
            "Export pipeline completed. Published=%s | SkippedRegistry=%s",
            published,
            skipped_registry,
        )

    workers = [asyncio.create_task(translator_worker(i + 1)) for i in range(worker_count)]
    publisher = asyncio.create_task(publisher_worker())

    await asyncio.gather(*workers, publisher)

async def run_live_mode(args: argparse.Namespace, translator: OpenAITranslator) -> None:
    validate_common_args(args, require_source=True)
    source_channel = normalize_channel(args.source_channel, "source_channel")
    target_channel = normalize_channel(args.target_channel, "target_channel")
    registry_file = Path(args.registry_file) if getattr(args, "registry_file", None) else None
    heartbeat_file = Path(args.heartbeat_file) if getattr(args, "heartbeat_file", None) else None
    heartbeat_interval_seconds = max(
        0.0,
        float(getattr(args, "heartbeat_interval_seconds", DEFAULT_HEARTBEAT_INTERVAL_SECONDS)),
    )
    edit_window_hours = float(getattr(args, "edit_window_hours", DEFAULT_EDIT_WINDOW_HOURS))
    source_reconcile_interval_minutes = max(
        0.0,
        float(
            getattr(
                args,
                "source_reconcile_interval_minutes",
                DEFAULT_SOURCE_RECONCILE_INTERVAL_MINUTES,
            )
        ),
    )
    source_reconcile_lookback_hours = max(
        0.0,
        float(
            getattr(
                args,
                "source_reconcile_lookback_hours",
                DEFAULT_SOURCE_RECONCILE_LOOKBACK_HOURS,
            )
        ),
    )
    dedupe_interval_minutes = max(
        0.0,
        float(
            getattr(
                args,
                "target_dedupe_interval_minutes",
                DEFAULT_TARGET_DEDUPE_SCAN_INTERVAL_MINUTES,
            )
        ),
    )
    dedupe_lookback_hours = max(
        0.0,
        float(
            getattr(
                args,
                "target_dedupe_lookback_hours",
                DEFAULT_TARGET_DEDUPE_LOOKBACK_HOURS,
            )
        ),
    )
    dedupe_cluster_gap_minutes = max(
        0.0,
        float(
            getattr(
                args,
                "target_dedupe_cluster_gap_minutes",
                DEFAULT_TARGET_DEDUPE_CLUSTER_GAP_MINUTES,
            )
        ),
    )
    background_tasks_suspend_until_raw = getattr(
        args,
        "live_background_tasks_suspend_until",
        "",
    )
    background_tasks_suspend_until = parse_datetime_value(background_tasks_suspend_until_raw)
    background_tasks_suspended = False
    if background_tasks_suspend_until_raw and background_tasks_suspend_until is None:
        LOGGER.warning(
            "Invalid LIVE background suspension timestamp %r. Ignoring it.",
            background_tasks_suspend_until_raw,
        )
    elif background_tasks_suspend_until is not None:
        background_tasks_suspended = background_tasks_suspend_until > datetime.now(timezone.utc)

    client = await start_telegram_client(
        session_name=args.session,
        api_id=args.api_id,
        api_hash=args.api_hash,
        bot_token=args.bot_token,
        auth_mode=args.auth_mode,
    )

    await ensure_channel_access(client, source_channel, "source")
    await ensure_channel_access(client, target_channel, "target")

    message_map = load_message_registry(registry_file, source_channel_id=source_channel)
    source_event_locks: Dict[str, asyncio.Lock] = {}
    source_event_refs: Dict[str, int] = {}
    target_action_lock = asyncio.Lock()

    def persist_mapping() -> None:
        persist_message_registry(registry_file, message_map, dry_run=args.dry_run)

    def build_live_heartbeat_payload(status: str) -> Dict[str, Any]:
        return {
            "status": status,
            "updated_at": format_datetime_value(datetime.now(timezone.utc)),
            "pid": os.getpid(),
            "source_channel": source_channel,
            "target_channel": target_channel,
            "auth_mode": args.auth_mode,
            "registry_file": str(registry_file) if registry_file else None,
            "background_tasks_suspend_until": format_datetime_value(background_tasks_suspend_until),
            "source_reconcile_interval_minutes": source_reconcile_interval_minutes,
            "source_reconcile_lookback_hours": source_reconcile_lookback_hours,
            "target_dedupe_interval_minutes": dedupe_interval_minutes,
            "target_dedupe_lookback_hours": dedupe_lookback_hours,
        }

    async def forward_or_update(event: events.NewMessage.Event, is_edit: bool) -> None:
        original_text = event.raw_text or ""
        if not original_text and not event.media:
            return

        source_key = build_source_message_key(event.id, source_channel)
        source_event_refs[source_key] = source_event_refs.get(source_key, 0) + 1
        source_lock = source_event_locks.setdefault(source_key, asyncio.Lock())

        try:
            async with source_lock:
                mapped_record = lookup_registered_target_record(
                    message_map,
                    event.id,
                    source_channel_id=source_channel,
                )
                mapped_target_id = (
                    int(mapped_record["target_message_id"])
                    if mapped_record and mapped_record.get("target_message_id") is not None
                    else None
                )

                try:
                    if not is_edit and mapped_target_id is not None:
                        LOGGER.info(
                            "Source message %s already mapped to target %s. Skipping duplicate publish.",
                            event.id,
                            mapped_target_id,
                        )
                        return

                    if is_edit and mapped_target_id is None:
                        LOGGER.info(
                            "Edit for source %s has no mapped target message. Skipping to avoid duplicate publish.",
                            event.id,
                        )
                        return

                    if is_edit and mapped_target_id is not None and not is_edit_within_window(
                        mapped_record,
                        event.date,
                        edit_window_hours,
                    ):
                        LOGGER.info(
                            "Edit for source %s arrived outside the %.1f-hour window. Skipping target update %s.",
                            event.id,
                            edit_window_hours,
                            mapped_target_id,
                        )
                        return

                    LOGGER.info(
                        "%s message %s received. Translating...",
                        "Edited" if is_edit else "New",
                        event.id,
                    )
                    translated = await translator.translate(original_text) if original_text else ""
                    final_text = translated if translated else original_text

                    async with target_action_lock:
                        mapped_record = lookup_registered_target_record(
                            message_map,
                            event.id,
                            source_channel_id=source_channel,
                        )
                        mapped_target_id = (
                            int(mapped_record["target_message_id"])
                            if mapped_record and mapped_record.get("target_message_id") is not None
                            else None
                        )

                        if not is_edit and mapped_target_id is not None:
                            LOGGER.info(
                                "Source message %s already mapped to target %s after lock refresh. Skipping duplicate publish.",
                                event.id,
                                mapped_target_id,
                            )
                            return

                        if is_edit and mapped_target_id is None:
                            LOGGER.info(
                                "Edit for source %s still has no mapped target message after lock refresh. Skipping to avoid duplicate publish.",
                                event.id,
                            )
                            return

                        if is_edit and mapped_target_id is not None and not is_edit_within_window(
                            mapped_record,
                            event.date,
                            edit_window_hours,
                        ):
                            LOGGER.info(
                                "Edit for source %s arrived outside the %.1f-hour window after lock refresh. Skipping target update %s.",
                                event.id,
                                edit_window_hours,
                                mapped_target_id,
                            )
                            return

                        if is_edit and mapped_target_id is not None:
                            if final_text:
                                await edit_message(
                                    client=client,
                                    target_channel=target_channel,
                                    message_id=mapped_target_id,
                                    translated_text=final_text,
                                    dry_run=args.dry_run,
                                )
                                LOGGER.info(
                                    "Updated target message %s for source %s",
                                    mapped_target_id,
                                    event.id,
                                )
                            else:
                                LOGGER.info(
                                    "Edit received for source %s without text; keeping existing target message %s",
                                    event.id,
                                    mapped_target_id,
                                )
                            return

                        sent_result = await publish_message(
                            client=client,
                            target_channel=target_channel,
                            text=final_text,
                            media=event.media,
                            dry_run=args.dry_run,
                        )
                        if args.dry_run:
                            LOGGER.info("[DRY RUN] Simulated forwarding source message %s", event.id)
                            return

                        sent_message_id = ensure_sent_message_id(
                            sent_result,
                            f"Forwarded source message {event.id}",
                        )
                        if register_target_message(
                            message_map,
                            event.id,
                            sent_message_id,
                            source_channel_id=source_channel,
                            published_at=event.date,
                        ) is not None:
                            try:
                                persist_mapping()
                            except Exception as exc:
                                LOGGER.critical(
                                    "State persistence failed after forwarding source message %s to target %s: %s",
                                    event.id,
                                    sent_message_id,
                                    exc,
                                )
                                await client.disconnect()
                                return
                        LOGGER.info("Forwarded source message %s", event.id)
                except StatePersistenceError as exc:
                    LOGGER.critical("Failed to capture publish result for source message %s: %s", event.id, exc)
                    await client.disconnect()
                except Exception as exc:
                    LOGGER.error("Failed forwarding source message %s: %s", event.id, exc)
        finally:
            remaining_refs = source_event_refs.get(source_key, 1) - 1
            if remaining_refs <= 0:
                source_event_refs.pop(source_key, None)
                if not source_lock.locked():
                    source_event_locks.pop(source_key, None)
            else:
                source_event_refs[source_key] = remaining_refs

    @client.on(events.NewMessage(chats=source_channel))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        await forward_or_update(event, is_edit=False)

    @client.on(events.MessageEdited(chats=source_channel))
    async def on_edited_message(event: events.MessageEdited.Event) -> None:
        await forward_or_update(event, is_edit=True)

    async def periodic_source_reconciliation() -> None:
        LOGGER.info(
            "Source reconciliation enabled. Interval=%.1f minutes | Lookback=%.1f hours",
            source_reconcile_interval_minutes,
            source_reconcile_lookback_hours,
        )
        try:
            while True:
                await asyncio.sleep(source_reconcile_interval_minutes * 60)
                try:
                    await reconcile_recent_source_messages(
                        client=client,
                        source_channel=source_channel,
                        lookback_hours=source_reconcile_lookback_hours,
                        process_message=lambda message: forward_or_update(message, is_edit=False),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.error("Periodic source reconciliation failed: %s", exc)
        except asyncio.CancelledError:
            LOGGER.info("Source reconciliation stopped.")
            raise

    async def periodic_duplicate_sweep() -> None:
        LOGGER.info(
            "Target duplicate sweep enabled. Interval=%.1f minutes | Lookback=%.1f hours | ClusterGap=%.1f minutes",
            dedupe_interval_minutes,
            dedupe_lookback_hours,
            dedupe_cluster_gap_minutes,
        )
        try:
            while True:
                await asyncio.sleep(dedupe_interval_minutes * 60)
                try:
                    await sweep_recent_target_duplicates(
                        client=client,
                        target_channel=target_channel,
                        message_map=message_map,
                        registry_file=registry_file,
                        lookback_hours=dedupe_lookback_hours,
                        cluster_gap_minutes=dedupe_cluster_gap_minutes,
                        dry_run=args.dry_run,
                        action_lock=target_action_lock,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.error("Periodic duplicate sweep failed: %s", exc)
        except asyncio.CancelledError:
            LOGGER.info("Target duplicate sweep stopped.")
            raise

    async def heartbeat_loop() -> None:
        LOGGER.info(
            "Live heartbeat enabled. Interval=%.1f seconds | File=%s",
            heartbeat_interval_seconds,
            heartbeat_file,
        )
        try:
            while True:
                persist_live_heartbeat(
                    heartbeat_file,
                    build_live_heartbeat_payload("running"),
                    dry_run=args.dry_run,
                )
                await asyncio.sleep(heartbeat_interval_seconds)
        except asyncio.CancelledError:
            LOGGER.info("Live heartbeat stopped.")
            raise

    source_reconcile_enabled = (
        source_reconcile_interval_minutes > 0 and source_reconcile_lookback_hours > 0
    )
    duplicate_sweep_enabled = dedupe_interval_minutes > 0 and dedupe_lookback_hours > 0

    async def start_live_background_tasks() -> List[asyncio.Task[Any]]:
        child_tasks: List[asyncio.Task[Any]] = []

        if source_reconcile_enabled:
            try:
                await reconcile_recent_source_messages(
                    client=client,
                    source_channel=source_channel,
                    lookback_hours=source_reconcile_lookback_hours,
                    process_message=lambda message: forward_or_update(message, is_edit=False),
                )
            except Exception as exc:
                LOGGER.error("Initial source reconciliation failed: %s", exc)
            child_tasks.append(asyncio.create_task(periodic_source_reconciliation()))

        if duplicate_sweep_enabled:
            try:
                await sweep_recent_target_duplicates(
                    client=client,
                    target_channel=target_channel,
                    message_map=message_map,
                    registry_file=registry_file,
                    lookback_hours=dedupe_lookback_hours,
                    cluster_gap_minutes=dedupe_cluster_gap_minutes,
                    dry_run=args.dry_run,
                    action_lock=target_action_lock,
                )
            except Exception as exc:
                LOGGER.error("Initial duplicate sweep failed: %s", exc)
            child_tasks.append(asyncio.create_task(periodic_duplicate_sweep()))

        return child_tasks

    async def background_task_manager() -> None:
        child_tasks: List[asyncio.Task[Any]] = []
        try:
            if background_tasks_suspend_until is not None:
                delay_seconds = (
                    background_tasks_suspend_until - datetime.now(timezone.utc)
                ).total_seconds()
                if delay_seconds > 0:
                    LOGGER.info(
                        "Live background tasks suspended until %s. Realtime listener handlers remain active.",
                        format_datetime_value(background_tasks_suspend_until),
                    )
                    await asyncio.sleep(delay_seconds)
                    LOGGER.info(
                        "Live background task suspension expired. Starting background maintenance."
                    )

            child_tasks = await start_live_background_tasks()
            if child_tasks:
                await asyncio.gather(*child_tasks)
        except asyncio.CancelledError:
            LOGGER.info("Live background task manager stopped.")
            raise
        finally:
            for task in child_tasks:
                if not task.done():
                    task.cancel()
            if child_tasks:
                await asyncio.gather(*child_tasks, return_exceptions=True)

    background_manager_task: Optional[asyncio.Task[Any]] = None
    if source_reconcile_enabled or duplicate_sweep_enabled:
        background_manager_task = asyncio.create_task(background_task_manager())

    heartbeat_task: Optional[asyncio.Task[Any]] = None
    if heartbeat_file is not None and heartbeat_interval_seconds > 0:
        persist_live_heartbeat(
            heartbeat_file,
            build_live_heartbeat_payload("starting"),
            dry_run=args.dry_run,
        )
        heartbeat_task = asyncio.create_task(heartbeat_loop())

    LOGGER.info("Live mode is running. Listening on %s -> %s", source_channel, target_channel)
    try:
        await client.run_until_disconnected()
    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            persist_live_heartbeat(
                heartbeat_file,
                build_live_heartbeat_payload("stopped"),
                dry_run=args.dry_run,
            )
        if background_manager_task is not None:
            background_manager_task.cancel()
            await asyncio.gather(background_manager_task, return_exceptions=True)
        await client.disconnect()

async def run_import_json_mode(args: argparse.Namespace, translator: OpenAITranslator) -> None:
    validate_common_args(args)
    target_channel = normalize_channel(args.target_channel, "target_channel")
    client = await start_telegram_client(
        session_name=args.session,
        api_id=args.api_id,
        api_hash=args.api_hash,
        bot_token=args.bot_token,
        auth_mode=args.auth_mode,
    )

    await ensure_channel_access(client, target_channel, "target")

    try:
        await run_export_pipeline(
            client=client,
            translator=translator,
            json_file=Path(args.json_file),
            target_channel=target_channel,
            worker_count=args.workers,
            publish_delay=args.delay,
            progress_file=Path(args.progress_file) if args.progress_file else None,
            registry_file=Path(args.registry_file) if args.registry_file else None,
            source_channel_id=getattr(args, "source_channel", None),
            min_id=args.min_id,
            max_id=args.max_id,
            filter_only_english=args.only_english,
            translate_only_english=args.only_english,
            dry_run=args.dry_run,
        )
    finally:
        await client.disconnect()


async def run_repair_export_mode(args: argparse.Namespace, translator: OpenAITranslator) -> None:
    validate_common_args(args)
    target_channel = normalize_channel(args.target_channel, "target_channel")
    client = await start_telegram_client(
        session_name=args.session,
        api_id=args.api_id,
        api_hash=args.api_hash,
        bot_token=args.bot_token,
        auth_mode=args.auth_mode,
    )

    await ensure_channel_access(client, target_channel, "target")

    try:
        await run_export_pipeline(
            client=client,
            translator=translator,
            json_file=Path(args.json_file),
            target_channel=target_channel,
            worker_count=args.workers,
            publish_delay=args.delay,
            progress_file=Path(args.progress_file) if args.progress_file else None,
            registry_file=Path(args.registry_file) if args.registry_file else None,
            source_channel_id=getattr(args, "source_channel", None),
            min_id=args.min_id,
            max_id=args.max_id,
            filter_only_english=True,
            translate_only_english=True,
            dry_run=args.dry_run,
        )
    finally:
        await client.disconnect()


async def run_fix_channel_mode(args: argparse.Namespace, translator: OpenAITranslator) -> None:
    validate_common_args(args)
    target_channel = normalize_channel(args.target_channel, "target_channel")
    client = await start_telegram_client(
        session_name=args.session,
        api_id=args.api_id,
        api_hash=args.api_hash,
        bot_token=args.bot_token,
        auth_mode=args.auth_mode,
    )

    await ensure_channel_access(client, target_channel, "target")

    scanned = 0
    fixed = 0
    try:
        LOGGER.info("Scanning last %s messages in %s", args.limit, target_channel)
        async for msg in client.iter_messages(target_channel, limit=args.limit):
            scanned += 1
            text = msg.message or ""
            if not text:
                continue
            if not is_mostly_english(text):
                continue

            LOGGER.info("English message found: %s", msg.id)
            translated = await translator.translate(text)
            if not translated:
                continue

            try:
                await edit_message(
                    client=client,
                    target_channel=target_channel,
                    message_id=msg.id,
                    translated_text=translated,
                    dry_run=args.dry_run,
                )
                fixed += 1
                LOGGER.info("Fixed message %s", msg.id)
            except Exception as exc:
                LOGGER.error("Failed editing message %s: %s", msg.id, exc)

            if args.delay > 0:
                await asyncio.sleep(args.delay)

        LOGGER.info("Scan completed. Scanned=%s | Fixed=%s", scanned, fixed)
    finally:
        await client.disconnect()


def run_export_stats_mode(args: argparse.Namespace) -> None:
    json_file = Path(args.json_file)
    if not json_file.exists():
        raise FileNotFoundError(f"JSON file not found: {json_file}")

    messages = read_export_messages(json_file)
    valid = [
        m
        for m in messages
        if m.get("type") == "message" and (extract_text(m) or m.get("photo") or m.get("file"))
    ]
    print(f"Total valid messages: {len(valid)}")
    if valid:
        print(f"First ID: {valid[0].get('id')}, Last ID: {valid[-1].get('id')}")


def run_lookup_mapping_mode(args: argparse.Namespace) -> None:
    source_channel = normalize_channel(args.source_channel, "source_channel")
    registry_file = Path(args.registry_file) if getattr(args, "registry_file", None) else None
    message_map = load_message_registry(registry_file, source_channel_id=source_channel)
    source_key = build_source_message_key(args.source_message_id, source_channel)
    record = message_map.get(source_key)
    if not record:
        print(f"No mapping found for {source_key}")
        return

    known_target_ids = extract_registered_target_message_ids(record)
    print(f"Source Key: {source_key}")
    print(f"Primary Target Message ID: {record.get('target_message_id')}")
    print(
        "Known Target Message IDs: "
        + (", ".join(str(message_id) for message_id in known_target_ids) if known_target_ids else "")
    )
    if record.get("published_at"):
        print(f"Published At: {record['published_at']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified Telegram translator/importer/repair pipeline."
    )
    parser.add_argument("--api-id", type=int, default=env_first_int(["TG_API_ID", "API_ID"]))
    parser.add_argument("--api-hash", default=env_first(["TG_API_HASH", "API_HASH"]))
    parser.add_argument("--bot-token", default=env_first(["TG_BOT_TOKEN", "BOT_TOKEN"]))
    parser.add_argument(
        "--auth-mode",
        choices=["auto", "bot", "user"],
        default=env_first(["TG_AUTH_MODE"], "auto"),
        help="auto=bot if token exists else user, bot=force bot login, user=force user login",
    )
    parser.add_argument("--openai-api-key", default=env_first(["OPENAI_API_KEY"]))
    parser.add_argument("--model", default=env_first(["OPENAI_MODEL"], "gpt-5-mini"))
    parser.add_argument("--session", default=env_first(["TG_SESSION"], "news_translator_session"))
    parser.add_argument("--log-level", default=env_first(["LOG_LEVEL"], "INFO"))

    sub = parser.add_subparsers(dest="command", required=True)

    live = sub.add_parser("live", help="Listen to source channel and forward translated posts.")
    live.add_argument(
        "--source-channel",
        type=int,
        default=env_first_int(["SOURCE_CHANNEL"]),
    )
    live.add_argument(
        "--target-channel",
        type=int,
        default=env_first_int(["TARGET_CHANNEL"]),
    )
    live.add_argument("--dry-run", action="store_true")
    live.add_argument(
        "--state-file",
        "--registry-file",
        dest="registry_file",
        default=env_first(["MESSAGE_REGISTRY_FILE"], DEFAULT_LIVE_STATE_FILE),
    )
    live.add_argument(
        "--edit-window-hours",
        type=float,
        default=env_first_float(["LIVE_EDIT_WINDOW_HOURS"], float(DEFAULT_EDIT_WINDOW_HOURS)),
        help="Update mapped target messages for edits only within this many hours. Use 0 for no limit.",
    )
    live.add_argument(
        "--source-reconcile-interval-minutes",
        type=float,
        default=env_first_float(
            ["SOURCE_RECONCILE_INTERVAL_MINUTES"],
            DEFAULT_SOURCE_RECONCILE_INTERVAL_MINUTES,
        ),
        help="How often to scan the source channel for missed posts. Use 0 to disable.",
    )
    live.add_argument(
        "--source-reconcile-lookback-hours",
        type=float,
        default=env_first_float(
            ["SOURCE_RECONCILE_LOOKBACK_HOURS"],
            DEFAULT_SOURCE_RECONCILE_LOOKBACK_HOURS,
        ),
        help="How far back the source reconciliation should inspect for missed posts.",
    )
    live.add_argument(
        "--target-dedupe-interval-minutes",
        type=float,
        default=env_first_float(
            ["TARGET_DEDUPE_SCAN_INTERVAL_MINUTES"],
            DEFAULT_TARGET_DEDUPE_SCAN_INTERVAL_MINUTES,
        ),
        help="How often to scan the target channel for recent duplicate posts. Use 0 to disable.",
    )
    live.add_argument(
        "--target-dedupe-lookback-hours",
        type=float,
        default=env_first_float(
            ["TARGET_DEDUPE_LOOKBACK_HOURS"],
            DEFAULT_TARGET_DEDUPE_LOOKBACK_HOURS,
        ),
        help="How far back the duplicate sweep should inspect the target channel.",
    )
    live.add_argument(
        "--target-dedupe-cluster-gap-minutes",
        type=float,
        default=env_first_float(
            ["TARGET_DEDUPE_CLUSTER_GAP_MINUTES"],
            DEFAULT_TARGET_DEDUPE_CLUSTER_GAP_MINUTES,
        ),
        help="Only collapse identical posts when they were published within this gap window.",
    )
    live.add_argument(
        "--live-background-tasks-suspend-until",
        default=env_first(["LIVE_BACKGROUND_TASKS_SUSPEND_UNTIL"], ""),
        help="Temporarily disable live background scans until this ISO timestamp; realtime listening remains active.",
    )
    live.add_argument(
        "--heartbeat-file",
        default=env_first(["LIVE_HEARTBEAT_FILE"], DEFAULT_HEARTBEAT_FILE),
        help="Write a small heartbeat JSON file while live mode is running.",
    )
    live.add_argument(
        "--heartbeat-interval-seconds",
        type=float,
        default=env_first_float(["LIVE_HEARTBEAT_INTERVAL_SECONDS"], DEFAULT_HEARTBEAT_INTERVAL_SECONDS),
        help="How often to refresh the live heartbeat file. Use 0 to disable.",
    )

    import_json = sub.add_parser(
        "import-json", help="Import messages from Telegram export JSON and publish in order."
    )
    import_json.add_argument("--json-file", required=True)
    import_json.add_argument(
        "--target-channel",
        type=int,
        default=env_first_int(["TARGET_CHANNEL"]),
    )
    import_json.add_argument("--progress-file", default="progress_import.txt")
    import_json.add_argument(
        "--source-channel",
        type=int,
        default=env_first_int(["SOURCE_CHANNEL"]),
    )
    import_json.add_argument(
        "--registry-file",
        default=env_first(["MESSAGE_REGISTRY_FILE"], DEFAULT_MESSAGE_REGISTRY_FILE),
    )
    import_json.add_argument("--workers", type=int, default=30)
    import_json.add_argument("--delay", type=float, default=2.0)
    import_json.add_argument("--min-id", type=int)
    import_json.add_argument("--max-id", type=int)
    import_json.add_argument(
        "--only-english",
        action="store_true",
        help="Process only messages detected as mostly English.",
    )
    import_json.add_argument("--dry-run", action="store_true")

    repair_export = sub.add_parser(
        "repair-export",
        help="Repair missed English posts from export JSON (English-only mode).",
    )
    repair_export.add_argument("--json-file", required=True)
    repair_export.add_argument(
        "--target-channel",
        type=int,
        default=env_first_int(["TARGET_CHANNEL"]),
    )
    repair_export.add_argument("--progress-file", default="progress_repair.txt")
    repair_export.add_argument(
        "--source-channel",
        type=int,
        default=env_first_int(["SOURCE_CHANNEL"]),
    )
    repair_export.add_argument(
        "--registry-file",
        default=env_first(["MESSAGE_REGISTRY_FILE"], DEFAULT_MESSAGE_REGISTRY_FILE),
    )
    repair_export.add_argument("--workers", type=int, default=30)
    repair_export.add_argument("--delay", type=float, default=2.0)
    repair_export.add_argument("--min-id", type=int)
    repair_export.add_argument("--max-id", type=int)
    repair_export.add_argument("--dry-run", action="store_true")

    fix_channel = sub.add_parser(
        "fix-channel",
        help="Scan recent messages in target channel and edit English messages in place.",
    )
    fix_channel.add_argument(
        "--target-channel",
        type=int,
        default=env_first_int(["TARGET_CHANNEL"]),
    )
    fix_channel.add_argument("--limit", type=int, default=3000)
    fix_channel.add_argument("--delay", type=float, default=1.0)
    fix_channel.add_argument("--dry-run", action="store_true")

    stats = sub.add_parser("export-stats", help="Print quick stats for an export JSON file.")
    stats.add_argument("--json-file", required=True)

    lookup = sub.add_parser(
        "lookup-mapping",
        help="Show the target-channel message id(s) mapped to a specific source message id.",
    )
    lookup.add_argument("--source-message-id", type=int, required=True)
    lookup.add_argument(
        "--source-channel",
        type=int,
        default=env_first_int(["SOURCE_CHANNEL"]),
    )
    lookup.add_argument(
        "--registry-file",
        default=env_first(["MESSAGE_REGISTRY_FILE"], DEFAULT_MESSAGE_REGISTRY_FILE),
    )

    return parser


async def run_async(args: argparse.Namespace) -> None:
    if args.command == "export-stats":
        run_export_stats_mode(args)
        return
    if args.command == "lookup-mapping":
        run_lookup_mapping_mode(args)
        return

    translator = OpenAITranslator(
        api_key=args.openai_api_key,
        model=args.model,
        retries=3,
    )

    if args.command == "live":
        await run_live_mode(args, translator)
    elif args.command == "import-json":
        await run_import_json_mode(args, translator)
    elif args.command == "repair-export":
        await run_repair_export_mode(args, translator)
    elif args.command == "fix-channel":
        await run_fix_channel_mode(args, translator)
    else:
        raise ValueError(f"Unknown command: {args.command}")


def main() -> None:
    configure_stdout_utf8()
    apply_env_file(Path(DEFAULT_ENV_FILE))
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    try:
        asyncio.run(run_async(args))
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user.")
    except Exception as exc:
        LOGGER.error("Fatal error: %s", exc)
        raise


if __name__ == "__main__":
    main()

































