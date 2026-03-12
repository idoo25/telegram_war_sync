import argparse
import asyncio
import io
import json
import os
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch


if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")

    class _AsyncOpenAIStub:
        def __init__(self, *args, **kwargs):
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(create=AsyncMock())
            )

    openai_stub.AsyncOpenAI = _AsyncOpenAIStub
    sys.modules["openai"] = openai_stub


if "telethon" not in sys.modules:
    telethon_stub = types.ModuleType("telethon")

    class _TelegramClientStub:
        def __init__(self, *args, **kwargs):
            pass

    class _EventsStub:
        class NewMessage:
            def __init__(self, *args, **kwargs):
                pass

        class MessageEdited:
            def __init__(self, *args, **kwargs):
                pass

    telethon_stub.TelegramClient = _TelegramClientStub
    telethon_stub.events = _EventsStub
    sys.modules["telethon"] = telethon_stub


import telegram_news_pipeline as pipeline


def _args(**overrides):
    base = {
        "api_id": 1,
        "api_hash": "hash",
        "bot_token": None,
        "openai_api_key": "key",
        "target_channel": -100111,
        "source_channel": -100222,
        "auth_mode": "user",
        "session": "test_session",
        "dry_run": False,
        "registry_file": "message_registry.json",
        "edit_window_hours": 24,
        "source_reconcile_interval_minutes": 0,
        "source_reconcile_lookback_hours": 24,
        "target_dedupe_interval_minutes": 0,
        "target_dedupe_lookback_hours": 24,
        "target_dedupe_cluster_gap_minutes": 60,
        "live_background_tasks_suspend_until": "",
    }
    base.update(overrides)
    return argparse.Namespace(**base)


class TestSyncHelpers(unittest.TestCase):
    def test_extract_text_handles_string_and_rich_text(self):
        self.assertEqual(pipeline.extract_text({"text": "hello"}), "hello")
        rich = {
            "text": ["a", {"type": "bold", "text": "b"}, "c", {"text": "d"}],
        }
        self.assertEqual(pipeline.extract_text(rich), "abcd")

    def test_is_mostly_english_threshold(self):
        self.assertTrue(
            pipeline.is_mostly_english(
                "This is a long English sentence with enough letters to match the threshold"
            )
        )
        self.assertFalse(pipeline.is_mostly_english("שלום עולם זה טקסט בעברית"))
        self.assertFalse(pipeline.is_mostly_english(""))

    def test_get_media_path_returns_existing_file(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            media = base / "photo.jpg"
            media.write_text("x", encoding="utf-8")
            msg = {"photo": "photo.jpg"}
            self.assertEqual(pipeline.get_media_path(msg, base), media)
            self.assertIsNone(pipeline.get_media_path({"photo": "missing.jpg"}, base))

    def test_save_and_load_progress_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            progress = Path(td) / "progress.txt"
            pipeline.save_progress(progress, 123)
            self.assertEqual(pipeline.load_progress(progress), 123)

    def test_message_registry_roundtrip_and_legacy_upgrade(self):
        with tempfile.TemporaryDirectory() as td:
            registry = Path(td) / "message_registry.json"
            legacy_registry = Path(td) / "live_message_map.json"
            legacy_registry.write_text(
                json.dumps({"source_to_target": {"123": 456}}),
                encoding="utf-8",
            )
            loaded = pipeline.load_message_registry(registry, source_channel_id=-100222)
            self.assertEqual(loaded["-100222:123"]["target_message_id"], 456)
            self.assertEqual(
                loaded["-100222:123"]["known_target_message_ids"],
                [456],
            )
            pipeline.register_target_message(loaded, 124, 457, source_channel_id=-100222)
            pipeline.persist_message_registry(registry, loaded)
            persisted = json.loads(registry.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["source_to_target"]["-100222:124"]["target_message_id"], 457
            )
            self.assertEqual(
                persisted["source_to_target"]["-100222:124"]["known_target_message_ids"],
                [457],
            )
            self.assertEqual(
                persisted["target_to_source"]["457"],
                "-100222:124",
            )

    def test_persist_message_registry_creates_backup_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            registry = Path(td) / "message_registry.json"
            registry.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:1": {
                                "target_message_id": 456,
                                "known_target_message_ids": [456],
                            }
                        },
                        "target_to_source": {"456": "-100222:1"},
                    }
                ),
                encoding="utf-8",
            )

            loaded = pipeline.load_message_registry(registry, source_channel_id=-100222)
            pipeline.register_target_message(loaded, 2, 457, source_channel_id=-100222)
            pipeline.persist_message_registry(registry, loaded)

            backup = registry.with_name("message_registry.json.bak")
            self.assertTrue(backup.exists())
            backup_data = json.loads(backup.read_text(encoding="utf-8"))
            self.assertIn("-100222:1", backup_data["source_to_target"])
            self.assertNotIn("-100222:2", backup_data["source_to_target"])

    def test_persist_message_registry_continues_when_backup_snapshot_fails(self):
        with tempfile.TemporaryDirectory() as td:
            registry = Path(td) / "message_registry.json"
            registry.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:1": {
                                "target_message_id": 456,
                                "known_target_message_ids": [456],
                            }
                        },
                        "target_to_source": {"456": "-100222:1"},
                    }
                ),
                encoding="utf-8",
            )

            loaded = pipeline.load_message_registry(registry, source_channel_id=-100222)
            pipeline.register_target_message(loaded, 2, 457, source_channel_id=-100222)

            with patch.object(pipeline, "create_backup_snapshot", side_effect=OSError("backup failed")):
                pipeline.persist_message_registry(registry, loaded)

            persisted = json.loads(registry.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["source_to_target"]["-100222:2"]["target_message_id"], 457
            )

    def test_load_message_registry_recovers_from_backup_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            registry = Path(td) / "message_registry.json"
            backup = Path(td) / "message_registry.json.bak"
            registry.write_text("{not-valid-json", encoding="utf-8")
            backup.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:1": {
                                "target_message_id": 456,
                                "known_target_message_ids": [456],
                            }
                        },
                        "target_to_source": {"456": "-100222:1"},
                    }
                ),
                encoding="utf-8",
            )

            loaded = pipeline.load_message_registry(registry, source_channel_id=-100222)

            self.assertEqual(loaded["-100222:1"]["target_message_id"], 456)
            restored = json.loads(registry.read_text(encoding="utf-8"))
            self.assertEqual(restored["source_to_target"]["-100222:1"]["target_message_id"], 456)

    def test_apply_env_file_sets_missing_values_without_overwriting(self):
        with tempfile.TemporaryDirectory() as td:
            env_file = Path(td) / ".env"
            env_file.write_text("TG_API_ID=123\nOPENAI_MODEL=gpt-5-mini\n", encoding="utf-8")
            with patch.dict(os.environ, {"TG_API_ID": "999"}, clear=False):
                values = pipeline.apply_env_file(env_file)
                self.assertEqual(values["TG_API_ID"], "123")
                self.assertEqual(os.getenv("TG_API_ID"), "999")
                self.assertEqual(os.getenv("OPENAI_MODEL"), "gpt-5-mini")

    def test_load_message_registry_raises_on_corrupt_json(self):
        with tempfile.TemporaryDirectory() as td:
            registry = Path(td) / "message_registry.json"
            registry.write_text("{not-valid-json", encoding="utf-8")
            with self.assertRaises(ValueError):
                pipeline.load_message_registry(registry, source_channel_id=-100222)

    def test_validate_args_requires_bot_token_in_bot_mode(self):
        args = _args(auth_mode="bot", bot_token=None)
        with self.assertRaises(ValueError) as err:
            pipeline.validate_common_args(args)
        self.assertIn("--bot-token", str(err.exception))

    def test_validate_args_user_mode_without_bot_token(self):
        args = _args(auth_mode="user", bot_token=None)
        pipeline.validate_common_args(args)

    def test_normalize_channel_raises_on_none(self):
        with self.assertRaises(ValueError):
            pipeline.normalize_channel(None, "target")

    def test_read_export_messages_sorted(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            payload = {
                "messages": [
                    {"id": 3, "type": "message", "text": "c"},
                    {"id": 1, "type": "message", "text": "a"},
                    {"id": 2, "type": "message", "text": "b"},
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")
            messages = pipeline.read_export_messages(json_file)
            self.assertEqual([m["id"] for m in messages], [1, 2, 3])

    def test_run_export_stats_mode_prints_counts(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            payload = {
                "messages": [
                    {"id": 5, "type": "message", "text": "a"},
                    {"id": 6, "type": "message", "text": ""},
                    {"id": 7, "type": "service", "text": "ignored"},
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")
            out = io.StringIO()
            with redirect_stdout(out):
                pipeline.run_export_stats_mode(argparse.Namespace(json_file=str(json_file)))
            text = out.getvalue()
            self.assertIn("Total valid messages: 1", text)
            self.assertIn("First ID: 5, Last ID: 5", text)

    def test_run_lookup_mapping_mode_prints_primary_and_known_target_ids(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            registry_file.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:16625": {
                                "target_message_id": 3001,
                                "known_target_message_ids": [2999, 3001],
                                "published_at": "2026-03-11T10:00:00+00:00",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            out = io.StringIO()
            with redirect_stdout(out):
                pipeline.run_lookup_mapping_mode(
                    argparse.Namespace(
                        source_message_id=16625,
                        source_channel=-100222,
                        registry_file=str(registry_file),
                    )
                )
            text = out.getvalue()
            self.assertIn("Source Key: -100222:16625", text)
            self.assertIn("Primary Target Message ID: 3001", text)
            self.assertIn("Known Target Message IDs: 2999, 3001", text)

    def test_build_parser_supports_commands(self):
        parser = pipeline.build_parser()
        live_args = parser.parse_args(
            ["live", "--source-channel", "-1001", "--target-channel", "-1002"]
        )
        self.assertEqual(live_args.command, "live")

        import_args = parser.parse_args(
            ["import-json", "--json-file", "result.json", "--target-channel", "-1002"]
        )
        self.assertEqual(import_args.command, "import-json")

        lookup_args = parser.parse_args(
            ["lookup-mapping", "--source-message-id", "16625", "--source-channel", "-1001"]
        )
        self.assertEqual(lookup_args.command, "lookup-mapping")


class TestAsyncFlow(unittest.IsolatedAsyncioTestCase):
    class FakeLiveClient:
        def __init__(self, emissions):
            self.emissions = emissions
            self.handlers = []
            self.disconnect = AsyncMock()

        def on(self, _event_descriptor):
            def decorator(func):
                self.handlers.append(func)
                return func

            return decorator

        async def run_until_disconnected(self):
            for handler_index, emitted_event in self.emissions:
                await self.handlers[handler_index](emitted_event)

    async def test_reconcile_recent_source_messages_processes_chronologically(self):
        now = datetime.now(timezone.utc)
        oldest = types.SimpleNamespace(id=10, message="Oldest", raw_text="Oldest", media=None, date=now - timedelta(hours=3))
        middle = types.SimpleNamespace(id=11, message="Middle", raw_text="Middle", media=None, date=now - timedelta(hours=2))
        newest = types.SimpleNamespace(id=12, message="Newest", raw_text="Newest", media=None, date=now - timedelta(hours=1))
        too_old = types.SimpleNamespace(id=9, message="Too old", raw_text="Too old", media=None, date=now - timedelta(hours=25))

        class FakeClient:
            async def iter_messages(self, channel, limit=None):
                for item in (newest, middle, oldest, too_old):
                    yield item

        processed_ids = []

        async def process_message(message):
            processed_ids.append(message.id)

        summary = await pipeline.reconcile_recent_source_messages(
            client=FakeClient(),
            source_channel=-100222,
            lookback_hours=24,
            process_message=process_message,
        )

        self.assertEqual(processed_ids, [10, 11, 12])
        self.assertEqual(summary["processed"], 3)
        self.assertEqual(summary["scanned"], 3)

    async def test_start_telegram_client_bot_and_user_modes(self):
        created = []

        class FakeClient:
            def __init__(self, *args, **kwargs):
                self.start = AsyncMock()
                created.append(self)

        with patch.object(pipeline, "TelegramClient", FakeClient):
            bot_client = await pipeline.start_telegram_client(
                "session", 1, "hash", "token-123", "auto"
            )
            bot_client.start.assert_awaited_once_with(bot_token="token-123")

            user_client = await pipeline.start_telegram_client("session", 1, "hash", None, "user")
            user_client.start.assert_awaited_once_with()

        self.assertEqual(len(created), 2)

    async def test_start_telegram_client_invalid_mode(self):
        class FakeClient:
            def __init__(self, *args, **kwargs):
                self.start = AsyncMock()

        with patch.object(pipeline, "TelegramClient", FakeClient):
            with self.assertRaises(ValueError):
                await pipeline.start_telegram_client("s", 1, "h", None, "invalid")

    async def test_ensure_channel_access_wraps_error(self):
        client = types.SimpleNamespace(get_entity=AsyncMock(side_effect=Exception("entity error")))
        with self.assertRaises(RuntimeError) as err:
            await pipeline.ensure_channel_access(client, -100, "source")
        self.assertIn("Cannot access source channel -100", str(err.exception))

    async def test_publish_message_dry_run_skips_client_calls(self):
        client = types.SimpleNamespace(send_file=AsyncMock(), send_message=AsyncMock())
        await pipeline.publish_message(client, -100, "hello", None, dry_run=True)
        client.send_file.assert_not_called()
        client.send_message.assert_not_called()

    async def test_publish_message_text_only(self):
        client = types.SimpleNamespace(send_file=AsyncMock(), send_message=AsyncMock())
        await pipeline.publish_message(client, -100, "hello", None, dry_run=False)
        client.send_message.assert_awaited_once_with(-100, "hello", link_preview=False)
        client.send_file.assert_not_called()

    async def test_publish_message_caption_too_long_fallback(self):
        state = {"calls": 0}

        async def send_file_side_effect(*args, **kwargs):
            state["calls"] += 1
            if state["calls"] == 1:
                raise Exception("Caption too long")
            return None

        client = types.SimpleNamespace(
            send_file=AsyncMock(side_effect=send_file_side_effect),
            send_message=AsyncMock(),
        )

        await pipeline.publish_message(
            client,
            -100,
            text="translated text",
            media=Path("any.jpg"),
            dry_run=False,
        )

        self.assertEqual(client.send_file.await_count, 2)
        first_kwargs = client.send_file.await_args_list[0].kwargs
        second_kwargs = client.send_file.await_args_list[1].kwargs
        self.assertEqual(first_kwargs["caption"], "translated text")
        self.assertIsNone(second_kwargs["caption"])
        client.send_message.assert_awaited_once_with(-100, "translated text", link_preview=False)

    async def test_edit_message_retries_then_success(self):
        client = types.SimpleNamespace(
            edit_message=AsyncMock(side_effect=[Exception("temporary"), None])
        )

        with patch.object(pipeline.asyncio, "sleep", new=AsyncMock()) as sleep_mock:
            await pipeline.edit_message(client, -100, 10, "שלום", dry_run=False)

        self.assertEqual(client.edit_message.await_count, 2)
        sleep_mock.assert_awaited_once()

    async def test_run_export_pipeline_filters_english_and_saves_progress(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            progress_file = Path(td) / "progress.txt"
            payload = {
                "messages": [
                    {
                        "id": 3,
                        "type": "message",
                        "date": "2026-03-10T10:00:00",
                        "text": "Second long English message for testing translation order",
                    },
                    {"id": 2, "type": "message", "text": "שלום זה טקסט בעברית"},
                    {
                        "id": 1,
                        "type": "message",
                        "date": "2026-03-10T08:00:00",
                        "text": "First long English message for testing translation order",
                    },
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")

            registry_file = Path(td) / "message_registry.json"
            translator = types.SimpleNamespace(
                translate=AsyncMock(side_effect=lambda text: f"TR:{text}")
            )
            publish_mock = AsyncMock(
                side_effect=[
                    types.SimpleNamespace(id=901),
                    types.SimpleNamespace(id=902),
                ]
            )

            with patch.object(pipeline, "publish_message", new=publish_mock), patch.object(
                pipeline.asyncio, "sleep", new=AsyncMock()
            ):
                await pipeline.run_export_pipeline(
                    client=object(),
                    translator=translator,
                    json_file=json_file,
                    target_channel=-100,
                    worker_count=3,
                    publish_delay=0,
                    progress_file=progress_file,
                    registry_file=registry_file,
                    source_channel_id=-100222,
                    min_id=None,
                    max_id=None,
                    filter_only_english=True,
                    translate_only_english=True,
                    dry_run=False,
                )

            self.assertEqual(publish_mock.await_count, 2)
            published_texts = [c.kwargs["text"] for c in publish_mock.await_args_list]
            self.assertTrue(published_texts[0].startswith("TR:First long English"))
            self.assertTrue(published_texts[1].startswith("TR:Second long English"))
            self.assertEqual(progress_file.read_text(encoding="utf-8"), "3")
            registry_data = json.loads(registry_file.read_text(encoding="utf-8"))
            self.assertEqual(
                registry_data["source_to_target"]["-100222:1"]["target_message_id"], 901
            )
            self.assertEqual(
                registry_data["source_to_target"]["-100222:3"]["target_message_id"], 902
            )
            self.assertIsNotNone(
                registry_data["source_to_target"]["-100222:1"].get("published_at")
            )

    async def test_run_export_pipeline_skips_registered_messages(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            registry_file = Path(td) / "message_registry.json"
            registry_file.write_text(
                json.dumps({"source_to_target": {"-100222:1": 901}}),
                encoding="utf-8",
            )
            payload = {
                "messages": [
                    {
                        "id": 1,
                        "type": "message",
                        "text": "First long English message for testing translation order",
                    },
                    {
                        "id": 3,
                        "type": "message",
                        "text": "Second long English message for testing translation order",
                    },
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")

            translator = types.SimpleNamespace(
                translate=AsyncMock(side_effect=lambda text: f"TR:{text}")
            )
            publish_mock = AsyncMock(return_value=types.SimpleNamespace(id=902))

            with patch.object(pipeline, "publish_message", new=publish_mock), patch.object(
                pipeline.asyncio, "sleep", new=AsyncMock()
            ):
                await pipeline.run_export_pipeline(
                    client=object(),
                    translator=translator,
                    json_file=json_file,
                    target_channel=-100,
                    worker_count=2,
                    publish_delay=0,
                    progress_file=None,
                    registry_file=registry_file,
                    source_channel_id=-100222,
                    min_id=None,
                    max_id=None,
                    filter_only_english=True,
                    translate_only_english=True,
                    dry_run=False,
                )

            self.assertEqual(translator.translate.await_count, 1)
            publish_mock.assert_awaited_once()
            registry_data = json.loads(registry_file.read_text(encoding="utf-8"))
            self.assertEqual(
                registry_data["source_to_target"]["-100222:1"]["target_message_id"], 901
            )
            self.assertEqual(
                registry_data["source_to_target"]["-100222:3"]["target_message_id"], 902
            )

    async def test_run_export_pipeline_dry_run_does_not_write_state(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            progress_file = Path(td) / "progress.txt"
            registry_file = Path(td) / "message_registry.json"
            payload = {
                "messages": [
                    {
                        "id": 1,
                        "type": "message",
                        "date": "2026-03-10T08:00:00",
                        "text": "First long English message for dry run testing",
                    }
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")

            translator = types.SimpleNamespace(
                translate=AsyncMock(side_effect=lambda text: f"TR:{text}")
            )
            publish_mock = AsyncMock(return_value=None)

            with patch.object(pipeline, "publish_message", new=publish_mock), patch.object(
                pipeline.asyncio, "sleep", new=AsyncMock()
            ):
                await pipeline.run_export_pipeline(
                    client=object(),
                    translator=translator,
                    json_file=json_file,
                    target_channel=-100,
                    worker_count=1,
                    publish_delay=0,
                    progress_file=progress_file,
                    registry_file=registry_file,
                    source_channel_id=-100222,
                    min_id=None,
                    max_id=None,
                    filter_only_english=True,
                    translate_only_english=True,
                    dry_run=True,
                )

            publish_mock.assert_awaited_once()
            self.assertFalse(progress_file.exists())
            self.assertFalse(registry_file.exists())

    async def test_run_export_pipeline_raises_when_state_persistence_fails_after_publish(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            payload = {
                "messages": [
                    {
                        "id": 1,
                        "type": "message",
                        "date": "2026-03-10T08:00:00",
                        "text": "First long English message for persistence failure testing",
                    }
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")

            translator = types.SimpleNamespace(
                translate=AsyncMock(side_effect=lambda text: f"TR:{text}")
            )
            publish_mock = AsyncMock(return_value=types.SimpleNamespace(id=901))

            with patch.object(pipeline, "publish_message", new=publish_mock), patch.object(
                pipeline,
                "persist_registry_and_progress",
                side_effect=pipeline.StatePersistenceError("disk full"),
            ), patch.object(
                pipeline.asyncio, "sleep", new=AsyncMock()
            ):
                with self.assertRaises(pipeline.StatePersistenceError):
                    await pipeline.run_export_pipeline(
                        client=object(),
                        translator=translator,
                        json_file=json_file,
                        target_channel=-100,
                        worker_count=1,
                        publish_delay=0,
                        progress_file=Path(td) / "progress.txt",
                        registry_file=Path(td) / "message_registry.json",
                        source_channel_id=-100222,
                        min_id=None,
                        max_id=None,
                        filter_only_english=True,
                        translate_only_english=True,
                        dry_run=False,
                    )

    async def test_run_export_pipeline_no_pending_messages(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            payload = {
                "messages": [
                    {"id": 1, "type": "message", "text": "שלום עולם"},
                    {"id": 2, "type": "service", "text": "ignored"},
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")

            translator = types.SimpleNamespace(translate=AsyncMock())
            publish_mock = AsyncMock(return_value=None)

            with patch.object(pipeline, "publish_message", new=publish_mock):
                await pipeline.run_export_pipeline(
                    client=object(),
                    translator=translator,
                    json_file=json_file,
                    target_channel=-100,
                    worker_count=2,
                    publish_delay=0,
                    progress_file=None,
                    registry_file=None,
                    source_channel_id=None,
                    min_id=None,
                    max_id=None,
                    filter_only_english=True,
                    translate_only_english=True,
                    dry_run=False,
                )

            publish_mock.assert_not_called()
            translator.translate.assert_not_called()

    async def test_sweep_recent_target_duplicates_deletes_older_messages_and_repairs_registry(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            registry_file.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:10": {
                                "target_message_id": 500,
                                "known_target_message_ids": [500, 501],
                                "published_at": "2026-03-11T10:00:00+00:00",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            message_map = pipeline.load_message_registry(registry_file, source_channel_id=-100222)
            now = datetime.now(timezone.utc)
            duplicate_text = "Duplicate long translated message kept only once in the target channel"
            keep_message = types.SimpleNamespace(id=501, message=duplicate_text, raw_text=duplicate_text, date=now)
            delete_message = types.SimpleNamespace(
                id=500,
                message=duplicate_text,
                raw_text=duplicate_text,
                date=now - timedelta(minutes=2),
            )
            unique_message = types.SimpleNamespace(
                id=400,
                message="Unique long translated message that should remain untouched",
                raw_text="Unique long translated message that should remain untouched",
                date=now - timedelta(minutes=10),
            )

            class FakeSweepClient:
                def __init__(self):
                    self.delete_messages = AsyncMock()

                async def iter_messages(self, channel, limit=None):
                    for item in (keep_message, delete_message, unique_message):
                        yield item

            client = FakeSweepClient()
            summary = await pipeline.sweep_recent_target_duplicates(
                client=client,
                target_channel=-100111,
                message_map=message_map,
                registry_file=registry_file,
                lookback_hours=24,
                cluster_gap_minutes=60,
                dry_run=False,
            )

            client.delete_messages.assert_awaited_once_with(-100111, [500])
            self.assertEqual(summary["deleted"], 1)
            registry_data = json.loads(registry_file.read_text(encoding="utf-8"))
            self.assertEqual(
                registry_data["source_to_target"]["-100222:10"]["target_message_id"], 501
            )

    async def test_run_live_mode_runs_initial_source_reconciliation(self):
        args = _args(source_reconcile_interval_minutes=5, target_dedupe_interval_minutes=0)

        class FakeIdleLiveClient:
            def __init__(self):
                self.handlers = []
                self.disconnect = AsyncMock()

            def on(self, _event_descriptor):
                def decorator(func):
                    self.handlers.append(func)
                    return func

                return decorator

            async def run_until_disconnected(self):
                await asyncio.sleep(0)
                await asyncio.sleep(0)

        fake_client = FakeIdleLiveClient()
        translator = types.SimpleNamespace(translate=AsyncMock())
        reconcile_mock = AsyncMock(return_value={"processed": 0})

        with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
            pipeline, "ensure_channel_access", AsyncMock()
        ), patch.object(
            pipeline, "reconcile_recent_source_messages", new=reconcile_mock
        ):
            await pipeline.run_live_mode(args, translator)

        reconcile_mock.assert_awaited_once()
        fake_client.disconnect.assert_awaited_once()

    async def test_run_live_mode_skips_duplicate_new_message(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            registry_file.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:10": {
                                "target_message_id": 500,
                                "known_target_message_ids": [500, 501],
                                "published_at": "2026-03-11T10:00:00+00:00",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            args = _args(registry_file=str(registry_file), dry_run=False, edit_window_hours=24)
            event = types.SimpleNamespace(
                id=10,
                raw_text="Duplicate long English message for live mode testing",
                media=None,
                date=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
            )

            fake_client = self.FakeLiveClient([(0, event)])
            translator = types.SimpleNamespace(translate=AsyncMock(return_value="TR:duplicate"))
            publish_mock = AsyncMock()
            edit_mock = AsyncMock()

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ), patch.object(
                pipeline, "publish_message", new=publish_mock
            ), patch.object(
                pipeline, "edit_message", new=edit_mock
            ):
                await pipeline.run_live_mode(args, translator)

            translator.translate.assert_not_awaited()
            publish_mock.assert_not_awaited()
            edit_mock.assert_not_awaited()
            fake_client.disconnect.assert_awaited_once()

    async def test_run_live_mode_updates_edit_within_window(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            registry_file.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:10": {
                                "target_message_id": 500,
                                "known_target_message_ids": [500, 501],
                                "published_at": "2026-03-11T10:00:00+00:00",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            args = _args(registry_file=str(registry_file), dry_run=False, edit_window_hours=24)
            event = types.SimpleNamespace(
                id=10,
                raw_text="Edited long English message for live mode testing",
                media=None,
                date=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
            )

            fake_client = self.FakeLiveClient([(1, event)])
            translator = types.SimpleNamespace(translate=AsyncMock(return_value="TR:edited"))
            publish_mock = AsyncMock()
            edit_mock = AsyncMock()

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ), patch.object(
                pipeline, "publish_message", new=publish_mock
            ), patch.object(
                pipeline, "edit_message", new=edit_mock
            ):
                await pipeline.run_live_mode(args, translator)

            translator.translate.assert_awaited_once()
            publish_mock.assert_not_awaited()
            edit_mock.assert_awaited_once_with(
                client=fake_client,
                target_channel=-100111,
                message_id=500,
                translated_text="TR:edited",
                dry_run=False,
            )
            fake_client.disconnect.assert_awaited_once()

    async def test_run_live_mode_skips_edit_outside_window(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            registry_file.write_text(
                json.dumps(
                    {
                        "source_to_target": {
                            "-100222:10": {
                                "target_message_id": 500,
                                "known_target_message_ids": [500, 501],
                                "published_at": "2026-03-11T10:00:00+00:00",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            args = _args(registry_file=str(registry_file), dry_run=False, edit_window_hours=24)
            event = types.SimpleNamespace(
                id=10,
                raw_text="Late edit long English message for live mode testing",
                media=None,
                date=datetime(2026, 3, 12, 11, 0, tzinfo=timezone.utc),
            )

            fake_client = self.FakeLiveClient([(1, event)])
            translator = types.SimpleNamespace(translate=AsyncMock(return_value="TR:late-edit"))
            publish_mock = AsyncMock()
            edit_mock = AsyncMock()

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ), patch.object(
                pipeline, "publish_message", new=publish_mock
            ), patch.object(
                pipeline, "edit_message", new=edit_mock
            ):
                await pipeline.run_live_mode(args, translator)

            translator.translate.assert_not_awaited()
            publish_mock.assert_not_awaited()
            edit_mock.assert_not_awaited()
            fake_client.disconnect.assert_awaited_once()
    async def test_run_live_mode_skips_edit_without_existing_mapping(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            args = _args(registry_file=str(registry_file), dry_run=False, edit_window_hours=24)
            event = types.SimpleNamespace(
                id=10,
                raw_text="Edited long English message without mapping",
                media=None,
                date=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
            )

            fake_client = self.FakeLiveClient([(1, event)])
            translator = types.SimpleNamespace(translate=AsyncMock(return_value="TR:edited"))
            publish_mock = AsyncMock()
            edit_mock = AsyncMock()

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ), patch.object(
                pipeline, "publish_message", new=publish_mock
            ), patch.object(
                pipeline, "edit_message", new=edit_mock
            ):
                await pipeline.run_live_mode(args, translator)

            translator.translate.assert_not_awaited()
            publish_mock.assert_not_awaited()
            edit_mock.assert_not_awaited()
            fake_client.disconnect.assert_awaited_once()

    async def test_run_live_mode_suspends_background_tasks_until_future_timestamp(self):
        args = _args(
            source_reconcile_interval_minutes=5,
            target_dedupe_interval_minutes=5,
            live_background_tasks_suspend_until="2099-01-01T00:00:00+00:00",
        )

        class FakeIdleLiveClient:
            def __init__(self):
                self.handlers = []
                self.disconnect = AsyncMock()

            def on(self, _event_descriptor):
                def decorator(func):
                    self.handlers.append(func)
                    return func

                return decorator

            async def run_until_disconnected(self):
                return None

        fake_client = FakeIdleLiveClient()
        translator = types.SimpleNamespace(translate=AsyncMock())
        reconcile_mock = AsyncMock(return_value={"processed": 0})
        sweep_mock = AsyncMock(return_value={"deleted": 0})

        with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
            pipeline, "ensure_channel_access", AsyncMock()
        ), patch.object(
            pipeline, "reconcile_recent_source_messages", new=reconcile_mock
        ), patch.object(
            pipeline, "sweep_recent_target_duplicates", new=sweep_mock
        ):
            await pipeline.run_live_mode(args, translator)

        reconcile_mock.assert_not_awaited()
        sweep_mock.assert_not_awaited()
        fake_client.disconnect.assert_awaited_once()

    async def test_run_live_mode_disconnects_if_registry_persist_fails_after_publish(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            args = _args(registry_file=str(registry_file), dry_run=False, edit_window_hours=24)
            event = types.SimpleNamespace(
                id=10,
                raw_text="New long English message for live persistence testing",
                media=None,
                date=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
            )

            fake_client = self.FakeLiveClient([(0, event)])
            translator = types.SimpleNamespace(translate=AsyncMock(return_value="TR:new"))
            publish_mock = AsyncMock(return_value=types.SimpleNamespace(id=700))

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ), patch.object(
                pipeline, "publish_message", new=publish_mock
            ), patch.object(
                pipeline, "persist_message_registry", side_effect=OSError("disk full")
            ):
                await pipeline.run_live_mode(args, translator)

            publish_mock.assert_awaited_once()
            self.assertGreaterEqual(fake_client.disconnect.await_count, 2)

    async def test_run_live_mode_writes_heartbeat_file(self):
        with tempfile.TemporaryDirectory() as td:
            heartbeat_file = Path(td) / "live_heartbeat.json"
            args = _args(
                source_reconcile_interval_minutes=0,
                target_dedupe_interval_minutes=0,
                heartbeat_file=str(heartbeat_file),
                heartbeat_interval_seconds=60,
            )

            class FakeIdleLiveClient:
                def __init__(self):
                    self.handlers = []
                    self.disconnect = AsyncMock()

                def on(self, _event_descriptor):
                    def decorator(func):
                        self.handlers.append(func)
                        return func

                    return decorator

                async def run_until_disconnected(self):
                    await asyncio.sleep(0)

            fake_client = FakeIdleLiveClient()
            translator = types.SimpleNamespace(translate=AsyncMock())

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ):
                await pipeline.run_live_mode(args, translator)

            self.assertTrue(heartbeat_file.exists())
            heartbeat_data = json.loads(heartbeat_file.read_text(encoding="utf-8"))
            self.assertEqual(heartbeat_data["status"], "stopped")
            self.assertEqual(heartbeat_data["source_channel"], -100222)
            self.assertEqual(heartbeat_data["target_channel"], -100111)
    async def test_run_live_mode_resumes_background_tasks_after_suspension(self):
        args = _args(
            source_reconcile_interval_minutes=10,
            target_dedupe_interval_minutes=5,
            live_background_tasks_suspend_until="2099-01-01T00:00:00+00:00",
        )

        class FakeIdleLiveClient:
            def __init__(self):
                self.handlers = []
                self.disconnect = AsyncMock()

            def on(self, _event_descriptor):
                def decorator(func):
                    self.handlers.append(func)
                    return func

                return decorator

            async def run_until_disconnected(self):
                await real_sleep(0)
                await real_sleep(0)

        fake_client = FakeIdleLiveClient()
        translator = types.SimpleNamespace(translate=AsyncMock())
        reconcile_mock = AsyncMock(return_value={"processed": 0})
        sweep_mock = AsyncMock(return_value={"deleted": 0})
        real_sleep = asyncio.sleep
        real_create_task = asyncio.create_task
        created_periodic = []
        sleep_calls = []

        async def fake_sleep(seconds):
            sleep_calls.append(seconds)
            return None

        def fake_create_task(coro):
            coro_name = getattr(getattr(coro, "cr_code", None), "co_name", "")
            if coro_name in {"periodic_source_reconciliation", "periodic_duplicate_sweep"}:
                created_periodic.append(coro_name)
                coro.close()

                async def done_task():
                    return None

                return real_create_task(done_task())
            return real_create_task(coro)

        with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
            pipeline, "ensure_channel_access", AsyncMock()
        ), patch.object(
            pipeline, "reconcile_recent_source_messages", new=reconcile_mock
        ), patch.object(
            pipeline, "sweep_recent_target_duplicates", new=sweep_mock
        ), patch.object(
            pipeline.asyncio, "sleep", new=fake_sleep
        ), patch.object(
            pipeline.asyncio, "create_task", new=fake_create_task
        ):
            await pipeline.run_live_mode(args, translator)

        reconcile_mock.assert_awaited_once()
        sweep_mock.assert_awaited_once()
        self.assertIn("periodic_source_reconciliation", created_periodic)
        self.assertIn("periodic_duplicate_sweep", created_periodic)
        self.assertTrue(any(seconds > 0 for seconds in sleep_calls))
        fake_client.disconnect.assert_awaited_once()

    async def test_run_live_mode_serializes_concurrent_new_and_edit_for_same_source(self):
        with tempfile.TemporaryDirectory() as td:
            registry_file = Path(td) / "message_registry.json"
            args = _args(registry_file=str(registry_file), dry_run=False, edit_window_hours=24)
            new_event = types.SimpleNamespace(
                id=10,
                raw_text="Original long English message for concurrent live mode testing",
                media=None,
                date=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
            )
            edit_event = types.SimpleNamespace(
                id=10,
                raw_text="Edited long English message for concurrent live mode testing",
                media=None,
                date=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
            )
            publish_started = asyncio.Event()
            release_publish = asyncio.Event()

            class FakeConcurrentLiveClient:
                def __init__(self):
                    self.handlers = []
                    self.disconnect = AsyncMock()

                def on(self, _event_descriptor):
                    def decorator(func):
                        self.handlers.append(func)
                        return func

                    return decorator

                async def run_until_disconnected(self):
                    new_task = asyncio.create_task(self.handlers[0](new_event))
                    await publish_started.wait()
                    edit_task = asyncio.create_task(self.handlers[1](edit_event))
                    release_publish.set()
                    await asyncio.gather(new_task, edit_task)

            fake_client = FakeConcurrentLiveClient()
            translator = types.SimpleNamespace(
                translate=AsyncMock(side_effect=lambda text: f"TR:{text}")
            )

            async def publish_side_effect(*args, **kwargs):
                publish_started.set()
                await release_publish.wait()
                return types.SimpleNamespace(id=700)

            publish_mock = AsyncMock(side_effect=publish_side_effect)
            edit_mock = AsyncMock()

            with patch.object(pipeline, "start_telegram_client", AsyncMock(return_value=fake_client)), patch.object(
                pipeline, "ensure_channel_access", AsyncMock()
            ), patch.object(
                pipeline, "publish_message", new=publish_mock
            ), patch.object(
                pipeline, "edit_message", new=edit_mock
            ):
                await pipeline.run_live_mode(args, translator)

            self.assertEqual(publish_mock.await_count, 1)
            edit_mock.assert_awaited_once_with(
                client=fake_client,
                target_channel=-100111,
                message_id=700,
                translated_text="TR:Edited long English message for concurrent live mode testing",
                dry_run=False,
            )
            self.assertEqual(translator.translate.await_count, 2)
            registry_data = json.loads(registry_file.read_text(encoding="utf-8"))
            self.assertEqual(
                registry_data["source_to_target"]["-100222:10"]["target_message_id"], 700
            )
            fake_client.disconnect.assert_awaited_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)














