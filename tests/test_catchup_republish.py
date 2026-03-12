import argparse
import json
import os
import sys
import tempfile
import types
import unittest
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


import catchup_republish as catchup


class TestCatchupHelpers(unittest.TestCase):
    def test_parse_min_date_and_marker(self):
        self.assertEqual(str(catchup.parse_min_date("2026-03-08")), "2026-03-08")
        self.assertEqual(catchup.marker_for_dedupe("  ABCdef   long\ntext "), "abcdef long text")
        with self.assertRaises(ValueError):
            catchup.parse_min_date("08-03-2026")

    def test_progress_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "progress.txt"
            catchup.save_progress(path, 42)
            self.assertEqual(catchup.load_progress(path), 42)

    def test_extract_message_date(self):
        self.assertEqual(str(catchup.extract_message_date({"date": "2026-03-10T08:00:00"})), "2026-03-10")
        self.assertIsNone(catchup.extract_message_date({"date": "bad-date"}))


class TestCatchupMain(unittest.IsolatedAsyncioTestCase):
    async def test_main_stops_when_required_settings_missing(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            json_file.write_text(json.dumps({"messages": []}), encoding="utf-8")

            args = argparse.Namespace(
                json_file=str(json_file),
                target_channel=0,
                source_channel=0,
                session="user_live_session",
                existing_limit=10,
                min_date="2026-03-08",
                workers=5,
                delay=0,
                progress_file=str(Path(td) / "progress.txt"),
                registry_file=str(Path(td) / "message_registry.json"),
                dry_run=True,
            )

            with patch.object(catchup, "parse_args", return_value=args), patch.object(
                catchup, "apply_env_file", return_value={}
            ), patch.dict(
                os.environ,
                {"TG_API_ID": "", "TG_API_HASH": "", "OPENAI_API_KEY": "", "TARGET_CHANNEL": "0"},
                clear=False,
            ), patch("builtins.print") as print_mock, patch.object(
                catchup, "start_telegram_client"
            ) as client_mock:
                await catchup.main()

            client_mock.assert_not_called()
            printed = "\n".join(str(call.args[0]) for call in print_mock.call_args_list if call.args)
            self.assertIn("Missing required settings", printed)

    async def test_main_skips_duplicates_and_non_english(self):
        with tempfile.TemporaryDirectory() as td:
            json_file = Path(td) / "result.json"
            progress_file = Path(td) / "progress_catchup.txt"
            payload = {
                "messages": [
                    {
                        "id": 1,
                        "type": "message",
                        "date": "2026-03-10T08:00:00",
                        "text": "First long English message for duplicate detection in tests",
                    },
                    {
                        "id": 2,
                        "type": "message",
                        "date": "2026-03-10T09:00:00",
                        "text": "שלום זה טקסט בעברית",
                    },
                    {
                        "id": 3,
                        "type": "message",
                        "date": "2026-03-10T10:00:00",
                        "text": "Second long English message that should be published",
                    },
                    {
                        "id": 5,
                        "type": "message",
                        "date": "2026-03-07T23:59:00",
                        "text": "Old long English message before cutoff date",
                    },
                ]
            }
            json_file.write_text(json.dumps(payload), encoding="utf-8")

            registry_file = Path(td) / "message_registry.json"
            args = argparse.Namespace(
                json_file=str(json_file),
                target_channel=0,
                source_channel=-100123,
                session="user_live_session",
                existing_limit=500,
                min_date="2026-03-08",
                workers=10,
                delay=0,
                progress_file=str(progress_file),
                registry_file=str(registry_file),
                dry_run=False,
            )

            class FakeMsg:
                def __init__(self, message: str, message_id: int):
                    self.message = message
                    self.id = message_id

            class FakeClient:
                def __init__(self):
                    self.disconnect = AsyncMock()

                async def iter_messages(self, channel, limit):
                    yield FakeMsg(
                        "TR:First long English message for duplicate detection in tests",
                        501,
                    )

            translator = types.SimpleNamespace(
                translate=AsyncMock(side_effect=lambda text: f"TR:{text}")
            )
            publish_mock = AsyncMock(return_value=types.SimpleNamespace(id=777))
            fake_client = FakeClient()

            with patch.object(catchup, "parse_args", return_value=args), patch.object(
                catchup, "apply_env_file", return_value={}
            ), patch.dict(
                os.environ,
                {
                    "TG_API_ID": "123",
                    "TG_API_HASH": "hash",
                    "OPENAI_API_KEY": "key",
                    "OPENAI_MODEL": "gpt-5-mini",
                    "TARGET_CHANNEL": "-100999",
                    "SOURCE_CHANNEL": "-100123",
                    "TG_AUTH_MODE": "user",
                    "TG_SESSION": "user_live_session",
                },
                clear=False,
            ), patch.object(
                catchup, "start_telegram_client", AsyncMock(return_value=fake_client)
            ), patch.object(
                catchup, "ensure_channel_access", AsyncMock()
            ), patch.object(
                catchup, "OpenAITranslator", return_value=translator
            ), patch.object(
                catchup, "publish_message", new=publish_mock
            ), patch.object(
                catchup.asyncio, "sleep", new=AsyncMock()
            ), patch("builtins.print"):
                await catchup.main()

            self.assertEqual(translator.translate.await_count, 2)
            self.assertEqual(publish_mock.await_count, 1)
            call = publish_mock.await_args_list[0]
            self.assertEqual(call.args[1], -100999)
            self.assertTrue(call.kwargs["text"].startswith("TR:Second long English"))
            self.assertEqual(progress_file.read_text(encoding="utf-8"), "3")
            registry_data = json.loads(registry_file.read_text(encoding="utf-8"))
            self.assertEqual(registry_data["source_to_target"]["-100123:1"]["target_message_id"], 501)
            self.assertEqual(registry_data["source_to_target"]["-100123:3"]["target_message_id"], 777)
            fake_client.disconnect.assert_awaited_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)



