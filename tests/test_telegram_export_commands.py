import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.telegram_bot import TelegramBot


class _FakeSentMessage:
    def __init__(self, text: str = ""):
        self.text = text
        self.message_id = 1


class _FakeMessage:
    def __init__(self):
        self.message_id = 123
        self.texts: list[str] = []

    async def reply_text(self, text: str, parse_mode=None, **kwargs):
        self.texts.append(text)
        return _FakeSentMessage(text=text)


class TelegramExportCommandTests(unittest.IsolatedAsyncioTestCase):
    def _build_bot(self) -> TelegramBot:
        bot = TelegramBot(
            SimpleNamespace(
                telegram_chat_id="1",
                execution=SimpleNamespace(order_size_usd=10.0, dry_run=True),
                cli=SimpleNamespace(allow_mutating_commands=False),
            ),
            tracker=object(),
            risk_manager=object(),
        )
        bot._track = lambda msg: msg
        return bot

    async def test_export_command_redirects_to_dashboard(self):
        bot = self._build_bot()
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            message=_FakeMessage(),
        )

        await bot._cmd_export(update, SimpleNamespace(args=["today"]))

        self.assertTrue(any("HA dashboard" in text for text in update.message.texts))

    async def test_sync_repo_latest_command_redirects_to_dashboard(self):
        bot = self._build_bot()
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            message=_FakeMessage(),
        )

        await bot._cmd_sync_repo_latest(update, context=None)

        self.assertTrue(any("HA dashboard" in text for text in update.message.texts))


if __name__ == "__main__":
    unittest.main()
