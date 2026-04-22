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
        self.texts: list[str] = []

    async def reply_text(self, text: str, parse_mode=None, **kwargs):
        self.texts.append(text)
        return _FakeSentMessage(text=text)


class _FakeCallbackQuery:
    def __init__(self, data: str):
        self.data = data
        self.message = _FakeMessage()
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append({"text": text, "show_alert": show_alert})

    async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
        self.edits.append({"text": text, "parse_mode": parse_mode, "reply_markup": reply_markup})


class ExportRecentFlowTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_export_recent_button_redirects_to_dashboard(self):
        bot = self._build_bot()
        query = _FakeCallbackQuery(data="export_recent")
        update = SimpleNamespace(
            callback_query=query,
            effective_chat=SimpleNamespace(id="1"),
        )

        await bot._handle_button(update, context=None)

        self.assertEqual(query.answers[0]["text"], "Moved to HA dashboard")
        self.assertTrue(query.answers[0]["show_alert"])
        self.assertIn("HA dashboard", query.edits[0]["text"])

    async def test_session_export_button_redirects_to_dashboard(self):
        bot = self._build_bot()
        query = _FakeCallbackQuery(data="session_export")
        update = SimpleNamespace(
            callback_query=query,
            effective_chat=SimpleNamespace(id="1"),
        )

        await bot._handle_button(update, context=None)

        self.assertEqual(query.answers[0]["text"], "Moved to HA dashboard")
        self.assertIn("HA dashboard", query.edits[0]["text"])


if __name__ == "__main__":
    unittest.main()
