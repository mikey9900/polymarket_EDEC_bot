import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.telegram_bot import TelegramBot


class _FakeRiskManager:
    def __init__(self):
        self.resume_calls = 0
        self.deactivate_calls = 0
        self.pause_calls = 0
        self.status = {
            "kill_switch": False,
            "paused": False,
            "daily_pnl": 4.5,
            "open_positions": 1,
        }

    def resume(self):
        self.resume_calls += 1

    def pause(self):
        self.pause_calls += 1

    def deactivate_kill_switch(self):
        self.deactivate_calls += 1

    def get_status(self):
        return dict(self.status)


class _FakeStrategyEngine:
    def __init__(self):
        self.start_calls = 0
        self.stop_calls = 0
        self.mode = "both"
        self.is_active = True

    def start_scanning(self):
        self.start_calls += 1
        self.is_active = True

    def stop_scanning(self):
        self.stop_calls += 1
        self.is_active = False


class _FakeTracker:
    def get_recent_trades(self, limit: int = 5):
        return []

    def get_paper_capital(self):
        return (100.0, 100.0)

    def get_daily_stats(self, date=None):
        return {
            "date": "2026-04-21",
            "total_evaluations": 10,
            "signals": 3,
            "skips": 1,
            "trades_executed": 2,
            "successful": 2,
            "aborted": 0,
        }


class _FakeExecutor:
    order_size_usd = 10.0

    def set_order_size(self, usd: float):
        self.order_size_usd = usd


class _FakeMessage:
    def __init__(self):
        self.message_id = 123


class _FakeCallbackQuery:
    def __init__(self, data: str):
        self.data = data
        self.message = _FakeMessage()
        self.answers: list[dict] = []
        self.edits: list[dict] = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append({"text": text, "show_alert": show_alert})

    async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
        self.edits.append(
            {
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )


class TelegramButtonControllerTests(unittest.IsolatedAsyncioTestCase):
    def _build_bot(self) -> TelegramBot:
        bot = TelegramBot(
            SimpleNamespace(
                telegram_chat_id="1",
                execution=SimpleNamespace(order_size_usd=5.0, dry_run=True),
                cli=SimpleNamespace(allow_mutating_commands=False),
            ),
            tracker=_FakeTracker(),
            risk_manager=_FakeRiskManager(),
            strategy_engine=_FakeStrategyEngine(),
            executor=_FakeExecutor(),
        )
        bot._track = lambda msg: msg
        return bot

    async def test_start_button_uses_shared_control_plane(self):
        bot = self._build_bot()
        query = _FakeCallbackQuery("start")
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            callback_query=query,
        )

        await bot._handle_button(update, context=None)

        self.assertEqual(bot.strategy_engine.start_calls, 1)
        self.assertEqual(bot.risk_manager.resume_calls, 1)
        self.assertEqual(bot.risk_manager.deactivate_calls, 1)
        self.assertEqual(query.answers[0]["text"], "▶ Scanning started")

    async def test_status_button_edits_message_with_backup_panel(self):
        bot = self._build_bot()
        query = _FakeCallbackQuery("status")
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            callback_query=query,
        )

        await bot._handle_button(update, context=None)

        self.assertEqual(len(query.answers), 1)
        self.assertEqual(query.answers[0]["text"], None)
        self.assertEqual(len(query.edits), 1)
        self.assertIn("*Status*", query.edits[0]["text"])
        self.assertIn("RUNNING", query.edits[0]["text"])
        self.assertIn("Budget: $10/trade", query.edits[0]["text"])

    async def test_export_recent_button_redirects_to_dashboard(self):
        bot = self._build_bot()
        query = _FakeCallbackQuery("export_recent")
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            callback_query=query,
        )

        await bot._handle_button(update, context=None)

        self.assertEqual(query.answers[0]["text"], "Moved to HA dashboard")
        self.assertTrue(query.answers[0]["show_alert"])
        self.assertIn("HA dashboard", query.edits[0]["text"])


if __name__ == "__main__":
    unittest.main()
