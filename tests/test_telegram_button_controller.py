import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.telegram_bot import TelegramBot


class _FakeRiskManager:
    def __init__(self):
        self.resume_calls = 0
        self.deactivate_calls = 0
        self.status = {
            "kill_switch": False,
            "paused": False,
            "daily_pnl": 4.5,
            "open_positions": 1,
        }

    def resume(self):
        self.resume_calls += 1

    def deactivate_kill_switch(self):
        self.deactivate_calls += 1

    def get_status(self):
        return dict(self.status)


class _FakeStrategyEngine:
    def __init__(self):
        self.start_calls = 0
        self.mode = "both"
        self.is_active = True

    def start_scanning(self):
        self.start_calls += 1


class _FakeTracker:
    def get_recent_trades(self, limit: int = 5):
        return []


class _FakeExecutor:
    order_size_usd = 10.0


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
                execution=SimpleNamespace(order_size_usd=5.0),
            ),
            tracker=_FakeTracker(),
            risk_manager=_FakeRiskManager(),
            strategy_engine=_FakeStrategyEngine(),
            executor=_FakeExecutor(),
        )
        bot._track = lambda msg: msg
        return bot

    async def test_start_button_uses_controller_and_runs_start_flow(self):
        bot = self._build_bot()
        cleanup_calls: list[bool] = []
        scheduled: list[str] = []
        tasks: list[asyncio.Task] = []

        async def _refresh_then_cleanup():
            cleanup_calls.append(True)

        def _spawn_background_task(coro, *, label: str):
            scheduled.append(label)
            task = asyncio.create_task(coro)
            tasks.append(task)
            return task

        bot._refresh_then_cleanup = _refresh_then_cleanup
        bot._spawn_background_task = _spawn_background_task

        query = _FakeCallbackQuery("start")
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            callback_query=query,
        )

        await bot._handle_button(update, context=None)
        await asyncio.gather(*tasks)

        self.assertEqual(bot.strategy_engine.start_calls, 1)
        self.assertEqual(bot.risk_manager.resume_calls, 1)
        self.assertEqual(bot.risk_manager.deactivate_calls, 1)
        self.assertEqual(scheduled, ["start-button"])
        self.assertEqual(cleanup_calls, [True])
        self.assertEqual(query.answers[0]["text"], "▶ Scanning started")

    async def test_status_button_edits_message_with_shared_status_panel(self):
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

    async def test_export_recent_button_uses_shared_export_flow_and_reposts_dashboard(self):
        bot = self._build_bot()
        export_messages = []
        repost_calls = []
        scheduled: list[str] = []
        tasks: list[asyncio.Task] = []

        async def _handle_recent_export_request(message):
            export_messages.append(message)

        async def _repost_dashboard():
            repost_calls.append(True)

        def _spawn_background_task(coro, *, label: str):
            scheduled.append(label)
            task = asyncio.create_task(coro)
            tasks.append(task)
            return task

        bot._handle_recent_export_request = _handle_recent_export_request
        bot._repost_dashboard = _repost_dashboard
        bot._spawn_background_task = _spawn_background_task

        query = _FakeCallbackQuery("export_recent")
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            callback_query=query,
        )

        await bot._handle_button(update, context=None)
        await asyncio.gather(*tasks)

        self.assertEqual(scheduled, ["button-export_recent"])
        self.assertEqual(export_messages, [query.message])
        self.assertEqual(repost_calls, [True])


if __name__ == "__main__":
    unittest.main()
