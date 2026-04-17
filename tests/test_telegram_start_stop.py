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
        self.pause_calls = 0
        self.deactivate_calls = 0

    def resume(self):
        self.resume_calls += 1

    def pause(self):
        self.pause_calls += 1

    def deactivate_kill_switch(self):
        self.deactivate_calls += 1


class _FakeStrategyEngine:
    def __init__(self):
        self.start_calls = 0
        self.stop_calls = 0

    def start_scanning(self):
        self.start_calls += 1

    def stop_scanning(self):
        self.stop_calls += 1


class _FakeSentMessage:
    def __init__(self, text: str):
        self.text = text
        self.message_id = 999


class _FakeMessage:
    def __init__(self):
        self.message_id = 123
        self.texts: list[str] = []

    async def reply_text(self, text: str, parse_mode=None):
        self.texts.append(text)
        return _FakeSentMessage(text)


class TelegramStartStopTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_command_resumes_risk_and_strategy_scanning(self):
        risk_manager = _FakeRiskManager()
        strategy_engine = _FakeStrategyEngine()
        bot = TelegramBot(
            SimpleNamespace(telegram_chat_id="1"),
            tracker=object(),
            risk_manager=risk_manager,
            strategy_engine=strategy_engine,
        )
        bot._track = lambda msg: msg
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            message=_FakeMessage(),
        )

        await bot._cmd_start(update, context=None)

        self.assertEqual(strategy_engine.start_calls, 1)
        self.assertEqual(risk_manager.resume_calls, 1)
        self.assertEqual(risk_manager.deactivate_calls, 1)
        self.assertIn("Trading resumed", update.message.texts[0])

    async def test_stop_command_pauses_risk_and_strategy_scanning(self):
        risk_manager = _FakeRiskManager()
        strategy_engine = _FakeStrategyEngine()
        bot = TelegramBot(
            SimpleNamespace(telegram_chat_id="1"),
            tracker=object(),
            risk_manager=risk_manager,
            strategy_engine=strategy_engine,
        )
        bot._track = lambda msg: msg
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id="1"),
            message=_FakeMessage(),
        )

        await bot._cmd_stop(update, context=None)

        self.assertEqual(strategy_engine.stop_calls, 1)
        self.assertEqual(risk_manager.pause_calls, 1)
        self.assertIn("Trading paused", update.message.texts[0])


if __name__ == "__main__":
    unittest.main()
