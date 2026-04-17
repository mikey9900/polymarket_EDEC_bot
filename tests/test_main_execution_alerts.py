import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

import main as app_main
from bot.models import MarketInfo, TradeSignal


class _OneShotQueue:
    def __init__(self, item):
        self.item = item
        self._sent = False

    async def get(self):
        if self._sent:
            raise asyncio.CancelledError
        self._sent = True
        return self.item


class _FakeExecutor:
    def __init__(self, result):
        self.result = result

    async def execute(self, _signal):
        return self.result


class _FakeTelegram:
    def __init__(self):
        self.single_leg_alerts: list[dict] = []
        self.dual_leg_alerts: list[tuple] = []
        self.abort_alerts: list[tuple] = []

    async def alert_single_leg(self, *args, **kwargs):
        self.single_leg_alerts.append({"args": args, "kwargs": kwargs})

    async def alert_dual_leg(self, *args, **kwargs):
        self.dual_leg_alerts.append((args, kwargs))

    async def alert_abort(self, *args, **kwargs):
        self.abort_alerts.append((args, kwargs))


def _build_market() -> MarketInfo:
    now = datetime.now(timezone.utc)
    return MarketInfo(
        event_id="evt-1",
        condition_id="cond-1",
        slug="btc-updown-5m-test",
        coin="btc",
        up_token_id="up-token",
        down_token_id="down-token",
        start_time=now - timedelta(minutes=1),
        end_time=now + timedelta(minutes=4),
        fee_rate=0.02,
        tick_size="0.01",
        neg_risk=False,
    )


def _build_signal(strategy_type: str) -> TradeSignal:
    market = _build_market()
    return TradeSignal(
        market=market,
        strategy_type=strategy_type,
        side="up",
        entry_price=0.42,
        target_sell_price=0.57,
        expected_profit=0.08,
    )


class ExecutionLoopAlertTests(unittest.IsolatedAsyncioTestCase):
    async def test_execution_loop_alerts_open_repricing_positions(self):
        for strategy_type in ("single_leg", "lead_lag", "swing_leg"):
            with self.subTest(strategy_type=strategy_type):
                signal = _build_signal(strategy_type)
                result = SimpleNamespace(status="open", shares=11, error="", abort_cost=0.0)
                executor = _FakeExecutor(result)
                telegram = _FakeTelegram()
                queue = _OneShotQueue(signal)

                await app_main.execution_loop(
                    executor,
                    queue,
                    tracker=object(),
                    telegram=telegram,
                    config=object(),
                )

                self.assertEqual(len(telegram.single_leg_alerts), 1)
                alert = telegram.single_leg_alerts[0]
                self.assertEqual(alert["kwargs"]["strategy_type"], strategy_type)
                self.assertEqual(alert["args"][0], signal.market.slug)
                self.assertEqual(alert["args"][1], signal.market.coin)
                self.assertEqual(alert["args"][2], signal.side)
                self.assertEqual(alert["args"][5], result.shares)


if __name__ == "__main__":
    unittest.main()
