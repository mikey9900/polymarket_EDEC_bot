import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.runtime import RuntimeCoordinator


class _FakeFeed:
    def __init__(self):
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1


class _FakeAggregator:
    def __init__(self):
        self.stop_calls = 0
        self.run_calls = 0
        self.snapshot_ready = True

    async def run(self, queue):
        self.run_calls += 1
        await asyncio.sleep(60)

    def stop(self):
        self.stop_calls += 1

    def get_all_coins_snapshot(self):
        return {"btc": 1.0} if self.snapshot_ready else {}


class _FakeScanner:
    def __init__(self):
        self.stop_calls = 0
        self.close_calls = 0
        self.run_calls = 0

    async def run(self):
        self.run_calls += 1
        await asyncio.sleep(60)

    def stop(self):
        self.stop_calls += 1

    async def aclose(self):
        self.close_calls += 1

    def get_all_active(self):
        return ["market"]


class _FakeStrategy:
    def __init__(self):
        self.stop_calls = 0
        self.run_calls = 0
        self.mode = "both"
        self.is_active = True

    async def run(self, queue):
        self.run_calls += 1
        await asyncio.sleep(60)

    def stop(self):
        self.stop_calls += 1


class _FakeExecutor:
    def __init__(self):
        self.order_size_usd = 10.0
        self.close_calls = 0
        self.config = SimpleNamespace(execution=SimpleNamespace(dry_run=True))

    async def aclose(self):
        self.close_calls += 1

    def get_open_positions(self):
        return {}


class _FakeTracker:
    def __init__(self):
        self.runtime_context = {}
        self.save_calls = 0
        self.paper_capital = (100.0, 100.0)

    def load_runtime_state(self):
        return None

    def save_runtime_state(self, state):
        self.save_calls += 1

    def set_runtime_context(self, context):
        self.runtime_context = dict(context)

    def get_runtime_context(self):
        return dict(self.runtime_context)

    def get_paper_capital(self):
        return self.paper_capital

    def get_open_paper_trades(self):
        return []


class _FakeRiskManager:
    pass


class _FakeTelegram:
    def __init__(self):
        self.start_calls = 0
        self.stop_calls = 0
        self.alerts = []

    async def start(self):
        self.start_calls += 1

    async def stop(self):
        self.stop_calls += 1

    async def send_alert(self, text: str, **kwargs):
        self.alerts.append(text)

    async def alert_archive_complete(self, result):
        self.alerts.append("archive-complete")


class RuntimeCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_stops_services_and_persists_state(self):
        tracker = _FakeTracker()
        aggregator = _FakeAggregator()
        scanner = _FakeScanner()
        strategy = _FakeStrategy()
        executor = _FakeExecutor()
        telegram = _FakeTelegram()
        feed = _FakeFeed()
        coordinator = RuntimeCoordinator(
            config=SimpleNamespace(coins=["btc"], execution=SimpleNamespace(dry_run=True)),
            tracker=tracker,
            risk_manager=_FakeRiskManager(),
            aggregator=aggregator,
            scanner=scanner,
            strategy=strategy,
            executor=executor,
            telegram=telegram,
            archive_fn=None,
            archive_enabled=False,
            archive_send_files_to_telegram=False,
            default_mode="both",
            config_path="config.yaml",
            config_hash="abc123",
            feed_starter=lambda config, queue: [(asyncio.create_task(asyncio.sleep(60)), feed)],
        )
        feed_task = asyncio.create_task(asyncio.sleep(60))
        coordinator.feed_pairs = [(feed_task, feed)]
        coordinator.tasks = [asyncio.create_task(asyncio.sleep(60)), feed_task]

        await coordinator.shutdown()

        self.assertEqual(feed.stop_calls, 1)
        self.assertEqual(aggregator.stop_calls, 1)
        self.assertEqual(scanner.stop_calls, 1)
        self.assertEqual(scanner.close_calls, 1)
        self.assertEqual(strategy.stop_calls, 1)
        self.assertEqual(executor.close_calls, 1)
        self.assertGreaterEqual(tracker.save_calls, 1)
        self.assertEqual(telegram.stop_calls, 1)


if __name__ == "__main__":
    unittest.main()
