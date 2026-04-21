import signal
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

import main as app_main
from bot.clob_ws_feed import ClobWebSocketFeed
from bot.execution import ExecutionEngine
from bot.market_scanner import MarketScanner


class _FakeWebSocket:
    def __init__(self):
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


class _FakeWsFeed:
    def __init__(self):
        self.close_calls = 0
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1

    async def aclose(self):
        self.close_calls += 1


class _FakeLoop:
    def __init__(self, unsupported: bool = False):
        self.unsupported = unsupported
        self.installed: list[tuple[object, object]] = []
        self.removed: list[object] = []

    def add_signal_handler(self, sig, callback):
        if self.unsupported:
            raise NotImplementedError
        self.installed.append((sig, callback))

    def remove_signal_handler(self, sig):
        self.removed.append(sig)


class ShutdownCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_signal_helpers_install_and_remove_supported_signals(self):
        loop = _FakeLoop()

        installed = app_main._install_shutdown_signal_handlers(loop, lambda: None)
        app_main._remove_shutdown_signal_handlers(loop, installed)

        expected = tuple(
            sig
            for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None))
            if sig is not None
        )
        self.assertEqual(installed, expected)
        self.assertEqual([sig for sig, _ in loop.installed], list(expected))
        self.assertEqual(loop.removed, list(expected))

    async def test_shutdown_signal_helpers_skip_unsupported_loops(self):
        loop = _FakeLoop(unsupported=True)

        installed = app_main._install_shutdown_signal_handlers(loop, lambda: None)
        app_main._remove_shutdown_signal_handlers(loop, installed)

        self.assertEqual(installed, ())
        self.assertEqual(loop.installed, [])
        self.assertEqual(loop.removed, [])

    async def test_clob_ws_feed_aclose_closes_active_socket(self):
        feed = ClobWebSocketFeed()
        fake_ws = _FakeWebSocket()
        feed._running = True
        feed._ws = fake_ws

        await feed.aclose()

        self.assertFalse(feed._running)
        self.assertIsNone(feed._ws)
        self.assertEqual(fake_ws.close_calls, 1)

    async def test_market_scanner_aclose_closes_http_and_feed(self):
        scanner = MarketScanner(SimpleNamespace(coins=("btc",)))
        fake_ws_feed = _FakeWsFeed()
        scanner._ws_feed = fake_ws_feed

        await scanner.aclose()

        self.assertTrue(scanner._http.is_closed)
        self.assertEqual(fake_ws_feed.close_calls, 1)
        self.assertEqual(fake_ws_feed.stop_calls, 1)
        self.assertFalse(scanner._running)

    async def test_execution_engine_aclose_closes_http(self):
        config = SimpleNamespace(
            execution=SimpleNamespace(order_size_usd=10.0, dry_run=False),
            single_leg=SimpleNamespace(order_size_usd=10.0),
            lead_lag=SimpleNamespace(order_size_usd=10.0),
            swing_leg=SimpleNamespace(order_size_usd=10.0),
        )
        engine = ExecutionEngine(config, clob_client=None, risk_manager=None, tracker=object())

        await engine.aclose()

        self.assertTrue(engine._http.is_closed)


if __name__ == "__main__":
    unittest.main()
