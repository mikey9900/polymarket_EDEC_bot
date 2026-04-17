import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.market_scanner import MarketScanner
from bot.models import MarketInfo


def _build_market(slug: str = "btc-updown-5m-test") -> MarketInfo:
    now = datetime.now(timezone.utc)
    return MarketInfo(
        event_id="evt-1",
        condition_id="cond-1",
        slug=slug,
        coin="btc",
        up_token_id="up-token",
        down_token_id="down-token",
        start_time=now - timedelta(minutes=1),
        end_time=now + timedelta(minutes=4),
        fee_rate=0.02,
        tick_size="0.01",
        neg_risk=False,
    )


class MarketScannerQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_expired_market_dedupes_by_slug(self):
        scanner = MarketScanner(SimpleNamespace(coins=("btc",)))
        self.addAsyncCleanup(scanner.aclose)
        market = _build_market()

        scanner.queue_expired_market(market)
        scanner.queue_expired_market(market)

        expired = scanner.pop_expired_markets()

        self.assertEqual([m.slug for m in expired], [market.slug])
        self.assertEqual(scanner.pop_expired_markets(), [])

    async def test_market_can_be_requeued_after_pop(self):
        scanner = MarketScanner(SimpleNamespace(coins=("btc",)))
        self.addAsyncCleanup(scanner.aclose)
        market = _build_market()

        scanner.queue_expired_market(market)
        self.assertEqual(len(scanner.pop_expired_markets()), 1)

        scanner.queue_expired_market(market)
        expired = scanner.pop_expired_markets()

        self.assertEqual([m.slug for m in expired], [market.slug])


if __name__ == "__main__":
    unittest.main()
