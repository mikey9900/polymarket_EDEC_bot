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


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _FakeHttpClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def get(self, url, params=None):
        self.calls.append((url, params))
        return _FakeResponse(200, self.payload)

    async def aclose(self):
        return None


class _FakeRoutingHttpClient:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    async def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        key = (url, tuple(sorted((params or {}).items())))
        payload = self.routes[key]
        return _FakeResponse(200, payload)

    async def aclose(self):
        return None


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

    async def test_refresh_market_metadata_updates_volume_in_place(self):
        cfg = SimpleNamespace(
            coins=("btc",),
            polymarket=SimpleNamespace(gamma_base_url="https://gamma.example"),
        )
        scanner = MarketScanner(cfg)
        await scanner._http.aclose()
        fake_http = _FakeHttpClient([
            {
                "slug": "btc-updown-5m-test",
                "volume": "999.0",
                "markets": [
                    {
                        "acceptingOrders": False,
                        "volumeClob": "23456.7",
                    }
                ],
            }
        ])
        scanner._http = fake_http
        self.addAsyncCleanup(scanner.aclose)
        market = _build_market()
        market.volume = 10.0

        await scanner._refresh_market_metadata("btc", market)

        self.assertEqual(market.volume, 23456.7)
        self.assertFalse(market.accepting_orders)
        self.assertEqual(
            fake_http.calls,
            [("https://gamma.example/events", {"slug": market.slug, "limit": 1})],
        )

    async def test_refresh_recent_resolutions_reads_last_four_closed_markets(self):
        cfg = SimpleNamespace(
            coins=("btc",),
            polymarket=SimpleNamespace(gamma_base_url="https://gamma.example"),
        )
        scanner = MarketScanner(cfg)
        await scanner._http.aclose()
        base_url = "https://gamma.example/events"
        routes = {}
        winners = {
            "btc-updown-5m-1200": '["1", "0"]',
            "btc-updown-5m-900": '["0", "1"]',
            "btc-updown-5m-600": '["1", "0"]',
            "btc-updown-5m-300": '["0", "1"]',
        }
        for slug, prices in winners.items():
            routes[(base_url, (("limit", 1), ("slug", slug)))] = [
                {
                    "markets": [
                        {
                            "closed": True,
                            "acceptingOrders": False,
                            "outcomes": '["Up", "Down"]',
                            "outcomePrices": prices,
                        }
                    ]
                }
            ]
        scanner._http = _FakeRoutingHttpClient(routes)
        self.addAsyncCleanup(scanner.aclose)

        recent = await scanner._refresh_recent_resolutions("btc", anchor_ts=1500, limit=4)

        self.assertEqual(
            recent,
            [
                {"winner": "UP", "slug": "btc-updown-5m-1200"},
                {"winner": "DOWN", "slug": "btc-updown-5m-900"},
                {"winner": "UP", "slug": "btc-updown-5m-600"},
                {"winner": "DOWN", "slug": "btc-updown-5m-300"},
            ],
        )

    def test_extract_reference_info_requires_directional_price_phrase(self):
        reference, label = MarketScanner._extract_reference_info(
            "Will BTC be above $84,250.75 at 3:45 PM ET?"
        )
        self.assertEqual(reference, 84250.75)
        self.assertIn("above", label.lower())

        missing_reference, _ = MarketScanner._extract_reference_info(
            "Bitcoin Up or Down - April 21, 3:45 PM ET"
        )
        self.assertIsNone(missing_reference)


if __name__ == "__main__":
    unittest.main()
