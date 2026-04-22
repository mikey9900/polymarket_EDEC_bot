import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

import httpx


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from research.sources import FillCursor
from research.sources import GammaMarketSource
from research.sources import GoldskyFillSource
from research.sources import build_goldsky_query
from research.sync import sync_fills, sync_markets, sync_recent_5m_fills
from research.warehouse import ResearchWarehouse


class _FakeMarketSource:
    def __init__(self):
        self.calls = 0

    def fetch_markets(self, *, offset: int, limit: int):
        self.calls += 1
        if offset > 0:
            return []
        return [
            {
                "id": "m1",
                "createdAt": "2026-04-20T00:00:00Z",
                "slug": "btc-updown-5m-1713577200",
                "question": "Will BTC go up in 5 minutes?",
                "outcomes": ["Up", "Down"],
                "clobTokenIds": ["tok-up", "tok-down"],
                "conditionId": "cond-1",
                "volume": "1234.5",
                "closedTime": "2026-04-20T00:05:00Z",
                "eventStartTime": "2026-04-20T00:00:00Z",
                "endDate": "2026-04-20T00:05:00Z",
                "acceptingOrders": True,
                "negRisk": False,
                "feeSchedule": {"rate": 0.072},
                "events": [{"ticker": "BTC"}],
            }
        ]


class _FakeFillSource:
    def fetch_fills(self, *, cursor: FillCursor, limit: int):
        if cursor == FillCursor():
            return (
                [
                    {
                        "id": "fill-1",
                        "timestamp": "100",
                        "maker": "maker-1",
                        "makerAmountFilled": "1000000",
                        "makerAssetId": "tok-up",
                        "taker": "taker-1",
                        "takerAmountFilled": "550000",
                        "takerAssetId": "0",
                        "transactionHash": "tx-1",
                    }
                ],
                FillCursor(last_timestamp=0, last_id="fill-1", sticky_timestamp=100),
            )
        if cursor == FillCursor(last_timestamp=0, last_id="fill-1", sticky_timestamp=100):
            return (
                [
                    {
                        "id": "fill-2",
                        "timestamp": "100",
                        "maker": "maker-2",
                        "makerAmountFilled": "1000000",
                        "makerAssetId": "tok-down",
                        "taker": "taker-2",
                        "takerAmountFilled": "450000",
                        "takerAssetId": "0",
                        "transactionHash": "tx-2",
                    }
                ],
                FillCursor(last_timestamp=0, last_id="fill-2", sticky_timestamp=100),
            )
        if cursor == FillCursor(last_timestamp=0, last_id="fill-2", sticky_timestamp=100):
            return [], FillCursor(last_timestamp=100, last_id=None, sticky_timestamp=None)
        return [], cursor


class _FakeRecentFillSource:
    def __init__(self):
        self.called = False

    def fetch_fills_for_assets(self, *, asset_ids, cursor: FillCursor, limit: int, until_timestamp=None):
        if "tok-up" not in asset_ids and "tok-down" not in asset_ids:
            return [], cursor
        if not self.called and cursor.last_id is None and cursor.sticky_timestamp is None and int(cursor.last_timestamp or 0) > 0:
            self.called = True
            return (
                [
                    {
                        "id": "recent-fill-1",
                        "timestamp": "200",
                        "maker": "maker-recent",
                        "makerAmountFilled": "1000000",
                        "makerAssetId": "tok-up",
                        "taker": "taker-recent",
                        "takerAmountFilled": "650000",
                        "takerAssetId": "0",
                        "transactionHash": "tx-recent-1",
                    }
                ],
                FillCursor(last_timestamp=200, last_id=None, sticky_timestamp=None),
            )
        return [], cursor


class _FlakyHttpClient:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if not self.outcomes:
            raise AssertionError("unexpected request")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self):
        pass


class ResearchSyncTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"research_sync_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def test_sync_builds_markets_registry_and_enriched_fills(self):
        warehouse = ResearchWarehouse(self.tmpdir / "warehouse.duckdb")
        self.addCleanup(warehouse.close)

        market_result = sync_markets(warehouse, _FakeMarketSource(), batch_size=50)
        fill_result = sync_fills(warehouse, _FakeFillSource(), batch_size=1)

        self.assertEqual(market_result["fetched"], 1)
        self.assertEqual(fill_result["fetched"], 2)
        self.assertEqual(warehouse.get_market_offset(), 1)
        self.assertEqual(warehouse.get_fill_cursor(), FillCursor(last_timestamp=100, last_id=None, sticky_timestamp=None))

        market_count = warehouse.conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        registry_count = warehouse.conn.execute("SELECT COUNT(*) FROM market_5m_registry").fetchone()[0]
        raw_count = warehouse.conn.execute("SELECT COUNT(*) FROM fills_raw").fetchone()[0]
        enriched = warehouse.conn.execute(
            """
            SELECT market_slug, token_side, price, usd_amount, token_amount, is_5m_updown
            FROM fills_enriched
            ORDER BY event_id ASC
            """
        ).fetchall()

        self.assertEqual(market_count, 1)
        self.assertEqual(registry_count, 1)
        self.assertEqual(raw_count, 2)
        self.assertEqual(len(enriched), 2)
        self.assertEqual(enriched[0][0], "btc-updown-5m-1713577200")
        self.assertEqual(enriched[0][1], "up")
        self.assertAlmostEqual(enriched[0][2], 0.55, places=6)
        self.assertAlmostEqual(enriched[1][2], 0.45, places=6)
        self.assertTrue(all(bool(row[5]) for row in enriched))
        self.assertTrue(Path(fill_result["parquet"]["markets"]).exists())
        self.assertTrue(Path(fill_result["parquet"]["fills_enriched"]).exists())

    def test_recent_5m_fill_sync_filters_by_registry_tokens(self):
        warehouse = ResearchWarehouse(self.tmpdir / "warehouse_recent.duckdb")
        self.addCleanup(warehouse.close)
        warehouse.insert_markets(
            [
                {
                    "market_id": "m-recent",
                    "created_at": "2026-04-20T00:00:00Z",
                    "market_slug": "btc-updown-5m-1713577200",
                    "question": "Will BTC go up in 5 minutes?",
                    "answer1": "Up",
                    "answer2": "Down",
                    "token1": "tok-up",
                    "token2": "tok-down",
                    "condition_id": "cond-recent",
                    "volume": 10.0,
                    "ticker": "BTC",
                    "closed_time": "2026-04-20T00:05:00Z",
                    "start_time": "2026-04-20T00:00:00Z",
                    "end_time": "2026-04-20T00:05:00Z",
                    "active": True,
                    "accepting_orders": True,
                    "neg_risk": False,
                    "fee_rate": 0.072,
                    "raw_json": "{}",
                }
            ]
        )
        warehouse.rebuild_market_5m_registry()

        result = sync_recent_5m_fills(
            warehouse,
            _FakeRecentFillSource(),
            lookback_hours=100000,
            batch_size=1000,
            asset_chunk_size=10,
        )

        self.assertEqual(result["fetched"], 1)
        self.assertEqual(result["fills_enriched_rows"], 1)
        enriched = warehouse.conn.execute(
            "select market_slug, token_side, price, usd_amount from fills_enriched"
        ).fetchall()
        self.assertEqual(enriched[0][0], "btc-updown-5m-1713577200")
        self.assertEqual(enriched[0][1], "up")
        self.assertAlmostEqual(enriched[0][2], 0.65, places=6)

    def test_goldsky_query_can_target_asset_ids(self):
        query = build_goldsky_query(
            cursor=FillCursor(last_timestamp=123),
            limit=50,
            asset_ids=["tok-a", "tok-b"],
        )
        self.assertIn('timestamp_gt: "123"', query)
        self.assertIn('makerAssetId_in: ["tok-a", "tok-b"]', query)
        self.assertIn('takerAssetId_in: ["tok-a", "tok-b"]', query)

    def test_goldsky_source_retries_transient_connection_errors(self):
        request = httpx.Request("POST", "https://example.test/graphql")
        client = _FlakyHttpClient(
            [
                httpx.ConnectError("connect failed", request=request),
                httpx.Response(
                    200,
                    request=request,
                    json={
                        "data": {
                            "orderFilledEvents": [
                                {
                                    "id": "fill-1",
                                    "timestamp": "100",
                                    "makerAmountFilled": "1000000",
                                    "makerAssetId": "tok-up",
                                    "takerAmountFilled": "500000",
                                    "takerAssetId": "0",
                                }
                            ]
                        }
                    },
                ),
            ]
        )
        source = GoldskyFillSource(
            url="https://example.test/graphql",
            client=client,
            retry_attempts=2,
            retry_backoff_seconds=0.5,
        )

        with mock.patch("research.sources.time.sleep") as sleep_mock:
            rows, next_cursor = source.fetch_fills(cursor=FillCursor(), limit=10)

        self.assertEqual(len(client.requests), 2)
        self.assertEqual(rows[0]["id"], "fill-1")
        self.assertEqual(next_cursor.last_timestamp, 100)
        sleep_mock.assert_called_once_with(0.5)

    def test_gamma_source_retries_retryable_status_codes(self):
        request = httpx.Request("GET", "https://gamma.example/markets")
        client = _FlakyHttpClient(
            [
                httpx.Response(503, request=request, text="service unavailable"),
                httpx.Response(200, request=request, json=[{"id": "m-1"}]),
            ]
        )
        source = GammaMarketSource(
            base_url="https://gamma.example/markets",
            client=client,
            retry_attempts=2,
            retry_backoff_seconds=0.5,
        )

        with mock.patch("research.sources.time.sleep") as sleep_mock:
            rows = source.fetch_markets(offset=0, limit=1)

        self.assertEqual(len(client.requests), 2)
        self.assertEqual(rows[0]["id"], "m-1")
        sleep_mock.assert_called_once_with(0.5)


if __name__ == "__main__":
    unittest.main()
