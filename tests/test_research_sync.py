import shutil
import sys
import unittest
from datetime import datetime, timedelta, timezone
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
from research.sync import sync_fills, sync_markets, sync_recent_5m_fills, sync_recent_markets
from research.warehouse import ResearchWarehouse


class _FakeMarketSource:
    def __init__(self):
        self.calls = 0
        self.asc_calls = []
        self.closed_calls = []
        self.order_calls = []

    def fetch_markets(self, *, offset: int, limit: int, ascending: bool = True, closed=None, order="createdAt"):
        self.calls += 1
        self.asc_calls.append(ascending)
        self.closed_calls.append(closed)
        self.order_calls.append(order)
        if not ascending and closed is True:
            if offset > 0:
                return []
            return [
                {
                    "id": "m2",
                    "createdAt": "2026-04-22T00:00:00Z",
                    "slug": "eth-updown-5m-1713744000",
                    "question": "Will ETH go up in 5 minutes?",
                    "outcomes": ["Up", "Down"],
                    "clobTokenIds": ["tok-eth-up", "tok-eth-down"],
                    "conditionId": "cond-2",
                    "volume": "4321.0",
                    "closedTime": "2026-04-22T00:05:00Z",
                    "eventStartTime": "2026-04-22T00:00:00Z",
                    "endDate": "2026-04-22T00:05:00Z",
                    "acceptingOrders": True,
                    "negRisk": False,
                    "feeSchedule": {"rate": 0.072},
                    "events": [{"ticker": "ETH"}],
                },
                {
                    "id": "old-market",
                    "createdAt": "2025-01-01T00:00:00Z",
                    "slug": "old-updown-5m-1",
                    "question": "Old market",
                    "outcomes": ["Up", "Down"],
                    "clobTokenIds": ["old-up", "old-down"],
                    "conditionId": "cond-old",
                    "volume": "1.0",
                    "closedTime": "2025-01-01T00:05:00Z",
                    "eventStartTime": "2025-01-01T00:00:00Z",
                    "endDate": "2025-01-01T00:05:00Z",
                    "acceptingOrders": False,
                    "negRisk": False,
                    "feeSchedule": {"rate": 0.072},
                    "events": [{"ticker": "OLD"}],
                },
            ]
        if not ascending:
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


class _FakeRollingFillSource:
    def __init__(self):
        self.seen_assets: list[tuple[str, ...]] = []

    def fetch_fills_for_assets(self, *, asset_ids, cursor: FillCursor, limit: int, until_timestamp=None):
        ordered = tuple(sorted(asset_ids))
        self.seen_assets.append(ordered)
        if ordered == ("tok-old-down", "tok-old-up") and int(cursor.last_timestamp or 0) > 0 and cursor.last_id is None:
            return (
                [
                    {
                        "id": "history-fill-1",
                        "timestamp": "150",
                        "maker": "maker-old",
                        "makerAmountFilled": "1000000",
                        "makerAssetId": "tok-old-up",
                        "taker": "taker-old",
                        "takerAmountFilled": "600000",
                        "takerAssetId": "0",
                        "transactionHash": "tx-old-1",
                    }
                ],
                FillCursor(last_timestamp=150, last_id=None, sticky_timestamp=None),
            )
        if ordered == ("tok-recent-down", "tok-recent-up") and int(cursor.last_timestamp or 0) > 0 and cursor.last_id is None:
            return (
                [
                    {
                        "id": "recent-fill-rolling-1",
                        "timestamp": "250",
                        "maker": "maker-recent",
                        "makerAmountFilled": "1000000",
                        "makerAssetId": "tok-recent-up",
                        "taker": "taker-recent",
                        "takerAmountFilled": "700000",
                        "takerAssetId": "0",
                        "transactionHash": "tx-recent-rolling-1",
                    }
                ],
                FillCursor(last_timestamp=250, last_id=None, sticky_timestamp=None),
            )
        return [], cursor


class _RecordingFillSource:
    def __init__(self):
        self.seen_assets: list[tuple[str, ...]] = []

    def fetch_fills_for_assets(self, *, asset_ids, cursor: FillCursor, limit: int, until_timestamp=None):
        self.seen_assets.append(tuple(sorted(asset_ids)))
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

    def test_recent_market_sync_fetches_descending_until_cutoff(self):
        warehouse = ResearchWarehouse(self.tmpdir / "warehouse_markets_recent.duckdb")
        self.addCleanup(warehouse.close)
        source = _FakeMarketSource()

        result = sync_recent_markets(
            warehouse,
            source,
            lookback_days=30,
            batch_size=50,
        )

        self.assertEqual(result["inserted"], 2)
        self.assertIn(False, source.asc_calls)
        self.assertIn(True, source.closed_calls)
        self.assertIn("closedTime", source.order_calls)
        rows = warehouse.conn.execute("SELECT market_slug FROM markets ORDER BY market_slug ASC").fetchall()
        self.assertEqual([row[0] for row in rows], ["btc-updown-5m-1713577200", "eth-updown-5m-1713744000"])

    def test_recent_market_sync_uses_closed_time_cutoff_for_closed_feed(self):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        recent_closed = now - timedelta(days=1)
        recent_start = recent_closed - timedelta(minutes=5)
        stale_created = now - timedelta(days=45)
        old_closed = now - timedelta(days=40)
        old_start = old_closed - timedelta(minutes=5)

        class _ClosedTimeMarketSource:
            def fetch_markets(self, *, offset: int, limit: int, ascending: bool = True, closed=None, order="createdAt"):
                if closed is True and offset == 0:
                    return [
                        {
                            "id": "m-closed-recent",
                            "createdAt": stale_created.isoformat().replace("+00:00", "Z"),
                            "slug": "btc-updown-5m-closed-recent",
                            "question": "Recently closed BTC market",
                            "outcomes": ["Up", "Down"],
                            "clobTokenIds": ["tok-closed-up", "tok-closed-down"],
                            "conditionId": "cond-closed-recent",
                            "volume": "42.0",
                            "closedTime": recent_closed.isoformat().replace("+00:00", "Z"),
                            "eventStartTime": recent_start.isoformat().replace("+00:00", "Z"),
                            "endDate": recent_closed.isoformat().replace("+00:00", "Z"),
                            "acceptingOrders": False,
                            "negRisk": False,
                            "feeSchedule": {"rate": 0.072},
                            "events": [{"ticker": "BTC"}],
                        },
                        {
                            "id": "m-closed-old",
                            "createdAt": stale_created.isoformat().replace("+00:00", "Z"),
                            "slug": "btc-updown-5m-closed-old",
                            "question": "Old closed BTC market",
                            "outcomes": ["Up", "Down"],
                            "clobTokenIds": ["tok-old-up", "tok-old-down"],
                            "conditionId": "cond-closed-old",
                            "volume": "10.0",
                            "closedTime": old_closed.isoformat().replace("+00:00", "Z"),
                            "eventStartTime": old_start.isoformat().replace("+00:00", "Z"),
                            "endDate": old_closed.isoformat().replace("+00:00", "Z"),
                            "acceptingOrders": False,
                            "negRisk": False,
                            "feeSchedule": {"rate": 0.072},
                            "events": [{"ticker": "BTC"}],
                        },
                    ]
                return []

        warehouse = ResearchWarehouse(self.tmpdir / "warehouse_markets_closed_cutoff.duckdb")
        self.addCleanup(warehouse.close)
        source = _ClosedTimeMarketSource()

        result = sync_recent_markets(
            warehouse,
            source,
            lookback_days=30,
            batch_size=50,
        )

        self.assertEqual(result["closed_markets"]["inserted"], 1)
        rows = warehouse.conn.execute("SELECT market_slug FROM markets").fetchall()
        self.assertEqual([row[0] for row in rows], ["btc-updown-5m-closed-recent"])

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

    def test_recent_5m_fill_sync_maintains_recent_and_history_windows(self):
        warehouse = ResearchWarehouse(self.tmpdir / "warehouse_recent_history.duckdb")
        self.addCleanup(warehouse.close)
        warehouse.insert_markets(
            [
                {
                    "market_id": "m-old",
                    "created_at": "2026-04-01T00:00:00Z",
                    "market_slug": "btc-updown-5m-old",
                    "question": "Old BTC market",
                    "answer1": "Up",
                    "answer2": "Down",
                    "token1": "tok-old-up",
                    "token2": "tok-old-down",
                    "condition_id": "cond-old",
                    "volume": 10.0,
                    "ticker": "BTC",
                    "closed_time": "2026-04-01T00:05:00Z",
                    "start_time": "2026-04-01T00:00:00Z",
                    "end_time": "2026-04-01T00:05:00Z",
                    "active": False,
                    "accepting_orders": False,
                    "neg_risk": False,
                    "fee_rate": 0.072,
                    "raw_json": "{}",
                },
                {
                    "market_id": "m-recent",
                    "created_at": "2026-04-22T00:00:00Z",
                    "market_slug": "btc-updown-5m-recent",
                    "question": "Recent BTC market",
                    "answer1": "Up",
                    "answer2": "Down",
                    "token1": "tok-recent-up",
                    "token2": "tok-recent-down",
                    "condition_id": "cond-recent",
                    "volume": 10.0,
                    "ticker": "BTC",
                    "closed_time": "2026-04-22T00:05:00Z",
                    "start_time": "2026-04-22T00:00:00Z",
                    "end_time": "2026-04-22T00:05:00Z",
                    "active": True,
                    "accepting_orders": True,
                    "neg_risk": False,
                    "fee_rate": 0.072,
                    "raw_json": "{}",
                },
            ]
        )
        warehouse.rebuild_market_5m_registry()

        with mock.patch("research.sync.datetime") as dt_mock:
            dt_mock.now.return_value = datetime.fromisoformat("2026-04-22T12:00:00+00:00")
            dt_mock.side_effect = datetime
            result = sync_recent_5m_fills(
                warehouse,
                _FakeRollingFillSource(),
                lookback_hours=24,
                history_lookback_days=30,
                batch_size=1000,
                asset_chunk_size=10,
                bucket_minutes=60,
                history_bucket_minutes=720,
                max_batches_per_chunk=1,
                max_history_batches_per_chunk=1,
            )

        self.assertEqual(result["recent"]["fetched"], 1)
        self.assertEqual(result["history"]["fetched"], 1)
        self.assertEqual(result["fills_enriched_rows"], 2)
        self.assertEqual(result["history_lookback_days"], 30)

    def test_recent_5m_fill_sync_excludes_future_windows_from_recent_pass(self):
        warehouse = ResearchWarehouse(self.tmpdir / "warehouse_recent_future.duckdb")
        self.addCleanup(warehouse.close)
        warehouse.insert_markets(
            [
                {
                    "market_id": "m-past",
                    "created_at": "2026-04-22T04:59:00Z",
                    "market_slug": "btc-updown-5m-1776831600",
                    "question": "Past BTC 5m market",
                    "answer1": "Up",
                    "answer2": "Down",
                    "token1": "tok-past-up",
                    "token2": "tok-past-down",
                    "condition_id": "cond-past",
                    "volume": 10.0,
                    "ticker": "BTC",
                    "closed_time": None,
                    "start_time": "2026-04-23T05:15:00Z",
                    "end_time": "2026-04-23T05:20:00Z",
                    "active": True,
                    "accepting_orders": True,
                    "neg_risk": False,
                    "fee_rate": 0.072,
                    "raw_json": "{}",
                },
                {
                    "market_id": "m-future",
                    "created_at": "2026-04-23T05:32:00Z",
                    "market_slug": "btc-updown-5m-1777008300",
                    "question": "Future BTC 5m market",
                    "answer1": "Up",
                    "answer2": "Down",
                    "token1": "tok-future-up",
                    "token2": "tok-future-down",
                    "condition_id": "cond-future",
                    "volume": 10.0,
                    "ticker": "BTC",
                    "closed_time": None,
                    "start_time": "2026-04-24T05:25:00Z",
                    "end_time": "2026-04-24T05:30:00Z",
                    "active": True,
                    "accepting_orders": True,
                    "neg_risk": False,
                    "fee_rate": 0.072,
                    "raw_json": "{}",
                },
            ]
        )
        warehouse.rebuild_market_5m_registry()
        source = _RecordingFillSource()

        with mock.patch("research.sync.datetime") as dt_mock:
            dt_mock.now.return_value = datetime.fromisoformat("2026-04-23T05:23:00+00:00")
            dt_mock.side_effect = datetime
            result = sync_recent_5m_fills(
                warehouse,
                source,
                lookback_hours=24,
                history_lookback_days=0,
                batch_size=1000,
                asset_chunk_size=10,
                bucket_minutes=60,
                max_batches_per_chunk=1,
                max_history_batches_per_chunk=0,
            )

        self.assertEqual(result["recent"]["asset_window_count"], 1)
        queried_assets = {asset for chunk in source.seen_assets for asset in chunk}
        self.assertIn("tok-past-up", queried_assets)
        self.assertIn("tok-past-down", queried_assets)
        self.assertNotIn("tok-future-up", queried_assets)
        self.assertNotIn("tok-future-down", queried_assets)

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
