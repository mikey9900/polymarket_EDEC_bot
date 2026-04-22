import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.execution import ExecutionEngine
from bot.models import MarketInfo, TradeSignal
from bot.risk_manager import RiskManager


class _FakeTracker:
    def __init__(self):
        self.logged_trades: list[tuple[int, str, float]] = []
        self.updated_trades: list[tuple[int, dict]] = []

    def has_paper_capital(self, _cost: float) -> bool:
        return True

    def log_paper_trade(self, *args, **kwargs) -> int:
        return 1

    def log_trade(self, decision_id: int, result) -> int:
        self.logged_trades.append((decision_id, result.status, result.shares))
        return len(self.logged_trades)

    def update_live_trade(self, trade_id: int, **updates) -> None:
        self.updated_trades.append((trade_id, updates))


class _FakeClient:
    def __init__(self, post_responses=None, order_statuses=None):
        self.post_responses = list(post_responses or [])
        self.order_statuses = list(order_statuses or [])
        self.canceled: list[str] = []

    def create_order(self, order_args, options):
        return {"order_args": order_args, "options": options}

    def post_order(self, _order, _order_type):
        return self.post_responses.pop(0)

    def get_order(self, _order_id):
        if not self.order_statuses:
            return {"status": "live"}
        return self.order_statuses.pop(0)

    def cancel(self, order_id: str):
        self.canceled.append(order_id)
        return {"canceled": order_id}


def _build_config(
    *,
    hold_if_unfilled: bool = True,
    execution_order_size_usd: float = 10.0,
    single_leg_order_size_usd: float = 10.0,
    lead_lag_order_size_usd: float = 10.0,
    swing_leg_order_size_usd: float = 10.0,
):
    return SimpleNamespace(
        execution=SimpleNamespace(dry_run=False, order_size_usd=execution_order_size_usd),
        risk=SimpleNamespace(
            max_daily_loss_usd=1000.0,
            max_open_positions=5,
            max_trades_per_hour=50,
            session_profit_target=0.0,
        ),
        single_leg=SimpleNamespace(
            hold_if_unfilled=hold_if_unfilled,
            order_size_usd=single_leg_order_size_usd,
            scalp_take_profit_bid=0.65,
            high_confidence_bid=0.9,
            scalp_min_profit_usd=0.01,
            loss_cut_max_factor=2.0,
            time_pressure_s=180.0,
            loss_cut_pct=0.2,
        ),
        swing_leg=SimpleNamespace(
            order_size_usd=swing_leg_order_size_usd,
            high_confidence_bid=0.9,
            loss_cut_max_factor=2.0,
            time_pressure_s=180.0,
            loss_cut_pct=0.2,
        ),
        lead_lag=SimpleNamespace(
            coin_overrides={},
            order_size_usd=lead_lag_order_size_usd,
            profit_take_delta=0.03,
            profit_take_cap=0.3,
            stall_window_s=15.0,
            min_progress_delta=0.01,
            hard_stop_loss_pct=0.2,
            min_velocity_30s=0.0,
            min_entry=0.0,
            max_entry=1.0,
            min_book_depth_usd=0.0,
        ),
    )


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


def _build_signal(market: MarketInfo, *, decision_id: int = 123) -> TradeSignal:
    return TradeSignal(
        market=market,
        strategy_type="single_leg",
        decision_id=decision_id,
        side="up",
        entry_price=0.4,
        target_sell_price=0.55,
        fee_total=0.0048,
        expected_profit=0.1,
    )


def _build_lead_lag_signal(market: MarketInfo, *, decision_id: int = 456) -> TradeSignal:
    return TradeSignal(
        market=market,
        strategy_type="lead_lag",
        decision_id=decision_id,
        side="up",
        entry_price=0.5,
        target_sell_price=0.62,
        fee_total=0.005,
        expected_profit=0.08,
    )


def _discard_task(coro):
    coro.close()
    return SimpleNamespace(cancel=lambda: None)


class PositionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.market = _build_market()

    async def test_resting_entry_stays_submitted_and_not_open(self):
        config = _build_config(hold_if_unfilled=True)
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(post_responses=[{"orderID": "buy-1", "status": "live"}])
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)

        with mock.patch.object(engine, "_schedule_background_task", side_effect=_discard_task):
            result = await engine.execute_single_leg(_build_signal(self.market))

        self.assertEqual(result.status, "submitted")
        self.assertEqual(len(risk_manager.open_positions), 0)
        self.assertIn("buy-1", engine._pending_single_entries)
        self.assertEqual(tracker.logged_trades, [(123, "submitted", 25)])

    async def test_resting_entry_opens_only_after_fill(self):
        config = _build_config(hold_if_unfilled=True)
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(
            post_responses=[{"orderID": "buy-1", "status": "live"}],
            order_statuses=[{"status": "matched", "size_matched": "8"}],
        )
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)

        with mock.patch.object(engine, "_schedule_background_task", side_effect=_discard_task):
            result = await engine.execute_single_leg(_build_signal(self.market))

        position = engine._pending_single_entries["buy-1"]
        engine._monitor_single_leg = mock.AsyncMock(return_value=None)
        with mock.patch("bot.execution.asyncio.sleep", new=mock.AsyncMock(return_value=None)):
            await engine._monitor_single_leg_entry(position, result)

        self.assertEqual(result.status, "open")
        self.assertEqual(result.shares, 8)
        self.assertEqual(len(risk_manager.open_positions), 1)
        self.assertNotIn("buy-1", engine._pending_single_entries)
        self.assertIn("buy-1", engine.get_open_positions())
        self.assertEqual(tracker.logged_trades, [(123, "submitted", 25)])
        self.assertEqual(len(tracker.updated_trades), 1)
        self.assertEqual(tracker.updated_trades[0][0], 1)
        self.assertEqual(tracker.updated_trades[0][1]["status"], "open")
        self.assertEqual(tracker.updated_trades[0][1]["shares_filled"], 8)

    async def test_market_resolution_realizes_live_position_and_clears_engine_state(self):
        config = _build_config(hold_if_unfilled=False)
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(post_responses=[{"orderID": "buy-1", "status": "matched"}])
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)

        with mock.patch.object(engine, "_schedule_background_task", side_effect=_discard_task):
            result = await engine.execute_single_leg(_build_signal(self.market))

        expected_profit = (1.0 - 0.4 - (0.02 * 0.4 * 0.6)) * result.shares
        pnl = engine.resolve_market_positions(self.market.slug, "UP")

        self.assertAlmostEqual(pnl, expected_profit, places=6)
        self.assertEqual(len(risk_manager.open_positions), 0)
        self.assertEqual(engine.get_open_positions(), {})
        self.assertAlmostEqual(risk_manager.daily_pnl, expected_profit, places=6)
        self.assertEqual(tracker.logged_trades, [(123, "open", result.shares)])

    async def test_market_resolution_loss_includes_buy_fee(self):
        config = _build_config(hold_if_unfilled=False)
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(post_responses=[{"orderID": "buy-1", "status": "matched"}])
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)

        with mock.patch.object(engine, "_schedule_background_task", side_effect=_discard_task):
            result = await engine.execute_single_leg(_build_signal(self.market))

        expected_loss = -(
            (0.4 * result.shares)
            + (engine._per_share_fee(0.4, self.market.fee_rate) * result.shares)
        )
        pnl = engine.resolve_market_positions(self.market.slug, "DOWN")

        self.assertAlmostEqual(pnl, expected_loss, places=6)
        self.assertEqual(len(risk_manager.open_positions), 0)
        self.assertAlmostEqual(risk_manager.daily_pnl, expected_loss, places=6)

    async def test_lead_lag_execution_uses_lead_lag_order_size(self):
        config = _build_config(
            hold_if_unfilled=False,
            execution_order_size_usd=100.0,
            single_leg_order_size_usd=10.0,
            lead_lag_order_size_usd=25.0,
        )
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(post_responses=[{"orderID": "buy-1", "status": "matched"}])
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)

        with mock.patch.object(engine, "_schedule_background_task", side_effect=_discard_task):
            result = await engine.execute_single_leg(_build_lead_lag_signal(self.market))

        self.assertEqual(result.status, "open")
        self.assertEqual(result.shares_requested, 50)
        self.assertEqual(result.shares, 50)
        self.assertEqual(tracker.logged_trades, [(456, "open", 50)])

    async def test_runtime_budget_override_applies_to_repricing_entries(self):
        config = _build_config(
            hold_if_unfilled=False,
            execution_order_size_usd=100.0,
            single_leg_order_size_usd=10.0,
            lead_lag_order_size_usd=25.0,
        )
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(post_responses=[{"orderID": "buy-1", "status": "matched"}])
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)
        engine.set_order_size(12.0)

        with mock.patch.object(engine, "_schedule_background_task", side_effect=_discard_task):
            result = await engine.execute_single_leg(_build_lead_lag_signal(self.market))

        self.assertEqual(result.status, "open")
        self.assertEqual(result.shares_requested, 24)
        self.assertEqual(result.shares, 24)

    async def test_abort_sell_uses_market_fee_rate(self):
        config = _build_config()
        risk_manager = RiskManager(config)
        tracker = _FakeTracker()
        client = _FakeClient(post_responses=[{"status": "matched"}])
        engine = ExecutionEngine(config, client, risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)

        abort_cost = await engine._abort_sell(
            token_id="up-token",
            shares=10,
            entry_price=0.50,
            fee_rate=0.05,
            tick_size="0.01",
            neg_risk=False,
        )

        expected = (0.50 - 0.48) * 10 + engine._per_share_fee(0.48, 0.05) * 10
        self.assertAlmostEqual(abort_cost, expected, places=6)


if __name__ == "__main__":
    unittest.main()
