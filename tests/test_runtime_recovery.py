import sys
import shutil
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.execution import ExecutionEngine
from bot.models import Decision, FilterResult, MarketInfo, TradeResult, TradeSignal
from bot.recovery import recover_runtime
from bot.risk_manager import RiskManager
from bot.tracker import DecisionTracker


def _build_config():
    return SimpleNamespace(
        execution=SimpleNamespace(dry_run=False, order_size_usd=10.0),
        risk=SimpleNamespace(
            max_daily_loss_usd=1000.0,
            max_open_positions=5,
            max_trades_per_hour=50,
            session_profit_target=0.0,
        ),
        single_leg=SimpleNamespace(
            hold_if_unfilled=True,
            order_size_usd=10.0,
            scalp_take_profit_bid=0.65,
            high_confidence_bid=0.9,
            scalp_min_profit_usd=0.01,
            loss_cut_max_factor=2.0,
            time_pressure_s=180.0,
            loss_cut_pct=0.2,
        ),
        swing_leg=SimpleNamespace(
            order_size_usd=10.0,
            high_confidence_bid=0.9,
            loss_cut_max_factor=2.0,
            time_pressure_s=180.0,
            loss_cut_pct=0.2,
        ),
        lead_lag=SimpleNamespace(
            coin_overrides={},
            order_size_usd=10.0,
            profit_take_delta=0.03,
            profit_take_cap=0.3,
            stall_window_s=15.0,
            min_progress_delta=0.01,
            hard_stop_loss_pct=0.2,
            min_velocity_30s=0.0,
            min_entry=0.0,
            max_entry=1.0,
            min_book_depth_usd=0.0,
            resignal_cooldown_s=0.0,
            min_price_improvement=0.0,
        ),
        polymarket=SimpleNamespace(tick_size="0.01"),
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


def _build_decision(market: MarketInfo, strategy_type: str) -> Decision:
    now = datetime.now(timezone.utc)
    return Decision(
        timestamp=now,
        market_slug=market.slug,
        coin=market.coin,
        market_end_time=market.end_time,
        market_start_time=market.start_time,
        window_id=market.slug,
        strategy_type=strategy_type,
        up_best_ask=0.5,
        down_best_ask=0.5,
        combined_cost=1.0,
        btc_price=87000.0,
        coin_velocity_30s=0.0,
        coin_velocity_60s=0.0,
        up_depth_usd=10.0,
        down_depth_usd=10.0,
        time_remaining_s=120.0,
        feed_count=3,
        filter_results=[FilterResult("ok", True, "1", "1")],
        action="TRADE",
        reason="test",
        dry_run=False,
        order_size_usd=10.0,
        paper_capital_total=5000.0,
    )


def _discard_task(coro):
    coro.close()
    return SimpleNamespace(cancel=lambda: None)


class _FakeClient:
    def __init__(self, order_status=None):
        self.order_status = order_status or {"status": "live"}

    def get_order(self, _order_id):
        return self.order_status


class _FakeScanner:
    def __init__(self, market: MarketInfo):
        self.market = market

    async def get_market_by_slug(self, slug: str, coin: str | None = None):
        if slug == self.market.slug:
            return self.market
        return None


class RuntimeRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_state_roundtrip(self):
        scratch = ROOT / ".tmp_testdata" / "runtime_recovery_state"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        tracker = DecisionTracker(str(scratch / "decisions.db"))
        self.addCleanup(tracker.close)

        tracker.save_runtime_state({"daily_pnl": 12.5, "paused": True}, version=3)
        state = tracker.load_runtime_state()

        self.assertEqual(state["version"], 3)
        self.assertEqual(state["daily_pnl"], 12.5)
        self.assertTrue(state["paused"])

    async def test_recover_submitted_single_leg_restores_pending_entry(self):
        scratch = ROOT / ".tmp_testdata" / "runtime_recovery_submitted"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        market = _build_market()
        tracker = DecisionTracker(str(scratch / "decisions.db"))
        self.addCleanup(tracker.close)
        tracker.set_paper_capital(5000.0)
        tracker.set_runtime_context({"run_id": "run-1", "dry_run": False, "order_size_usd": 10.0})
        decision_id = tracker.log_decision(_build_decision(market, "single_leg"))
        signal = TradeSignal(
            market=market,
            strategy_type="single_leg",
            decision_id=decision_id,
            side="up",
            entry_price=0.4,
            target_sell_price=0.55,
            fee_total=0.0048,
        )
        submitted = TradeResult(
            signal=signal,
            strategy_type="single_leg",
            side="up",
            status="submitted",
            buy_order_id="buy-1",
            shares=25,
            shares_requested=25,
            shares_filled=0,
            total_cost=10.0,
            entry_order_submitted_at=datetime.now(timezone.utc).isoformat(),
            entry_limit_price=0.4,
        )
        tracker.log_trade(decision_id, submitted)

        config = _build_config()
        risk_manager = RiskManager(config)
        engine = ExecutionEngine(config, _FakeClient({"status": "live"}), risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)
        scanner = _FakeScanner(market)

        with mock.patch("bot.recovery._schedule_background_task", side_effect=_discard_task):
            summary = await recover_runtime(engine, tracker, scanner)

        self.assertEqual(summary["live_rows"], 1)
        self.assertEqual(summary["live_pending"], 1)
        self.assertIn("buy-1", engine._pending_single_entries)
        self.assertAlmostEqual(engine.reserved_collateral_usd(), 10.0, places=6)

    async def test_recover_dual_leg_success_restores_resolution_tracking(self):
        scratch = ROOT / ".tmp_testdata" / "runtime_recovery_dual"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        market = _build_market()
        tracker = DecisionTracker(str(scratch / "decisions.db"))
        self.addCleanup(tracker.close)
        tracker.set_paper_capital(5000.0)
        tracker.set_runtime_context({"run_id": "run-1", "dry_run": False, "order_size_usd": 10.0})
        decision_id = tracker.log_decision(_build_decision(market, "dual_leg"))
        signal = TradeSignal(
            market=market,
            strategy_type="dual_leg",
            decision_id=decision_id,
            up_price=0.48,
            down_price=0.47,
            combined_cost=0.95,
            fee_total=0.01,
        )
        success = TradeResult(
            signal=signal,
            strategy_type="dual_leg",
            status="success",
            shares=10,
            shares_requested=10,
            shares_filled=10,
            total_cost=0.95,
            fee_total=0.01,
            up_order_id="up-1",
            down_order_id="down-1",
        )
        tracker.log_trade(decision_id, success)

        config = _build_config()
        risk_manager = RiskManager(config)
        engine = ExecutionEngine(config, _FakeClient(), risk_manager, tracker)
        self.addAsyncCleanup(engine._http.aclose)
        scanner = _FakeScanner(market)

        summary = await recover_runtime(engine, tracker, scanner)

        self.assertEqual(summary["live_rows"], 1)
        self.assertEqual(summary["live_resolution_only"], 1)
        self.assertEqual(len(risk_manager.open_positions), 1)
        self.assertEqual(risk_manager.open_positions[0].trade_id, 1)


if __name__ == "__main__":
    unittest.main()
