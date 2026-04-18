import shutil
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.models import Decision, FilterResult, MarketInfo, TradeResult, TradeSignal
from bot.tracker import DecisionTracker


class TrackerResolutionOutcomeTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"edec_resolution_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def test_log_outcome_backfills_resolution_fields_for_taken_positions(self):
        db_path = self.tmpdir / "decisions.db"
        tracker = DecisionTracker(str(db_path))
        self.addCleanup(tracker.close)
        tracker.set_paper_capital(1000.0)
        tracker.set_runtime_context({"dry_run": False})

        now = datetime.now(timezone.utc)
        market_slug = "btc-updown-2026-04-18-1000"
        decision_id = tracker.log_decision(
            Decision(
                timestamp=now,
                market_slug=market_slug,
                window_id=market_slug,
                coin="btc",
                market_end_time=now + timedelta(minutes=5),
                market_start_time=now,
                strategy_type="single_leg",
                up_best_ask=0.56,
                down_best_ask=0.44,
                combined_cost=1.0,
                btc_price=85000.0,
                coin_velocity_30s=0.05,
                coin_velocity_60s=0.08,
                up_depth_usd=25.0,
                down_depth_usd=20.0,
                time_remaining_s=120.0,
                feed_count=3,
                filter_results=[FilterResult("risk_limits", True, "ok", "ok")],
                action="TRADE",
                reason="resolution attribution test",
            )
        )

        market = MarketInfo(
            event_id="evt-1",
            condition_id="cond-1",
            slug=market_slug,
            coin="btc",
            up_token_id="up-token",
            down_token_id="down-token",
            start_time=now,
            end_time=now + timedelta(minutes=5),
            fee_rate=0.02,
            tick_size="0.01",
            neg_risk=False,
        )
        signal = TradeSignal(
            market=market,
            strategy_type="single_leg",
            decision_id=decision_id,
            side="up",
            entry_price=0.56,
            target_sell_price=0.62,
        )
        live_result = TradeResult(
            signal=signal,
            strategy_type="single_leg",
            side="up",
            status="closed_loss",
            shares=10,
            shares_filled=10,
            shares_requested=10,
            fee_total=0.1,
            total_cost=5.6,
            exit_reason="loss_cut",
            realized_pnl=-0.8,
        )
        tracker.log_trade(decision_id, live_result)

        paper_trade_id = tracker.log_paper_trade(
            market_slug=market_slug,
            coin="btc",
            strategy_type="single_leg",
            side="up",
            entry_price=0.56,
            target_price=0.62,
            shares=10,
            fee_total=0.1,
            decision_id=decision_id,
            window_id=market_slug,
        )
        tracker.close_paper_trade_early(
            paper_trade_id,
            exit_price=0.52,
            pnl=-0.4,
            status="closed_loss",
            exit_reason="loss_cut",
        )

        tracker.log_outcome(market_slug, "UP", btc_open=84500.0, btc_close=85000.0)

        live_row = tracker.conn.execute(
            "SELECT resolution_winner, resolution_side_match FROM trades WHERE decision_id = ?",
            (decision_id,),
        ).fetchone()
        paper_row = tracker.conn.execute(
            "SELECT resolution_winner, resolution_side_match FROM paper_trades WHERE id = ?",
            (paper_trade_id,),
        ).fetchone()

        self.assertEqual(live_row[0], "UP")
        self.assertEqual(live_row[1], 1)
        self.assertEqual(paper_row[0], "UP")
        self.assertEqual(paper_row[1], 1)


if __name__ == "__main__":
    unittest.main()
