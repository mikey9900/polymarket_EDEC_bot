import csv
import gzip
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.archive import export_recent_signals_csv_gz, export_recent_trades_csv_gz
from bot.config import load_config
from bot.execution import ExecutionEngine
from bot.models import Decision, FilterResult
from bot.runtime_defaults import default_strategy_mode
from bot.strategy import StrategyEngine
from bot.export import export_to_excel
from bot.tracker import DecisionTracker


class _DummyTracker:
    def get_paper_capital(self):
        return (5000.0, 5000.0)

    def get_runtime_context(self):
        return {}

    def log_decision(self, decision):
        return 1

    def update_decision_signal_context(self, *args, **kwargs):
        return None

    def suppress_decision(self, *args, **kwargs):
        return None


class ExplorationProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(str(ROOT / "edec_bot" / "config_phase_a_single.yaml"))

    def test_default_strategy_mode_is_both(self):
        with mock.patch.dict("os.environ", {}, clear=False):
            self.assertEqual(default_strategy_mode(), "both")

    def test_env_override_wins_for_default_strategy_mode(self):
        with mock.patch.dict("os.environ", {"EDEC_DEFAULT_MODE": "single"}, clear=False):
            self.assertEqual(default_strategy_mode(), "single")

    def test_start_scanning_restores_both_from_off(self):
        engine = StrategyEngine(self.config, aggregator=None, scanner=None, tracker=_DummyTracker())
        self.assertEqual(engine.mode, "off")
        engine.start_scanning()
        self.assertEqual(engine.mode, "both")
        self.assertTrue(engine.is_active)

    def test_config_parses_new_repricing_fields_and_coin_overrides(self):
        cfg = self.config
        self.assertEqual(cfg.single_leg.resignal_cooldown_s, 2.0)
        self.assertEqual(cfg.single_leg.min_price_improvement, 0.0)
        self.assertEqual(cfg.lead_lag.profit_take_delta, 0.06)
        self.assertEqual(cfg.lead_lag.profit_take_cap, 0.68)
        self.assertEqual(cfg.lead_lag.stall_window_s, 30)
        self.assertEqual(cfg.lead_lag.hard_stop_loss_pct, 0.10)
        self.assertIn("xrp", cfg.lead_lag.coin_overrides)
        self.assertEqual(cfg.lead_lag.coin_overrides["xrp"].min_velocity_30s, 0.18)
        self.assertEqual(cfg.lead_lag.coin_overrides["xrp"].max_entry, 0.60)

    def test_xrp_override_applies_only_to_xrp(self):
        engine = StrategyEngine(self.config, aggregator=None, scanner=None, tracker=_DummyTracker())
        xrp = engine._lead_lag_params("xrp")
        btc = engine._lead_lag_params("btc")
        self.assertEqual(xrp["min_velocity_30s"], 0.18)
        self.assertEqual(xrp["min_entry"], 0.52)
        self.assertEqual(xrp["max_entry"], 0.60)
        self.assertEqual(xrp["min_book_depth_usd"], 20.0)
        self.assertEqual(btc["min_velocity_30s"], 0.08)
        self.assertEqual(btc["max_entry"], 0.66)
        self.assertEqual(btc["min_book_depth_usd"], 6.0)

    def test_lead_lag_target_price_uses_delta_and_cap(self):
        engine = StrategyEngine(self.config, aggregator=None, scanner=None, tracker=_DummyTracker())
        self.assertAlmostEqual(engine._lead_lag_target_price(0.55, "btc"), 0.61, places=6)
        self.assertAlmostEqual(engine._lead_lag_target_price(0.64, "btc"), 0.68, places=6)

    def test_lead_lag_stall_exit_triggers_after_30_seconds(self):
        engine = ExecutionEngine(self.config, clob_client=None, risk_manager=None, tracker=_DummyTracker())
        self.addCleanup(lambda: __import__("asyncio").run(engine._http.aclose()))
        exit_reason, _net_pnl, _loss_pct = engine._lead_lag_exit_reason(
            coin="btc",
            entry_price=0.55,
            target_price=0.61,
            bid=0.56,
            remaining=120,
            elapsed_s=31,
            max_bid_seen=0.56,
            ever_profitable=False,
            shares=18,
            fee_rate=0.072,
        )
        self.assertEqual(exit_reason, "stall_exit")

    def test_lead_lag_hard_stop_triggers_at_10_percent(self):
        engine = ExecutionEngine(self.config, clob_client=None, risk_manager=None, tracker=_DummyTracker())
        self.addCleanup(lambda: __import__("asyncio").run(engine._http.aclose()))
        exit_reason, _net_pnl, _loss_pct = engine._lead_lag_exit_reason(
            coin="btc",
            entry_price=0.55,
            target_price=0.61,
            bid=0.49,
            remaining=120,
            elapsed_s=10,
            max_bid_seen=0.55,
            ever_profitable=False,
            shares=18,
            fee_rate=0.072,
        )
        self.assertEqual(exit_reason, "loss_cut")

    def test_archive_exports_both_compact_csvs_with_new_fields(self):
        scratch_root = ROOT / ".tmp_testdata" / "archive_case"
        if scratch_root.exists():
            shutil.rmtree(scratch_root, ignore_errors=True)
        scratch_root.mkdir(parents=True, exist_ok=True)
        db_path = str(scratch_root / "decisions.db")
        out_dir = scratch_root / "exports"
        tracker = DecisionTracker(db_path)
        self.addCleanup(lambda: shutil.rmtree(scratch_root, ignore_errors=True))
        self.addCleanup(tracker.conn.close)
        tracker.set_paper_capital(5000.0)
        tracker.set_runtime_context(
            {
                "run_id": "run-1",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "app_version": "3.4.test",
                "strategy_version": "3.2.test",
                "config_path": "config_phase_a_single.yaml",
                "config_hash": "abc123",
                "dry_run": True,
                "mode": "both",
                "order_size_usd": 10.0,
                "paper_capital_total": 5000.0,
            }
        )
        now = datetime.now(timezone.utc)
        filters = [FilterResult("risk_limits", True, "ok", "ok")]
        decision_id = tracker.log_decision(
            Decision(
                timestamp=now,
                market_slug="btc-2026-04-15-0000",
                window_id="btc-2026-04-15-0000",
                coin="btc",
                market_end_time=now + timedelta(minutes=5),
                market_start_time=now,
                strategy_type="lead_lag",
                up_best_ask=0.58,
                down_best_ask=0.42,
                combined_cost=1.0,
                btc_price=85000.0,
                coin_velocity_30s=0.15,
                coin_velocity_60s=0.21,
                up_depth_usd=32.0,
                down_depth_usd=18.0,
                time_remaining_s=140.0,
                feed_count=3,
                filter_results=filters,
                action="DRY_RUN_SIGNAL",
                reason="Lead-lag UP @0.58",
                run_id="run-1",
                app_version="3.4.test",
                strategy_version="3.2.test",
                config_path="config_phase_a_single.yaml",
                config_hash="abc123",
                mode="both",
                dry_run=True,
                order_size_usd=10.0,
                paper_capital_total=5000.0,
                signal_context="single_leg+lead_lag",
                signal_overlap_count=1,
                entry_price=0.58,
                target_price=0.64,
                expected_profit_per_share=0.02,
                entry_bid=0.57,
                entry_ask=0.58,
                entry_spread=0.01,
                entry_depth_side_usd=32.0,
                opposite_depth_usd=18.0,
                depth_ratio=32.0 / 18.0,
                signal_score=72.5,
                score_velocity=20.0,
                score_entry=12.0,
                score_depth=14.0,
                score_spread=8.0,
                score_time=9.0,
                score_balance=9.5,
                resignal_cooldown_s=2.0,
                min_price_improvement=0.0,
                last_signal_age_s=3.0,
            )
        )
        tracker.log_paper_trade(
            market_slug="btc-2026-04-15-0000",
            coin="btc",
            strategy_type="lead_lag",
            side="up",
            entry_price=0.58,
            target_price=0.64,
            shares=17,
            fee_total=0.6,
            market_end_time=(now + timedelta(minutes=5)).isoformat(),
            market_start_time=now.isoformat(),
            signal_context="single_leg+lead_lag",
            signal_overlap_count=1,
            order_size_usd=10.0,
            entry_bid=0.57,
            entry_ask=0.58,
            entry_spread=0.01,
            entry_depth_side_usd=32.0,
            opposite_depth_usd=18.0,
            depth_ratio=32.0 / 18.0,
            window_id="btc-2026-04-15-0000",
            signal_score=72.5,
            score_velocity=20.0,
            score_entry=12.0,
            score_depth=14.0,
            score_spread=8.0,
            score_time=9.0,
            score_balance=9.5,
            target_delta=0.06,
            hard_stop_delta=0.058,
        )
        trade_id = tracker.conn.execute("SELECT MAX(id) FROM paper_trades").fetchone()[0]
        tracker.record_paper_trade_path(
            trade_id,
            max_bid_seen=0.65,
            min_bid_seen=0.54,
            time_to_max_bid_s=12.0,
            time_to_min_bid_s=5.0,
            first_profit_time_s=7.0,
            mfe=0.07,
            mae=0.04,
            peak_net_pnl=1.2,
            trough_net_pnl=-0.6,
            scalp_hit=True,
            high_confidence_hit=False,
        )
        tracker.close_paper_trade_early(
            trade_id,
            exit_price=0.60,
            pnl=0.28,
            status="closed_win",
            exit_reason="stall_exit",
            time_remaining_s=90.0,
            bid_at_exit=0.60,
            ask_at_exit=0.61,
            stall_exit_triggered=True,
        )
        tracker.suppress_decision(
            decision_id,
            "cooldown_active:lead_lag:age=1.0s",
            resignal_cooldown_s=2.0,
            min_price_improvement=0.0,
            last_signal_age_s=1.0,
        )

        trades_path, trades_count, _, _ = export_recent_trades_csv_gz(db_path, str(out_dir), "EDEC-BOT", 500)
        signals_path, signals_count, _, _ = export_recent_signals_csv_gz(db_path, str(out_dir), "EDEC-BOT", 500)

        self.assertEqual(trades_count, 1)
        self.assertEqual(signals_count, 1)
        self.assertTrue(Path(trades_path).exists())
        self.assertTrue(Path(signals_path).exists())

        with gzip.open(trades_path, "rt", encoding="utf-8", newline="") as fh:
            trades_rows = list(csv.reader(fh))
        with gzip.open(signals_path, "rt", encoding="utf-8", newline="") as fh:
            signals_rows = list(csv.reader(fh))

        self.assertIn("sg", trades_rows[0])
        self.assertIn("sx", trades_rows[0])
        self.assertEqual(trades_rows[1][trades_rows[0].index("sx")], "1")
        self.assertIn("act", signals_rows[0])
        self.assertIn("sup", signals_rows[0])
        self.assertEqual(signals_rows[1][signals_rows[0].index("act")], "SUPPRESSED")

    def test_trade_exports_keep_decision_metadata_for_same_window_refires(self):
        scratch_root = ROOT / ".tmp_testdata" / "decision_link_case"
        if scratch_root.exists():
            shutil.rmtree(scratch_root, ignore_errors=True)
        scratch_root.mkdir(parents=True, exist_ok=True)
        db_path = str(scratch_root / "decisions.db")
        out_dir = scratch_root / "exports"
        tracker = DecisionTracker(db_path)
        self.addCleanup(lambda: shutil.rmtree(scratch_root, ignore_errors=True))
        self.addCleanup(tracker.conn.close)
        tracker.set_paper_capital(5000.0)
        tracker.set_runtime_context(
            {
                "run_id": "run-refire",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "app_version": "3.4.test",
                "strategy_version": "3.2.test",
                "config_path": "config_phase_a_single.yaml",
                "config_hash": "abc123",
                "dry_run": True,
                "mode": "both",
                "order_size_usd": 10.0,
                "paper_capital_total": 5000.0,
            }
        )
        now = datetime.now(timezone.utc)
        market_slug = "xrp-2026-04-15-1200"
        filters = [FilterResult("risk_limits", True, "ok", "ok")]

        first_id = tracker.log_decision(
            Decision(
                timestamp=now,
                market_slug=market_slug,
                window_id=market_slug,
                coin="xrp",
                market_end_time=now + timedelta(minutes=5),
                market_start_time=now,
                strategy_type="lead_lag",
                up_best_ask=0.55,
                down_best_ask=0.45,
                combined_cost=1.0,
                btc_price=85000.0,
                coin_velocity_30s=0.11,
                coin_velocity_60s=0.14,
                up_depth_usd=30.0,
                down_depth_usd=14.0,
                time_remaining_s=150.0,
                feed_count=3,
                filter_results=filters,
                action="DRY_RUN_SIGNAL",
                reason="first refire",
                run_id="run-refire",
                mode="both",
                dry_run=True,
                order_size_usd=10.0,
                paper_capital_total=5000.0,
                signal_context="lead_lag",
                signal_overlap_count=1,
                entry_price=0.55,
                target_price=0.61,
                expected_profit_per_share=0.02,
            )
        )
        trade_one = tracker.log_paper_trade(
            market_slug=market_slug,
            coin="xrp",
            strategy_type="lead_lag",
            side="up",
            entry_price=0.55,
            target_price=0.61,
            shares=18,
            fee_total=0.5,
            decision_id=first_id,
            window_id=market_slug,
        )
        tracker.close_paper_trade_early(
            trade_one,
            exit_price=0.57,
            pnl=0.22,
            status="closed_win",
            exit_reason="profit_target",
            time_remaining_s=110.0,
            bid_at_exit=0.57,
            ask_at_exit=0.58,
        )

        second_id = tracker.log_decision(
            Decision(
                timestamp=now + timedelta(seconds=3),
                market_slug=market_slug,
                window_id=market_slug,
                coin="xrp",
                market_end_time=now + timedelta(minutes=5),
                market_start_time=now,
                strategy_type="lead_lag",
                up_best_ask=0.58,
                down_best_ask=0.42,
                combined_cost=1.0,
                btc_price=85000.0,
                coin_velocity_30s=0.29,
                coin_velocity_60s=0.33,
                up_depth_usd=42.0,
                down_depth_usd=13.0,
                time_remaining_s=145.0,
                feed_count=3,
                filter_results=filters,
                action="DRY_RUN_SIGNAL",
                reason="second refire",
                run_id="run-refire",
                mode="both",
                dry_run=True,
                order_size_usd=10.0,
                paper_capital_total=5000.0,
                signal_context="lead_lag",
                signal_overlap_count=1,
                entry_price=0.58,
                target_price=0.64,
                expected_profit_per_share=0.02,
            )
        )
        trade_two = tracker.log_paper_trade(
            market_slug=market_slug,
            coin="xrp",
            strategy_type="lead_lag",
            side="up",
            entry_price=0.58,
            target_price=0.64,
            shares=17,
            fee_total=0.5,
            decision_id=second_id,
            window_id=market_slug,
        )
        tracker.close_paper_trade_early(
            trade_two,
            exit_price=0.54,
            pnl=-0.68,
            status="closed_loss",
            exit_reason="loss_cut",
            time_remaining_s=100.0,
            bid_at_exit=0.54,
            ask_at_exit=0.55,
        )

        trades_path, trades_count, _, _ = export_recent_trades_csv_gz(db_path, str(out_dir), "EDEC-BOT", 10)
        workbook_path = export_to_excel(db_path, str(out_dir))

        self.assertEqual(trades_count, 2)
        self.assertTrue(Path(workbook_path).exists())

        with gzip.open(trades_path, "rt", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))

        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id[str(trade_one)]["why"], "first refire")
        self.assertEqual(by_id[str(trade_two)]["why"], "second refire")
        self.assertEqual(by_id[str(trade_one)]["v30"], "0.11")
        self.assertEqual(by_id[str(trade_two)]["v30"], "0.29")


if __name__ == "__main__":
    unittest.main()
