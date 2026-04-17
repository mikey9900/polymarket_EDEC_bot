import csv
import shutil
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.export import export_recent_to_excel
from bot.models import Decision, FilterResult
from bot.tracker import DecisionTracker


class ExportRecentTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"edec_recent_export_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def test_recent_export_writes_direct_csv_with_decision_context(self):
        db_path = self.tmpdir / "decisions.db"
        out_dir = self.tmpdir / "exports"
        tracker = DecisionTracker(str(db_path))
        self.addCleanup(tracker.close)
        tracker.set_paper_capital(5000.0)
        tracker.set_runtime_context(
            {
                "run_id": "run-recent",
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
        decision_id = tracker.log_decision(
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
                filter_results=[FilterResult("risk_limits", True, "ok", "ok")],
                action="DRY_RUN_SIGNAL",
                reason="recent export check",
                run_id="run-recent",
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
        trade_id = tracker.log_paper_trade(
            market_slug=market_slug,
            coin="xrp",
            strategy_type="lead_lag",
            side="up",
            entry_price=0.55,
            target_price=0.61,
            shares=18,
            fee_total=0.5,
            decision_id=decision_id,
            window_id=market_slug,
        )
        tracker.close_paper_trade_early(
            trade_id,
            exit_price=0.57,
            pnl=0.22,
            status="closed_win",
            exit_reason="profit_target",
            time_remaining_s=110.0,
            bid_at_exit=0.57,
            ask_at_exit=0.58,
            stall_exit_triggered=True,
        )

        export_path = Path(export_recent_to_excel(str(db_path), str(out_dir), limit=10))
        latest_path = out_dir / "edec_recent10_latest.csv"

        self.assertTrue(export_path.exists())
        self.assertTrue(latest_path.exists())

        with export_path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["id"], str(trade_id))
        self.assertEqual(row["te"], "150.0")
        self.assertEqual(row["v30"], "0.11")
        self.assertEqual(row["v60"], "0.14")
        self.assertEqual(row["why"], "recent export check")
        self.assertEqual(row["status"], "closed_win")
        self.assertEqual(row["sx"], "1")


if __name__ == "__main__":
    unittest.main()
