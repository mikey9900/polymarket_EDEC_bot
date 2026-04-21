import csv
import json
import shutil
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.models import Decision, FilterResult
from bot.tracker import DecisionTracker
from research.artifacts import build_artifacts
from research.warehouse import ResearchWarehouse


class ResearchArtifactsTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"research_artifacts_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def test_build_artifacts_writes_policy_and_report(self):
        db_path = self.tmpdir / "decisions.db"
        tracker = DecisionTracker(str(db_path))
        self.addCleanup(tracker.close)
        tracker.set_paper_capital(5000.0)
        tracker.set_runtime_context(
            {
                "run_id": "run-research",
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
        for idx in range(30):
            decision_id = tracker.log_decision(
                Decision(
                    timestamp=now - timedelta(hours=idx),
                    market_slug=f"btc-updown-5m-{idx}",
                    window_id=f"btc-updown-5m-{idx}",
                    coin="btc",
                    market_end_time=now + timedelta(minutes=5),
                    market_start_time=now,
                    strategy_type="lead_lag",
                    up_best_ask=0.55,
                    down_best_ask=0.45,
                    combined_cost=1.0,
                    btc_price=85000.0,
                    coin_velocity_30s=0.11,
                    coin_velocity_60s=0.15,
                    up_depth_usd=30.0,
                    down_depth_usd=15.0,
                    time_remaining_s=150.0,
                    feed_count=3,
                    filter_results=filters,
                    action="DRY_RUN_SIGNAL",
                    reason="artifact build test",
                    run_id="run-research",
                    mode="both",
                    dry_run=True,
                    order_size_usd=10.0,
                    paper_capital_total=5000.0,
                    entry_price=0.55,
                    target_price=0.61,
                    expected_profit_per_share=0.02,
                )
            )
            trade_id = tracker.log_paper_trade(
                market_slug=f"btc-updown-5m-{idx}",
                coin="btc",
                strategy_type="lead_lag",
                side="up",
                entry_price=0.55,
                target_price=0.61,
                shares=18,
                fee_total=0.5,
                decision_id=decision_id,
                window_id=f"btc-updown-5m-{idx}",
            )
            tracker.close_paper_trade_early(
                trade_id,
                exit_price=0.52,
                pnl=-0.25,
                status="closed_loss",
                exit_reason="loss_cut",
                time_remaining_s=90.0,
                bid_at_exit=0.52,
                ask_at_exit=0.53,
            )

        export_path = self.tmpdir / "session.csv"
        with export_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["id", "rid", "c", "st", "ep", "v30", "te", "pnl", "xt"])
            writer.writeheader()
            writer.writerow(
                {
                    "id": "1",
                    "rid": "run-research",
                    "c": "btc",
                    "st": "lead_lag",
                    "ep": "0.55",
                    "v30": "0.11",
                    "te": "150.0",
                    "pnl": "-0.25",
                    "xt": now.isoformat(),
                }
            )

        with mock.patch("research.artifacts.discover_session_export_files", return_value=[export_path]):
            result = build_artifacts(
                warehouse_path=self.tmpdir / "warehouse.duckdb",
                tracker_db=db_path,
                policy_path=self.tmpdir / "runtime_policy.json",
                report_json_path=self.tmpdir / "research_report.json",
                report_md_path=self.tmpdir / "research_report.md",
                lookback_days=30,
            )

        self.assertEqual(result["outcome_count"], 30)
        with (self.tmpdir / "runtime_policy.json").open("r", encoding="utf-8") as fh:
            policy = json.load(fh)
        clusters = list(policy["clusters"].values())
        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0]["policy_action"], "paper_blocked")
        self.assertEqual(clusters[0]["sample_size"], 30)
        self.assertTrue((self.tmpdir / "research_report.json").exists())
        self.assertTrue((self.tmpdir / "research_report.md").exists())

    def test_build_artifacts_emits_coin_features_from_fill_flow(self):
        warehouse = ResearchWarehouse(self.tmpdir / "warehouse.duckdb")
        now = datetime.now(timezone.utc)
        warehouse.conn.executemany(
            """
            INSERT INTO fills_enriched (
                event_id, event_timestamp, event_time, event_date, transaction_hash,
                maker, taker, maker_asset_id, taker_asset_id, market_id, market_slug,
                coin, token_side, token_id, price, usd_amount, token_amount, is_5m_updown
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("btc-1", 1, now, now.date(), "tx-b1", "0xa", "0xb", "tok", "0", "m-btc", "btc-updown-5m-1", "btc", "up", "tok", 0.55, 1000.0, 1800.0, True),
                ("btc-2", 2, now, now.date(), "tx-b2", "0xc", "0xd", "tok", "0", "m-btc", "btc-updown-5m-1", "btc", "up", "tok", 0.56, 900.0, 1600.0, True),
                ("btc-3", 3, now, now.date(), "tx-b3", "0xe", "0xf", "tok", "0", "m-btc", "btc-updown-5m-1", "btc", "down", "tok", 0.54, 800.0, 1500.0, True),
                ("btc-4", 4, now, now.date(), "tx-b4", "0xg", "0xh", "tok", "0", "m-btc", "btc-updown-5m-1", "btc", "down", "tok", 0.53, 700.0, 1400.0, True),
                ("sol-1", 5, now, now.date(), "tx-s1", "0xwhale", "0xs1", "tok", "0", "m-sol", "sol-updown-5m-1", "sol", "up", "tok", 0.44, 700.0, 1500.0, True),
                ("sol-2", 6, now, now.date(), "tx-s2", "0xwhale", "0xs2", "tok", "0", "m-sol", "sol-updown-5m-1", "sol", "up", "tok", 0.43, 700.0, 1600.0, True),
                ("sol-3", 7, now, now.date(), "tx-s3", "0xwhale", "0xs3", "tok", "0", "m-sol", "sol-updown-5m-1", "sol", "down", "tok", 0.42, 700.0, 1700.0, True),
            ],
        )
        warehouse.conn.commit()
        warehouse.close()

        result = build_artifacts(
            warehouse_path=self.tmpdir / "warehouse.duckdb",
            tracker_db=self.tmpdir / "missing_tracker.db",
            policy_path=self.tmpdir / "runtime_policy_coin.json",
            report_json_path=self.tmpdir / "research_report_coin.json",
            report_md_path=self.tmpdir / "research_report_coin.md",
            lookback_days=30,
        )

        self.assertEqual(result["fill_flow_rows"], 2)
        with (self.tmpdir / "runtime_policy_coin.json").open("r", encoding="utf-8") as fh:
            policy = json.load(fh)
        self.assertIn("coin_features", policy)
        self.assertGreater(policy["coin_features"]["btc"]["liquidity_score_1d"], policy["coin_features"]["sol"]["liquidity_score_1d"])
        self.assertLess(policy["coin_features"]["btc"]["crowding_score_1d"], policy["coin_features"]["sol"]["crowding_score_1d"])
        self.assertGreater(
            policy["coin_features"]["btc"]["signal_score_adjustment"],
            policy["coin_features"]["sol"]["signal_score_adjustment"],
        )


if __name__ == "__main__":
    unittest.main()
