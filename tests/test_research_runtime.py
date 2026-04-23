import json
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.config import load_config
from bot.models import AggregatedPrice, MarketInfo, OrderBookSnapshot
from research import paths as research_paths
from bot.strategy import StrategyEngine
from research.runtime import ResearchSnapshotProvider


class _CapturingTracker:
    def __init__(self):
        self.decisions = []

    def get_paper_capital(self):
        return (5000.0, 5000.0)

    def get_runtime_context(self):
        return {}

    def log_decision(self, decision):
        self.decisions.append(decision)
        return len(self.decisions)

    def update_decision_signal_context(self, *args, **kwargs):
        return None

    def suppress_decision(self, *args, **kwargs):
        return None


class _StubResearchProvider:
    def lookup(self, **kwargs):
        return {
            "research_cluster_id": "single_leg|btc|0.54-0.56|0.10-0.12|120-180",
            "research_cluster_n": 42,
            "research_cluster_win_pct": 40.0,
            "research_cluster_avg_pnl": -0.18,
            "research_policy_action": "paper_blocked",
        }


class _OverlayResearchProvider:
    def lookup(self, **kwargs):
        return {
            "research_cluster_id": "single_leg|btc|0.54-0.56|0.10-0.12|120-180",
            "research_cluster_n": 18,
            "research_cluster_win_pct": 55.0,
            "research_cluster_avg_pnl": 0.11,
            "research_policy_action": "advisory",
            "research_market_regime_1d": "liquid_balanced",
            "research_liquidity_score_1d": 84.0,
            "research_crowding_score_1d": 20.0,
            "research_score_flow_1d": 3.4,
            "research_score_crowding_1d": 1.2,
            "research_signal_score_adjustment": 2.2,
        }


class _ThinCrowdedResearchProvider:
    def lookup(self, **kwargs):
        return {
            "research_cluster_id": "lead_lag|sol|0.54-0.56|0.10-0.12|120-180",
            "research_cluster_n": 9,
            "research_cluster_win_pct": 44.0,
            "research_cluster_avg_pnl": -0.09,
            "research_policy_action": "advisory",
            "research_market_regime_1d": "thin_crowded",
            "research_liquidity_score_1d": 3.3,
            "research_crowding_score_1d": 65.1,
            "research_score_flow_1d": -4.67,
            "research_score_crowding_1d": 3.91,
            "research_signal_score_adjustment": -8.57,
        }


class ResearchRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_config = load_config(str(ROOT / "edec_bot" / "config_phase_a_single.yaml"))

    def test_snapshot_provider_returns_unclassified_when_missing(self):
        provider = ResearchSnapshotProvider(ROOT / ".tmp_testdata" / "missing_policy.json")
        payload = provider.lookup(
            strategy_type="lead_lag",
            coin="btc",
            entry_price=0.55,
            velocity_30s=0.11,
            time_remaining_s=150.0,
        )
        self.assertEqual(payload["research_cluster_n"], 0)
        self.assertEqual(payload["research_policy_action"], "unclassified")
        self.assertEqual(payload["research_market_regime_1d"], "")
        self.assertEqual(payload["research_signal_score_adjustment"], 0.0)
        self.assertTrue(payload["research_cluster_id"].startswith("lead_lag|btc|"))

    def test_snapshot_provider_returns_coin_feature_overlay(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        policy_path = tmp_root / f"policy_{uuid4().hex}.json"
        self.addCleanup(lambda: policy_path.unlink(missing_ok=True))
        policy_path.write_text(
            json.dumps(
                {
                    "clusters": {},
                    "coin_features": {
                        "btc": {
                            "market_regime_1d": "liquid_balanced",
                            "liquidity_score_1d": 82.5,
                            "crowding_score_1d": 18.4,
                            "score_flow_1d": 3.25,
                            "score_crowding_1d": 1.1,
                            "signal_score_adjustment": 2.15,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        provider = ResearchSnapshotProvider(policy_path)
        payload = provider.lookup(
            strategy_type="lead_lag",
            coin="btc",
            entry_price=0.55,
            velocity_30s=0.11,
            time_remaining_s=150.0,
        )

        self.assertEqual(payload["research_market_regime_1d"], "liquid_balanced")
        self.assertEqual(payload["research_liquidity_score_1d"], 82.5)
        self.assertEqual(payload["research_signal_score_adjustment"], 2.15)

        status = provider.status()
        self.assertTrue(status["artifact_exists"])
        self.assertEqual(status["reload_count"], 1)
        self.assertEqual(status["coin_feature_count"], 1)
        self.assertEqual(status["cluster_count"], 0)
        self.assertIsNotNone(status["last_loaded_at"])

    def test_snapshot_provider_resolves_data_relative_artifact_path_to_shared_root(self):
        with tempfile.TemporaryDirectory() as tmp_root_str:
            tmp_root = Path(tmp_root_str)
            repo_root = tmp_root / "repo"
            shared_root = tmp_root / "share" / "edec"
            policy_path = shared_root / "research" / "runtime_policy.json"
            policy_path.parent.mkdir(parents=True, exist_ok=True)
            policy_path.write_text(json.dumps({"clusters": {}, "coin_features": {}}), encoding="utf-8")

            with (
                mock.patch.object(research_paths, "REPO_ROOT", repo_root),
                mock.patch.object(research_paths, "SHARED_DATA_ROOT", shared_root),
            ):
                provider = ResearchSnapshotProvider("data/research/runtime_policy.json")
                status = provider.status()

        self.assertEqual(status["artifact_path"], str(policy_path))
        self.assertTrue(status["artifact_exists"])
        self.assertEqual(status["reload_count"], 1)

    def test_single_leg_paper_gate_suppresses_dry_run_signal(self):
        tracker = _CapturingTracker()
        config = replace(
            self.base_config,
            research=replace(self.base_config.research, enabled=True, paper_gate_enabled=True),
        )
        engine = StrategyEngine(config, aggregator=None, scanner=None, tracker=tracker, research_provider=_StubResearchProvider())
        now = datetime.now(timezone.utc)
        market = MarketInfo(
            event_id="evt-btc",
            condition_id="cond-btc",
            slug="btc-updown-5m-1713577200",
            coin="btc",
            up_token_id="up-token",
            down_token_id="down-token",
            start_time=now - timedelta(minutes=1),
            end_time=now + timedelta(minutes=3),
            fee_rate=0.02,
            tick_size="0.01",
            neg_risk=False,
        )
        up_book = OrderBookSnapshot("up-token", best_bid=0.54, best_ask=0.55, bid_depth_usd=20.0, ask_depth_usd=20.0, timestamp=now.timestamp())
        down_book = OrderBookSnapshot("down-token", best_bid=0.45, best_ask=0.52, bid_depth_usd=20.0, ask_depth_usd=20.0, timestamp=now.timestamp())
        agg = AggregatedPrice(
            price=85000.0,
            timestamp=now.timestamp(),
            velocity_30s=0.13,
            velocity_60s=0.13,
            is_trending=True,
            source_count=3,
            sources={"binance": 85000.0, "coinbase": 85010.0, "coingecko": 84990.0},
        )

        signal = engine._evaluate_single_leg("btc", market, up_book, down_book, agg)

        self.assertIsNone(signal)
        self.assertEqual(tracker.decisions[-1].action, "SUPPRESSED")
        self.assertEqual(tracker.decisions[-1].research_policy_action, "paper_blocked")
        self.assertIn("research_policy:paper_blocked", tracker.decisions[-1].reason)

    def test_single_leg_score_uses_research_overlay(self):
        tracker = _CapturingTracker()
        config = replace(
            self.base_config,
            research=replace(self.base_config.research, enabled=True, paper_gate_enabled=False),
        )
        engine = StrategyEngine(config, aggregator=None, scanner=None, tracker=tracker, research_provider=_OverlayResearchProvider())
        now = datetime.now(timezone.utc)
        market = MarketInfo(
            event_id="evt-btc",
            condition_id="cond-btc",
            slug="btc-updown-5m-1713577200",
            coin="btc",
            up_token_id="up-token",
            down_token_id="down-token",
            start_time=now - timedelta(minutes=1),
            end_time=now + timedelta(minutes=3),
            fee_rate=0.02,
            tick_size="0.01",
            neg_risk=False,
        )
        up_book = OrderBookSnapshot("up-token", best_bid=0.54, best_ask=0.55, bid_depth_usd=20.0, ask_depth_usd=20.0, timestamp=now.timestamp())
        down_book = OrderBookSnapshot("down-token", best_bid=0.45, best_ask=0.52, bid_depth_usd=20.0, ask_depth_usd=20.0, timestamp=now.timestamp())
        agg = AggregatedPrice(
            price=85000.0,
            timestamp=now.timestamp(),
            velocity_30s=0.13,
            velocity_60s=0.13,
            is_trending=True,
            source_count=3,
            sources={"binance": 85000.0, "coinbase": 85010.0, "coingecko": 84990.0},
        )

        signal = engine._evaluate_single_leg("btc", market, up_book, down_book, agg)

        self.assertIsNotNone(signal)
        self.assertEqual(tracker.decisions[-1].research_market_regime_1d, "liquid_balanced")
        self.assertEqual(tracker.decisions[-1].score_research_flow, 3.4)
        self.assertEqual(tracker.decisions[-1].score_research_crowding, -1.2)
        self.assertGreater(tracker.decisions[-1].order_size_usd, self.base_config.single_leg.order_size_usd)
        self.assertEqual(signal.score_research_flow, 3.4)
        self.assertEqual(signal.score_research_crowding, -1.2)
        self.assertGreater(signal.order_size_multiplier, 1.0)
        self.assertGreater(signal.signal_score, 0.0)

    def test_thin_crowded_regime_suppresses_dry_run_signal(self):
        tracker = _CapturingTracker()
        config = replace(
            self.base_config,
            research=replace(self.base_config.research, enabled=True, paper_gate_enabled=False),
        )
        engine = StrategyEngine(config, aggregator=None, scanner=None, tracker=tracker, research_provider=_ThinCrowdedResearchProvider())
        now = datetime.now(timezone.utc)
        market = MarketInfo(
            event_id="evt-sol",
            condition_id="cond-sol",
            slug="sol-updown-5m-1713577200",
            coin="sol",
            up_token_id="up-token",
            down_token_id="down-token",
            start_time=now - timedelta(minutes=1),
            end_time=now + timedelta(minutes=3),
            fee_rate=0.02,
            tick_size="0.01",
            neg_risk=False,
        )
        up_book = OrderBookSnapshot("up-token", best_bid=0.54, best_ask=0.55, bid_depth_usd=20.0, ask_depth_usd=20.0, timestamp=now.timestamp())
        down_book = OrderBookSnapshot("down-token", best_bid=0.45, best_ask=0.52, bid_depth_usd=20.0, ask_depth_usd=20.0, timestamp=now.timestamp())
        agg = AggregatedPrice(
            price=600.0,
            timestamp=now.timestamp(),
            velocity_30s=0.13,
            velocity_60s=0.13,
            is_trending=True,
            source_count=3,
            sources={"binance": 600.0, "coinbase": 601.0, "coingecko": 599.0},
        )

        signal = engine._evaluate_single_leg("sol", market, up_book, down_book, agg)

        self.assertIsNone(signal)
        self.assertEqual(tracker.decisions[-1].action, "SUPPRESSED")
        self.assertEqual(tracker.decisions[-1].research_market_regime_1d, "thin_crowded")
        self.assertIn("research_regime:thin_crowded_block", tracker.decisions[-1].reason)


if __name__ == "__main__":
    unittest.main()
