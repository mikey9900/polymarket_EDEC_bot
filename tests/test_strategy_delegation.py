import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.config import load_config
from bot.strategy import StrategyEngine


class _DummyTracker:
    def get_paper_capital(self):
        return (5000.0, 5000.0)

    def get_runtime_context(self):
        return {}

    def log_decision(self, decision):
        return 1


class StrategyDelegationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(str(ROOT / "edec_bot" / "config_phase_a_single.yaml"))

    def setUp(self):
        self.engine = StrategyEngine(self.config, aggregator=None, scanner=None, tracker=_DummyTracker())

    def test_strategy_engine_evaluators_delegate_to_split_modules(self):
        cases = [
            ("_evaluate_dual_leg", "bot.strategy.dual_leg_strategy.evaluate"),
            ("_evaluate_single_leg", "bot.strategy.single_leg_strategy.evaluate"),
            ("_evaluate_lead_lag", "bot.strategy.lead_lag_strategy.evaluate"),
            ("_evaluate_swing_leg", "bot.strategy.swing_leg_strategy.evaluate"),
        ]

        for method_name, patch_target in cases:
            with self.subTest(method=method_name):
                sentinel = object()
                with mock.patch(patch_target, return_value=sentinel) as patched:
                    result = getattr(self.engine, method_name)("btc", "market", "up", "down", "agg")
                self.assertIs(result, sentinel)
                patched.assert_called_once_with(self.engine, "btc", "market", "up", "down", "agg")


if __name__ == "__main__":
    unittest.main()
