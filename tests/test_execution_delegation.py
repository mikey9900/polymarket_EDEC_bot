import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.execution import ExecutionEngine


class ExecutionDelegationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        config = SimpleNamespace(
            execution=SimpleNamespace(order_size_usd=10.0, dry_run=False),
            single_leg=SimpleNamespace(order_size_usd=10.0),
            lead_lag=SimpleNamespace(order_size_usd=10.0),
            swing_leg=SimpleNamespace(order_size_usd=10.0),
        )
        self.engine = ExecutionEngine(config, clob_client=None, risk_manager=object(), tracker=object())
        self.addAsyncCleanup(self.engine._http.aclose)

    async def test_execution_engine_methods_delegate_to_split_modules(self):
        sentinel = object()
        cases = [
            ("_execute_dual_leg", "bot.execution.dual_leg_execution.execute", ("signal", 7), sentinel),
            ("execute_single_leg", "bot.execution.single_leg_execution.execute", ("signal", 7), sentinel),
            ("_monitor_single_leg_entry", "bot.execution.single_leg_execution.monitor_entry", ("position", "result"), None),
            ("_monitor_single_leg", "bot.execution.single_leg_execution.monitor_position", ("position", "result"), None),
            ("execute_swing_leg", "bot.execution.swing_leg_execution.execute", ("signal", 7), sentinel),
            ("_monitor_swing_entry", "bot.execution.swing_leg_execution.monitor_entry", ("position", "result"), None),
            ("_monitor_swing_leg", "bot.execution.swing_leg_execution.monitor_position", ("position", "result"), None),
            (
                "_monitor_paper_single_leg",
                "bot.execution.single_leg_execution.monitor_paper_position",
                ("trade", "market", "token", 0.4, 0.5, 10, "single_leg"),
                None,
            ),
        ]

        for method_name, patch_target, args, expected in cases:
            with self.subTest(method=method_name):
                with mock.patch(patch_target, new=mock.AsyncMock(return_value=expected)) as patched:
                    result = await getattr(self.engine, method_name)(*args)
                self.assertIs(result, expected)
                patched.assert_awaited_once_with(self.engine, *args)


if __name__ == "__main__":
    unittest.main()
