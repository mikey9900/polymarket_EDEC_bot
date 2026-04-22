import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.control_plane import ControlPlane, ControlRequest


class _FakeTracker:
    def __init__(self):
        self.reset_calls = 0

    def reset_paper_stats(self):
        self.reset_calls += 1


class _FakeRiskManager:
    def __init__(self):
        self.kill_switch = False
        self.paused = False
        self.resume_calls = 0
        self.pause_calls = 0
        self.deactivate_calls = 0
        self.kill_calls = []
        self.reset_daily_calls = 0

    def get_status(self):
        return {
            "kill_switch": self.kill_switch,
            "paused": self.paused,
            "daily_pnl": 0.0,
            "open_positions": 0,
        }

    def resume(self):
        self.resume_calls += 1
        self.paused = False

    def pause(self):
        self.pause_calls += 1
        self.paused = True

    def deactivate_kill_switch(self):
        self.deactivate_calls += 1
        self.kill_switch = False

    def activate_kill_switch(self, reason: str):
        self.kill_calls.append(reason)
        self.kill_switch = True

    def reset_daily_stats(self):
        self.reset_daily_calls += 1


class _FakeStrategy:
    def __init__(self):
        self.is_active = True
        self.mode = "both"
        self.start_calls = 0
        self.stop_calls = 0

    def start_scanning(self):
        self.start_calls += 1
        self.is_active = True

    def stop_scanning(self):
        self.stop_calls += 1
        self.is_active = False

    def set_mode(self, mode: str) -> bool:
        self.mode = mode
        self.is_active = mode != "off"
        return True


class _FakeExecutor:
    def __init__(self):
        self.order_size_usd = 10.0

    def set_order_size(self, usd: float):
        self.order_size_usd = usd


class ControlPlaneTests(unittest.TestCase):
    def _build_plane(self) -> ControlPlane:
        return ControlPlane(
            config=SimpleNamespace(execution=SimpleNamespace(order_size_usd=10.0)),
            tracker=_FakeTracker(),
            risk_manager=_FakeRiskManager(),
            strategy_engine=_FakeStrategy(),
            executor=_FakeExecutor(),
        )

    def test_start_stop_and_mode_share_same_control_surface(self):
        plane = self._build_plane()

        start = plane.apply_sync(ControlRequest("start"))
        mode = plane.apply_sync(ControlRequest("mode", "lead"))
        stop = plane.apply_sync(ControlRequest("stop"))

        self.assertTrue(start.ok)
        self.assertTrue(mode.ok)
        self.assertTrue(stop.ok)
        self.assertEqual(plane.strategy_engine.mode, "lead")
        payload = plane.build_controls_payload()
        self.assertEqual(payload["state"], "paused")
        self.assertEqual(payload["mode"], "lead")

    def test_reset_stats_updates_both_tracker_and_risk(self):
        plane = self._build_plane()

        result = plane.apply_sync(ControlRequest("reset_stats"))

        self.assertTrue(result.ok)
        self.assertEqual(plane.tracker.reset_calls, 1)
        self.assertEqual(plane.risk_manager.reset_daily_calls, 1)


if __name__ == "__main__":
    unittest.main()
