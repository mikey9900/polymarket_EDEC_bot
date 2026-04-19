import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.dashboard_state import DashboardStateService


class _FakeTracker:
    db_path = None

    def __init__(self):
        self.reset_calls = 0

    def get_recent_signals_by_coin(self, max_age_s=30.0):
        return {}

    def get_session_stats_by_coin(self):
        return {}

    def get_coin_recent_resolutions(self, coin: str, limit: int = 4):
        return [{"winner": "DOWN", "market_slug": "tracker-fallback"}][:limit]

    def get_paper_capital(self):
        return (100.0, 115.0)

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
            "session_pnl": 0.0,
            "open_positions": 0,
            "trades_this_hour": 0,
        }

    def resume(self):
        self.paused = False
        self.resume_calls += 1

    def pause(self):
        self.paused = True
        self.pause_calls += 1

    def deactivate_kill_switch(self):
        self.kill_switch = False
        self.deactivate_calls += 1

    def activate_kill_switch(self, reason: str):
        self.kill_switch = True
        self.kill_calls.append(reason)

    def reset_daily_stats(self):
        self.reset_daily_calls += 1


class _FakeStrategyEngine:
    def __init__(self):
        self.mode = "both"
        self.is_active = True
        self.start_calls = 0
        self.stop_calls = 0

    def start_scanning(self):
        self.start_calls += 1
        if self.mode == "off":
            self.mode = "both"
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

    def get_open_positions(self):
        return {}


class _FakeScanner:
    def get_market(self, _coin: str):
        return None

    def get_books(self, _coin: str):
        return (None, None)

    def get_recent_resolutions(self, _coin: str, limit: int = 4):
        return [{"winner": "UP", "slug": "poly-1"}, {"winner": "DOWN", "slug": "poly-2"}][:limit]


class _FakeAggregator:
    def get_aggregated_price(self, _coin: str):
        return None


class DashboardControlTests(unittest.IsolatedAsyncioTestCase):
    def _build_service(self, *, with_callbacks: bool = False) -> DashboardStateService:
        config = SimpleNamespace(
            coins=["btc"],
            execution=SimpleNamespace(dry_run=True, order_size_usd=10.0),
        )
        tracker = _FakeTracker()
        risk_manager = _FakeRiskManager()
        callbacks = {}
        if with_callbacks:
            callbacks = {
                "export_fn": lambda today_only=False: f"data/{'today' if today_only else 'all'}.xlsx",
                "export_recent_fn": lambda: "data/recent.xlsx",
                "archive_fn": lambda: {
                    "index_path": "data/EDEC-BOT_latest_index.json",
                    "row_counts": {"recent_trades_rows": 12, "recent_signals_rows": 8},
                },
                "archive_latest_fn": lambda: {
                    "latest_excel": "data/EDEC-BOT_latest_last24h.xlsx",
                    "latest_trades": "data/EDEC-BOT_latest_trades.csv.gz",
                    "latest_signals": "data/EDEC-BOT_latest_signals.csv.gz",
                    "latest_index": "data/EDEC-BOT_latest_index.json",
                },
                "archive_health_fn": lambda: {
                    "local": {
                        "latest_excel_exists": True,
                        "latest_trades_exists": True,
                        "latest_signals_exists": False,
                        "latest_index_exists": True,
                    },
                    "dropbox_live": {"ok": False},
                },
                "repo_sync_fn": lambda: {
                    "ok": True,
                    "output_dir": "data/dropbox_sync",
                    "downloads": {
                        "latest_last24h_xlsx": {"ok": True},
                        "latest_trades_csv_gz": {"ok": True},
                        "latest_signals_csv_gz": {"ok": False},
                        "latest_index_json": {"ok": True},
                    },
                },
                "session_export_fn": lambda: {"trade_count": 7, "signal_count": 14},
                "fetch_github_fn": lambda limit=3: {
                    "ok": True,
                    "fetched_count": limit,
                    "output_dir": "data/github_exports",
                },
            }
        service = DashboardStateService(
            config=config,
            tracker=tracker,
            risk_manager=risk_manager,
            scanner=_FakeScanner(),
            strategy_engine=_FakeStrategyEngine(),
            executor=_FakeExecutor(),
            aggregator=_FakeAggregator(),
            **callbacks,
        )
        service._slow_cache["paper_capital"] = (100.0, 115.0)
        return service

    def test_snapshot_includes_control_payload(self):
        service = self._build_service()

        snapshot = service._build_snapshot()

        self.assertEqual(snapshot["controls"]["state"], "running")
        self.assertEqual(snapshot["controls"]["mode"], "both")
        self.assertEqual(snapshot["controls"]["order_size_usd"], 10.0)
        self.assertEqual(snapshot["summary"]["paper"]["pnl"], 15.0)
        self.assertFalse(snapshot["controls"]["available_actions"]["export_today"])
        self.assertEqual(snapshot["controls"]["last_message"], "CONTROL LINK STANDBY")

    async def test_apply_control_async_updates_mode_and_budget(self):
        service = self._build_service()

        mode_result = await service._apply_control_async("mode", "lead")
        budget_result = await service._apply_control_async("budget", 15)

        self.assertTrue(mode_result["ok"])
        self.assertEqual(mode_result["state"]["controls"]["mode"], "lead")
        self.assertTrue(budget_result["ok"])
        self.assertEqual(budget_result["state"]["controls"]["order_size_usd"], 15.0)

    def test_apply_control_handles_start_stop_kill_and_reset(self):
        service = self._build_service()
        risk = service.risk_manager
        strategy = service.strategy_engine
        tracker = service.tracker

        stop_result = service._apply_control("stop")
        self.assertTrue(stop_result["ok"])
        self.assertTrue(risk.paused)
        self.assertFalse(strategy.is_active)

        start_result = service._apply_control("start")
        self.assertTrue(start_result["ok"])
        self.assertFalse(risk.paused)
        self.assertEqual(risk.resume_calls, 1)
        self.assertEqual(risk.deactivate_calls, 1)
        self.assertTrue(strategy.is_active)

        kill_result = service._apply_control("kill")
        self.assertTrue(kill_result["ok"])
        self.assertTrue(risk.kill_switch)
        self.assertEqual(risk.kill_calls, ["Manual kill via HA dashboard"])
        self.assertFalse(strategy.is_active)

        reset_result = service._apply_control("reset_stats")
        self.assertTrue(reset_result["ok"])
        self.assertEqual(tracker.reset_calls, 1)
        self.assertEqual(risk.reset_daily_calls, 1)

    def test_apply_control_rejects_invalid_values(self):
        service = self._build_service()

        bad_mode = service._apply_control("mode", "weird")
        bad_budget = service._apply_control("budget", -1)

        self.assertFalse(bad_mode["ok"])
        self.assertEqual(bad_mode["status"], 400)
        self.assertFalse(bad_budget["ok"])
        self.assertEqual(bad_budget["status"], 400)

    def test_refresh_slow_cache_prefers_scanner_recent_resolutions(self):
        service = self._build_service()

        service._refresh_slow_cache()

        self.assertEqual(
            service._slow_cache["recent_resolutions_by_coin"]["btc"],
            [{"winner": "UP", "slug": "poly-1"}, {"winner": "DOWN", "slug": "poly-2"}],
        )

    async def test_apply_control_async_runs_export_archive_and_sync_actions(self):
        service = self._build_service(with_callbacks=True)

        export_result = await service._apply_control_async("export_today")
        session_result = await service._apply_control_async("session_export")
        health_result = await service._apply_control_async("archive_health")
        sync_result = await service._apply_control_async("sync_repo_latest")
        github_result = await service._apply_control_async("fetch_github", 2)

        self.assertTrue(export_result["ok"])
        self.assertIn("today.xlsx", export_result["message"])
        self.assertTrue(session_result["ok"])
        self.assertIn("7 trades, 14 signals", session_result["message"])
        self.assertFalse(health_result["ok"])
        self.assertIn("Archive health: local 3/4 | Dropbox warn.", health_result["message"])
        self.assertTrue(sync_result["ok"])
        self.assertIn("Repo sync ok: 3/3 files", sync_result["message"])
        self.assertTrue(github_result["ok"])
        self.assertIn("2 folder(s)", github_result["message"])
        self.assertEqual(
            github_result["state"]["controls"]["available_actions"]["session_export"],
            True,
        )
        self.assertEqual(
            github_result["state"]["controls"]["last_action"],
            "fetch_github",
        )


if __name__ == "__main__":
    unittest.main()
