import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.dashboard_state import DashboardStateService
from bot.models import MarketInfo


class _FakeTracker:
    db_path = None

    def __init__(self):
        self.reset_calls = 0
        self.open_paper_trades = []

    def get_recent_signals_by_coin(self, max_age_s=30.0):
        return {}

    def get_session_stats_by_coin(self):
        return {}

    def get_coin_recent_resolutions(self, coin: str, limit: int = 4):
        return [{"winner": "DOWN", "market_slug": "tracker-fallback"}][:limit]

    def get_paper_capital(self):
        return (100.0, 115.0)

    def get_open_paper_trades(self):
        return list(self.open_paper_trades)

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
    def __init__(self):
        self.market = None
        self.up_book = None
        self.down_book = None

    def get_market(self, _coin: str):
        return self.market

    def get_books(self, _coin: str):
        return (self.up_book, self.down_book)

    def get_recent_resolutions(self, _coin: str, limit: int = 4):
        return [{"winner": "UP", "slug": "poly-1"}, {"winner": "DOWN", "slug": "poly-2"}][:limit]


class _FakeAggregator:
    def __init__(self):
        self.price = None

    def get_aggregated_price(self, _coin: str):
        if self.price is None:
            return None
        return SimpleNamespace(price=self.price)


class _FakeCodexManager:
    def __init__(self):
        self.calls = []
        self.schedule_enabled = True
        self.cadence = "weekly"
        self.skip_next = False

    def snapshot(self):
        return {
            "codex": {
                "healthy": True,
                "last_heartbeat_at": "2026-04-22T12:00:00+00:00",
                "queue_depth": 1,
                "active_run": {"job_type": "daily_research_refresh"},
                "last_run": {
                    "job_type": "tuning_proposal",
                    "ok": True,
                    "summary": "Weekly desktop review bundle status: ready.",
                    "finished_at": "2026-04-22T11:58:00+00:00",
                },
                "next_queued_job": {
                    "job_type": "daily_research_refresh",
                    "requested_at": "2026-04-22T11:59:00+00:00",
                },
                "latest_candidate": {
                    "candidate_id": "cand-1",
                    "status": "ready",
                    "summary": "Candidate ready for review.",
                    "paths": {},
                },
                "daily_local_candidate": {
                    "candidate_id": "local-1",
                    "status": "ready",
                    "summary": "Daily local candidate ready.",
                    "paths": {},
                },
                "weekly_ai_candidate": {
                    "candidate_id": "weekly-1",
                    "status": "ready",
                    "summary": "Weekly AI candidate ready.",
                    "paths": {},
                },
                "weekly_review_bundle": {
                    "status": "ready",
                    "summary": "Weekly desktop review bundle ready.",
                    "paths": {
                        "bundle_md": "data/research/weekly_review_bundle.md",
                        "desktop_prompt": "data/research/weekly_desktop_prompt.txt",
                    },
                },
                "primary_candidate_source": "weekly_ai",
            },
            "tuner": {
                "running": False,
                "schedule_enabled": self.schedule_enabled,
                "cadence": self.cadence,
                "skip_next_auto_run": self.skip_next,
                "next_auto_run_at": "2026-04-27T12:30:00+00:00",
                "last_run_at": "2026-04-21T12:30:00+00:00",
                "last_result": "success",
                "daily_local_last_run_at": "2026-04-22T12:15:00+00:00",
                "daily_local_last_result": "success",
                "weekly_ai_last_run_at": "2026-04-21T12:30:00+00:00",
                "weekly_ai_last_result": "success",
                "daily_local_candidate": {
                    "candidate_id": "local-1",
                    "status": "ready",
                    "summary": "Daily local candidate ready.",
                },
                "weekly_ai_candidate": {
                    "candidate_id": "weekly-1",
                    "status": "ready",
                    "summary": "Weekly AI candidate ready.",
                },
                "weekly_review_bundle": {
                    "status": "ready",
                    "summary": "Weekly desktop review bundle ready.",
                    "paths": {
                        "bundle_md": "data/research/weekly_review_bundle.md",
                        "desktop_prompt": "data/research/weekly_desktop_prompt.txt",
                    },
                },
                "primary_candidate_source": "weekly_ai",
                "candidate_available": True,
                "candidate_status": "ready",
                "candidate_summary": "Weekly AI candidate ready.",
            },
        }

    def enqueue_daily_refresh(self, *, requested_by: str = "dashboard", args=None):
        self.calls.append(("research_run_now", requested_by, args))
        return {"queued": True}

    def enqueue_tuning_proposal(self, *, requested_by: str = "dashboard", args=None):
        self.calls.append(("tuner_run_now", requested_by, args))
        return {"queued": True}

    def pause_tuner_schedule(self):
        self.calls.append(("tuner_schedule_pause",))
        self.schedule_enabled = False
        return {"ok": True, "message": "Weekly tuning schedule paused."}

    def resume_tuner_schedule(self):
        self.calls.append(("tuner_schedule_resume",))
        self.schedule_enabled = True
        return {"ok": True, "message": "Weekly tuning schedule resumed."}

    def set_tuner_cadence(self, cadence: str):
        self.calls.append(("tuner_set_cadence", cadence))
        self.cadence = cadence
        return {"ok": True, "message": f"Tuner cadence set to {cadence}."}

    def skip_next_tuner_run(self):
        self.calls.append(("tuner_skip_next",))
        self.skip_next = True
        return {"ok": True, "message": "Next automatic tuning run will be skipped."}

    def enqueue_promote_candidate(self, *, requested_by: str = "dashboard", candidate_id=None):
        self.calls.append(("tuner_promote_latest", requested_by, candidate_id))
        return {"queued": True}

    def enqueue_reject_candidate(self, *, requested_by: str = "dashboard", candidate_id=None, reason="Rejected by operator."):
        self.calls.append(("tuner_reject_latest", requested_by, candidate_id, reason))
        return {"queued": True}


class DashboardControlTests(unittest.IsolatedAsyncioTestCase):
    def _build_service(self, *, with_callbacks: bool = False, with_codex: bool = False) -> DashboardStateService:
        config = SimpleNamespace(
            coins=["btc"],
            execution=SimpleNamespace(dry_run=True, order_size_usd=10.0),
        )
        tracker = _FakeTracker()
        risk_manager = _FakeRiskManager()
        callbacks = {}
        if with_callbacks:
            callbacks = {
                "session_export_fn": lambda: {
                    "trade_count": 7,
                    "signal_count": 14,
                    "excel_path": "data/session_export.xlsx",
                    "session_dir": "data/exports/2026-04-19_170000_EDEC-BOT_session_export",
                },
            }
        scanner = _FakeScanner()
        aggregator = _FakeAggregator()
        service = DashboardStateService(
            config=config,
            tracker=tracker,
            risk_manager=risk_manager,
            scanner=scanner,
            strategy_engine=_FakeStrategyEngine(),
            executor=_FakeExecutor(),
            aggregator=aggregator,
            **callbacks,
        )
        service._slow_cache["paper_capital"] = (100.0, 115.0)
        if with_codex:
            service.control_plane.codex_manager = _FakeCodexManager()
        return service

    def test_snapshot_includes_control_payload(self):
        service = self._build_service()

        snapshot = service._build_snapshot()

        self.assertEqual(snapshot["controls"]["state"], "running")
        self.assertEqual(snapshot["controls"]["mode"], "both")
        self.assertEqual(snapshot["controls"]["order_size_usd"], 10.0)
        self.assertEqual(snapshot["summary"]["paper"]["pnl"], 15.0)
        self.assertFalse(snapshot["controls"]["available_actions"]["session_export"])
        self.assertEqual(snapshot["controls"]["last_message"], "CONTROL LINK STANDBY")

    def test_snapshot_includes_codex_and_tuner_sections(self):
        service = self._build_service(with_codex=True)

        snapshot = service._build_snapshot()

        self.assertEqual(snapshot["codex"]["queue_depth"], 1)
        self.assertEqual(snapshot["codex"]["latest_candidate"]["status"], "ready")
        self.assertEqual(snapshot["codex"]["primary_candidate_source"], "weekly_ai")
        self.assertEqual(snapshot["tuner"]["cadence"], "weekly")
        self.assertEqual(snapshot["tuner"]["primary_candidate_source"], "weekly_ai")
        self.assertEqual(
            snapshot["tuner"]["weekly_review_bundle"]["paths"]["desktop_prompt"],
            "data/research/weekly_desktop_prompt.txt",
        )
        self.assertTrue(snapshot["controls"]["available_actions"]["research_run_now"])

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

    async def test_apply_control_async_runs_session_export_action(self):
        service = self._build_service(with_callbacks=True)

        session_result = await service._apply_control_async("session_export")
        self.assertTrue(session_result["ok"])
        self.assertIn("7 trades, 14 signals", session_result["message"])
        self.assertIn("2026-04-19_170000_EDEC-BOT_session_export", session_result["message"])
        self.assertEqual(
            session_result["state"]["controls"]["available_actions"]["session_export"],
            True,
        )
        self.assertEqual(
            session_result["state"]["controls"]["last_message"],
            session_result["message"],
        )

    async def test_apply_control_async_reports_session_export_failure_reason(self):
        service = self._build_service(with_callbacks=True)

        def _boom():
            raise RuntimeError("dropbox app secret missing")

        service.control_plane.session_export_fn = _boom

        session_result = await service._apply_control_async("session_export")

        self.assertFalse(session_result["ok"])
        self.assertEqual(session_result["status"], 400)
        self.assertIn("Session export failed", session_result["message"])
        self.assertIn("dropbox app secret missing", session_result["message"])

    async def test_apply_control_async_handles_codex_actions(self):
        service = self._build_service(with_codex=True)

        research_result = await service._apply_control_async("research_run_now")
        cadence_result = await service._apply_control_async("tuner_set_cadence", "manual")

        self.assertTrue(research_result["ok"])
        self.assertIn("queued", research_result["message"].lower())
        self.assertIn("manual", cadence_result["message"].lower())
        self.assertTrue(cadence_result["ok"])
        self.assertEqual(cadence_result["state"]["tuner"]["cadence"], "manual")

    def test_market_payload_falls_back_when_reference_price_is_implausible(self):
        service = self._build_service()
        now = datetime.now(timezone.utc)
        service.scanner.market = MarketInfo(
            event_id="evt-1",
            condition_id="cond-1",
            slug="btc-updown-5m-123",
            coin="btc",
            up_token_id="up",
            down_token_id="down",
            start_time=now - timedelta(minutes=1),
            end_time=now + timedelta(minutes=4),
            fee_rate=0.02,
            tick_size="0.01",
            neg_risk=False,
            question="Bitcoin Up or Down - April 21, 3:45 PM ET",
            reference_price=21.0,
        )
        service.aggregator.price = 84500.0

        payload = service._build_market_payload("btc", live_price=84500.0)

        self.assertEqual(payload["strike"], 84500.0)

    def test_snapshot_marks_open_paper_trades_to_market_for_pnl_pills(self):
        service = self._build_service()
        now = datetime.now(timezone.utc)
        service._slow_cache["paper_capital"] = (100.0, 90.0)
        service.scanner.market = MarketInfo(
            event_id="evt-1",
            condition_id="cond-1",
            slug="btc-updown-5m-123",
            coin="btc",
            up_token_id="up",
            down_token_id="down",
            start_time=now - timedelta(minutes=1),
            end_time=now + timedelta(minutes=4),
            fee_rate=0.0,
            tick_size="0.01",
            neg_risk=False,
        )
        service.scanner.up_book = SimpleNamespace(best_bid=0.60, best_ask=0.61)
        service.scanner.down_book = SimpleNamespace(best_bid=0.40, best_ask=0.41)
        service._slow_cache["open_paper_trades"] = [
            {
                "coin": "btc",
                "market_slug": "btc-updown-5m-123",
                "strategy_type": "single_leg",
                "side": "up",
                "entry_price": 0.50,
                "shares": 20.0,
                "cost": 10.0,
                "fee_total": 0.0,
            }
        ]

        snapshot = service._build_snapshot()

        self.assertAlmostEqual(snapshot["summary"]["paper"]["balance"], 102.0)
        self.assertAlmostEqual(snapshot["summary"]["paper"]["pnl"], 2.0)
        self.assertEqual(snapshot["coins"]["btc"]["session"]["open"], 1)
        self.assertAlmostEqual(snapshot["coins"]["btc"]["session"]["pnl"], 2.0)


if __name__ == "__main__":
    unittest.main()
