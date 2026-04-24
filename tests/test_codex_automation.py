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

from research.codex_automation import CodexAutomationManager


class CodexAutomationManagerTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"codex_manager_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

        self.manager = CodexAutomationManager(
            state_path=self.tmpdir / "state.json",
            latest_path=self.tmpdir / "latest.json",
            queue_root=self.tmpdir / "queue",
            runs_root=self.tmpdir / "runs",
            lock_path=self.tmpdir / "runner.lock",
            config_path=self.tmpdir / "config_phase_a_single.yaml",
            tuner_state_path=self.tmpdir / "tuner_state.json",
        )
        self.manager.config_path.write_text("single_leg:\n  min_velocity_30s: 0.12\n", encoding="utf-8")

    def test_enqueue_dedupes_same_job_type(self):
        first = self.manager.enqueue_daily_refresh(requested_by="test")
        second = self.manager.enqueue_daily_refresh(requested_by="test")

        self.assertTrue(first["queued"])
        self.assertFalse(second["queued"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(self.manager.queue_depth(), 1)
        snapshot = self.manager.snapshot()
        self.assertEqual(snapshot["codex"]["next_queued_job"]["job_type"], "daily_research_refresh")

    def test_snapshot_excludes_active_job_from_queue_depth(self):
        queued = self.manager.enqueue_daily_refresh(requested_by="test")
        state = self.manager.read_state()
        state["active_run"] = {
            "run_id": "run-1",
            "job_type": "daily_research_refresh",
            "request_id": queued["request_id"],
            "started_at": "2026-04-23T19:06:00+00:00",
            "phase": "syncing fills",
            "detail": "Refreshing recent Goldsky 5m fills.",
        }
        self.manager.save_state(state)

        snapshot = self.manager.snapshot()

        self.assertEqual(snapshot["codex"]["queue_depth"], 0)
        self.assertIsNone(snapshot["codex"]["next_queued_job"])

    def test_run_once_clears_orphaned_active_run_without_lock(self):
        state = self.manager.read_state()
        state["active_run"] = {
            "run_id": "stale-run",
            "job_type": "daily_research_refresh",
            "request_id": "stale-request",
            "started_at": "2026-04-23T19:06:00+00:00",
        }
        self.manager.save_state(state)

        result = self.manager.run_once()
        snapshot = self.manager.snapshot()

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "idle")
        self.assertIsNone(snapshot["codex"]["active_run"])

    def test_run_once_clears_stale_legacy_lock_and_active_run(self):
        stale_at = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        self.manager.lock_path.write_text(json.dumps({"created_at": stale_at}), encoding="utf-8")
        state = self.manager.read_state()
        state["active_run"] = {
            "run_id": "stale-run",
            "job_type": "daily_research_refresh",
            "request_id": "stale-request",
            "started_at": "2026-04-23T19:06:00+00:00",
        }
        self.manager.save_state(state)

        result = self.manager.run_once()
        snapshot = self.manager.snapshot()

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "idle")
        self.assertFalse(self.manager.lock_path.exists())
        self.assertIsNone(snapshot["codex"]["active_run"])

    def test_run_once_clears_stale_active_run_even_without_lock_cleanup(self):
        state = self.manager.read_state()
        state["active_run"] = {
            "run_id": "stale-run",
            "job_type": "daily_research_refresh",
            "request_id": "stale-request",
            "started_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        }
        self.manager.save_state(state)

        result = self.manager.run_once()
        snapshot = self.manager.snapshot()

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "idle")
        self.assertIsNone(snapshot["codex"]["active_run"])

    def test_reset_runner_state_clears_daily_queue_active_run_and_lock(self):
        queued = self.manager.enqueue_daily_refresh(requested_by="dashboard")
        state = self.manager.read_state()
        state["active_run"] = {
            "run_id": "run-1",
            "job_type": "daily_research_refresh",
            "request_id": queued["request_id"],
            "started_at": "2026-04-23T19:06:00+00:00",
        }
        self.manager.save_state(state)
        self.manager.lock_path.write_text(json.dumps({"created_at": "2026-04-23T19:06:00+00:00"}), encoding="utf-8")

        result = self.manager.reset_runner_state()
        snapshot = self.manager.snapshot()

        self.assertTrue(result["ok"])
        self.assertTrue(result["lock_removed"])
        self.assertEqual(result["removed_queue"], 1)
        self.assertFalse(self.manager.lock_path.exists())
        self.assertIsNone(snapshot["codex"]["active_run"])
        self.assertEqual(snapshot["codex"]["queue_depth"], 0)

    def test_tuner_schedule_controls_update_snapshot(self):
        pause = self.manager.pause_tuner_schedule()
        self.assertTrue(pause["ok"])
        snapshot = self.manager.snapshot()
        self.assertFalse(snapshot["tuner"]["schedule_enabled"])

        manual = self.manager.set_tuner_cadence("manual")
        self.assertTrue(manual["ok"])
        snapshot = self.manager.snapshot()
        self.assertEqual(snapshot["tuner"]["cadence"], "manual")
        self.assertIsNone(snapshot["tuner"]["next_auto_run_at"])

        skip = self.manager.skip_next_tuner_run()
        self.assertTrue(skip["ok"])
        snapshot = self.manager.snapshot()
        self.assertTrue(snapshot["tuner"]["skip_next_auto_run"])

        resume = self.manager.resume_tuner_schedule()
        self.assertTrue(resume["ok"])
        snapshot = self.manager.snapshot()
        self.assertTrue(snapshot["tuner"]["schedule_enabled"])

    def test_snapshot_surfaces_daily_and_weekly_candidate_sources(self):
        report_json = self.tmpdir / "tuner_report.json"
        report_json.write_text(
            json.dumps(
                {
                    "data": {"closed": 48, "win_pct": 54.2, "total_pnl": 3.15},
                    "changes": [
                        {
                            "path": "single_leg.min_velocity_30s",
                            "current": 0.12,
                            "recommended": 0.15,
                            "evidence": "Velocity buckets improved above 0.15.",
                        }
                    ],
                    "advisories": ["depth_check is rejecting too many signals."],
                    "no_change": ["single_leg.entry_max already fits the viable band."],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.tmpdir / "tuner_state.json").write_text(
            json.dumps(
                {
                    "daily_local_candidate": {
                        "candidate_id": "local-1",
                        "status": "ready",
                        "summary": "Daily local candidate ready.",
                        "paths": {"report_json": str(report_json)},
                        "generated_at": "2026-04-21T12:00:00+00:00",
                        "last_result": "ready",
                    },
                    "weekly_ai_candidate": {
                        "candidate_id": "weekly-1",
                        "status": "ready",
                        "summary": "Weekly AI candidate ready.",
                        "paths": {},
                        "generated_at": "2026-04-22T12:00:00+00:00",
                        "last_result": "ready",
                    },
                    "weekly_review_bundle": {
                        "generated_at": "2026-04-22T12:30:00+00:00",
                        "status": "ready",
                        "summary": "Weekly review bundle ready.",
                        "paths": {"bundle_md": "data/research/weekly_review_bundle.md"},
                        "last_result": "ready",
                    },
                    "primary_candidate_source": "weekly_ai",
                    "latest_candidate_id": "weekly-1",
                    "latest_candidate_status": "ready",
                    "latest_candidate_summary": "Weekly AI candidate ready.",
                    "latest_candidate_paths": {},
                    "latest_candidate_source": "weekly_ai",
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        snapshot = self.manager.snapshot()

        self.assertEqual(snapshot["codex"]["primary_candidate_source"], "weekly_ai")
        self.assertEqual(snapshot["codex"]["weekly_ai_candidate"]["candidate_id"], "weekly-1")
        self.assertEqual(snapshot["codex"]["daily_local_candidate_details"]["candidate_id"], "local-1")
        self.assertEqual(snapshot["codex"]["daily_local_candidate_details"]["change_count"], 1)
        self.assertEqual(snapshot["codex"]["weekly_review_bundle"]["status"], "ready")
        self.assertEqual(snapshot["tuner"]["daily_local_candidate"]["candidate_id"], "local-1")
        self.assertEqual(snapshot["tuner"]["candidate_summary"], "Weekly AI candidate ready.")

    def test_research_controls_default_and_update(self):
        snapshot = self.manager.snapshot()
        self.assertEqual(snapshot["codex"]["research_controls"]["proposal_aggressiveness_level"], 5)
        self.assertEqual(snapshot["codex"]["research_controls"]["live_aggressiveness_level"], 5)

        proposal = self.manager.set_proposal_aggressiveness(8, requested_by="test")
        live = self.manager.set_live_aggressiveness(7, requested_by="test")
        snapshot = self.manager.snapshot()

        self.assertTrue(proposal["ok"])
        self.assertTrue(live["ok"])
        self.assertEqual(snapshot["codex"]["research_controls"]["proposal_aggressiveness_level"], 8)
        self.assertEqual(snapshot["codex"]["research_controls"]["live_aggressiveness_level"], 7)

    def test_snapshot_surfaces_latest_daily_research_metrics(self):
        run_dir = self.tmpdir / "runs" / "20260422T180000Z-daily"
        run_dir.mkdir(parents=True, exist_ok=True)
        result_path = run_dir / "result.json"
        result_path.write_text(
            json.dumps(
                {
                    "run_id": "run-1",
                    "job_type": "daily_research_refresh",
                    "finished_at": "2026-04-22T18:00:00+00:00",
                    "ok": True,
                    "result": {
                        "build": {
                            "ok": True,
                            "result": {
                                "cluster_count": 48,
                                "outcome_count": 235,
                                "fill_flow_rows": 3,
                            },
                        },
                        "sync": {
                            "ok": True,
                            "result": {
                                "markets": {
                                    "fetched": 1200,
                                    "inserted": 1200,
                                    "open_markets": {"fetched": 600, "inserted": 600},
                                    "closed_markets": {"fetched": 600, "inserted": 600},
                                },
                                "fills": {
                                    "fetched": 42,
                                    "inserted": 42,
                                    "fills_enriched_rows": 19,
                                    "market_5m_registry_rows": 2070,
                                    "recent": {
                                        "asset_window_count": 5,
                                        "asset_count": 10,
                                        "fetched": 12,
                                        "inserted": 12,
                                    },
                                    "history": {
                                        "asset_window_count": 8,
                                        "asset_count": 16,
                                        "fetched": 30,
                                        "inserted": 30,
                                    },
                                }
                            },
                        },
                        "daily_local_tuning": {
                            "ok": True,
                            "result": {
                                "candidate_status": "none",
                            },
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.tmpdir / "latest.json").write_text(
            json.dumps(
                {
                    "daily_research_refresh": {
                        "run_id": "run-1",
                        "finished_at": "2026-04-22T18:00:00+00:00",
                        "result_path": str(result_path),
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        snapshot = self.manager.snapshot()

        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["fill_flow_rows"], 3)
        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["market_fetched_count"], 1200)
        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["closed_market_fetched_count"], 600)
        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["fetched_fill_count"], 42)
        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["recent_window_count"], 5)
        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["recent_fetched_fill_count"], 12)
        self.assertEqual(snapshot["codex"]["daily_research_metrics"]["fills_enriched_rows"], 19)
        self.assertEqual(snapshot["tuner"]["daily_research_metrics"]["candidate_status"], "none")

    def test_run_once_processes_repo_task(self):
        self.manager.enqueue_job(
            "repo_task",
            requested_by="test",
            args={"command": [sys.executable, "-c", "print('codex-sidecar-ok')"]},
        )

        result = self.manager.run_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["returncode"], 0)
        self.assertIn("codex-sidecar-ok", result["result"]["stdout"])
        self.assertEqual(self.manager.queue_depth(), 0)
        runs = list((self.tmpdir / "runs").glob("*/result.json"))
        self.assertEqual(len(runs), 1)
        payload = json.loads(runs[0].read_text(encoding="utf-8"))
        self.assertEqual(payload["job_type"], "repo_task")

    def test_tuning_proposal_job_builds_weekly_review_bundle(self):
        self.manager.enqueue_tuning_proposal(requested_by="test")

        with mock.patch(
            "research.codex_automation.build_weekly_review_bundle",
            return_value={"ok": True, "status": "ready", "bundle_md_path": "data/research/weekly_review_bundle.md"},
        ):
            result = self.manager.run_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["status"], "ready")

    def test_daily_refresh_runner_defaults_to_recent_only_scan(self):
        fake_warehouse = mock.MagicMock()
        fake_market_source = mock.MagicMock()
        fake_market_source.close = mock.MagicMock()
        fake_fill_source = mock.MagicMock()
        fake_fill_source.close = mock.MagicMock()
        fake_warehouse.close = mock.MagicMock()

        with (
            mock.patch("research.codex_automation.ResearchWarehouse", return_value=fake_warehouse),
            mock.patch("research.codex_automation.GammaMarketSource", return_value=fake_market_source),
            mock.patch("research.codex_automation.GoldskyFillSource", return_value=fake_fill_source),
            mock.patch("research.codex_automation.sync_recent_markets", return_value={"fetched": 10}) as market_sync,
            mock.patch("research.codex_automation.sync_recent_5m_fills", return_value={"fetched": 5}) as fill_sync,
            mock.patch("research.codex_automation.build_artifacts", return_value={"cluster_count": 1}),
            mock.patch("research.codex_automation.propose_tuning", return_value={"candidate_status": "none"}) as propose,
            mock.patch("research.codex_automation.build_weekly_ai_context", return_value={"context_path": "data/research/weekly_ai_context.json"}) as weekly,
        ):
            result = self.manager._run_daily_refresh({})

        self.assertTrue(result["sync"]["ok"])
        market_sync.assert_called_once()
        self.assertEqual(market_sync.call_args.kwargs["lookback_days"], 1)
        self.assertEqual(market_sync.call_args.kwargs["max_batches"], 2)
        fill_sync.assert_called_once()
        self.assertEqual(fill_sync.call_args.kwargs["lookback_hours"], 24)
        self.assertEqual(fill_sync.call_args.kwargs["history_lookback_days"], 1)
        self.assertEqual(propose.call_args.kwargs["proposal_aggressiveness_level"], 5)
        self.assertEqual(weekly.call_args.kwargs["proposal_aggressiveness_level"], 5)

    def test_candidate_specific_queue_validation_requires_ready_match(self):
        (self.tmpdir / "tuner_state.json").write_text(
            json.dumps(
                {
                    "daily_local_candidate": {
                        "candidate_id": "local-1",
                        "status": "ready",
                        "summary": "Daily local candidate ready.",
                    },
                    "latest_candidate_id": "local-1",
                    "latest_candidate_status": "ready",
                    "latest_candidate_source": "daily_local",
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        promote = self.manager.enqueue_promote_candidate(requested_by="test", candidate_id="local-1")
        self.assertTrue(promote["queued"])
        with self.assertRaisesRegex(Exception, "No ready tuning candidate matches"):
            self.manager.enqueue_reject_candidate(requested_by="test", candidate_id="weekly-404")


if __name__ == "__main__":
    unittest.main()
