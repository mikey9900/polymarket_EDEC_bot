import json
import shutil
import sys
import unittest
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
