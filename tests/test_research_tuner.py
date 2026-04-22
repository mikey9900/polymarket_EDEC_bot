import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from research.tuner import TuningError
from research.tuner import promote_tuning_candidate
from research.tuner import propose_tuning
from research.tuner import reject_tuning_candidate


class ResearchTunerTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"research_tuner_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

        self.export_root = self.tmpdir / "github_exports" / "2026-04-21_170000_EDEC-BOT_session_export"
        self.export_root.mkdir(parents=True, exist_ok=True)
        self.trades_csv = self.export_root / "2026-04-21_170000_EDEC-BOT_session_trades.csv"
        self.signals_csv = self.export_root / "2026-04-21_170000_EDEC-BOT_session_signals.csv"
        self._write_trades_csv()
        self._write_signals_csv()

        self.config_path = self.tmpdir / "config_phase_a_single.yaml"
        self.config_path.write_text((ROOT / "edec_bot" / "config_phase_a_single.yaml").read_text(encoding="utf-8"), encoding="utf-8")
        self.tuner_state_path = self.tmpdir / "tuner_state.json"
        self.report_json_path = self.tmpdir / "tuner_report.json"
        self.report_md_path = self.tmpdir / "tuner_report.md"
        self.patch_path = self.tmpdir / "tuner_active_patch.diff"
        self.candidates_root = self.tmpdir / "config_candidates"
        self.candidates_root.mkdir(parents=True, exist_ok=True)

    def test_propose_tuning_builds_candidate_and_reports(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            result = propose_tuning(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["candidate_status"], "ready")
        self.assertGreaterEqual(result["change_count"], 5)
        self.assertTrue(Path(result["candidate_config_path"]).exists())
        self.assertTrue(self.report_json_path.exists())
        self.assertTrue(self.report_md_path.exists())
        self.assertTrue(self.patch_path.exists())

        candidate = yaml.safe_load(Path(result["candidate_config_path"]).read_text(encoding="utf-8"))
        self.assertEqual(candidate["single_leg"]["min_velocity_30s"], 0.15)
        self.assertEqual(candidate["single_leg"]["entry_min"], 0.56)
        self.assertEqual(candidate["single_leg"]["entry_max"], 0.58)
        self.assertEqual(candidate["single_leg"]["high_confidence_bid"], 0.72)
        self.assertEqual(candidate["single_leg"]["loss_cut_pct"], 0.13)

        report_payload = json.loads(self.report_json_path.read_text(encoding="utf-8"))
        self.assertEqual(report_payload["candidate_status"], "ready")
        self.assertIn("depth_check", "\n".join(report_payload["advisories"]))

    def test_promote_candidate_updates_config_and_bumps_version(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            proposal = propose_tuning(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )

        version_path = self.tmpdir / "version.py"
        addon_config_path = self.tmpdir / "config.json"
        version_path.write_text('__version__ = "5.2.9"\n', encoding="utf-8")
        addon_config_path.write_text(json.dumps({"version": "5.2.9"}, indent=2) + "\n", encoding="utf-8")

        result = promote_tuning_candidate(
            candidate_id=proposal["candidate_id"],
            config_path=self.config_path,
            tuner_state_path=self.tuner_state_path,
            version_path=version_path,
            addon_config_path=addon_config_path,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["version"], "5.2.11")
        self.assertIn("0.15", self.config_path.read_text(encoding="utf-8"))
        self.assertIn('5.2.11', version_path.read_text(encoding="utf-8"))
        self.assertEqual(json.loads(addon_config_path.read_text(encoding="utf-8"))["version"], "5.2.11")

    def test_reject_candidate_blocks_later_promotion(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            proposal = propose_tuning(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )

        reject = reject_tuning_candidate(candidate_id=proposal["candidate_id"], tuner_state_path=self.tuner_state_path)
        self.assertTrue(reject["ok"])
        with self.assertRaises(TuningError):
            promote_tuning_candidate(candidate_id=proposal["candidate_id"], config_path=self.config_path, tuner_state_path=self.tuner_state_path)

    def _write_trades_csv(self) -> None:
        rows = [
            "id,ts,c,st,sd,ep,tp,eb,ea,es,cs,fee,status,xp,xb,er,tx,pnl,v30,v60,eds,ods,drt,maxb,minb,mfe,mae,tfp,sc,hc,sx,fp,ff,te,sg",
        ]
        for index in range(5):
            rows.append(
                f"{index+1},2026-04-21T17:00:0{index}Z,btc,single_leg,up,0.55,0.60,0.54,0.55,0.01,10,0.1,closed_loss,0.49,0.49,loss_cut,120,-1.0,0.09,0.12,10,8,2.3,0.78,0.45,0.05,-0.06,12,0,1,0,vel_ok,,180,5.0"
            )
        for index in range(5):
            rows.append(
                f"{index+6},2026-04-21T17:01:0{index}Z,btc,single_leg,up,0.57,0.64,0.56,0.57,0.01,10,0.1,closed_win,0.64,0.64,profit_target,140,1.2,0.16,0.18,15,11,1.4,0.84,0.55,0.12,-0.03,8,1,1,0,vel_ok,,185,7.5"
            )
        self.trades_csv.write_text("\n".join(rows) + "\n", encoding="utf-8")

    def _write_signals_csv(self) -> None:
        rows = [
            "id,ts,c,st,act,sup,ep,v30,v60,te,eds,ods,sg,fp,ff,why",
        ]
        for index in range(10):
            failed = "depth_check" if index < 4 else ""
            rows.append(
                f"{index+1},2026-04-21T17:00:{index:02d}Z,btc,single_leg,TRADE,,0.57,0.16,0.18,180,12,10,6.0,vel_ok,{failed},rule"
            )
        self.signals_csv.write_text("\n".join(rows) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
