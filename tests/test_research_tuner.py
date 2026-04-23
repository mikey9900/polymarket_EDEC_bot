import json
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.tracker import DecisionTracker
from research.tuner import TuningError
from research.tuner import build_weekly_ai_context
from research.tuner import build_weekly_review_bundle
from research.tuner import load_tuner_state
from research.tuner import promote_tuning_candidate
from research.tuner import propose_tuning
from research.tuner import propose_weekly_ai_tuning
from research.tuner import reject_tuning_candidate


class _FakeResponsesClient:
    def __init__(self, payload: dict):
        self.payload = payload

    def create(self, **kwargs):
        return SimpleNamespace(
            id="resp-test",
            model=kwargs.get("model", "gpt-5.4-mini"),
            output_text=json.dumps(self.payload),
            usage={"input_tokens": 123, "output_tokens": 45},
            output=[],
        )


class _FakeOpenAIClient:
    def __init__(self, payload: dict):
        self.responses = _FakeResponsesClient(payload)


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
        self.config_path.write_text(
            (ROOT / "edec_bot" / "config_phase_a_single.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        self.tuner_state_path = self.tmpdir / "tuner_state.json"
        self.report_json_path = self.tmpdir / "tuner_report.json"
        self.report_md_path = self.tmpdir / "tuner_report.md"
        self.patch_path = self.tmpdir / "tuner_active_patch.diff"
        self.weekly_context_path = self.tmpdir / "weekly_ai_context.json"
        self.weekly_report_json_path = self.tmpdir / "weekly_ai_tuner_report.json"
        self.weekly_report_md_path = self.tmpdir / "weekly_ai_tuner_report.md"
        self.weekly_prompt_bundle_path = self.tmpdir / "weekly_ai_prompt_bundle.json"
        self.weekly_response_path = self.tmpdir / "weekly_ai_response.json"
        self.weekly_patch_path = self.tmpdir / "weekly_ai_patch.diff"
        self.weekly_review_bundle_json = self.tmpdir / "weekly_review_bundle.json"
        self.weekly_review_bundle_md = self.tmpdir / "weekly_review_bundle.md"
        self.weekly_desktop_prompt = self.tmpdir / "weekly_desktop_prompt.txt"
        self.research_report_json_path = self.tmpdir / "research_report.json"
        self.research_report_json_path.write_text(
            json.dumps(
                {
                    "generated_at": "2026-04-21T18:00:00+00:00",
                    "policy": {"cluster_count": 4, "outcome_count": 18},
                    "cluster_winners": [{"cluster_id": "btc_single_a", "sample_size": 8, "avg_pnl": 1.2}],
                    "cluster_losers": [{"cluster_id": "sol_lead_b", "sample_size": 6, "avg_pnl": -0.7}],
                    "by_coin": [{"name": "btc", "sample_size": 12, "total_pnl": 3.0, "paper_blocked_clusters": 1}],
                    "by_strategy": [{"name": "single_leg", "sample_size": 12, "total_pnl": 3.0, "paper_blocked_clusters": 1}],
                    "fill_flow_5m_1d": [{"coin": "btc", "fill_count": 2, "usd_volume": 5.5, "avg_price": 0.58}],
                    "trader_concentration_5m_1d": [{"coin": "btc", "top_trader": "0xabc", "top_trader_share_pct": 44.0, "top_3_share_pct": 77.0, "unique_trader_count": 5}],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.candidates_root = self.tmpdir / "config_candidates"
        self.candidates_root.mkdir(parents=True, exist_ok=True)
        self.tracker_db_path = self.tmpdir / "decisions.db"

    def test_propose_tuning_builds_candidate_and_reports(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            result = propose_tuning(
                config_path=self.config_path,
                tracker_db_path=self.tmpdir / "missing_decisions.db",
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
                research_report_json_path=self.research_report_json_path,
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
        self.assertEqual(report_payload["research_rollups"]["cluster_count"], 4)
        self.assertIn("Warehouse", self.report_md_path.read_text(encoding="utf-8"))

    def test_build_weekly_ai_context_compacts_exports_without_paths(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            propose_tuning(
                config_path=self.config_path,
                tracker_db_path=self.tmpdir / "missing_decisions.db",
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )
            result = build_weekly_ai_context(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                context_path=self.weekly_context_path,
                report_json_path=self.research_report_json_path,
                window_days=7,
            )

        self.assertTrue(result["ok"])
        payload = json.loads(self.weekly_context_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["config"]["name"], "config_phase_a_single.yaml")
        self.assertEqual(payload["daily_local_candidate"]["status"], "ready")
        self.assertTrue(payload["daily_snapshots"])
        self.assertEqual(payload["raw_export_refs"][0]["export_id"], self.export_root.name)
        self.assertNotIn(str(self.tmpdir), json.dumps(payload))
        self.assertNotIn("session_trades.csv", json.dumps(payload))

    def test_propose_weekly_ai_tuning_builds_candidate_and_becomes_primary(self):
        weekly_payload = {
            "summary": "Tighten single-leg velocity after a stronger recent-flow regime.",
            "recommended_changes": [
                {
                    "path": "single_leg.min_velocity_30s",
                    "current": 0.12,
                    "recommended": 0.18,
                    "evidence": "The recent daily snapshots improved once velocity cleared 0.15.",
                    "confidence": 0.74,
                }
            ],
            "risks": ["Trade count may fall if flow softens."],
            "followups": ["Watch BTC fill flow and crowded-trader share next week."],
            "raw_refs_used": [self.export_root.name],
        }
        fake_openai_module = SimpleNamespace(OpenAI=lambda api_key=None: _FakeOpenAIClient(weekly_payload))

        with (
            mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]),
            mock.patch("research.tuner.require_openai", return_value=fake_openai_module),
        ):
            propose_tuning(
                config_path=self.config_path,
                tracker_db_path=self.tmpdir / "missing_decisions.db",
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )
            result = propose_weekly_ai_tuning(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                context_path=self.weekly_context_path,
                report_json_path=self.weekly_report_json_path,
                report_md_path=self.weekly_report_md_path,
                prompt_bundle_path=self.weekly_prompt_bundle_path,
                response_path=self.weekly_response_path,
                patch_path=self.weekly_patch_path,
                candidates_root=self.candidates_root,
                api_key="test-key",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["candidate_source"], "weekly_ai")
        self.assertEqual(result["candidate_status"], "ready")
        self.assertTrue(Path(result["candidate_config_path"]).exists())
        self.assertTrue(self.weekly_prompt_bundle_path.exists())
        self.assertTrue(self.weekly_response_path.exists())
        self.assertTrue(self.weekly_patch_path.exists())

        state = load_tuner_state(self.tuner_state_path)
        self.assertEqual(state["primary_candidate_source"], "weekly_ai")
        self.assertEqual(state["weekly_ai_candidate"]["status"], "ready")
        self.assertEqual(state["latest_candidate_source"], "weekly_ai")

    def test_propose_weekly_ai_tuning_blocks_without_api_key(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            result = propose_weekly_ai_tuning(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                context_path=self.weekly_context_path,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "blocked")
        self.assertIn("OPENAI_API_KEY", result["message"])

    def test_build_weekly_review_bundle_writes_desktop_bundle(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            propose_tuning(
                config_path=self.config_path,
                tracker_db_path=self.tmpdir / "missing_decisions.db",
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )
            result = build_weekly_review_bundle(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                context_path=self.weekly_context_path,
                bundle_json_path=self.weekly_review_bundle_json,
                bundle_md_path=self.weekly_review_bundle_md,
                prompt_path=self.weekly_desktop_prompt,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(self.weekly_review_bundle_json.exists())
        self.assertTrue(self.weekly_review_bundle_md.exists())
        self.assertTrue(self.weekly_desktop_prompt.exists())
        payload = json.loads(self.weekly_review_bundle_json.read_text(encoding="utf-8"))
        self.assertEqual(payload["review_mode"], "desktop_manual")
        self.assertIn("desktop_review_prompt", payload)
        self.assertIn("desktop_weekly_prompt", payload)
        self.assertEqual(payload["artifacts"]["desktop_prompt"], self.weekly_desktop_prompt.name)
        self.assertNotIn(str(self.tmpdir), json.dumps(payload))
        prompt_text = self.weekly_desktop_prompt.read_text(encoding="utf-8").strip()
        self.assertIn("Open these files in Codex desktop:", prompt_text)
        self.assertIn(self.weekly_review_bundle_md.name, prompt_text)
        self.assertIn(self.weekly_context_path.name, prompt_text)
        self.assertIn(self.config_path.name, prompt_text)
        state = load_tuner_state(self.tuner_state_path)
        self.assertEqual(state["weekly_review_bundle"]["status"], "ready")
        self.assertEqual(
            Path(state["weekly_review_bundle"]["paths"]["desktop_prompt"]).name,
            self.weekly_desktop_prompt.name,
        )

    def test_promote_candidate_uses_primary_weekly_ai_candidate(self):
        weekly_payload = {
            "summary": "Weekly AI candidate ready.",
            "recommended_changes": [
                {
                    "path": "single_leg.min_velocity_30s",
                    "current": 0.12,
                    "recommended": 0.18,
                    "evidence": "Recent snapshots were better at higher velocity.",
                    "confidence": 0.81,
                }
            ],
            "risks": [],
            "followups": ["Confirm signal count stays acceptable."],
            "raw_refs_used": [self.export_root.name],
        }
        fake_openai_module = SimpleNamespace(OpenAI=lambda api_key=None: _FakeOpenAIClient(weekly_payload))

        with (
            mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]),
            mock.patch("research.tuner.require_openai", return_value=fake_openai_module),
        ):
            propose_tuning(
                config_path=self.config_path,
                tracker_db_path=self.tmpdir / "missing_decisions.db",
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )
            weekly = propose_weekly_ai_tuning(
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
                context_path=self.weekly_context_path,
                report_json_path=self.weekly_report_json_path,
                report_md_path=self.weekly_report_md_path,
                prompt_bundle_path=self.weekly_prompt_bundle_path,
                response_path=self.weekly_response_path,
                patch_path=self.weekly_patch_path,
                candidates_root=self.candidates_root,
                api_key="test-key",
            )

        version_path = self.tmpdir / "version.py"
        addon_config_path = self.tmpdir / "config.json"
        version_path.write_text('__version__ = "5.2.9"\n', encoding="utf-8")
        addon_config_path.write_text(json.dumps({"version": "5.2.9"}, indent=2) + "\n", encoding="utf-8")

        result = promote_tuning_candidate(
            candidate_id=weekly["candidate_id"],
            config_path=self.config_path,
            tuner_state_path=self.tuner_state_path,
            version_path=version_path,
            addon_config_path=addon_config_path,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["candidate_source"], "weekly_ai")
        self.assertEqual(result["version"], "5.2.11")
        state = load_tuner_state(self.tuner_state_path)
        self.assertEqual(state["weekly_ai_candidate"]["status"], "promoted")
        self.assertEqual(state["daily_local_candidate"]["status"], "rejected")

    def test_reject_candidate_blocks_later_promotion(self):
        with mock.patch("research.tuner.discover_session_export_roots", return_value=[self.tmpdir / "github_exports"]):
            proposal = propose_tuning(
                config_path=self.config_path,
                tracker_db_path=self.tmpdir / "missing_decisions.db",
                tuner_state_path=self.tuner_state_path,
                report_json_path=self.report_json_path,
                report_md_path=self.report_md_path,
                patch_path=self.patch_path,
                candidates_root=self.candidates_root,
            )

        reject = reject_tuning_candidate(candidate_id=proposal["candidate_id"], tuner_state_path=self.tuner_state_path)
        self.assertTrue(reject["ok"])
        with self.assertRaises(TuningError):
            promote_tuning_candidate(
                candidate_id=proposal["candidate_id"],
                config_path=self.config_path,
                tuner_state_path=self.tuner_state_path,
            )

    def test_propose_tuning_prefers_tracker_db_when_shared_db_has_recent_outcomes(self):
        tracker = DecisionTracker(str(self.tracker_db_path))
        try:
            tracker.conn.execute(
                "INSERT OR REPLACE INTO paper_capital (id, total_capital, current_balance, reset_at) VALUES (1, 5000.0, 5000.0, ?)",
                ("2026-04-21T00:00:00+00:00",),
            )
            tracker.conn.execute(
                """
                INSERT INTO decisions (
                    timestamp, market_slug, coin, strategy_type, market_end_time,
                    action, coin_velocity_30s, coin_velocity_60s, filter_failed, entry_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-21T17:00:00+00:00",
                    "btc-updown-5m-1",
                    "btc",
                    "single_leg",
                    "2026-04-21T17:05:00+00:00",
                    "TRADE",
                    0.16,
                    0.18,
                    "depth_check",
                    0.57,
                ),
            )
            decision_id = tracker.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            tracker.conn.execute(
                """
                INSERT INTO paper_trades (
                    decision_id, timestamp, market_slug, coin, strategy_type, side,
                    entry_price, target_price, shares, cost, fee_total, status, pnl,
                    exit_reason, depth_ratio, max_bid_seen, mae
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    "2026-04-21T17:01:00+00:00",
                    "btc-updown-5m-1",
                    "btc",
                    "single_leg",
                    "up",
                    0.57,
                    0.64,
                    20.0,
                    11.4,
                    0.1,
                    "closed_win",
                    1.2,
                    "profit_target",
                    1.4,
                    0.84,
                    -0.03,
                ),
            )
            tracker.conn.commit()
        finally:
            tracker.close()

        result = propose_tuning(
            config_path=self.config_path,
            tracker_db_path=self.tracker_db_path,
            tuner_state_path=self.tuner_state_path,
            report_json_path=self.report_json_path,
            report_md_path=self.report_md_path,
            patch_path=self.patch_path,
            candidates_root=self.candidates_root,
            research_report_json_path=self.research_report_json_path,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["input_source"], "tracker_db")
        self.assertEqual(result["export_id"], f"tracker_db:{self.tracker_db_path.name}")
        payload = json.loads(self.report_json_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["inputs"]["source"], "tracker_db")
        self.assertEqual(payload["inputs"]["tracker_db"], str(self.tracker_db_path))

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
