import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from research import config_apply


class ConfigApplyTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / f"config_apply_{uuid4().hex}"
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))
        self.config_path = self.tmpdir / "active_config.yaml"
        shutil.copyfile(ROOT / "edec_bot" / "config_phase_a_single.yaml", self.config_path)
        self.history_root = self.tmpdir / "history"
        self.last_receipt_path = self.tmpdir / "last_apply_receipt.json"
        self.restart_request_path = self.tmpdir / "restart_request.json"

    def _patch_apply_paths(self):
        return mock.patch.multiple(
            config_apply,
            CONFIG_HISTORY_ROOT=self.history_root,
            LAST_CONFIG_APPLY_RECEIPT_PATH=self.last_receipt_path,
            CODEX_RESTART_REQUEST_PATH=self.restart_request_path,
        )

    def test_apply_reviewed_patch_preserves_unrelated_settings(self):
        before = config_apply._load_yaml(self.config_path)
        with self._patch_apply_paths():
            result = config_apply.apply_reviewed_patch(
                changes=[
                    {
                        "path": "single_leg.entry_max",
                        "current": before["single_leg"]["entry_max"],
                        "recommended": 0.61,
                        "evidence": "Reviewed exploration pass wants a tighter entry ceiling.",
                    }
                ],
                config_path=self.config_path,
                action="apply_reviewed_config",
                summary="Apply reviewed entry ceiling tweak.",
                source_type="manual_review",
                source_ref="weekly-review",
                approval_id="approval-1",
                requested_by="test",
            )
        after = config_apply._load_yaml(self.config_path)

        self.assertTrue(result["ok"])
        self.assertEqual(after["single_leg"]["entry_max"], 0.61)
        self.assertEqual(after["execution"]["order_size_usd"], before["execution"]["order_size_usd"])
        self.assertTrue(self.restart_request_path.exists())
        self.assertTrue(self.last_receipt_path.exists())
        self.assertTrue(Path(result["receipt_path"]).exists())

    def test_apply_reviewed_patch_detects_conflict(self):
        before = config_apply._load_yaml(self.config_path)
        with self._patch_apply_paths():
            with self.assertRaises(config_apply.ConfigConflictError):
                config_apply.apply_reviewed_patch(
                    changes=[
                        {
                            "path": "single_leg.entry_max",
                            "current": before["single_leg"]["entry_max"] + 0.05,
                            "recommended": 0.61,
                            "evidence": "Reviewed patch was authored against an older config.",
                        }
                    ],
                    config_path=self.config_path,
                    action="apply_reviewed_config",
                    summary="Apply reviewed entry ceiling tweak.",
                    source_type="manual_review",
                    source_ref="weekly-review",
                    approval_id="approval-1",
                    requested_by="test",
                )

    def test_reset_baseline_and_rollback_restore_previous_config(self):
        original = config_apply._load_yaml(self.config_path)
        tightened = config_apply._deep_copy(original)
        tightened["single_leg"]["entry_min"] = 0.57
        tightened["single_leg"]["min_velocity_30s"] = 0.16
        tightened["lead_lag"]["min_entry"] = 0.58
        tightened["lead_lag"]["min_velocity_30s"] = 0.14
        self.config_path.write_text(config_apply._dump_yaml(tightened), encoding="utf-8")

        with self._patch_apply_paths():
            baseline = config_apply.apply_loose_paper_baseline(config_path=self.config_path, requested_by="test")
            baseline_config = config_apply._load_yaml(self.config_path)
            rollback = config_apply.rollback_last_config_apply(
                config_path=self.config_path,
                receipt_path=self.last_receipt_path,
                requested_by="test",
            )
        rolled_back = config_apply._load_yaml(self.config_path)

        self.assertTrue(baseline["ok"])
        self.assertEqual(baseline_config["single_leg"]["entry_min"], 0.52)
        self.assertEqual(baseline_config["single_leg"]["min_velocity_30s"], 0.10)
        self.assertEqual(baseline_config["lead_lag"]["min_entry"], 0.52)
        self.assertEqual(baseline_config["lead_lag"]["min_velocity_30s"], 0.10)
        self.assertTrue(rollback["ok"])
        self.assertEqual(rolled_back["single_leg"]["entry_min"], tightened["single_leg"]["entry_min"])
        self.assertEqual(rolled_back["lead_lag"]["min_entry"], tightened["lead_lag"]["min_entry"])

    def test_publish_reviewed_config_writes_approval_artifacts(self):
        reviewed_config = config_apply._load_yaml(self.config_path)
        reviewed_config["single_leg"]["entry_max"] = 0.61
        reviewed_path = self.tmpdir / "reviewed_config.yaml"
        reviewed_path.write_text(config_apply._dump_yaml(reviewed_config), encoding="utf-8")
        data_repo_root = self.tmpdir / "edec-bot-data"
        live_snapshot = data_repo_root / "research_exports" / "latest" / "config"
        live_snapshot.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.config_path, live_snapshot / "active_config.yaml")

        result = config_apply.publish_reviewed_config(
            approved_config_path=reviewed_path,
            data_repo_root=data_repo_root,
            source_type="manual_review",
            source_ref="weekly-review",
            summary="Publish reviewed config patch.",
            apply_mode="manual",
        )

        self.assertTrue(result["ok"])
        manifest = json.loads((data_repo_root / "research_exports" / "approved" / "approved_manifest.json").read_text(encoding="utf-8"))
        patch = json.loads((data_repo_root / "research_exports" / "approved" / "approved_patch.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["source_type"], "manual_review")
        self.assertEqual(patch[0]["path"], "single_leg.entry_max")
        self.assertEqual(patch[0]["recommended"], 0.61)


if __name__ == "__main__":
    unittest.main()
