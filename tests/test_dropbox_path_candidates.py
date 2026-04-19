import sys
import shutil
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot import archive as archive_mod
from bot.archive import _dropbox_latest_remote_candidates


class DropboxPathCandidateTests(unittest.TestCase):
    def setUp(self):
        tmp_root = ROOT / ".tmp_testdata"
        tmp_root.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tmp_root / "archive_upload_skip"
        if self.tmpdir.exists():
            shutil.rmtree(self.tmpdir, ignore_errors=True)
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def test_includes_label_subfolder_fallback_when_root_is_slash(self):
        latest_filenames = {
            "latest_trades_csv_gz": "EDEC-BOT_latest_trades.csv.gz",
        }
        candidates = _dropbox_latest_remote_candidates("/", latest_filenames, label="EDEC-BOT")
        paths = candidates["latest_trades_csv_gz"]
        self.assertIn("/latest/EDEC-BOT_latest_trades.csv.gz", paths)
        self.assertIn("/EDEC-BOT/latest/EDEC-BOT_latest_trades.csv.gz", paths)

    def test_daily_archive_skips_excel_dropbox_upload_when_excel_export_fails(self):
        recent_path = self.tmpdir / "daily_recent_trades.csv.gz"
        recent_signals_path = self.tmpdir / "daily_recent_signals.csv.gz"
        recent_path.write_bytes(b"trades")
        recent_signals_path.write_bytes(b"signals")

        upload_calls: list[tuple[str, str]] = []

        def _fake_upload(local_path: str, remote_path: str, _dropbox_auth):
            upload_calls.append((Path(local_path).name, remote_path))
            return {"ok": True, "status": 200, "path": remote_path}

        with (
            mock.patch.object(archive_mod, "_utc_now", return_value=datetime(2026, 4, 19, tzinfo=timezone.utc)),
            mock.patch.object(archive_mod, "_build_dropbox_auth", return_value={"access_token": "token"}),
            mock.patch.object(archive_mod, "export_last_24h_excel", side_effect=RuntimeError("xlsx boom")),
            mock.patch.object(
                archive_mod,
                "export_recent_trades_csv_gz",
                return_value=(str(recent_path), 1, 1, 1),
            ),
            mock.patch.object(
                archive_mod,
                "export_recent_signals_csv_gz",
                return_value=(str(recent_signals_path), 1, 1, 1),
            ),
            mock.patch.object(archive_mod, "_latest_run_metadata", return_value={}),
            mock.patch.object(archive_mod, "_dropbox_upload_file", side_effect=_fake_upload),
        ):
            result = archive_mod.run_daily_archive(
                db_path="ignored.db",
                output_dir=str(self.tmpdir),
                label="EDEC-BOT",
                dropbox_token="token",
            )

        uploaded_names = [name for name, _remote in upload_calls]
        self.assertNotIn("EDEC-BOT_latest_last24h.xlsx", uploaded_names)
        self.assertEqual(result["dropbox_uploads"]["daily_last24h_xlsx"]["status"], "skipped")
        self.assertEqual(result["dropbox_uploads"]["latest_last24h_xlsx"]["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
