import sys
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot import archive as archive_mod
from bot.archive_services.storage import ArchiveStorageService
from bot.archive_services.workflows import ArchiveWorkflowService


class ArchiveServiceTests(unittest.TestCase):
    def test_storage_service_fetches_github_exports_with_configured_defaults(self):
        service = ArchiveStorageService(
            dropbox_token=None,
            dropbox_refresh_token=None,
            dropbox_app_key=None,
            dropbox_app_secret=None,
            dropbox_root="/",
            repo_sync_dir="dropbox_sync",
            label="EDEC-BOT",
            github_token="token",
            github_repo="owner/repo",
            github_branch="main",
            github_export_path="session_exports",
            output_dir="data/exports",
        )

        with mock.patch(
            "bot.archive_services.storage.fetch_github_session_exports",
            return_value={"ok": True, "folders": []},
        ) as mocked:
            result = service.fetch_github_exports(limit=5)

        self.assertTrue(result["ok"])
        mocked.assert_called_once()
        self.assertEqual(mocked.call_args.kwargs["limit"], 5)

    def test_workflow_service_latest_paths_delegates_with_label(self):
        service = ArchiveWorkflowService(
            db_path="data/decisions.db",
            output_dir="data/exports",
            label="EDEC-BOT",
            recent_limit=100,
            dropbox_token=None,
            dropbox_refresh_token=None,
            dropbox_app_key=None,
            dropbox_app_secret=None,
            dropbox_root="/",
            github_token=None,
            github_repo=None,
            github_branch="main",
            github_export_path="session_exports",
        )

        with mock.patch(
            "bot.archive_services.workflows.latest_archive_paths",
            return_value={"latest_excel": "data/exports/latest.xlsx"},
        ) as mocked:
            result = service.latest_paths()

        self.assertEqual(result["latest_excel"], "data/exports/latest.xlsx")
        mocked.assert_called_once_with(output_dir="data/exports", label="EDEC-BOT")

    def test_run_session_export_keeps_local_files_when_dropbox_config_is_invalid(self):
        with tempfile.TemporaryDirectory() as tmp_root_str:
            tmp_root = Path(tmp_root_str)
            db_path = tmp_root / "session_export_invalid_dropbox.db"
            output_dir = tmp_root / "session_export_invalid_dropbox_out"
            output_dir.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(db_path)
            try:
                conn.execute("CREATE TABLE paper_capital (id INTEGER PRIMARY KEY, total_capital REAL, current_balance REAL, reset_at TEXT)")
                conn.execute(
                    "INSERT INTO paper_capital (id, total_capital, current_balance, reset_at) VALUES (1, 100.0, 100.0, '2026-04-22T00:00:00+00:00')"
                )
                conn.commit()
            finally:
                conn.close()

            with (
                mock.patch.object(archive_mod, "export_session_trades_csv_gz", return_value=(str(output_dir / "trades.csv.gz"), 3, 1, 3)),
                mock.patch.object(archive_mod, "export_session_signals_csv_gz", return_value=(str(output_dir / "signals.csv.gz"), 8, 1, 8)),
                mock.patch.object(archive_mod, "_build_session_excel_export", return_value=str(output_dir / "session.xlsx")),
                mock.patch.object(archive_mod, "_build_dropbox_auth", side_effect=RuntimeError("Dropbox refresh token requires both dropbox_app_key and dropbox_app_secret.")),
            ):
                result = archive_mod.run_session_export(
                    db_path=str(db_path),
                    output_dir=str(output_dir),
                    label="EDEC-BOT",
                    dropbox_refresh_token="refresh",
                )

            self.assertEqual(result["trade_count"], 3)
            self.assertEqual(result["signal_count"], 8)
            self.assertIn("dropbox_app_key", result["dropbox_error"])
            self.assertTrue((Path(result["index_path"])).exists())


if __name__ == "__main__":
    unittest.main()
