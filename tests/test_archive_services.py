import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

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


if __name__ == "__main__":
    unittest.main()
