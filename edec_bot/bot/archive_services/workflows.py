"""Archive workflow services."""

from __future__ import annotations

from bot.archive import (
    archive_health_snapshot,
    latest_archive_paths,
    run_daily_archive,
    run_session_export,
)


class ArchiveWorkflowService:
    def __init__(
        self,
        *,
        db_path: str,
        output_dir: str,
        label: str,
        recent_limit: int,
        dropbox_token,
        dropbox_refresh_token,
        dropbox_app_key,
        dropbox_app_secret,
        dropbox_root,
        github_token,
        github_repo,
        github_branch,
        github_export_path,
    ):
        self.db_path = db_path
        self.output_dir = output_dir
        self.label = label
        self.recent_limit = int(recent_limit)
        self.dropbox_token = dropbox_token
        self.dropbox_refresh_token = dropbox_refresh_token
        self.dropbox_app_key = dropbox_app_key
        self.dropbox_app_secret = dropbox_app_secret
        self.dropbox_root = str(dropbox_root)
        self.github_token = github_token
        self.github_repo = github_repo
        self.github_branch = str(github_branch)
        self.github_export_path = str(github_export_path)

    def run_daily_archive(self) -> dict:
        return run_daily_archive(
            db_path=self.db_path,
            output_dir=self.output_dir,
            label=self.label,
            recent_limit=self.recent_limit,
            dropbox_token=self.dropbox_token,
            dropbox_refresh_token=self.dropbox_refresh_token,
            dropbox_app_key=self.dropbox_app_key,
            dropbox_app_secret=self.dropbox_app_secret,
            dropbox_root=self.dropbox_root,
        )

    def latest_paths(self) -> dict:
        return latest_archive_paths(output_dir=self.output_dir, label=self.label)

    def health_snapshot(self) -> dict:
        return archive_health_snapshot(
            output_dir=self.output_dir,
            label=self.label,
            dropbox_token=self.dropbox_token,
            dropbox_refresh_token=self.dropbox_refresh_token,
            dropbox_app_key=self.dropbox_app_key,
            dropbox_app_secret=self.dropbox_app_secret,
            dropbox_root=self.dropbox_root,
        )

    def run_session_export(self) -> dict:
        return run_session_export(
            db_path=self.db_path,
            output_dir=self.output_dir,
            label=self.label,
            dropbox_token=self.dropbox_token,
            dropbox_refresh_token=self.dropbox_refresh_token,
            dropbox_app_key=self.dropbox_app_key,
            dropbox_app_secret=self.dropbox_app_secret,
            dropbox_root=self.dropbox_root,
            github_token=self.github_token,
            github_repo=self.github_repo,
            github_branch=self.github_branch,
            github_export_path=self.github_export_path,
        )
