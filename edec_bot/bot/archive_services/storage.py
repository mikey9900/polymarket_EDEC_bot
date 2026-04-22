"""Archive external storage adapters."""

from __future__ import annotations

from bot.archive import (
    fetch_github_session_exports,
    get_or_upload_excel_link,
    sync_dropbox_latest_to_local,
)


class ArchiveStorageService:
    def __init__(
        self,
        *,
        dropbox_token,
        dropbox_refresh_token,
        dropbox_app_key,
        dropbox_app_secret,
        dropbox_root,
        repo_sync_dir,
        label,
        github_token,
        github_repo,
        github_branch,
        github_export_path,
        output_dir,
    ):
        self.dropbox_token = dropbox_token
        self.dropbox_refresh_token = dropbox_refresh_token
        self.dropbox_app_key = dropbox_app_key
        self.dropbox_app_secret = dropbox_app_secret
        self.dropbox_root = str(dropbox_root)
        self.repo_sync_dir = repo_sync_dir
        self.label = label
        self.github_token = github_token
        self.github_repo = github_repo
        self.github_branch = str(github_branch)
        self.github_export_path = str(github_export_path)
        self.output_dir = output_dir

    def sync_repo_latest(self) -> dict:
        if not self.dropbox_token and not self.dropbox_refresh_token:
            raise RuntimeError("Dropbox token or refresh-token auth is not configured")
        return sync_dropbox_latest_to_local(
            dropbox_token=self.dropbox_token,
            dropbox_refresh_token=self.dropbox_refresh_token,
            dropbox_app_key=self.dropbox_app_key,
            dropbox_app_secret=self.dropbox_app_secret,
            dropbox_root=self.dropbox_root,
            output_dir=self.repo_sync_dir,
            label=self.label,
            expand_trades_csv=True,
        )

    def fetch_github_exports(self, *, limit: int = 3) -> dict:
        if not self.github_token:
            raise RuntimeError("EDEC_GITHUB_TOKEN / github_token not configured")
        if not self.github_repo:
            raise RuntimeError("EDEC_GITHUB_REPO / github_repo not configured")
        return fetch_github_session_exports(
            github_token=self.github_token,
            github_repo=self.github_repo,
            github_branch=self.github_branch,
            github_export_path=self.github_export_path,
            output_dir="data/github_exports",
            limit=limit,
            expand_csv=True,
        )

    def excel_dropbox_link(self, local_path: str) -> tuple[str | None, str | None]:
        return get_or_upload_excel_link(
            local_path=local_path,
            output_dir=self.output_dir,
            label=self.label,
            dropbox_root=self.dropbox_root,
            dropbox_token=self.dropbox_token,
            dropbox_refresh_token=self.dropbox_refresh_token,
            dropbox_app_key=self.dropbox_app_key,
            dropbox_app_secret=self.dropbox_app_secret,
        )
