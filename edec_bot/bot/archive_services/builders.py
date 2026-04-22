"""Archive export builders."""

from __future__ import annotations

from bot.export import export_recent_to_excel, export_to_excel


class ArchiveBuilderService:
    def __init__(self, *, db_path: str, data_dir: str):
        self.db_path = db_path
        self.data_dir = data_dir

    def export_excel(self, *, today_only: bool = False) -> str:
        return export_to_excel(self.db_path, self.data_dir, today_only)

    def export_recent_excel(self, *, limit: int = 100) -> str:
        return export_recent_to_excel(self.db_path, self.data_dir, limit=limit)
