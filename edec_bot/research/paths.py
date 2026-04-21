"""Filesystem helpers for the research subsystem."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
RESEARCH_ROOT = DATA_ROOT / "research"
PARQUET_ROOT = RESEARCH_ROOT / "parquet"
WAREHOUSE_PATH = RESEARCH_ROOT / "warehouse.duckdb"
DEFAULT_POLICY_PATH = RESEARCH_ROOT / "runtime_policy.json"
DEFAULT_REPORT_JSON_PATH = RESEARCH_ROOT / "research_report.json"
DEFAULT_REPORT_MD_PATH = RESEARCH_ROOT / "research_report.md"
LOCAL_TRACKER_DB = DATA_ROOT / "decisions.db"


def resolve_repo_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def ensure_research_dirs() -> None:
    for path in (DATA_ROOT, RESEARCH_ROOT, PARQUET_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def discover_session_export_roots() -> list[Path]:
    candidates = [
        DATA_ROOT / "github_exports",
        REPO_ROOT / ".tmp_edec_data_repo" / "session_exports",
        REPO_ROOT / "edec_bot" / "data" / "github_exports",
    ]
    return [path for path in candidates if path.exists()]


def discover_session_export_files() -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for root in discover_session_export_roots():
        for pattern in ("*_session_trades.csv", "*_session_trades.csv.gz"):
            for path in sorted(root.rglob(pattern)):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                files.append(resolved)
    return files
