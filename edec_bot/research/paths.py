"""Filesystem helpers for the research subsystem."""

from __future__ import annotations

import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
RESEARCH_ROOT = DATA_ROOT / "research"
PARQUET_ROOT = RESEARCH_ROOT / "parquet"
WAREHOUSE_PATH = RESEARCH_ROOT / "warehouse.duckdb"
DEFAULT_POLICY_PATH = RESEARCH_ROOT / "runtime_policy.json"
DEFAULT_REPORT_JSON_PATH = RESEARCH_ROOT / "research_report.json"
DEFAULT_REPORT_MD_PATH = RESEARCH_ROOT / "research_report.md"
TUNER_STATE_PATH = RESEARCH_ROOT / "tuner_state.json"
TUNER_REPORT_JSON_PATH = RESEARCH_ROOT / "tuner_report.json"
TUNER_REPORT_MD_PATH = RESEARCH_ROOT / "tuner_report.md"
TUNER_ACTIVE_PATCH_PATH = RESEARCH_ROOT / "tuner_active_patch.diff"
WEEKLY_AI_CONTEXT_PATH = RESEARCH_ROOT / "weekly_ai_context.json"
WEEKLY_REVIEW_BUNDLE_JSON_PATH = RESEARCH_ROOT / "weekly_review_bundle.json"
WEEKLY_REVIEW_BUNDLE_MD_PATH = RESEARCH_ROOT / "weekly_review_bundle.md"
WEEKLY_DESKTOP_PROMPT_PATH = RESEARCH_ROOT / "weekly_desktop_prompt.txt"
WEEKLY_AI_REPORT_JSON_PATH = RESEARCH_ROOT / "weekly_ai_tuner_report.json"
WEEKLY_AI_REPORT_MD_PATH = RESEARCH_ROOT / "weekly_ai_tuner_report.md"
WEEKLY_AI_PROMPT_BUNDLE_PATH = RESEARCH_ROOT / "weekly_ai_prompt_bundle.json"
WEEKLY_AI_RESPONSE_PATH = RESEARCH_ROOT / "weekly_ai_response.json"
WEEKLY_AI_PATCH_PATH = RESEARCH_ROOT / "weekly_ai_patch.diff"
LOCAL_TRACKER_DB = DATA_ROOT / "decisions.db"
DEFAULT_CONFIG_PATH = REPO_ROOT / "edec_bot" / "config_phase_a_single.yaml"
CONFIG_CANDIDATES_ROOT = REPO_ROOT / "edec_bot" / "config_candidates"
SHARED_DATA_ROOT = (
    Path(os.getenv("EDEC_SHARED_DATA_ROOT", str(DATA_ROOT)))
    if os.getenv("EDEC_SHARED_DATA_ROOT")
    else DATA_ROOT
)
if not SHARED_DATA_ROOT.is_absolute():
    SHARED_DATA_ROOT = REPO_ROOT / SHARED_DATA_ROOT
CODEX_ROOT = SHARED_DATA_ROOT / "codex"
CODEX_QUEUE_ROOT = CODEX_ROOT / "queue"
CODEX_RUNS_ROOT = CODEX_ROOT / "runs"
CODEX_STATE_PATH = CODEX_ROOT / "state.json"
CODEX_LATEST_PATH = CODEX_ROOT / "latest.json"
CODEX_LOCK_PATH = CODEX_ROOT / "runner.lock"


def resolve_repo_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def ensure_research_dirs() -> None:
    for path in (DATA_ROOT, RESEARCH_ROOT, PARQUET_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def ensure_codex_dirs() -> None:
    for path in (SHARED_DATA_ROOT, CODEX_ROOT, CODEX_QUEUE_ROOT, CODEX_RUNS_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def ensure_tuner_dirs() -> None:
    for path in (RESEARCH_ROOT, CONFIG_CANDIDATES_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def discover_session_export_roots() -> list[Path]:
    candidates = [
        SHARED_DATA_ROOT / "exports",
        DATA_ROOT / "exports",
        REPO_ROOT / "edec_bot" / "data" / "exports",
        SHARED_DATA_ROOT / "github_exports",
        DATA_ROOT / "github_exports",
        REPO_ROOT / ".tmp_edec_data_repo" / "session_exports",
        REPO_ROOT / "edec_bot" / "data" / "github_exports",
    ]
    roots: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        roots.append(resolved)
    return roots


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
