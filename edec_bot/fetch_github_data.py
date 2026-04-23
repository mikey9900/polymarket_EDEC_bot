"""Fetch the latest session export CSVs from the GitHub data repo.

Reads credentials from environment variables or .env file.
Downloads to data/github_exports/ by default.

Usage:
    .\scripts\venv_python.cmd edec_bot/fetch_github_data.py
    .\scripts\venv_python.cmd edec_bot/fetch_github_data.py --limit 5
    .\scripts\venv_python.cmd edec_bot/fetch_github_data.py --output-dir data/github_exports
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Allow running from repo root without edec_bot on sys.path
_here = Path(__file__).resolve().parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from bot.archive import fetch_github_session_exports


def _load_env(env_path: Path) -> None:
    """Minimal .env loader — sets missing env vars from KEY=VALUE lines."""
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_ha_options(path: str = "/data/options.json") -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch latest session export CSVs from the GitHub data repo."
    )
    parser.add_argument("--limit", type=int, default=3,
                        help="Number of most-recent export folders to fetch (default: 3)")
    parser.add_argument("--output-dir", default="data/github_exports",
                        help="Local directory for downloaded files (default: data/github_exports)")
    parser.add_argument("--github-token", default=None)
    parser.add_argument("--github-repo", default=None)
    parser.add_argument("--github-branch", default=None)
    parser.add_argument("--github-export-path", default=None)
    parser.add_argument("--no-expand-csv", action="store_true",
                        help="Keep only .csv.gz, do not decompress to .csv")
    return parser.parse_args()


def main() -> int:
    # Load .env from the edec_bot directory if present
    _load_env(_here / ".env")
    _load_env(_here.parent / ".env")

    ha = _load_ha_options()
    args = _parse_args()

    github_token = (
        args.github_token
        or os.getenv("EDEC_GITHUB_TOKEN")
        or ha.get("github_token")
        or ""
    ).strip()
    github_repo = (
        args.github_repo
        or os.getenv("EDEC_GITHUB_REPO")
        or ha.get("github_repo")
        or ""
    ).strip()
    github_branch = (
        args.github_branch
        or os.getenv("EDEC_GITHUB_BRANCH")
        or ha.get("github_branch")
        or "main"
    ).strip()
    github_export_path = (
        args.github_export_path
        or os.getenv("EDEC_GITHUB_EXPORT_PATH")
        or ha.get("github_export_path")
        or "session_exports"
    ).strip()

    if not github_token:
        print("ERROR: GitHub token not found.")
        print("Set EDEC_GITHUB_TOKEN in your environment or .env file.")
        return 1
    if not github_repo:
        print("ERROR: GitHub repo not found.")
        print("Set EDEC_GITHUB_REPO in your environment or .env file (e.g. mikey9900/edec-data).")
        return 1

    print(f"Fetching last {args.limit} session export folder(s)")
    print(f"  Repo:        {github_repo} @ {github_branch}")
    print(f"  Export path: {github_export_path}/")
    print(f"  Output dir:  {args.output_dir}")
    print()

    result = fetch_github_session_exports(
        github_token=github_token,
        github_repo=github_repo,
        github_branch=github_branch,
        github_export_path=github_export_path,
        output_dir=args.output_dir,
        limit=args.limit,
        expand_csv=not args.no_expand_csv,
    )

    if not result.get("ok"):
        print(f"ERROR: {result.get('error', 'unknown')}")
        return 1

    folders = result.get("folders", [])
    if not folders:
        note = result.get("note", "")
        print(f"No export folders found. {note}")
        return 0

    print(f"Downloaded {result['fetched_count']} folder(s) to: {result['output_dir']}\n")
    for folder in folders:
        if "error" in folder:
            print(f"  [FAIL] {folder['folder']}: {folder['error']}")
            continue
        files = folder.get("files", [])
        csv_files = [f for f in files if f.endswith(".csv") and not f.endswith(".csv.gz")]
        gz_files = [f for f in files if f.endswith(".csv.gz")]
        errs = folder.get("errors", [])
        print(f"  {folder['folder']}/")
        print(f"    CSV files : {len(csv_files)}")
        print(f"    GZ files  : {len(gz_files)}")
        if errs:
            for e in errs:
                print(f"    WARNING   : {e}")
        for f in sorted(files):
            print(f"    {f}")
        print()

    print("Ready for analysis. CSV files are at:")
    for folder in folders:
        if "local_dir" in folder:
            for f in folder.get("files", []):
                if f.endswith(".csv") and not f.endswith(".csv.gz"):
                    print(f"  {folder['local_dir']}/{f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
