"""Pull latest EDEC archive files from Dropbox into local repo storage."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from bot.archive import sync_dropbox_latest_to_local


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync stable latest archive files from Dropbox to local repo folder."
    )
    parser.add_argument("--dropbox-token", default=os.getenv("EDEC_DROPBOX_TOKEN"))
    parser.add_argument("--dropbox-root", default=os.getenv("EDEC_DROPBOX_ROOT", "/EDEC-BOT"))
    default_output_dir = str(Path(__file__).resolve().parent / "dropbox_sync")
    parser.add_argument("--output-dir", default=os.getenv("EDEC_REPO_SYNC_DIR", default_output_dir))
    parser.add_argument("--label", default=os.getenv("EDEC_ARCHIVE_LABEL", "EDEC-BOT"))
    parser.add_argument(
        "--no-expand-trades-csv",
        action="store_true",
        help="Keep only .csv.gz (do not also create decompressed .csv)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.dropbox_token:
        raise SystemExit("Missing Dropbox token. Set --dropbox-token or EDEC_DROPBOX_TOKEN.")

    result = sync_dropbox_latest_to_local(
        dropbox_token=args.dropbox_token,
        dropbox_root=args.dropbox_root,
        output_dir=args.output_dir,
        label=args.label,
        expand_trades_csv=not args.no_expand_trades_csv,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
