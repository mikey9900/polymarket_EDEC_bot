"""CLI entrypoints for the research subsystem."""

from __future__ import annotations

import argparse
import json

from .artifacts import build_artifacts
from .paths import DEFAULT_POLICY_PATH, LOCAL_TRACKER_DB, WAREHOUSE_PATH
from .sources import GammaMarketSource, GoldskyFillSource
from .sync import sync_fills, sync_markets, sync_recent_5m_fills
from .warehouse import ResearchWarehouse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m edec_bot.research")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_markets_parser = subparsers.add_parser("sync-markets", help="Sync Gamma market metadata into the warehouse")
    sync_markets_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    sync_markets_parser.add_argument("--batch-size", type=int, default=500)
    sync_markets_parser.add_argument("--max-batches", type=int, default=None)

    sync_fills_parser = subparsers.add_parser("sync-fills", help="Sync Goldsky fills into the warehouse")
    sync_fills_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    sync_fills_parser.add_argument("--batch-size", type=int, default=1000)
    sync_fills_parser.add_argument("--max-batches", type=int, default=None)

    sync_recent_fills_parser = subparsers.add_parser(
        "sync-recent-5m-fills",
        help="Sync recent fills only for recent 5-minute market token ids",
    )
    sync_recent_fills_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    sync_recent_fills_parser.add_argument("--lookback-hours", type=int, default=24)
    sync_recent_fills_parser.add_argument("--batch-size", type=int, default=1000)
    sync_recent_fills_parser.add_argument("--asset-chunk-size", type=int, default=50)
    sync_recent_fills_parser.add_argument("--bucket-minutes", type=int, default=60)
    sync_recent_fills_parser.add_argument("--bucket-buffer-seconds", type=int, default=900)
    sync_recent_fills_parser.add_argument("--max-batches-per-chunk", type=int, default=None)

    build_artifacts_parser = subparsers.add_parser("build-artifacts", help="Build runtime policy and reports")
    build_artifacts_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    build_artifacts_parser.add_argument("--tracker-db", default=str(LOCAL_TRACKER_DB))
    build_artifacts_parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    build_artifacts_parser.add_argument("--lookback-days", type=int, default=30)

    report_parser = subparsers.add_parser("report", help="Refresh the research report outputs")
    report_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    report_parser.add_argument("--tracker-db", default=str(LOCAL_TRACKER_DB))
    report_parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    report_parser.add_argument("--lookback-days", type=int, default=30)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "sync-markets":
        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GammaMarketSource()
        try:
            result = sync_markets(warehouse, source, batch_size=args.batch_size, max_batches=args.max_batches)
        finally:
            source.close()
            warehouse.close()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "sync-fills":
        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GoldskyFillSource()
        try:
            result = sync_fills(warehouse, source, batch_size=args.batch_size, max_batches=args.max_batches)
        finally:
            source.close()
            warehouse.close()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "sync-recent-5m-fills":
        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GoldskyFillSource()
        try:
            result = sync_recent_5m_fills(
                warehouse,
                source,
                lookback_hours=args.lookback_hours,
                batch_size=args.batch_size,
                asset_chunk_size=args.asset_chunk_size,
                bucket_minutes=args.bucket_minutes,
                bucket_buffer_seconds=args.bucket_buffer_seconds,
                max_batches_per_chunk=args.max_batches_per_chunk,
            )
        finally:
            source.close()
            warehouse.close()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    result = build_artifacts(
        warehouse_path=args.warehouse_path,
        tracker_db=args.tracker_db,
        policy_path=args.policy_path,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0
