"""CLI entrypoints for the research subsystem."""

from __future__ import annotations

import argparse
import json

from .codex_automation import CodexAutomationManager
from .paths import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_POLICY_PATH,
    LOCAL_TRACKER_DB,
    WAREHOUSE_PATH,
)
from .sources import GammaMarketSource, GoldskyFillSource
from .sync import sync_fills, sync_markets, sync_recent_5m_fills
from .tuner import (
    build_weekly_ai_context,
    build_weekly_review_bundle,
    promote_tuning_candidate,
    propose_tuning,
    propose_weekly_ai_tuning,
    reject_tuning_candidate,
    tuner_status,
)


def _add_http_retry_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--http-timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP request timeout for Gamma/Goldsky calls in seconds",
    )
    parser.add_argument(
        "--http-retry-attempts",
        type=int,
        default=3,
        help="Total attempts for transient HTTP failures",
    )
    parser.add_argument(
        "--http-retry-backoff-seconds",
        type=float,
        default=1.5,
        help="Initial backoff between retry attempts",
    )
    parser.add_argument(
        "--http-retry-max-backoff-seconds",
        type=float,
        default=10.0,
        help="Maximum backoff between retry attempts",
    )


def _add_build_artifact_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    parser.add_argument("--tracker-db", default=str(LOCAL_TRACKER_DB))
    parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    parser.add_argument("--lookback-days", type=int, default=30)


def _add_config_path_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_PATH))


def _recent_5m_sync_kwargs(args: argparse.Namespace) -> dict[str, object]:
    return {
        "lookback_hours": args.lookback_hours,
        "batch_size": args.batch_size,
        "asset_chunk_size": args.asset_chunk_size,
        "bucket_minutes": args.bucket_minutes,
        "bucket_buffer_seconds": args.bucket_buffer_seconds,
        "max_batches_per_chunk": args.max_batches_per_chunk,
    }


def _build_artifact_kwargs(args: argparse.Namespace) -> dict[str, object]:
    return {
        "warehouse_path": args.warehouse_path,
        "tracker_db": args.tracker_db,
        "policy_path": args.policy_path,
        "lookback_days": args.lookback_days,
    }


def _source_retry_kwargs(args: argparse.Namespace) -> dict[str, object]:
    return {
        "timeout_seconds": args.http_timeout_seconds,
        "retry_attempts": args.http_retry_attempts,
        "retry_backoff_seconds": args.http_retry_backoff_seconds,
        "retry_max_backoff_seconds": args.http_retry_max_backoff_seconds,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m edec_bot.research")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_markets_parser = subparsers.add_parser("sync-markets", help="Sync Gamma market metadata into the warehouse")
    sync_markets_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    sync_markets_parser.add_argument("--batch-size", type=int, default=500)
    sync_markets_parser.add_argument("--max-batches", type=int, default=None)
    _add_http_retry_args(sync_markets_parser)

    sync_fills_parser = subparsers.add_parser("sync-fills", help="Sync Goldsky fills into the warehouse")
    sync_fills_parser.add_argument("--warehouse-path", default=str(WAREHOUSE_PATH))
    sync_fills_parser.add_argument("--batch-size", type=int, default=1000)
    sync_fills_parser.add_argument("--max-batches", type=int, default=None)
    _add_http_retry_args(sync_fills_parser)

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
    _add_http_retry_args(sync_recent_fills_parser)

    build_artifacts_parser = subparsers.add_parser("build-artifacts", help="Build runtime policy and reports")
    _add_build_artifact_args(build_artifacts_parser)

    report_parser = subparsers.add_parser("report", help="Refresh the research report outputs")
    _add_build_artifact_args(report_parser)

    daily_refresh_parser = subparsers.add_parser(
        "daily-refresh",
        help="Run sync, rebuild research artifacts, propose local tuning, and refresh the weekly AI context",
    )
    daily_refresh_parser.add_argument("--lookback-hours", type=int, default=24)
    daily_refresh_parser.add_argument("--batch-size", type=int, default=1000)
    daily_refresh_parser.add_argument("--asset-chunk-size", type=int, default=20)
    daily_refresh_parser.add_argument("--bucket-minutes", type=int, default=60)
    daily_refresh_parser.add_argument("--bucket-buffer-seconds", type=int, default=900)
    daily_refresh_parser.add_argument("--max-batches-per-chunk", type=int, default=2)
    _add_http_retry_args(daily_refresh_parser)
    _add_build_artifact_args(daily_refresh_parser)
    _add_config_path_arg(daily_refresh_parser)

    propose_tuning_parser = subparsers.add_parser(
        "propose-tuning",
        help="Build a deterministic tuning candidate from the latest session exports",
    )
    _add_config_path_arg(propose_tuning_parser)

    weekly_context_parser = subparsers.add_parser(
        "build-weekly-ai-context",
        help="Build the rolling compact weekly context file used by the weekly OpenAI tuning pass",
    )
    _add_config_path_arg(weekly_context_parser)
    weekly_context_parser.add_argument("--window-days", type=int, default=7)

    weekly_ai_parser = subparsers.add_parser(
        "propose-weekly-ai-tuning",
        help="Use the weekly compact context to build a weekly AI tuning candidate",
    )
    _add_config_path_arg(weekly_ai_parser)
    weekly_ai_parser.add_argument("--model", default="gpt-5.4-mini")
    weekly_ai_parser.add_argument("--max-output-tokens", type=int, default=4000)

    weekly_review_parser = subparsers.add_parser(
        "build-weekly-review-bundle",
        help="Prepare the compact weekly desktop review bundle for manual Codex review",
    )
    _add_config_path_arg(weekly_review_parser)

    tuner_status_parser = subparsers.add_parser("tuner-status", help="Print current tuner state and candidate metadata")
    _add_config_path_arg(tuner_status_parser)

    promote_candidate_parser = subparsers.add_parser(
        "promote-tuning-candidate",
        help="Promote the latest ready tuning candidate into the active config",
    )
    _add_config_path_arg(promote_candidate_parser)
    promote_candidate_parser.add_argument("--candidate-id", default=None)

    reject_candidate_parser = subparsers.add_parser(
        "reject-tuning-candidate",
        help="Reject the latest ready tuning candidate without promoting it",
    )
    reject_candidate_parser.add_argument("--candidate-id", default=None)
    reject_candidate_parser.add_argument("--reason", default="Rejected by operator.")

    tuner_heartbeat_parser = subparsers.add_parser(
        "tuner-heartbeat",
        help="Honor shared tuner schedule state and run a proposal only when due",
    )
    _add_config_path_arg(tuner_heartbeat_parser)

    codex_runner_parser = subparsers.add_parser(
        "codex-runner",
        help="Process HA-local Codex queue items and scheduled jobs",
    )
    codex_runner_parser.add_argument("--poll-seconds", type=float, default=15.0)
    codex_runner_parser.add_argument("--run-once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "sync-markets":
        from .warehouse import ResearchWarehouse

        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GammaMarketSource(**_source_retry_kwargs(args))
        try:
            result = sync_markets(warehouse, source, batch_size=args.batch_size, max_batches=args.max_batches)
        finally:
            source.close()
            warehouse.close()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "sync-fills":
        from .warehouse import ResearchWarehouse

        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GoldskyFillSource(**_source_retry_kwargs(args))
        try:
            result = sync_fills(warehouse, source, batch_size=args.batch_size, max_batches=args.max_batches)
        finally:
            source.close()
            warehouse.close()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "sync-recent-5m-fills":
        from .warehouse import ResearchWarehouse

        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GoldskyFillSource(**_source_retry_kwargs(args))
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

    if args.command == "daily-refresh":
        from .artifacts import build_artifacts
        from .warehouse import ResearchWarehouse

        warehouse = ResearchWarehouse(args.warehouse_path)
        source = GoldskyFillSource(**_source_retry_kwargs(args))
        sync_result: dict[str, object] | None = None
        sync_error: dict[str, str] | None = None
        build_result: dict[str, object] | None = None
        build_error: dict[str, str] | None = None
        try:
            sync_result = sync_recent_5m_fills(warehouse, source, **_recent_5m_sync_kwargs(args))
        except Exception as exc:  # noqa: BLE001
            sync_error = _exception_payload(exc)
        finally:
            _close_quietly(source)
            _close_quietly(warehouse)
        try:
            build_result = build_artifacts(**_build_artifact_kwargs(args))
        except Exception as exc:  # noqa: BLE001
            build_error = _exception_payload(exc)
        local_tuning_result: dict[str, object] | None = None
        local_tuning_error: dict[str, str] | None = None
        weekly_context_result: dict[str, object] | None = None
        weekly_context_error: dict[str, str] | None = None
        try:
            local_tuning_result = propose_tuning(config_path=args.config_path)
        except Exception as exc:  # noqa: BLE001
            local_tuning_error = _exception_payload(exc)
        try:
            weekly_context_result = build_weekly_ai_context(config_path=args.config_path)
        except Exception as exc:  # noqa: BLE001
            weekly_context_error = _exception_payload(exc)

        result = {
            "command": "daily-refresh",
            "ok": build_error is None and local_tuning_error is None and weekly_context_error is None,
            "sync": {
                "ok": sync_error is None,
                "result": sync_result,
                "error": sync_error,
            },
            "build": {
                "ok": build_error is None,
                "result": build_result,
                "error": build_error,
            },
            "daily_local_tuning": {
                "ok": local_tuning_error is None,
                "result": local_tuning_result,
                "error": local_tuning_error,
            },
            "weekly_ai_context": {
                "ok": weekly_context_error is None,
                "result": weekly_context_result,
                "error": weekly_context_error,
            },
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1 if build_error is not None or local_tuning_error is not None or weekly_context_error is not None else 0

    if args.command == "propose-tuning":
        result = propose_tuning(config_path=args.config_path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "build-weekly-ai-context":
        result = build_weekly_ai_context(config_path=args.config_path, window_days=args.window_days)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "propose-weekly-ai-tuning":
        result = propose_weekly_ai_tuning(
            config_path=args.config_path,
            model=args.model,
            max_output_tokens=args.max_output_tokens,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("ok", False) else 1

    if args.command == "build-weekly-review-bundle":
        result = build_weekly_review_bundle(config_path=args.config_path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "tuner-status":
        result = tuner_status(config_path=args.config_path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "promote-tuning-candidate":
        result = promote_tuning_candidate(candidate_id=args.candidate_id, config_path=args.config_path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "reject-tuning-candidate":
        result = reject_tuning_candidate(candidate_id=args.candidate_id, reason=args.reason)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "tuner-heartbeat":
        result = CodexAutomationManager(config_path=args.config_path).run_tuner_heartbeat()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("ok", False) else 1

    if args.command == "codex-runner":
        manager = CodexAutomationManager()
        if args.run_once:
            result = manager.run_once()
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0 if result.get("ok", False) else 1
        try:
            manager.run_loop(poll_seconds=args.poll_seconds)
        except KeyboardInterrupt:
            pass
        return 0

    from .artifacts import build_artifacts

    result = build_artifacts(
        **_build_artifact_kwargs(args),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _exception_payload(exc: Exception) -> dict[str, str]:
    return {
        "type": exc.__class__.__name__,
        "message": str(exc),
    }


def _close_quietly(resource: object) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        try:
            close()
        except Exception:  # noqa: BLE001
            pass
