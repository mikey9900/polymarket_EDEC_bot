"""EDEC Bot - main entry point and composition root."""

from version import __version__  # noqa: F401

import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from bot.archive_services import (
    ArchiveBuilderService,
    ArchiveStorageService,
    ArchiveWorkflowService,
)
from bot.config import load_config
from bot.control_plane import ControlPlane
from bot.execution import ExecutionEngine
from bot.market_scanner import MarketScanner
from bot.polymarket_cli import PolymarketCli
from bot.price_aggregator import PriceAggregator
from bot.process_lock import acquire_pid_lock, default_lock_path
from bot.risk_manager import RiskManager
from bot.runtime import (
    RuntimeCoordinator,
    _install_shutdown_signal_handlers,
    _persist_runtime_state,
    _remove_shutdown_signal_handlers,
    _sync_runtime_context,
    archive_scheduler_loop,
    execution_loop,
    outcome_tracker_loop,
    runtime_state_loop,
)
from bot.runtime_defaults import default_strategy_mode
from bot.strategy import StrategyEngine
from bot.telegram_bot import TelegramBot
from bot.tracker import DecisionTracker
from research.codex_automation import CodexAutomationManager
from research.runtime import ResearchSnapshotProvider

_dashboard_api_import_error = None
try:
    from bot.dashboard_state import DashboardStateService
    from bot.live_api import LiveApiServer
except ModuleNotFoundError as exc:
    if exc.name not in ("bot.dashboard_state", "bot.live_api"):
        raise
    DashboardStateService = None
    LiveApiServer = None
    _dashboard_api_import_error = exc

logger = logging.getLogger("edec")


def _load_ha_options(ha_options_path: str = "/data/options.json") -> dict:
    try:
        with open(ha_options_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _strategy_version() -> str:
    return __version__


def _config_hash(config_path: str) -> str:
    try:
        return hashlib.sha1(Path(config_path).read_bytes()).hexdigest()[:12]
    except Exception:
        return "unknown"


def _as_bool(value, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() not in ("0", "false", "no", "off", "")


def _as_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _parse_hhmm(hhmm: str, default_h: int = 0, default_m: int = 5) -> tuple[int, int]:
    try:
        h_str, m_str = str(hhmm).split(":", 1)
        h = max(0, min(23, int(h_str)))
        m = max(0, min(59, int(m_str)))
        return h, m
    except Exception:
        return default_h, default_m


def setup_logging(config) -> None:
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    if config.logging.file:
        handlers.append(logging.FileHandler(config.logging.file, encoding="utf-8"))
    logging.basicConfig(level=log_level, format=fmt, handlers=handlers)


async def main():
    config_path = os.getenv("EDEC_CONFIG_PATH", "config_phase_a_single.yaml")
    config = load_config(config_path)
    setup_logging(config)

    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.5
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    started_at = datetime.now(timezone.utc).isoformat()
    strategy_version = _strategy_version()
    config_hash = _config_hash(config_path)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{config_hash}"

    logger.info("Using config: %s", config_path)
    logger.info("=" * 60)
    logger.info("EDEC Bot starting")
    logger.info("Coins: %s", ", ".join(config.coins))
    logger.info("Dry run: %s", config.execution.dry_run)
    logger.info("Dual-leg: enabled=%s, max_combined=%s", config.dual_leg.enabled, config.dual_leg.max_combined_cost)
    logger.info("Single-leg: enabled=%s, entry_max=%s", config.single_leg.enabled, config.single_leg.entry_max)
    logger.info("=" * 60)

    Path("data").mkdir(exist_ok=True)
    runtime_lock = acquire_pid_lock(default_lock_path())
    logger.info("Runtime PID lock acquired: %s", runtime_lock.path)

    tracker = DecisionTracker("data/decisions.db")
    ha_options = _load_ha_options()
    total, _ = tracker.get_paper_capital()
    if total == 0:
        tracker.set_paper_capital(5000.0)

    risk_manager = RiskManager(config)
    aggregator = PriceAggregator(
        staleness_max_s=config.feeds.price_staleness_max_s,
        max_velocity_30s=config.dual_leg.max_velocity_30s,
        max_velocity_60s=config.dual_leg.max_velocity_60s,
    )
    scanner = MarketScanner(config)
    research_provider = None
    if config.research.enabled:
        research_provider = ResearchSnapshotProvider(config.research.artifact_path)
        logger.info("Research runtime policy enabled: %s", config.research.artifact_path)
    strategy = StrategyEngine(
        config,
        aggregator,
        scanner,
        tracker,
        risk_manager,
        research_provider=research_provider,
    )

    default_mode = default_strategy_mode() or "both"
    logger.info("Default strategy mode target: %s", default_mode)

    polymarket_cli = PolymarketCli(config)
    cli_health = await polymarket_cli.startup_healthcheck()
    if cli_health.healthy:
        logger.info(cli_health.message)
    elif cli_health.available:
        logger.warning(cli_health.message)
    else:
        logger.info(cli_health.message)

    tracker.set_runtime_context(
        {
            "run_id": run_id,
            "started_at": started_at,
            "app_version": __version__,
            "strategy_version": strategy_version,
            "config_path": str(Path(config_path).resolve()),
            "config_hash": config_hash,
            "mode": default_mode,
            "dry_run": config.execution.dry_run,
            "order_size_usd": config.execution.order_size_usd,
            "order_size_override_active": False,
            "paper_capital_total": tracker.get_paper_capital()[0],
        }
    )

    clob_client = None
    if not config.execution.dry_run and config.private_key and "YOUR" not in config.private_key:
        try:
            from py_clob_client.client import ClobClient

            clob_client = ClobClient(
                host=config.polymarket.clob_base_url,
                key=config.private_key,
                chain_id=config.polymarket.chain_id,
            )
            clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
            logger.info("Polymarket CLOB client initialized")
        except Exception as exc:
            logger.error("Failed to initialize CLOB client: %s", exc)
            logger.warning("Falling back to dry-run mode")

    executor = ExecutionEngine(config, clob_client, risk_manager, tracker, scanner=scanner)

    archive_builders = ArchiveBuilderService(db_path="data/decisions.db", data_dir="data")

    archive_enabled = _as_bool(os.getenv("EDEC_ARCHIVE_ENABLED", ha_options.get("archive_enabled")), True)
    archive_output_dir = os.getenv("EDEC_ARCHIVE_OUTPUT_DIR", str(ha_options.get("archive_output_dir", "data/exports")))
    archive_label = os.getenv("EDEC_ARCHIVE_LABEL", str(ha_options.get("archive_label", "EDEC-BOT")))
    archive_recent_limit = _as_int(os.getenv("EDEC_ARCHIVE_RECENT_LIMIT", ha_options.get("archive_recent_limit")), 100)
    archive_hhmm = os.getenv("EDEC_ARCHIVE_TIME", str(ha_options.get("archive_time", "00:05")))
    archive_hour, archive_minute = _parse_hhmm(archive_hhmm)
    archive_send_files_to_telegram = _as_bool(
        os.getenv("EDEC_ARCHIVE_TELEGRAM_FILES", ha_options.get("archive_telegram_files")),
        True,
    )
    dropbox_token = os.getenv("EDEC_DROPBOX_TOKEN") or ha_options.get("dropbox_token")
    dropbox_refresh_token = os.getenv("EDEC_DROPBOX_REFRESH_TOKEN") or ha_options.get("dropbox_refresh_token")
    dropbox_app_key = os.getenv("EDEC_DROPBOX_APP_KEY") or ha_options.get("dropbox_app_key")
    dropbox_app_secret = os.getenv("EDEC_DROPBOX_APP_SECRET") or ha_options.get("dropbox_app_secret")
    dropbox_root = os.getenv("EDEC_DROPBOX_ROOT") or ha_options.get("dropbox_root") or "/"
    default_repo_sync_dir = str(Path(__file__).resolve().parent / "dropbox_sync")
    repo_sync_dir = os.getenv("EDEC_REPO_SYNC_DIR", default_repo_sync_dir)
    github_token = os.getenv("EDEC_GITHUB_TOKEN") or ha_options.get("github_token")
    github_repo = os.getenv("EDEC_GITHUB_REPO") or ha_options.get("github_repo")
    github_branch = os.getenv("EDEC_GITHUB_BRANCH") or ha_options.get("github_branch") or "main"
    github_export_path = os.getenv("EDEC_GITHUB_EXPORT_PATH") or ha_options.get("github_export_path") or "session_exports"
    dashboard_api_enabled = _as_bool(os.getenv("EDEC_DASHBOARD_API_ENABLED"), True)
    dashboard_api_host = os.getenv("EDEC_DASHBOARD_API_HOST", "0.0.0.0")
    dashboard_api_port = _as_int(os.getenv("EDEC_DASHBOARD_API_PORT"), 8099)
    dashboard_update_ms = _as_int(os.getenv("EDEC_DASHBOARD_UPDATE_MS"), 100)
    dashboard_history_sample_ms = _as_int(os.getenv("EDEC_DASHBOARD_HISTORY_SAMPLE_MS"), 500)
    dashboard_history_points = _as_int(os.getenv("EDEC_DASHBOARD_HISTORY_POINTS"), 600)
    dashboard_slow_refresh_ms = _as_int(os.getenv("EDEC_DASHBOARD_SLOW_REFRESH_MS"), 5000)

    archive_storage = ArchiveStorageService(
        dropbox_token=dropbox_token,
        dropbox_refresh_token=dropbox_refresh_token,
        dropbox_app_key=dropbox_app_key,
        dropbox_app_secret=dropbox_app_secret,
        dropbox_root=str(dropbox_root),
        repo_sync_dir=repo_sync_dir,
        label=archive_label,
        github_token=github_token,
        github_repo=github_repo,
        github_branch=str(github_branch),
        github_export_path=str(github_export_path),
        output_dir=archive_output_dir,
    )
    archive_workflows = ArchiveWorkflowService(
        db_path="data/decisions.db",
        output_dir=archive_output_dir,
        label=archive_label,
        recent_limit=archive_recent_limit,
        dropbox_token=dropbox_token,
        dropbox_refresh_token=dropbox_refresh_token,
        dropbox_app_key=dropbox_app_key,
        dropbox_app_secret=dropbox_app_secret,
        dropbox_root=str(dropbox_root),
        github_token=github_token,
        github_repo=github_repo,
        github_branch=str(github_branch),
        github_export_path=str(github_export_path),
    )
    codex_manager = CodexAutomationManager(config_path=config_path)

    control_plane = ControlPlane(
        config=config,
        tracker=tracker,
        risk_manager=risk_manager,
        strategy_engine=strategy,
        executor=executor,
        session_export_fn=archive_workflows.run_session_export,
        codex_manager=codex_manager,
    )

    telegram = TelegramBot(
        config,
        tracker,
        risk_manager,
        export_fn=archive_builders.export_excel,
        export_recent_fn=archive_builders.export_recent_excel,
        scanner=scanner,
        strategy_engine=strategy,
        executor=executor,
        aggregator=aggregator,
        archive_fn=archive_workflows.run_daily_archive,
        archive_latest_fn=archive_workflows.latest_paths,
        archive_health_fn=archive_workflows.health_snapshot,
        repo_sync_fn=archive_storage.sync_repo_latest,
        session_export_fn=archive_workflows.run_session_export,
        excel_dropbox_link_fn=archive_storage.excel_dropbox_link,
        fetch_github_fn=archive_storage.fetch_github_exports,
        polymarket_cli=polymarket_cli,
        control_plane=control_plane,
    )

    dashboard_state = None
    live_api = None
    if dashboard_api_enabled:
        if DashboardStateService is None or LiveApiServer is None:
            logger.warning(
                "Dashboard API disabled because optional modules are unavailable: %s",
                _dashboard_api_import_error,
            )
        else:
            dashboard_state = DashboardStateService(
                config=config,
                tracker=tracker,
                risk_manager=risk_manager,
                scanner=scanner,
                strategy_engine=strategy,
                executor=executor,
                aggregator=aggregator,
                session_export_fn=archive_workflows.run_session_export,
                control_plane=control_plane,
                update_interval_s=max(0.05, dashboard_update_ms / 1000.0),
                history_sample_interval_s=max(dashboard_update_ms, dashboard_history_sample_ms) / 1000.0,
                history_points=dashboard_history_points,
                slow_refresh_interval_s=max(dashboard_update_ms, dashboard_slow_refresh_ms) / 1000.0,
            )
            live_api = LiveApiServer(dashboard_state, host=dashboard_api_host, port=dashboard_api_port)

    runtime = RuntimeCoordinator(
        config=config,
        tracker=tracker,
        risk_manager=risk_manager,
        aggregator=aggregator,
        scanner=scanner,
        strategy=strategy,
        executor=executor,
        telegram=telegram,
        dashboard_state=dashboard_state,
        live_api=live_api,
        archive_fn=archive_workflows.run_daily_archive,
        archive_enabled=archive_enabled,
        archive_hour=archive_hour,
        archive_minute=archive_minute,
        archive_send_files_to_telegram=archive_send_files_to_telegram,
        default_mode=default_mode,
        config_path=str(Path(config_path).resolve()),
        config_hash=config_hash,
    )

    try:
        await runtime.run()
    finally:
        tracker.close()
        runtime_lock.release()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
