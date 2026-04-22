"""EDEC Bot — Main entry point. Wires all components and runs the event loop."""

from version import __version__  # noqa: F401

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bot.config import load_config
from bot.archive import (
    archive_health_snapshot,
    fetch_github_session_exports,
    get_or_upload_excel_link,
    latest_archive_paths,
    run_daily_archive,
    run_session_export,
    sync_dropbox_latest_to_local,
)
from bot.export import export_to_excel, export_recent_to_excel
from bot.market_scanner import MarketScanner
from bot.polymarket_cli import PolymarketCli
from bot.price_aggregator import PriceAggregator
from bot.price_feeds import start_all_feeds
from bot.process_lock import acquire_pid_lock, default_lock_path
from bot.recovery import (
    apply_runtime_state,
    apply_strategy_runtime_state,
    recover_runtime,
    snapshot_runtime_state,
)
from bot.risk_manager import RiskManager
from bot.strategy import StrategyEngine
from bot.execution import ExecutionEngine
from bot.runtime_defaults import default_strategy_mode
from bot.tracker import DecisionTracker
from bot.telegram_bot import TelegramBot
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
        with open(ha_options_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _strategy_version() -> str:
    return __version__


def _config_hash(config_path: str) -> str:
    try:
        return hashlib.sha1(Path(config_path).read_bytes()).hexdigest()[:12]
    except Exception:
        return "unknown"


def _as_bool(v, default: bool) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() not in ("0", "false", "no", "off", "")


def _as_int(v, default: int) -> int:
    try:
        return int(v)
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


def setup_logging(config):
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    if config.logging.file:
        handlers.append(logging.FileHandler(config.logging.file, encoding="utf-8"))
    logging.basicConfig(level=log_level, format=fmt, handlers=handlers)


def _install_shutdown_signal_handlers(loop, cancel_cb) -> tuple:
    installed = []
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, cancel_cb)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        installed.append(sig)
    return tuple(installed)


def _remove_shutdown_signal_handlers(loop, installed_signals: tuple) -> None:
    for sig in installed_signals:
        try:
            loop.remove_signal_handler(sig)
        except Exception:
            continue


async def execution_loop(executor: ExecutionEngine, signal_queue: asyncio.Queue,
                         tracker: DecisionTracker, telegram: TelegramBot, config):
    """Consume trade signals and execute them. Only alert on live trades, not dry-run."""
    while True:
        try:
            signal_data = await signal_queue.get()
            result = await executor.execute(signal_data)

            # Dry-run: silent — data is tracked in DB, check via /stats or /trades
            if result.status == "dry_run":
                continue

            coin = signal_data.market.coin
            slug = signal_data.market.slug

            if signal_data.strategy_type == "dual_leg":
                if result.status == "success":
                    await telegram.alert_dual_leg(
                        slug, coin,
                        signal_data.up_price, signal_data.down_price,
                        signal_data.combined_cost, signal_data.expected_profit,
                        result.shares,
                    )
                elif result.status in ("aborted", "partial_abort"):
                    await telegram.alert_abort(slug, result.error, result.abort_cost)

            elif signal_data.strategy_type in ("single_leg", "lead_lag", "swing_leg"):
                if result.status == "open":
                    await telegram.alert_single_leg(
                        slug, coin, signal_data.side,
                        signal_data.entry_price, signal_data.target_sell_price,
                        result.shares, signal_data.expected_profit,
                        strategy_type=signal_data.strategy_type,
                    )

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Execution loop error: {e}")


async def outcome_tracker_loop(scanner: MarketScanner, tracker: DecisionTracker,
                                aggregator: PriceAggregator, risk_manager: RiskManager,
                                executor: ExecutionEngine, telegram: TelegramBot):
    """Drain the expired-market queue and resolve outcomes."""
    resolved_markets: set[str] = set()

    while True:
        try:
            await asyncio.sleep(15)

            expired: dict[str, object] = {
                market.slug: market for market in scanner.pop_expired_markets()
            }
            for market in executor.get_unresolved_live_markets():
                expired.setdefault(market.slug, market)
            for paper_trade in tracker.get_open_paper_trades():
                slug = str(paper_trade.get("market_slug") or "")
                if not slug or slug in expired or slug in resolved_markets:
                    continue
                market = await scanner.get_market_by_slug(slug, coin=str(paper_trade.get("coin") or ""))
                if market and market.end_time <= datetime.now(timezone.utc):
                    expired[slug] = market

            for market in expired.values():
                if not market:
                    continue
                if market.slug in resolved_markets:
                    continue

                # Give Polymarket a moment to settle the result
                await asyncio.sleep(10)

                outcome = await scanner.get_market_outcome(market)
                if outcome:
                    resolved_markets.add(market.slug)

                    agg = aggregator.get_aggregated_price(market.coin)
                    coin_close = agg.price if agg else 0

                    tracker.close_paper_trades(market.slug, outcome)
                    live_pnl = executor.resolve_market_positions(market.slug, outcome)
                    tracker.log_outcome(
                        market_slug=market.slug,
                        winner=outcome,
                        btc_open=0,
                        btc_close=coin_close,
                    )
                    await telegram.alert_resolution(market.slug, outcome, live_pnl)
                    logger.info(f"Resolved {market.slug} → {outcome}")
                else:
                    # Outcome not ready yet — put it back and retry next cycle
                    scanner.queue_expired_market(market)
                    logger.debug(f"Outcome not ready for {market.slug}, will retry")

            if len(resolved_markets) > 1000:
                resolved_markets.clear()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Outcome tracker error: {e}")


def _sync_runtime_context(
    tracker: DecisionTracker,
    *,
    strategy,
    executor,
    config_path: str,
    config_hash: str,
) -> None:
    context = dict(tracker.get_runtime_context() or {})
    context.update(
        {
            "mode": getattr(strategy, "mode", context.get("mode", "off")),
            "dry_run": bool(getattr(executor.config.execution, "dry_run", True)),
            "config_path": str(Path(config_path).resolve()),
            "config_hash": config_hash,
            "order_size_usd": float(executor.order_size_usd),
            "order_size_override_active": bool(getattr(executor, "order_size_override_active", False)),
            "paper_capital_total": tracker.get_paper_capital()[0],
        }
    )
    tracker.set_runtime_context(context)


def _persist_runtime_state(
    tracker: DecisionTracker,
    *,
    risk_manager: RiskManager,
    executor: ExecutionEngine,
    strategy,
    config_path: str,
    config_hash: str,
) -> None:
    _sync_runtime_context(
        tracker,
        strategy=strategy,
        executor=executor,
        config_path=config_path,
        config_hash=config_hash,
    )
    tracker.save_runtime_state(snapshot_runtime_state(risk_manager, executor, strategy))


async def runtime_state_loop(
    tracker: DecisionTracker,
    *,
    risk_manager: RiskManager,
    executor: ExecutionEngine,
    strategy,
    config_path: str,
    config_hash: str,
) -> None:
    while True:
        try:
            _persist_runtime_state(
                tracker,
                risk_manager=risk_manager,
                executor=executor,
                strategy=strategy,
                config_path=config_path,
                config_hash=config_hash,
            )
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Runtime state persistence failed: %s", exc)
            await asyncio.sleep(1.0)


async def _warmup_runtime(scanner: MarketScanner, aggregator: PriceAggregator, *, timeout_s: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if scanner.get_all_active() or aggregator.get_all_coins_snapshot():
            return
        await asyncio.sleep(0.25)


async def archive_scheduler_loop(
    telegram: TelegramBot,
    archive_fn,
    archive_enabled: bool,
    schedule_hour: int,
    schedule_minute: int,
    send_files_to_telegram: bool,
):
    if not archive_enabled:
        return

    logger.info(
        "Archive scheduler enabled: daily at %02d:%02d (local time)",
        schedule_hour,
        schedule_minute,
    )

    while True:
        try:
            now = datetime.now().astimezone()
            next_run = now.replace(hour=schedule_hour, minute=schedule_minute, second=0, microsecond=0)
            if next_run <= now:
                next_run = next_run + timedelta(days=1)
            sleep_s = max(1.0, (next_run - now).total_seconds())
            logger.info("Next archive run scheduled for %s", next_run.isoformat())
            await asyncio.sleep(sleep_s)

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, archive_fn)
            logger.info("Archive run complete: %s", result.get("index_path"))

            await telegram.alert_archive_complete(result)
            if send_files_to_telegram:
                await telegram.send_latest_archive_files(include_index=True)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("Archive scheduler error: %s", e)
            await telegram.send_alert(f"*Archive Failed*\n`{e}`")
            await asyncio.sleep(60)


async def main():
    config_path = os.getenv("EDEC_CONFIG_PATH", "config_phase_a_single.yaml")
    config = load_config(config_path)
    setup_logging(config)

    # Diagnostics: asyncio's built-in slow-callback warning names the offending
    # coroutine when something blocks the loop for too long.
    loop = asyncio.get_running_loop()
    installed_signal_handlers: tuple = ()
    main_task = asyncio.current_task()
    if main_task is not None:
        installed_signal_handlers = _install_shutdown_signal_handlers(loop, main_task.cancel)
    loop.set_debug(True)
    loop.slow_callback_duration = 0.5  # log any callback taking >500ms
    # Quiet the noisy "coroutine was never awaited" debug spam from set_debug;
    # we only care about slow-callback warnings.
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    started_at = datetime.now(timezone.utc).isoformat()
    strategy_version = _strategy_version()
    config_hash = _config_hash(config_path)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{config_hash}"
    logger.info(f"Using config: {config_path}")

    logger.info("=" * 60)
    logger.info("EDEC Bot starting")
    logger.info(f"Coins: {', '.join(config.coins)}")
    logger.info(f"Dry run: {config.execution.dry_run}")
    logger.info(f"Dual-leg: enabled={config.dual_leg.enabled}, max_combined={config.dual_leg.max_combined_cost}")
    logger.info(f"Single-leg: enabled={config.single_leg.enabled}, entry_max={config.single_leg.entry_max}")
    logger.info("=" * 60)

    Path("data").mkdir(exist_ok=True)
    runtime_lock = acquire_pid_lock(default_lock_path())
    logger.info("Runtime PID lock acquired: %s", runtime_lock.path)

    tracker = DecisionTracker("data/decisions.db")
    ha_options = _load_ha_options()
    # Init paper capital if not already set
    total, _ = tracker.get_paper_capital()
    if total == 0:
        tracker.set_paper_capital(5000.0)  # aggressive paper bankroll default
    risk_manager = RiskManager(config)
    price_queue: asyncio.Queue = asyncio.Queue()
    signal_queue: asyncio.Queue = asyncio.Queue()

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

    default_mode = default_strategy_mode()
    if not default_mode:
        default_mode = "both"
    logger.info("Default strategy mode target: %s", default_mode)

    polymarket_cli = PolymarketCli(config)
    cli_health = await polymarket_cli.startup_healthcheck()
    if cli_health.healthy:
        logger.info(cli_health.message)
    elif cli_health.available:
        logger.warning(cli_health.message)
    else:
        logger.info(cli_health.message)

    tracker.set_runtime_context({
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
    })

    # Initialize CLOB client
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
        except Exception as e:
            logger.error(f"Failed to initialize CLOB client: {e}")
            logger.warning("Falling back to dry-run mode")

    executor = ExecutionEngine(config, clob_client, risk_manager, tracker, scanner=scanner)

    def do_export(today_only: bool = False) -> str:
        return export_to_excel("data/decisions.db", "data", today_only)

    def do_export_recent() -> str:
        return export_recent_to_excel("data/decisions.db", "data", limit=100)

    archive_enabled = _as_bool(
        os.getenv("EDEC_ARCHIVE_ENABLED", ha_options.get("archive_enabled")),
        True,
    )
    archive_output_dir = os.getenv(
        "EDEC_ARCHIVE_OUTPUT_DIR",
        str(ha_options.get("archive_output_dir", "data/exports")),
    )
    archive_label = os.getenv(
        "EDEC_ARCHIVE_LABEL",
        str(ha_options.get("archive_label", "EDEC-BOT")),
    )
    archive_recent_limit = _as_int(
        os.getenv("EDEC_ARCHIVE_RECENT_LIMIT", ha_options.get("archive_recent_limit")),
        100,
    )
    archive_hhmm = os.getenv(
        "EDEC_ARCHIVE_TIME",
        str(ha_options.get("archive_time", "00:05")),
    )
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

    def do_archive() -> dict:
        return run_daily_archive(
            db_path="data/decisions.db",
            output_dir=archive_output_dir,
            label=archive_label,
            recent_limit=archive_recent_limit,
            dropbox_token=dropbox_token,
            dropbox_refresh_token=dropbox_refresh_token,
            dropbox_app_key=dropbox_app_key,
            dropbox_app_secret=dropbox_app_secret,
            dropbox_root=str(dropbox_root),
        )

    def do_archive_latest() -> dict:
        return latest_archive_paths(output_dir=archive_output_dir, label=archive_label)

    def do_archive_health() -> dict:
        return archive_health_snapshot(
            output_dir=archive_output_dir,
            label=archive_label,
            dropbox_token=dropbox_token,
            dropbox_refresh_token=dropbox_refresh_token,
            dropbox_app_key=dropbox_app_key,
            dropbox_app_secret=dropbox_app_secret,
            dropbox_root=str(dropbox_root),
        )

    def do_repo_sync_latest() -> dict:
        if not dropbox_token and not dropbox_refresh_token:
            raise RuntimeError("Dropbox token or refresh-token auth is not configured")
        return sync_dropbox_latest_to_local(
            dropbox_token=dropbox_token,
            dropbox_refresh_token=dropbox_refresh_token,
            dropbox_app_key=dropbox_app_key,
            dropbox_app_secret=dropbox_app_secret,
            dropbox_root=str(dropbox_root),
            output_dir=repo_sync_dir,
            label=archive_label,
            expand_trades_csv=True,
        )

    def do_session_export() -> dict:
        return run_session_export(
            db_path="data/decisions.db",
            output_dir=archive_output_dir,
            label=archive_label,
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

    def do_fetch_github_exports(limit: int = 3) -> dict:
        if not github_token:
            raise RuntimeError("EDEC_GITHUB_TOKEN / github_token not configured")
        if not github_repo:
            raise RuntimeError("EDEC_GITHUB_REPO / github_repo not configured")
        return fetch_github_session_exports(
            github_token=github_token,
            github_repo=github_repo,
            github_branch=str(github_branch),
            github_export_path=str(github_export_path),
            output_dir="data/github_exports",
            limit=limit,
            expand_csv=True,
        )

    def do_excel_dropbox_link(local_path: str) -> tuple[str | None, str | None]:
        return get_or_upload_excel_link(
            local_path=local_path,
            output_dir=archive_output_dir,
            label=archive_label,
            dropbox_root=str(dropbox_root),
            dropbox_token=dropbox_token,
            dropbox_refresh_token=dropbox_refresh_token,
            dropbox_app_key=dropbox_app_key,
            dropbox_app_secret=dropbox_app_secret,
        )

    telegram = TelegramBot(
        config, tracker, risk_manager,
        export_fn=do_export,
        export_recent_fn=do_export_recent,
        scanner=scanner,
        strategy_engine=strategy,
        executor=executor,
        aggregator=aggregator,
        archive_fn=do_archive,
        archive_latest_fn=do_archive_latest,
        archive_health_fn=do_archive_health,
        repo_sync_fn=do_repo_sync_latest,
        session_export_fn=do_session_export,
        excel_dropbox_link_fn=do_excel_dropbox_link,
        fetch_github_fn=do_fetch_github_exports,
        polymarket_cli=polymarket_cli,
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
                session_export_fn=do_session_export,
                update_interval_s=max(0.05, dashboard_update_ms / 1000.0),
                history_sample_interval_s=max(dashboard_update_ms, dashboard_history_sample_ms) / 1000.0,
                history_points=dashboard_history_points,
                slow_refresh_interval_s=max(dashboard_update_ms, dashboard_slow_refresh_ms) / 1000.0,
            )
            live_api = LiveApiServer(dashboard_state, host=dashboard_api_host, port=dashboard_api_port)

    feed_pairs = []
    tasks = []

    async def _loop_lag_monitor():
        """Detect when the asyncio loop is starved.

        Sleeps 500ms; when it wakes, compares actual elapsed time to expected.
        Logs a WARNING when the loop was held for >1s.
        """
        loop = asyncio.get_running_loop()
        target_interval = 0.5
        threshold_s = 1.0
        last = loop.time()
        while True:
            await asyncio.sleep(target_interval)
            now = loop.time()
            actual = now - last
            lag = actual - target_interval
            if lag >= threshold_s:
                logging.getLogger("edec.loop_lag").warning(
                    "Event loop blocked for %.2fs (expected sleep %.1fs, slept %.2fs)",
                    lag, target_interval, actual,
                )
            last = now

    try:
        feed_pairs = start_all_feeds(config, price_queue)
        for task, feed in feed_pairs:
            tasks.append(task)

        tasks.append(asyncio.create_task(_loop_lag_monitor(), name="loop-lag-monitor"))
        tasks.append(asyncio.create_task(aggregator.run(price_queue)))
        tasks.append(asyncio.create_task(scanner.run()))
        await _warmup_runtime(scanner, aggregator)

        saved_runtime_state = tracker.load_runtime_state()
        apply_runtime_state(saved_runtime_state, risk_manager, executor)
        recovery_summary = await recover_runtime(executor, tracker, scanner)
        applied_mode = apply_strategy_runtime_state(
            strategy,
            saved_runtime_state,
            default_mode=default_mode,
        )
        _sync_runtime_context(
            tracker,
            strategy=strategy,
            executor=executor,
            config_path=config_path,
            config_hash=config_hash,
        )
        logger.info(
            "Recovery complete: mode=%s, live_rows=%s, live_monitors=%s, pending=%s, paper_rows=%s",
            applied_mode,
            recovery_summary.get("live_rows", 0),
            recovery_summary.get("live_monitors", 0),
            recovery_summary.get("live_pending", 0),
            recovery_summary.get("paper_rows", 0),
        )

        if live_api:
            await dashboard_state.start()
            # Run on a dedicated thread so the dashboard stays responsive even
            # when the main bot loop is busy.
            live_api.start_threaded()
        await telegram.start()

        tasks.append(asyncio.create_task(strategy.run(signal_queue)))
        tasks.append(asyncio.create_task(
            execution_loop(executor, signal_queue, tracker, telegram, config)
        ))
        tasks.append(asyncio.create_task(
            outcome_tracker_loop(scanner, tracker, aggregator, risk_manager, executor, telegram)
        ))
        tasks.append(asyncio.create_task(
            runtime_state_loop(
                tracker,
                risk_manager=risk_manager,
                executor=executor,
                strategy=strategy,
                config_path=config_path,
                config_hash=config_hash,
            )
        ))
        tasks.append(asyncio.create_task(
            archive_scheduler_loop(
                telegram=telegram,
                archive_fn=do_archive,
                archive_enabled=archive_enabled,
                schedule_hour=archive_hour,
                schedule_minute=archive_minute,
                send_files_to_telegram=archive_send_files_to_telegram,
            )
        ))

        coins_str = ", ".join(c.upper() for c in config.coins)
        run_type = "🧪 Dry Run" if config.execution.dry_run else "🌊 Wet Run"
        _, paper_balance = tracker.get_paper_capital()
        logger.info(f"Sending startup Telegram message to chat_id={config.telegram_chat_id}")
        await telegram.send_alert(
            f"🤖 *EDEC Bot ready* — {run_type}\n"
            f"Coins: {coins_str}\n"
            f"Paper capital: ${paper_balance:.2f}\n"
            f"Recovered live rows: {recovery_summary.get('live_rows', 0)} | "
            f"paper rows: {recovery_summary.get('paper_rows', 0)}",
        )

        # Start live dashboard
        await telegram.start_dashboard()

        logger.info("All systems running. Press Ctrl+C to stop.")
        await asyncio.gather(*tasks)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown requested...")
    finally:
        logger.info("Shutting down...")
        _remove_shutdown_signal_handlers(loop, installed_signal_handlers)

        for _, feed in feed_pairs:
            feed.stop()

        aggregator.stop()
        scanner.stop()
        strategy.stop()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        cleanup_results = await asyncio.gather(
            executor.aclose(),
            scanner.aclose(),
            return_exceptions=True,
        )
        for result in cleanup_results:
            if isinstance(result, Exception):
                logger.warning(f"Network cleanup failed: {result}")

        await telegram.send_alert("🔴 EDEC Bot stopped")
        await telegram.stop()
        if live_api:
            live_api.stop_threaded()
            await dashboard_state.stop()

        try:
            _persist_runtime_state(
                tracker,
                risk_manager=risk_manager,
                executor=executor,
                strategy=strategy,
                config_path=config_path,
                config_hash=config_hash,
            )
        except Exception as exc:
            logger.warning("Final runtime state persistence failed: %s", exc)
        tracker.close()
        runtime_lock.release()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
