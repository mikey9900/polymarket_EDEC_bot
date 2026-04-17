"""EDEC Bot — Main entry point. Wires all components and runs the event loop."""

from version import __version__  # noqa: F401

import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bot.config import load_config
from bot.archive import (
    archive_health_snapshot,
    get_or_upload_excel_link,
    latest_archive_paths,
    run_daily_archive,
    sync_dropbox_latest_to_local,
)
from bot.export import export_to_excel, export_recent_to_excel
from bot.market_scanner import MarketScanner
from bot.price_aggregator import PriceAggregator
from bot.price_feeds import start_all_feeds
from bot.risk_manager import RiskManager
from bot.strategy import StrategyEngine
from bot.execution import ExecutionEngine
from bot.runtime_defaults import default_strategy_mode
from bot.tracker import DecisionTracker
from bot.telegram_bot import TelegramBot

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


def _strategy_version(doc_path: str = "STRATEGY.md") -> str:
    try:
        for line in Path(doc_path).read_text(encoding="utf-8").splitlines():
            if "Current version:" in line:
                return line.split("Current version:", 1)[1].strip()
    except Exception:
        pass
    return "unknown"


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
        handlers.append(logging.FileHandler(config.logging.file))
    logging.basicConfig(level=log_level, format=fmt, handlers=handlers)


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

            expired = scanner.pop_expired_markets()
            for market in expired:
                if market.slug in resolved_markets:
                    continue

                # Give Polymarket a moment to settle the result
                await asyncio.sleep(10)

                outcome = await scanner.get_market_outcome(market)
                if outcome:
                    resolved_markets.add(market.slug)

                    agg = aggregator.get_aggregated_price(market.coin)
                    coin_close = agg.price if agg else 0

                    tracker.log_outcome(
                        market_slug=market.slug,
                        winner=outcome,
                        btc_open=0,
                        btc_close=coin_close,
                    )
                    tracker.close_paper_trades(market.slug, outcome)
                    live_pnl = executor.resolve_market_positions(market.slug, outcome)
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
    started_at = datetime.now(timezone.utc).isoformat()
    strategy_version = _strategy_version(Path(__file__).resolve().parent.parent / "STRATEGY.md")
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
    strategy = StrategyEngine(config, aggregator, scanner, tracker, risk_manager)

    default_mode = default_strategy_mode()
    if default_mode:
        if strategy.set_mode(default_mode):
            logger.info(f"Default strategy mode from env: {default_mode}")
        else:
            logger.warning(f"Invalid EDEC_DEFAULT_MODE '{default_mode}', keeping mode={strategy.mode}")

    tracker.set_runtime_context({
        "run_id": run_id,
        "started_at": started_at,
        "app_version": __version__,
        "strategy_version": strategy_version,
        "config_path": str(Path(config_path).resolve()),
        "config_hash": config_hash,
        "mode": strategy.mode,
        "dry_run": config.execution.dry_run,
        "order_size_usd": config.execution.order_size_usd,
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
    dashboard_api_enabled = _as_bool(os.getenv("EDEC_DASHBOARD_API_ENABLED"), True)
    dashboard_api_host = os.getenv("EDEC_DASHBOARD_API_HOST", "0.0.0.0")
    dashboard_api_port = _as_int(os.getenv("EDEC_DASHBOARD_API_PORT"), 8099)
    dashboard_update_ms = _as_int(os.getenv("EDEC_DASHBOARD_UPDATE_MS"), 250)
    dashboard_history_sample_ms = _as_int(os.getenv("EDEC_DASHBOARD_HISTORY_SAMPLE_MS"), 1000)
    dashboard_history_points = _as_int(os.getenv("EDEC_DASHBOARD_HISTORY_POINTS"), 600)

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

    def do_excel_dropbox_link(local_path: str) -> str | None:
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
        excel_dropbox_link_fn=do_excel_dropbox_link,
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
                update_interval_s=max(0.1, dashboard_update_ms / 1000.0),
                history_sample_interval_s=max(dashboard_update_ms, dashboard_history_sample_ms) / 1000.0,
                history_points=dashboard_history_points,
            )
            live_api = LiveApiServer(dashboard_state, host=dashboard_api_host, port=dashboard_api_port)

    feed_pairs = []
    tasks = []

    try:
        if live_api:
            await dashboard_state.start()
            await live_api.start()
        await telegram.start()

        feed_pairs = start_all_feeds(config, price_queue)
        for task, feed in feed_pairs:
            tasks.append(task)

        tasks.append(asyncio.create_task(aggregator.run(price_queue)))
        tasks.append(asyncio.create_task(scanner.run()))
        tasks.append(asyncio.create_task(strategy.run(signal_queue)))
        tasks.append(asyncio.create_task(
            execution_loop(executor, signal_queue, tracker, telegram, config)
        ))
        tasks.append(asyncio.create_task(
            outcome_tracker_loop(scanner, tracker, aggregator, risk_manager, executor, telegram)
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
            f"Paper capital: ${paper_balance:.2f}",
        )

        # Start live dashboard
        await telegram.start_dashboard()

        logger.info("All systems running. Press Ctrl+C to stop.")
        await asyncio.gather(*tasks)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown requested...")
    finally:
        logger.info("Shutting down...")

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
            await live_api.stop()
            await dashboard_state.stop()

        tracker.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
