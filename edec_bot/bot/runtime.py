"""Runtime coordination for the EDEC bot."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bot.price_feeds import start_all_feeds
from bot.recovery import (
    apply_runtime_state,
    apply_strategy_runtime_state,
    recover_runtime,
    snapshot_runtime_state,
)

logger = logging.getLogger("edec")


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


async def execution_loop(executor, signal_queue: asyncio.Queue, tracker, telegram, config):
    """Consume trade signals and execute them. Only alert on live trades, not dry-run."""
    while True:
        try:
            signal_data = await signal_queue.get()
            result = await executor.execute(signal_data)
            if result.status == "dry_run":
                continue
            coin = signal_data.market.coin
            slug = signal_data.market.slug
            if signal_data.strategy_type == "dual_leg":
                if result.status == "success":
                    await telegram.alert_dual_leg(
                        slug,
                        coin,
                        signal_data.up_price,
                        signal_data.down_price,
                        signal_data.combined_cost,
                        signal_data.expected_profit,
                        result.shares,
                    )
                elif result.status in ("aborted", "partial_abort"):
                    await telegram.alert_abort(slug, result.error, result.abort_cost)
            elif signal_data.strategy_type in ("single_leg", "lead_lag", "swing_leg"):
                if result.status == "open":
                    await telegram.alert_single_leg(
                        slug,
                        coin,
                        signal_data.side,
                        signal_data.entry_price,
                        signal_data.target_sell_price,
                        result.shares,
                        signal_data.expected_profit,
                        strategy_type=signal_data.strategy_type,
                    )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Execution loop error")


async def outcome_tracker_loop(scanner, tracker, aggregator, risk_manager, executor, telegram):
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
                if not market or market.slug in resolved_markets:
                    continue
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
                    logger.info("Resolved %s -> %s", market.slug, outcome)
                else:
                    scanner.queue_expired_market(market)
                    logger.debug("Outcome not ready for %s, will retry", market.slug)
            if len(resolved_markets) > 1000:
                resolved_markets.clear()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Outcome tracker error")


def _sync_runtime_context(tracker, *, strategy, executor, config_path: str, config_hash: str) -> None:
    context = dict(tracker.get_runtime_context() or {})
    context.update(
        {
            "mode": getattr(strategy, "mode", context.get("mode", "off")),
            "dry_run": bool(getattr(executor.config.execution, "dry_run", True)),
            "config_path": config_path,
            "config_hash": config_hash,
            "order_size_usd": float(executor.order_size_usd),
            "order_size_override_active": bool(getattr(executor, "order_size_override_active", False)),
            "paper_capital_total": tracker.get_paper_capital()[0],
        }
    )
    tracker.set_runtime_context(context)


def _persist_runtime_state(
    tracker,
    *,
    risk_manager,
    executor,
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
    tracker,
    *,
    risk_manager,
    executor,
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


async def _warmup_runtime(scanner, aggregator, *, timeout_s: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if scanner.get_all_active() or aggregator.get_all_coins_snapshot():
            return
        await asyncio.sleep(0.25)


async def archive_scheduler_loop(
    telegram,
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
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, archive_fn)
            logger.info("Archive run complete: %s", result.get("index_path"))
            await telegram.alert_archive_complete(result)
            if send_files_to_telegram:
                await telegram.send_alert(
                    "Archive files are ready in the HA dashboard control plane."
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("Archive scheduler error: %s", exc)
            await telegram.send_alert(f"*Archive Failed*\n`{exc}`")
            await asyncio.sleep(60)


class RuntimeCoordinator:
    def __init__(
        self,
        *,
        config,
        tracker,
        risk_manager,
        aggregator,
        scanner,
        strategy,
        executor,
        telegram,
        dashboard_state=None,
        live_api=None,
        archive_fn=None,
        archive_enabled: bool = True,
        archive_hour: int = 0,
        archive_minute: int = 5,
        archive_send_files_to_telegram: bool = True,
        default_mode: str = "both",
        config_path: str,
        config_hash: str,
        restart_request_path: str | Path | None = None,
        feed_starter=start_all_feeds,
    ):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.aggregator = aggregator
        self.scanner = scanner
        self.strategy = strategy
        self.executor = executor
        self.telegram = telegram
        self.dashboard_state = dashboard_state
        self.live_api = live_api
        self.archive_fn = archive_fn
        self.archive_enabled = archive_enabled
        self.archive_hour = archive_hour
        self.archive_minute = archive_minute
        self.archive_send_files_to_telegram = archive_send_files_to_telegram
        self.default_mode = default_mode
        self.config_path = config_path
        self.config_hash = config_hash
        self.restart_request_path = Path(restart_request_path) if restart_request_path else None
        self.feed_starter = feed_starter
        self.price_queue: asyncio.Queue = asyncio.Queue()
        self.signal_queue: asyncio.Queue = asyncio.Queue()
        self.feed_pairs = []
        self.tasks: list[asyncio.Task] = []
        self.restart_requested = False
        self.restart_request: dict[str, object] = {}
        self._main_task: asyncio.Task | None = None
        self._handled_restart_request_id: str | None = None
        self._shutdown_started = False

    async def _loop_lag_monitor(self):
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
                    lag,
                    target_interval,
                    actual,
                )
            last = now

    async def _start_core_services(self) -> dict[str, int]:
        self.feed_pairs = self.feed_starter(self.config, self.price_queue)
        for task, _feed in self.feed_pairs:
            self.tasks.append(task)
        self.tasks.append(asyncio.create_task(self._loop_lag_monitor(), name="loop-lag-monitor"))
        if self.restart_request_path is not None:
            self.tasks.append(asyncio.create_task(self._restart_request_loop(), name="restart-request"))
        self.tasks.append(asyncio.create_task(self.aggregator.run(self.price_queue), name="aggregator"))
        self.tasks.append(asyncio.create_task(self.scanner.run(), name="scanner"))
        await _warmup_runtime(self.scanner, self.aggregator)

        saved_runtime_state = self.tracker.load_runtime_state()
        apply_runtime_state(saved_runtime_state, self.risk_manager, self.executor)
        recovery_summary = await recover_runtime(self.executor, self.tracker, self.scanner)
        applied_mode = apply_strategy_runtime_state(
            self.strategy,
            saved_runtime_state,
            default_mode=self.default_mode,
        )
        _sync_runtime_context(
            self.tracker,
            strategy=self.strategy,
            executor=self.executor,
            config_path=self.config_path,
            config_hash=self.config_hash,
        )
        logger.info(
            "Recovery complete: mode=%s, live_rows=%s, live_monitors=%s, pending=%s, paper_rows=%s",
            applied_mode,
            recovery_summary.get("live_rows", 0),
            recovery_summary.get("live_monitors", 0),
            recovery_summary.get("live_pending", 0),
            recovery_summary.get("paper_rows", 0),
        )
        if self.live_api:
            await self.dashboard_state.start()
            self.live_api.start_threaded()
        await self.telegram.start()
        self.tasks.append(asyncio.create_task(self.strategy.run(self.signal_queue), name="strategy"))
        self.tasks.append(
            asyncio.create_task(
                execution_loop(self.executor, self.signal_queue, self.tracker, self.telegram, self.config),
                name="execution",
            )
        )
        self.tasks.append(
            asyncio.create_task(
                outcome_tracker_loop(
                    self.scanner,
                    self.tracker,
                    self.aggregator,
                    self.risk_manager,
                    self.executor,
                    self.telegram,
                ),
                name="outcomes",
            )
        )
        self.tasks.append(
            asyncio.create_task(
                runtime_state_loop(
                    self.tracker,
                    risk_manager=self.risk_manager,
                    executor=self.executor,
                    strategy=self.strategy,
                    config_path=self.config_path,
                    config_hash=self.config_hash,
                ),
                name="runtime-state",
            )
        )
        if self.archive_fn:
            self.tasks.append(
                asyncio.create_task(
                    archive_scheduler_loop(
                        telegram=self.telegram,
                        archive_fn=self.archive_fn,
                        archive_enabled=self.archive_enabled,
                        schedule_hour=self.archive_hour,
                        schedule_minute=self.archive_minute,
                        send_files_to_telegram=self.archive_send_files_to_telegram,
                    ),
                    name="archive-scheduler",
                )
            )
        return recovery_summary

    def _consume_restart_request(self, payload: dict[str, object], *, stale_loaded: bool = False) -> None:
        if self.restart_request_path is not None:
            try:
                self.restart_request_path.unlink()
            except FileNotFoundError:
                pass
            except Exception as exc:
                logger.warning("Failed to clear restart request: %s", exc)
        request_id = str(payload.get("request_id") or "").strip() or None
        if request_id:
            self._handled_restart_request_id = request_id
        if stale_loaded:
            logger.info("Cleared stale restart request for already-loaded config hash %s", payload.get("config_hash"))
            return
        if self.restart_requested:
            return
        self.restart_requested = True
        self.restart_request = dict(payload or {})
        logger.info("Runtime restart requested by %s for action %s", payload.get("requested_by"), payload.get("action"))
        if self._main_task is not None and not self._main_task.done():
            self._main_task.cancel()

    async def _restart_request_loop(self) -> None:
        if self.restart_request_path is None:
            return
        while True:
            try:
                if self.restart_request_path.exists():
                    payload = json.loads(self.restart_request_path.read_text(encoding="utf-8"))
                    if isinstance(payload, dict):
                        request_id = str(payload.get("request_id") or "").strip() or None
                        if request_id and request_id == self._handled_restart_request_id:
                            self._consume_restart_request(payload, stale_loaded=True)
                        elif str(payload.get("config_hash") or "") == str(self.config_hash or ""):
                            self._consume_restart_request(payload, stale_loaded=True)
                        else:
                            self._consume_restart_request(payload)
                            return
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except json.JSONDecodeError as exc:
                logger.warning("Restart request is invalid JSON: %s", exc)
                await asyncio.sleep(1.0)
            except Exception as exc:
                logger.warning("Restart request watcher failed: %s", exc)
                await asyncio.sleep(1.0)

    async def _send_startup_alert(self, recovery_summary: dict[str, int]) -> None:
        coins_str = ", ".join(c.upper() for c in self.config.coins)
        run_type = "Dry Run" if self.config.execution.dry_run else "Wet Run"
        _, paper_balance = self.tracker.get_paper_capital()
        await self.telegram.send_alert(
            "*EDEC Bot ready*\n"
            f"Run type: {run_type}\n"
            f"Coins: {coins_str}\n"
            f"Paper capital: ${paper_balance:.2f}\n"
            f"Recovered live rows: {recovery_summary.get('live_rows', 0)} | "
            f"paper rows: {recovery_summary.get('paper_rows', 0)}"
        )

    async def shutdown(self) -> None:
        if self._shutdown_started:
            return
        self._shutdown_started = True
        for _, feed in self.feed_pairs:
            feed.stop()
        self.aggregator.stop()
        self.scanner.stop()
        self.strategy.stop()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        cleanup_results = await asyncio.gather(
            self.executor.aclose(),
            self.scanner.aclose(),
            return_exceptions=True,
        )
        for result in cleanup_results:
            if isinstance(result, Exception):
                logger.warning("Network cleanup failed: %s", result)
        if self.restart_requested:
            await self.telegram.send_alert("EDEC Bot restarting to apply reviewed config.")
        else:
            await self.telegram.send_alert("EDEC Bot stopped")
        await self.telegram.stop()
        if self.live_api:
            self.live_api.stop_threaded()
            await self.dashboard_state.stop()
        try:
            _persist_runtime_state(
                self.tracker,
                risk_manager=self.risk_manager,
                executor=self.executor,
                strategy=self.strategy,
                config_path=self.config_path,
                config_hash=self.config_hash,
            )
        except Exception as exc:
            logger.warning("Final runtime state persistence failed: %s", exc)

    async def run(self) -> bool:
        loop = asyncio.get_running_loop()
        installed_signal_handlers: tuple = ()
        main_task = asyncio.current_task()
        self._main_task = main_task
        if main_task is not None:
            installed_signal_handlers = _install_shutdown_signal_handlers(loop, main_task.cancel)
        try:
            recovery_summary = await self._start_core_services()
            await self._send_startup_alert(recovery_summary)
            logger.info("All systems running. Press Ctrl+C to stop.")
            await asyncio.gather(*self.tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            if self.restart_requested:
                logger.info("Restart requested...")
            else:
                logger.info("Shutdown requested...")
        finally:
            _remove_shutdown_signal_handlers(loop, installed_signal_handlers)
            await self.shutdown()
        return self.restart_requested
