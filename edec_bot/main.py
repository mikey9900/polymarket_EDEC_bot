"""EDEC Bot — Main entry point. Wires all components and runs the event loop."""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from bot.config import load_config
from bot.export import export_to_excel
from bot.market_scanner import MarketScanner
from bot.price_aggregator import PriceAggregator
from bot.price_feeds import start_all_feeds
from bot.risk_manager import RiskManager
from bot.strategy import StrategyEngine
from bot.execution import ExecutionEngine
from bot.tracker import DecisionTracker
from bot.telegram_bot import TelegramBot

logger = logging.getLogger("edec")


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

            elif signal_data.strategy_type == "single_leg":
                if result.status == "open":
                    await telegram.alert_single_leg(
                        slug, coin, signal_data.side,
                        signal_data.entry_price, signal_data.target_sell_price,
                        result.shares, signal_data.expected_profit,
                    )

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Execution loop error: {e}")


async def outcome_tracker_loop(scanner: MarketScanner, tracker: DecisionTracker,
                                aggregator: PriceAggregator, risk_manager: RiskManager,
                                telegram: TelegramBot):
    """Periodically check all coins for resolved markets and backfill outcomes."""
    resolved_markets: set[str] = set()

    while True:
        try:
            await asyncio.sleep(30)

            active = scanner.get_all_active()
            now = datetime.now(timezone.utc)

            for coin, market in list(active.items()):
                if now > market.end_time and market.slug not in resolved_markets:
                    await asyncio.sleep(10)

                    outcome = await scanner.get_market_outcome(market)
                    if outcome:
                        resolved_markets.add(market.slug)

                        agg = aggregator.get_aggregated_price(coin)
                        coin_close = agg.price if agg else 0

                        tracker.log_outcome(
                            market_slug=market.slug,
                            winner=outcome,
                            btc_open=0,
                            btc_close=coin_close,
                        )

                        await telegram.alert_resolution(market.slug, outcome, 0)

            if len(resolved_markets) > 1000:
                resolved_markets.clear()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Outcome tracker error: {e}")


async def main():
    config = load_config("config.yaml")
    setup_logging(config)

    logger.info("=" * 60)
    logger.info("EDEC Bot starting")
    logger.info(f"Coins: {', '.join(config.coins)}")
    logger.info(f"Dry run: {config.execution.dry_run}")
    logger.info(f"Dual-leg: enabled={config.dual_leg.enabled}, max_combined={config.dual_leg.max_combined_cost}")
    logger.info(f"Single-leg: enabled={config.single_leg.enabled}, entry_max={config.single_leg.entry_max}")
    logger.info("=" * 60)

    Path("data").mkdir(exist_ok=True)

    tracker = DecisionTracker("data/decisions.db")
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

    executor = ExecutionEngine(config, clob_client, risk_manager, tracker)

    def do_export(today_only: bool = False) -> str:
        return export_to_excel("data/decisions.db", "data", today_only)

    telegram = TelegramBot(
        config, tracker, risk_manager,
        export_fn=do_export,
        scanner=scanner,
        strategy_engine=strategy,
        executor=executor,
    )

    feed_pairs = []
    tasks = []

    try:
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
            outcome_tracker_loop(scanner, tracker, aggregator, risk_manager, telegram)
        ))

        coins_str = ", ".join(c.upper() for c in config.coins)
        mode_str = "DRY RUN 👀" if config.execution.dry_run else "LIVE 🔴"
        logger.info(f"Sending startup Telegram message to chat_id={config.telegram_chat_id}")
        await telegram.send_alert(
            f"🤖 *EDEC Bot started*\n"
            f"Mode: {mode_str}\n"
            f"Coins: {coins_str}\n"
            f"Dual-leg: ≤{config.dual_leg.max_combined_cost} combined\n"
            f"Single-leg: entry ≤{config.single_leg.entry_max} → sell @{config.single_leg.target_sell}\n\n"
            f"_Alerts only on live trades. Use buttons for data._",
            reply_markup=telegram._main_keyboard(),
        )

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

        await telegram.send_alert("🔴 EDEC Bot stopped")
        await telegram.stop()

        tracker.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
