"""Strategy engine — dual-leg and single-leg filter chains across all monitored coins."""

import asyncio
import logging
from datetime import datetime, timezone

from edec_bot.bot.config import Config
from edec_bot.bot.models import Decision, FilterResult, TradeSignal
from edec_bot.bot.price_aggregator import PriceAggregator
from edec_bot.bot.market_scanner import MarketScanner
from edec_bot.bot.tracker import DecisionTracker

logger = logging.getLogger(__name__)

# Valid runtime modes
VALID_MODES = {"dual", "single", "both", "off"}


class StrategyEngine:
    def __init__(self, config: Config, aggregator: PriceAggregator,
                 scanner: MarketScanner, tracker: DecisionTracker,
                 risk_manager=None):
        self.config = config
        self.aggregator = aggregator
        self.scanner = scanner
        self.tracker = tracker
        self.risk_manager = risk_manager
        self._running = False
        # Runtime mode — controls which strategies are active
        self._mode = "both"  # "dual", "single", "both", "off"

    @property
    def mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> bool:
        """Update active mode. Returns True if valid."""
        if mode not in VALID_MODES:
            return False
        self._mode = mode
        logger.info(f"Strategy mode set to: {mode}")
        return True

    def dual_leg_enabled(self) -> bool:
        return self._mode in ("dual", "both") and self.config.dual_leg.enabled

    def single_leg_enabled(self) -> bool:
        return self._mode in ("single", "both") and self.config.single_leg.enabled

    async def run(self, signal_queue: asyncio.Queue):
        """Main strategy loop — evaluate all coins every second."""
        self._running = True
        while self._running:
            try:
                if self._mode != "off":
                    signals = self._evaluate_all()
                    for signal in signals:
                        await signal_queue.put(signal)
            except Exception as e:
                logger.error(f"Strategy evaluation error: {e}")
            await asyncio.sleep(1)

    def stop(self):
        self._running = False

    def _evaluate_all(self) -> list[TradeSignal]:
        """Evaluate all active coin markets and return any signals."""
        signals = []
        active_markets = self.scanner.get_all_active()

        for coin, market in active_markets.items():
            up_book, down_book = self.scanner.get_books(coin)
            agg = self.aggregator.get_aggregated_price(coin)

            if self.dual_leg_enabled():
                signal = self._evaluate_dual_leg(coin, market, up_book, down_book, agg)
                if signal is not None:
                    signals.append(signal)

            if self.single_leg_enabled():
                signal = self._evaluate_single_leg(coin, market, up_book, down_book, agg)
                if signal is not None:
                    signals.append(signal)

        return signals

    # -----------------------------------------------------------------------
    # Dual-leg filter chain
    # -----------------------------------------------------------------------

    def _evaluate_dual_leg(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """Run dual-leg arb filter chain. Returns signal if all pass."""
        cfg = self.config.dual_leg
        filters: list[FilterResult] = []
        failed_reason = ""

        # Filter 1: Market accepting orders
        f = FilterResult("market_active", market.accepting_orders,
                         str(market.accepting_orders), "True")
        filters.append(f)
        if not f.passed:
            failed_reason = "Market not accepting orders"

        # Filter 2: Time remaining
        now = datetime.now(timezone.utc)
        remaining = (market.end_time - now).total_seconds()
        f = FilterResult("time_remaining", remaining > cfg.min_time_remaining_s,
                         f"{remaining:.0f}s", f">{cfg.min_time_remaining_s}s")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {remaining:.0f}s remaining"

        # Filter 3: Books available
        books_ok = up_book is not None and down_book is not None
        f = FilterResult("books_available", books_ok,
                         f"up={'yes' if up_book else 'no'}, down={'yes' if down_book else 'no'}",
                         "both available")
        filters.append(f)
        if not books_ok:
            if not failed_reason:
                failed_reason = "Order books not available"
            self._log_decision(coin, market, up_book, down_book, agg, remaining,
                               filters, "SKIP", failed_reason, "dual_leg")
            return None

        # Filter 4: Both sides below price threshold
        threshold = cfg.price_threshold
        f = FilterResult("price_threshold",
                         up_book.best_ask <= threshold and down_book.best_ask <= threshold,
                         f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}",
                         f"<={threshold}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Price above threshold: up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}"

        # Filter 5: Combined cost
        combined = up_book.best_ask + down_book.best_ask
        f = FilterResult("combined_cost", combined <= cfg.max_combined_cost,
                         f"{combined:.3f}", f"<={cfg.max_combined_cost}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Combined cost too high: {combined:.3f}"

        # Filter 6: Edge after fees
        fee_up = (1.0 - up_book.best_ask) * market.fee_rate
        fee_down = (1.0 - down_book.best_ask) * market.fee_rate
        fee_total = fee_up + fee_down
        total_cost = combined + fee_total
        expected_profit = 1.0 - total_cost
        f = FilterResult("edge_after_fees", expected_profit >= cfg.min_edge_after_fees,
                         f"${expected_profit:.4f}", f">=${cfg.min_edge_after_fees}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Edge too thin: ${expected_profit:.4f} after fees"

        # Filter 7: Coin velocity (trending is BAD for dual-leg mean-reversion)
        if agg is not None:
            vel_ok = (abs(agg.velocity_30s) <= cfg.max_velocity_30s
                      and abs(agg.velocity_60s) <= cfg.max_velocity_60s)
            f = FilterResult("coin_velocity", vel_ok,
                             f"30s={agg.velocity_30s:.3f}%, 60s={agg.velocity_60s:.3f}%",
                             f"30s<={cfg.max_velocity_30s}%, 60s<={cfg.max_velocity_60s}%")
        else:
            f = FilterResult("coin_velocity", False, "no price data", "price data required")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"{coin.upper()} trending: {f.value}"

        # Filter 8: Liquidity depth
        min_depth = cfg.min_book_depth_usd
        f = FilterResult("liquidity_depth",
                         up_book.ask_depth_usd >= min_depth and down_book.ask_depth_usd >= min_depth,
                         f"up=${up_book.ask_depth_usd:.1f}, down=${down_book.ask_depth_usd:.1f}",
                         f">=${min_depth}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Thin liquidity: {f.value}"

        # Filter 9: Feed count
        source_count = agg.source_count if agg else 0
        f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {source_count} live feed(s)"

        # Filter 10: Risk limits
        risk_ok = self.risk_manager.can_trade() if self.risk_manager else True
        f = FilterResult("risk_limits", risk_ok, "ok" if risk_ok else "blocked", "ok")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = "Risk limits breached"

        all_passed = all(f.passed for f in filters)
        action = ("DRY_RUN_SIGNAL" if self.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
        reason = "All filters passed" if all_passed else failed_reason

        self._log_decision(coin, market, up_book, down_book, agg, remaining,
                           filters, action, reason, "dual_leg")

        if not all_passed:
            return None

        signal = TradeSignal(
            market=market,
            strategy_type="dual_leg",
            up_price=up_book.best_ask,
            down_price=down_book.best_ask,
            combined_cost=combined,
            fee_total=fee_total,
            expected_profit=expected_profit,
            time_remaining_s=remaining,
            up_book=up_book,
            down_book=down_book,
            filter_results=filters,
        )
        logger.info(
            f"{'[DRY RUN] ' if self.config.execution.dry_run else ''}"
            f"DUAL-LEG SIGNAL [{coin.upper()}]: UP@{up_book.best_ask:.3f} + DOWN@{down_book.best_ask:.3f}"
            f" = {combined:.3f} | Profit: ${expected_profit:.4f}"
        )
        return signal

    # -----------------------------------------------------------------------
    # Single-leg filter chain
    # -----------------------------------------------------------------------

    def _evaluate_single_leg(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """Run single-leg momentum filter chain. Returns signal if all pass."""
        cfg = self.config.single_leg
        filters: list[FilterResult] = []
        failed_reason = ""

        # Filter 1: Market accepting orders
        f = FilterResult("market_active", market.accepting_orders,
                         str(market.accepting_orders), "True")
        filters.append(f)
        if not f.passed:
            failed_reason = "Market not accepting orders"

        # Filter 2: Time remaining
        now = datetime.now(timezone.utc)
        remaining = (market.end_time - now).total_seconds()
        f = FilterResult("time_remaining", remaining > cfg.min_time_remaining_s,
                         f"{remaining:.0f}s", f">{cfg.min_time_remaining_s}s")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {remaining:.0f}s remaining"

        # Filter 3: Books available
        books_ok = up_book is not None and down_book is not None
        f = FilterResult("books_available", books_ok,
                         f"up={'yes' if up_book else 'no'}, down={'yes' if down_book else 'no'}",
                         "both available")
        filters.append(f)
        if not books_ok:
            if not failed_reason:
                failed_reason = "Order books not available"
            self._log_decision(coin, market, up_book, down_book, agg, remaining,
                               filters, "SKIP", failed_reason, "single_leg")
            return None

        # Filter 4: One side cheap enough (entry_max), other side confirms the move (opposite_min)
        up_cheap = up_book.best_ask <= cfg.entry_max and down_book.best_ask >= cfg.opposite_min
        down_cheap = down_book.best_ask <= cfg.entry_max and up_book.best_ask >= cfg.opposite_min
        entry_ok = up_cheap or down_cheap

        if up_cheap:
            side = "up"
            entry_price = up_book.best_ask
            opposite_price = down_book.best_ask
            entry_depth = up_book.ask_depth_usd
        elif down_cheap:
            side = "down"
            entry_price = down_book.best_ask
            opposite_price = up_book.best_ask
            entry_depth = down_book.ask_depth_usd
        else:
            side = ""
            entry_price = min(up_book.best_ask, down_book.best_ask)
            opposite_price = max(up_book.best_ask, down_book.best_ask)
            entry_depth = 0.0

        f = FilterResult("entry_threshold", entry_ok,
                         f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}",
                         f"one side<={cfg.entry_max}, other>={cfg.opposite_min}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = (f"No cheap side: up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f} "
                             f"(need one <={cfg.entry_max}, other >={cfg.opposite_min})")

        # Filter 5: Liquidity depth at entry
        f = FilterResult("liquidity_depth", entry_depth >= cfg.min_book_depth_usd,
                         f"${entry_depth:.1f}", f">=${cfg.min_book_depth_usd}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Thin entry liquidity: ${entry_depth:.1f}"

        # Filter 6: Feed count
        source_count = agg.source_count if agg else 0
        f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {source_count} live feed(s)"

        # Filter 7: Risk limits
        risk_ok = self.risk_manager.can_trade() if self.risk_manager else True
        f = FilterResult("risk_limits", risk_ok, "ok" if risk_ok else "blocked", "ok")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = "Risk limits breached"

        all_passed = all(f.passed for f in filters)

        # Profit estimate
        target_sell = cfg.target_sell
        fee_buy = (1.0 - entry_price) * market.fee_rate
        fee_sell = (1.0 - target_sell) * market.fee_rate
        expected_profit = (target_sell - entry_price) - fee_buy - fee_sell

        action = ("DRY_RUN_SIGNAL" if self.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
        reason = f"Single-leg {side.upper() if side else '?'} @{entry_price:.3f}" if all_passed else failed_reason

        self._log_decision(coin, market, up_book, down_book, agg, remaining,
                           filters, action, reason, "single_leg")

        if not all_passed:
            return None

        signal = TradeSignal(
            market=market,
            strategy_type="single_leg",
            side=side,
            entry_price=entry_price,
            target_sell_price=target_sell,
            fee_total=fee_buy + fee_sell,
            expected_profit=expected_profit,
            time_remaining_s=remaining,
            up_book=up_book,
            down_book=down_book,
            filter_results=filters,
        )
        logger.info(
            f"{'[DRY RUN] ' if self.config.execution.dry_run else ''}"
            f"SINGLE-LEG SIGNAL [{coin.upper()}]: BUY {side.upper()}@{entry_price:.3f} → "
            f"SELL@{target_sell:.3f} | Est profit: ${expected_profit:.4f}"
        )
        return signal

    # -----------------------------------------------------------------------
    # Shared helpers
    # -----------------------------------------------------------------------

    def _log_decision(self, coin, market, up_book, down_book, agg, remaining,
                      filters, action, reason, strategy_type) -> int:
        """Log decision to the tracker."""
        decision = Decision(
            timestamp=datetime.now(timezone.utc),
            market_slug=market.slug,
            coin=coin,
            market_end_time=market.end_time,
            strategy_type=strategy_type,
            up_best_ask=up_book.best_ask if up_book else 0,
            down_best_ask=down_book.best_ask if down_book else 0,
            combined_cost=(up_book.best_ask + down_book.best_ask) if (up_book and down_book) else 0,
            btc_price=agg.price if agg else 0,
            coin_velocity_30s=agg.velocity_30s if agg else 0,
            coin_velocity_60s=agg.velocity_60s if agg else 0,
            up_depth_usd=up_book.ask_depth_usd if up_book else 0,
            down_depth_usd=down_book.ask_depth_usd if down_book else 0,
            time_remaining_s=remaining,
            feed_count=agg.source_count if agg else 0,
            filter_results=filters,
            action=action,
            reason=reason,
        )
        return self.tracker.log_decision(decision)
