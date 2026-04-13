"""Strategy engine — dual-leg and single-leg filter chains across all monitored coins."""

import asyncio
import logging
from datetime import datetime, timezone

from bot.config import Config
from bot.models import Decision, FilterResult, TradeSignal
from bot.price_aggregator import PriceAggregator
from bot.market_scanner import MarketScanner
from bot.tracker import DecisionTracker

logger = logging.getLogger(__name__)

# Valid runtime modes
VALID_MODES = {"dual", "single", "both", "lead", "swing", "off"}


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
        # Start in "off" mode — user must press Start in Telegram to begin
        self._mode = "off"
        self._active = False  # scanning on/off
        # Cooldown: track last signal price per (coin, strategy_type).
        # Only re-signal if price improves by more than MIN_IMPROVEMENT.
        # Key = (coin, strategy_type), value = (market_slug, last_entry_price)
        self._last_signal: dict[tuple, tuple] = {}
        self.MIN_PRICE_IMPROVEMENT = 0.03  # must be 3c cheaper to re-signal

    @property
    def mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> bool:
        """Update active mode. Returns True if valid."""
        if mode not in VALID_MODES:
            return False
        self._mode = mode
        self._active = (mode != "off")
        logger.info(f"Strategy mode set to: {mode}")
        return True

    def start_scanning(self):
        """Start scanning — restores last mode or defaults to 'both'."""
        if self._mode == "off":
            self._mode = "both"
        self._active = True
        logger.info(f"Scanning started (mode={self._mode})")

    def stop_scanning(self):
        """Stop scanning — preserves mode setting for next start."""
        self._active = False
        logger.info("Scanning stopped")

    @property
    def is_active(self) -> bool:
        return self._active

    def dual_leg_enabled(self) -> bool:
        return self._active and self._mode in ("dual", "both") and self.config.dual_leg.enabled

    def single_leg_enabled(self) -> bool:
        return self._active and self._mode in ("single", "both") and self.config.single_leg.enabled

    def lead_lag_enabled(self) -> bool:
        return (self._active and self._mode in ("lead", "both")
                and self.config.lead_lag.enabled)

    def swing_leg_enabled(self) -> bool:
        return (self._active and self._mode in ("swing", "both")
                and self.config.swing_leg.enabled)

    async def run(self, signal_queue: asyncio.Queue):
        """Main strategy loop — evaluates when active, sleeps when stopped."""
        self._running = True
        while self._running:
            try:
                if self._active:
                    signals = self._evaluate_all()
                    for signal in signals:
                        await signal_queue.put(signal)
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(2)  # idle — just wait for start command
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
                if signal is not None and self._is_price_improvement("dual_leg", coin, market.slug, signal.combined_cost):
                    self._last_signal[(coin, "dual_leg")] = (market.slug, signal.combined_cost)
                    signals.append(signal)

            if self.single_leg_enabled():
                signal = self._evaluate_single_leg(coin, market, up_book, down_book, agg)
                if signal is not None and self._is_price_improvement("single_leg", coin, market.slug, signal.entry_price):
                    self._last_signal[(coin, "single_leg")] = (market.slug, signal.entry_price)
                    signals.append(signal)

            if self.lead_lag_enabled():
                signal = self._evaluate_lead_lag(coin, market, up_book, down_book, agg)
                if signal is not None and self._is_price_improvement("lead_lag", coin, market.slug, signal.entry_price):
                    self._last_signal[(coin, "lead_lag")] = (market.slug, signal.entry_price)
                    signals.append(signal)

            if self.swing_leg_enabled():
                signal = self._evaluate_swing_leg(coin, market, up_book, down_book, agg)
                if signal is not None and self._is_price_improvement("swing_leg", coin, market.slug, signal.entry_price):
                    self._last_signal[(coin, "swing_leg")] = (market.slug, signal.entry_price)
                    signals.append(signal)

        return signals

    def _is_price_improvement(self, strategy: str, coin: str, slug: str, price: float) -> bool:
        """
        Returns True if this is worth signalling:
        - New market window (different slug) → always signal
        - Single-leg: ONE trade per window per coin — never re-signal same window.
          (Prevents stacking 3-4 entries as price drifts lower in a losing direction.)
        - Dual-leg: same window but price improved by MIN_PRICE_IMPROVEMENT → signal again
        - Same window, price same or worse → suppress
        """
        key = (coin, strategy)
        last = self._last_signal.get(key)
        if last is None:
            return True                          # first ever signal for this coin/strategy
        last_slug, last_price = last
        if last_slug != slug:
            return True                          # new 5-min window — always evaluate
        if strategy in ("single_leg", "lead_lag", "swing_leg"):
            return False  # one trade per window per coin
        return (last_price - price) >= self.MIN_PRICE_IMPROVEMENT  # dual-leg: price got cheaper

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

        # Filter 2b: Entry window — not too early (wait for direction to establish)
        f = FilterResult("entry_window", remaining <= cfg.max_time_remaining_s,
                         f"{remaining:.0f}s", f"<={cfg.max_time_remaining_s:.0f}s")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Too early: {remaining:.0f}s remaining (wait for direction)"

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

        # Filter 4: Coin must actually be moving — proves a real directional event, not a thin book
        min_vel = cfg.min_velocity_30s
        if agg is not None:
            vel_ok = abs(agg.velocity_30s) >= min_vel
            f = FilterResult("coin_velocity", vel_ok,
                             f"30s={agg.velocity_30s:.3f}%",
                             f">={min_vel}%")
        else:
            f = FilterResult("coin_velocity", False, "no price data", "price data required")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"{coin.upper()} not moving enough: {f.value} (need >={min_vel}%)"

        # Filter 5: One side cheap enough (entry_max), other side confirms the move (opposite_min)
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

        # Filter 5a: Entry floor — don't buy a side already priced near zero.
        # Sub-threshold asks mean the market has priced that side out; recovery is unreachable
        # in the remaining window and loss_cut bids can gap straight through.
        if side in ("up", "down"):
            floor_ok = entry_price >= cfg.entry_min
            f = FilterResult("entry_floor", floor_ok,
                             f"{entry_price:.3f}", f">={cfg.entry_min:.2f}")
        else:
            f = FilterResult("entry_floor", True, "n/a", "n/a")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = (f"Ask too low: {entry_price:.3f} < floor {cfg.entry_min:.2f} "
                             f"(market near-resolved)")

        # Filter 5b: Velocity divergence — 60s trend must not strongly oppose trade direction.
        # A barely-positive vel30s with a deeply-negative vel60s = entering against the real trend.
        if agg is not None and side in ("up", "down"):
            vel60 = agg.velocity_60s
            div_ok = (vel60 >= -cfg.max_vel_divergence if side == "up"
                      else vel60 <= cfg.max_vel_divergence)
            f = FilterResult("vel_divergence", div_ok,
                             f"30s={agg.velocity_30s:+.3f}% 60s={vel60:+.3f}%",
                             f"60s aligned with {side} (max_div={cfg.max_vel_divergence}%)")
        else:
            f = FilterResult("vel_divergence", True, "n/a", "n/a")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = (f"Vel divergence: 60s={agg.velocity_60s:+.3f}% "
                             f"opposes {side} direction")

        # Filter 6: Liquidity depth at entry
        f = FilterResult("liquidity_depth", entry_depth >= cfg.min_book_depth_usd,
                         f"${entry_depth:.1f}", f">=${cfg.min_book_depth_usd}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Thin entry liquidity: ${entry_depth:.1f}"

        # Filter 7: Feed count
        source_count = agg.source_count if agg else 0
        f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {source_count} live feed(s)"

        # Filter 8: Risk limits
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
    # Lead-lag filter chain
    # -----------------------------------------------------------------------

    def _evaluate_lead_lag(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """
        Lead-lag strategy: coin moved on Binance but Polymarket book hasn't fully repriced.
        Buy the WINNING side early (momentum-following) and exit when books catch up.

        Example: BTC drops 0.3% in 30s → DOWN should be worth ~0.65+ but still at 0.53.
        Buy DOWN at 0.53, sell at 0.67 when Polymarket reprices.
        """
        cfg = self.config.lead_lag
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
                               filters, "SKIP", failed_reason, "lead_lag")
            return None

        # Filter 4: Coin must be moving significantly
        if agg is None:
            f = FilterResult("coin_velocity", False, "no price data", "price data required")
            filters.append(f)
            self._log_decision(coin, market, up_book, down_book, agg, remaining,
                               filters, "SKIP", "No price data", "lead_lag")
            return None

        vel = agg.velocity_30s
        vel_ok = abs(vel) >= cfg.min_velocity_30s
        f = FilterResult("coin_velocity", vel_ok,
                         f"30s={vel:.3f}%", f">={cfg.min_velocity_30s}%")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"{coin.upper()} not moving enough: {vel:.3f}% (need >={cfg.min_velocity_30s}%)"

        # Filter 5: Identify direction and check if winning side is in the "lag window"
        # Coin UP (+vel) → UP more likely to win → buy UP if UP ask still in range
        # Coin DOWN (-vel) → DOWN more likely to win → buy DOWN if DOWN ask still in range
        if vel > 0:
            side = "up"
            entry_price = up_book.best_ask
            entry_depth = up_book.ask_depth_usd
        else:
            side = "down"
            entry_price = down_book.best_ask
            entry_depth = down_book.ask_depth_usd

        in_range = cfg.min_entry <= entry_price <= cfg.max_entry
        f = FilterResult("lag_window", in_range,
                         f"{side.upper()}@{entry_price:.3f}",
                         f"[{cfg.min_entry:.2f}, {cfg.max_entry:.2f}]")
        filters.append(f)
        if not f.passed and not failed_reason:
            if entry_price < cfg.min_entry:
                failed_reason = f"{side.upper()}@{entry_price:.3f} already in single-leg range (too repriced)"
            else:
                failed_reason = f"{side.upper()}@{entry_price:.3f} too high — market not moving in expected direction"

        # Filter 6: Liquidity depth
        f = FilterResult("liquidity_depth", entry_depth >= cfg.min_book_depth_usd,
                         f"${entry_depth:.1f}", f">=${cfg.min_book_depth_usd}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Thin entry liquidity: ${entry_depth:.1f}"

        # Filter 7: Feed count
        source_count = agg.source_count if agg else 0
        f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {source_count} live feed(s)"

        # Filter 8: Risk limits
        risk_ok = self.risk_manager.can_trade() if self.risk_manager else True
        f = FilterResult("risk_limits", risk_ok, "ok" if risk_ok else "blocked", "ok")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = "Risk limits breached"

        all_passed = all(f.passed for f in filters)

        # Profit estimate (entry → target sell)
        target_sell = cfg.target_sell
        fee_rate = market.fee_rate
        fee_buy = (1.0 - entry_price) * fee_rate
        fee_sell = (1.0 - target_sell) * fee_rate
        expected_profit = (target_sell - entry_price) - fee_buy - fee_sell

        action = ("DRY_RUN_SIGNAL" if self.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
        reason = (f"Lead-lag {side.upper()} @{entry_price:.3f} vel={vel:.3f}%"
                  if all_passed else failed_reason)

        self._log_decision(coin, market, up_book, down_book, agg, remaining,
                           filters, action, reason, "lead_lag")

        if not all_passed:
            return None

        signal = TradeSignal(
            market=market,
            strategy_type="lead_lag",
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
            f"LEAD-LAG SIGNAL [{coin.upper()}]: BUY {side.upper()}@{entry_price:.3f} "
            f"(vel={vel:+.3f}%) → SELL@{target_sell:.3f} | Est profit: ${expected_profit:.4f}"
        )
        return signal

    # -----------------------------------------------------------------------
    # Swing dual-leg filter chain
    # -----------------------------------------------------------------------

    def _evaluate_swing_leg(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """
        Swing dual-leg strategy: buy one cheap side and wait for the other to also dip.
        Once both legs are bought below 50c each, the combined payout guarantees profit.
        If the second leg never dips, exit the first leg at the exit price for a small gain.
        """
        cfg = self.config.swing_leg
        filters: list[FilterResult] = []
        failed_reason = ""

        # Filter 1: Market accepting orders
        f = FilterResult("market_active", market.accepting_orders,
                         str(market.accepting_orders), "True")
        filters.append(f)
        if not f.passed:
            failed_reason = "Market not accepting orders"

        # Filter 2: Time remaining — need enough runway to wait for second leg
        now = datetime.now(timezone.utc)
        remaining = (market.end_time - now).total_seconds()
        f = FilterResult("time_remaining", remaining > cfg.min_time_remaining_s,
                         f"{remaining:.0f}s", f">{cfg.min_time_remaining_s}s")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {remaining:.0f}s left — not enough time to leg in"

        # Filter 2b: Entry window — not too early (wait for direction to establish)
        f = FilterResult("entry_window", remaining <= cfg.max_time_remaining_s,
                         f"{remaining:.0f}s", f"<={cfg.max_time_remaining_s:.0f}s")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Too early: {remaining:.0f}s remaining (wait for direction)"

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
                               filters, "SKIP", failed_reason, "swing_leg")
            return None

        # Filter 4: One side must be cheap enough to enter as first leg
        up_cheap = up_book.best_ask <= cfg.first_leg_max
        dn_cheap = down_book.best_ask <= cfg.first_leg_max
        entry_ok = up_cheap or dn_cheap

        if up_cheap and not dn_cheap:
            side = "up"
            entry_price = up_book.best_ask
            entry_depth = up_book.ask_depth_usd
        elif dn_cheap and not up_cheap:
            side = "down"
            entry_price = down_book.best_ask
            entry_depth = down_book.ask_depth_usd
        elif up_cheap and dn_cheap:
            # Both cheap — pick cheaper one as first leg
            if up_book.best_ask <= down_book.best_ask:
                side = "up"
                entry_price = up_book.best_ask
                entry_depth = up_book.ask_depth_usd
            else:
                side = "down"
                entry_price = down_book.best_ask
                entry_depth = down_book.ask_depth_usd
        else:
            side = ""
            entry_price = 0.0
            entry_depth = 0.0

        f = FilterResult("first_leg_price", entry_ok,
                         f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}",
                         f"one side<={cfg.first_leg_max}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = (f"Neither side cheap enough: "
                             f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f} "
                             f"(need one <={cfg.first_leg_max})")

        # Filter 4b: First-leg floor — don't enter when the ask is already near zero.
        # At these prices the market has priced that side out; bids collapse before loss_cut fires.
        if side in ("up", "down"):
            floor_ok = entry_price >= cfg.first_leg_min
            f = FilterResult("first_leg_floor", floor_ok,
                             f"{entry_price:.3f}", f">={cfg.first_leg_min:.2f}")
        else:
            f = FilterResult("first_leg_floor", True, "n/a", "n/a")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = (f"First leg ask too low: {entry_price:.3f} < floor {cfg.first_leg_min:.2f} "
                             f"(market near-resolved, no recovery possible)")

        # Filter 5: Skip if already an outright arb (dual-leg handles that case)
        combined = up_book.best_ask + down_book.best_ask
        not_already_arb = combined > self.config.dual_leg.max_combined_cost
        f = FilterResult("not_already_arb", not_already_arb,
                         f"combined={combined:.3f}",
                         f">{self.config.dual_leg.max_combined_cost} (dual-leg handles cheaper)")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Already in arb range ({combined:.3f}) — dual-leg preferred"

        # Filter 6: Velocity not extreme — trending markets are bad for mean-reversion
        if agg is not None:
            vel_ok = abs(agg.velocity_30s) <= cfg.max_velocity_30s
            f = FilterResult("coin_velocity", vel_ok,
                             f"30s={agg.velocity_30s:.3f}%",
                             f"<={cfg.max_velocity_30s}%")
        else:
            f = FilterResult("coin_velocity", False, "no price data", "price data required")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"{coin.upper()} trending too hard: {f.value}"

        # Filter 7: Liquidity depth
        f = FilterResult("liquidity_depth", entry_depth >= cfg.min_book_depth_usd,
                         f"${entry_depth:.1f}", f">=${cfg.min_book_depth_usd}")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Thin liquidity: ${entry_depth:.1f}"

        # Filter 8: Feed count
        source_count = agg.source_count if agg else 0
        f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = f"Only {source_count} live feed(s)"

        # Filter 9: Risk limits
        risk_ok = self.risk_manager.can_trade() if self.risk_manager else True
        f = FilterResult("risk_limits", risk_ok, "ok" if risk_ok else "blocked", "ok")
        filters.append(f)
        if not f.passed and not failed_reason:
            failed_reason = "Risk limits breached"

        all_passed = all(f.passed for f in filters)
        action = ("DRY_RUN_SIGNAL" if self.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
        reason = (f"Swing {side.upper()}@{entry_price:.3f} — waiting for other leg"
                  if all_passed else failed_reason)

        self._log_decision(coin, market, up_book, down_book, agg, remaining,
                           filters, action, reason, "swing_leg")

        if not all_passed:
            return None

        signal = TradeSignal(
            market=market,
            strategy_type="swing_leg",
            side=side,
            entry_price=entry_price,
            target_sell_price=cfg.first_leg_exit,
            fee_total=(1.0 - entry_price) * market.fee_rate,
            expected_profit=0.0,   # unknown until second leg is secured
            time_remaining_s=remaining,
            up_book=up_book,
            down_book=down_book,
            filter_results=filters,
        )
        logger.info(
            f"{'[DRY RUN] ' if self.config.execution.dry_run else ''}"
            f"SWING SIGNAL [{coin.upper()}]: BUY {side.upper()}@{entry_price:.3f} "
            f"| other leg={combined - entry_price:.3f} "
            f"| exit if no 2nd leg @{cfg.first_leg_exit:.2f}"
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
