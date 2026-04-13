"""Execution engine — dual-leg FOK state machine + single-leg GTC buy/sell."""

import asyncio
import logging
import math
import time
from datetime import datetime, timezone

import httpx

from bot.config import Config
from bot.models import DualOrderState, OrderBookSnapshot, SingleLegPosition, SwingPosition, TradeResult, TradeSignal
from bot.risk_manager import RiskManager
from bot.tracker import DecisionTracker

logger = logging.getLogger(__name__)


class ExecutionEngine:
    def __init__(self, config: Config, clob_client, risk_manager: RiskManager,
                 tracker: DecisionTracker, scanner=None):
        self.config = config
        self.client = clob_client
        self.risk_manager = risk_manager
        self.tracker = tracker
        self.scanner = scanner  # MarketScanner — used for live WS book cache
        self.state = DualOrderState.IDLE

        # Runtime overrides (set via Telegram without restart)
        self._order_size_usd: float | None = None  # None = use config default

        # Open single-leg positions, keyed by buy_order_id
        self._open_positions: dict[str, SingleLegPosition] = {}

        # Open swing positions, keyed by coin (one per coin at a time)
        self._open_swing_positions: dict[str, SwingPosition] = {}

        # HTTP client for dry-run book price monitoring
        self._http = httpx.AsyncClient(timeout=5.0)

    async def execute(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Dispatch to the appropriate execution path based on strategy type."""
        if signal.strategy_type in ("single_leg", "lead_lag"):
            return await self.execute_single_leg(signal, decision_id)
        if signal.strategy_type == "swing_leg":
            return await self.execute_swing_leg(signal, decision_id)
        return await self._execute_dual_leg(signal, decision_id)

    # -----------------------------------------------------------------------
    # Dual-leg: FOK both sides atomically
    # -----------------------------------------------------------------------

    async def _execute_dual_leg(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Execute a dual-leg arb trade using FOK orders."""
        result = TradeResult(signal=signal, strategy_type="dual_leg")

        if self.config.execution.dry_run:
            result.status = "dry_run"
            result.up_fill_price = signal.up_price
            result.down_fill_price = signal.down_price
            result.total_cost = signal.combined_cost
            result.fee_total = signal.fee_total
            result.shares = self._calc_shares(signal.up_price)
            cost = signal.combined_cost * result.shares
            if self.tracker.has_paper_capital(cost):
                self.tracker.log_paper_trade(
                    signal.market.slug, signal.market.coin, "dual_leg", "both",
                    signal.combined_cost, 1.0, result.shares, signal.fee_total,
                    market_end_time=signal.market.end_time.isoformat(),
                )
                logger.info(
                    f"[DRY RUN] Paper trade: UP@{signal.up_price:.3f} + DOWN@{signal.down_price:.3f} "
                    f"({result.shares:.0f} shares, cost=${cost:.2f}) [{signal.market.coin.upper()}]"
                )
            else:
                logger.info(f"[DRY RUN] Skipped — insufficient paper capital (need ${cost:.2f})")
            return result

        try:
            self.state = DualOrderState.PLACING_FIRST
            shares = self._calc_shares(signal.up_price)
            if shares < 5:
                result.status = "failed"
                result.error = f"Shares too small: {shares} (min 5)"
                logger.warning(result.error)
                return result

            result.shares = shares
            tick_size = signal.market.tick_size
            neg_risk = signal.market.neg_risk

            up_order = await asyncio.to_thread(
                self.client.create_order,
                {"token_id": signal.market.up_token_id, "price": signal.up_price,
                 "size": shares, "side": "BUY"},
                {"tick_size": tick_size, "neg_risk": neg_risk},
            )
            down_order = await asyncio.to_thread(
                self.client.create_order,
                {"token_id": signal.market.down_token_id, "price": signal.down_price,
                 "size": shares, "side": "BUY"},
                {"tick_size": tick_size, "neg_risk": neg_risk},
            )

            # Place UP leg (FOK)
            logger.info(f"[{signal.market.coin.upper()}] Placing UP: {shares} @ {signal.up_price:.3f}")
            up_resp = await asyncio.to_thread(self.client.post_order, up_order, "FOK")

            if not self._is_filled(up_resp):
                result.status = "failed"
                result.error = f"UP order rejected: {up_resp}"
                self.state = DualOrderState.DONE
                logger.warning(result.error)
                return result

            result.up_order_id = up_resp.get("orderID", up_resp.get("id", ""))
            result.up_filled = True
            result.up_fill_price = signal.up_price
            self.state = DualOrderState.FIRST_PLACED

            # Place DOWN leg (FOK)
            self.state = DualOrderState.PLACING_SECOND
            logger.info(f"[{signal.market.coin.upper()}] Placing DOWN: {shares} @ {signal.down_price:.3f}")
            down_resp = await asyncio.to_thread(self.client.post_order, down_order, "FOK")

            if not self._is_filled(down_resp):
                self.state = DualOrderState.ABORTING
                logger.warning("DOWN rejected — aborting, selling UP position...")
                abort_cost = await self._abort_sell(
                    signal.market.up_token_id, shares, signal.up_price,
                    tick_size, neg_risk
                )
                result.status = "partial_abort"
                result.abort_cost = abort_cost
                result.error = f"DOWN rejected: {down_resp}"
                self.state = DualOrderState.DONE
                return result

            result.down_order_id = down_resp.get("orderID", down_resp.get("id", ""))
            result.down_filled = True
            result.down_fill_price = signal.down_price
            result.total_cost = signal.combined_cost
            result.fee_total = signal.fee_total
            result.status = "success"
            self.state = DualOrderState.DONE

            logger.info(
                f"[{signal.market.coin.upper()}] DUAL-LEG SUCCESS: "
                f"UP@{signal.up_price:.3f} + DOWN@{signal.down_price:.3f} "
                f"= {signal.combined_cost:.3f} | {shares:.0f} shares | "
                f"Est. profit: ${signal.expected_profit:.4f}"
            )
            return result

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            logger.error(f"Dual-leg execution error: {e}")

            if result.up_filled and not result.down_filled:
                self.state = DualOrderState.ABORTING
                try:
                    abort_cost = await self._abort_sell(
                        signal.market.up_token_id, result.shares, signal.up_price,
                        signal.market.tick_size, signal.market.neg_risk,
                    )
                    result.abort_cost = abort_cost
                    result.status = "partial_abort"
                except Exception as abort_err:
                    logger.critical(
                        f"ABORT SELL FAILED: {abort_err}. "
                        f"Naked position: {result.shares} UP shares in {signal.market.slug}"
                    )

            self.state = DualOrderState.DONE
            return result

        finally:
            if decision_id and result.status != "dry_run":
                self.tracker.log_trade(decision_id, result)
            if result.status != "dry_run":
                self.risk_manager.record_trade(result)

    # -----------------------------------------------------------------------
    # Single-leg: GTC buy + immediate GTC sell at target
    # -----------------------------------------------------------------------

    async def execute_single_leg(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Buy one side with a GTC limit, immediately place a GTC sell at target price."""
        result = TradeResult(signal=signal, strategy_type="single_leg",
                             side=signal.side, status="open")

        market = signal.market
        coin = market.coin.upper()

        if signal.side == "up":
            token_id = market.up_token_id
        else:
            token_id = market.down_token_id

        order_size = self.order_size_usd
        shares = math.floor(order_size / signal.entry_price)

        if self.config.execution.dry_run:
            result.status = "dry_run"
            result.shares = shares
            result.total_cost = signal.entry_price * shares
            result.fee_total = signal.fee_total
            cost = signal.entry_price * shares
            if self.tracker.has_paper_capital(cost):
                trade_id = self.tracker.log_paper_trade(
                    market.slug, market.coin, "single_leg", signal.side,
                    signal.entry_price, signal.target_sell_price, shares, signal.fee_total,
                    market_end_time=market.end_time.isoformat(),
                )
                logger.info(
                    f"[DRY RUN] [{coin}] Paper trade: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                    f"→ SELL@{signal.target_sell_price:.3f} | {shares} shares, cost=${cost:.2f}"
                )
                asyncio.create_task(self._monitor_paper_single_leg(
                    trade_id=trade_id,
                    market=market,
                    token_id=token_id,
                    entry_price=signal.entry_price,
                    target_sell=signal.target_sell_price,
                    shares=shares,
                ))
            else:
                logger.info(f"[DRY RUN] [{coin}] Skipped — insufficient paper capital (need ${cost:.2f})")
            return result

        if shares < 5:
            result.status = "failed"
            result.error = f"Shares too small: {shares} (min 5)"
            logger.warning(f"[{coin}] {result.error}")
            return result

        result.shares = shares

        try:
            # Place GTC buy limit at the current ask
            buy_order = await asyncio.to_thread(
                self.client.create_order,
                {"token_id": token_id, "price": signal.entry_price,
                 "size": shares, "side": "BUY"},
                {"tick_size": market.tick_size, "neg_risk": market.neg_risk},
            )
            buy_resp = await asyncio.to_thread(self.client.post_order, buy_order, "GTC")
            buy_order_id = buy_resp.get("orderID", buy_resp.get("id", ""))

            if not buy_order_id:
                result.status = "failed"
                result.error = f"Buy order rejected: {buy_resp}"
                logger.warning(f"[{coin}] {result.error}")
                return result

            result.buy_order_id = buy_order_id
            result.total_cost = signal.entry_price * shares

            logger.info(
                f"[{coin}] SINGLE-LEG BUY placed: {shares} {signal.side.upper()} "
                f"@ {signal.entry_price:.3f} (order {buy_order_id})"
            )

            # Place GTC sell at target price
            sell_order = await asyncio.to_thread(
                self.client.create_order,
                {"token_id": token_id, "price": signal.target_sell_price,
                 "size": shares, "side": "SELL"},
                {"tick_size": market.tick_size, "neg_risk": market.neg_risk},
            )
            sell_resp = await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
            sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))

            if sell_order_id:
                result.sell_order_id = sell_order_id
                logger.info(
                    f"[{coin}] SINGLE-LEG SELL placed: {shares} {signal.side.upper()} "
                    f"@ {signal.target_sell_price:.3f} (order {sell_order_id})"
                )
            else:
                logger.warning(f"[{coin}] Sell order placement failed: {sell_resp}. Will hold to resolution.")

            # Track this open position
            position = SingleLegPosition(
                market=market,
                side=signal.side,
                token_id=token_id,
                entry_price=signal.entry_price,
                target_price=signal.target_sell_price,
                shares=shares,
                buy_order_id=buy_order_id,
                sell_order_id=sell_order_id or None,
            )
            self._open_positions[buy_order_id] = position

            # Kick off monitor in background
            asyncio.create_task(self._monitor_single_leg(position, result))

            result.status = "open"
            return result

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            logger.error(f"[{coin}] Single-leg execution error: {e}")
            return result

        finally:
            if decision_id and result.status not in ("dry_run",):
                self.tracker.log_trade(decision_id, result)
            if result.status not in ("dry_run", "failed"):
                self.risk_manager.record_trade(result)

    async def _monitor_single_leg(self, position: SingleLegPosition, result: TradeResult):
        """Monitor an open single-leg live position with smart exit logic.

        Priority order each cycle:
        1. GTC sell already filled → done
        2. High-confidence bid (≥ high_confidence_bid) → cancel sell, hold to resolution
        3. Progressive loss cut → cancel sell, emergency sell now
        4. Near-close fallback (30s) → emergency sell if not holding high-confidence
        5. Market ended → clean up
        """
        coin = position.market.coin.upper()
        cfg = self.config.single_leg
        high_confidence_held = False  # True once we've decided to hold to resolution

        while True:
            await self._wait_book_update()

            now = datetime.now(timezone.utc)
            remaining = (position.market.end_time - now).total_seconds()

            if remaining <= 0:
                self._open_positions.pop(position.buy_order_id, None)
                return

            # ── 1. Check if GTC sell filled ──
            if position.sell_order_id and not high_confidence_held:
                try:
                    order_status = await asyncio.to_thread(
                        self.client.get_order, position.sell_order_id
                    )
                    status = (order_status or {}).get("status", "").lower()
                    if status in ("matched", "filled"):
                        result.status = "success"
                        logger.info(
                            f"[{coin}] SINGLE-LEG SELL FILLED @ {position.target_price:.3f}"
                        )
                        self._open_positions.pop(position.buy_order_id, None)
                        return
                except Exception as e:
                    logger.warning(f"[{coin}] Order status check failed: {e}")

            # ── Read current bid ──
            try:
                book = await self._fetch_book_price(position.token_id)
                bid = book.best_bid
            except Exception:
                continue

            if bid <= 0:
                continue

            loss_pct = (position.entry_price - bid) / position.entry_price
            time_factor = min(1.0, remaining / cfg.time_pressure_s)
            dynamic_loss_cut = cfg.loss_cut_pct * time_factor

            # ── 2. High-confidence: cancel sell, hold to resolution ──
            if bid >= cfg.high_confidence_bid and not high_confidence_held:
                if position.sell_order_id:
                    try:
                        await asyncio.to_thread(self.client.cancel, position.sell_order_id)
                    except Exception as e:
                        logger.warning(f"[{coin}] Cancel sell failed: {e}")
                high_confidence_held = True
                logger.info(
                    f"[{coin}] HIGH-CONFIDENCE @{bid:.3f} — "
                    f"cancelled sell, holding {position.side.upper()} to resolution "
                    f"({remaining:.0f}s left)"
                )
                continue

            # ── 3. Progressive loss cut ──
            if not high_confidence_held and loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                if position.sell_order_id:
                    try:
                        await asyncio.to_thread(self.client.cancel, position.sell_order_id)
                    except Exception:
                        pass
                try:
                    emergency_price = max(0.01, bid - 0.02)
                    sell_order = await asyncio.to_thread(
                        self.client.create_order,
                        {"token_id": position.token_id, "price": emergency_price,
                         "size": position.shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size,
                         "neg_risk": position.market.neg_risk},
                    )
                    await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                    logger.info(
                        f"[{coin}] LOSS CUT @{emergency_price:.3f} "
                        f"(loss={loss_pct:.0%} ≥ {dynamic_loss_cut:.0%}, {remaining:.0f}s left)"
                    )
                except Exception as e:
                    logger.error(f"[{coin}] Loss cut sell failed: {e}")
                self._open_positions.pop(position.buy_order_id, None)
                return

            # ── 4. Near-close fallback ──
            if not high_confidence_held and remaining <= 30:
                if position.sell_order_id:
                    try:
                        await asyncio.to_thread(self.client.cancel, position.sell_order_id)
                    except Exception:
                        pass
                try:
                    emergency_price = max(0.01, bid - 0.01)
                    sell_order = await asyncio.to_thread(
                        self.client.create_order,
                        {"token_id": position.token_id, "price": emergency_price,
                         "size": position.shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size,
                         "neg_risk": position.market.neg_risk},
                    )
                    await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                    logger.info(f"[{coin}] NEAR-CLOSE emergency sell @ {emergency_price:.3f}")
                except Exception as e:
                    logger.error(f"[{coin}] Near-close sell failed: {e}")
                self._open_positions.pop(position.buy_order_id, None)
                return

    # -----------------------------------------------------------------------
    # Swing dual-leg execution
    # -----------------------------------------------------------------------

    async def execute_swing_leg(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Buy first leg of a swing trade and start monitoring for the second leg."""
        result = TradeResult(signal=signal, strategy_type="swing_leg",
                             side=signal.side, status="open")
        market = signal.market
        coin = market.coin.upper()

        # Skip if already have a swing position open for this coin
        if market.coin in self._open_swing_positions:
            result.status = "failed"
            result.error = f"Swing position already open for {coin}"
            logger.debug(result.error)
            return result

        token_id = market.up_token_id if signal.side == "up" else market.down_token_id
        second_token_id = market.down_token_id if signal.side == "up" else market.up_token_id
        second_side = "down" if signal.side == "up" else "up"
        shares = math.floor(self.config.swing_leg.order_size_usd / signal.entry_price)

        if self.config.execution.dry_run:
            result.status = "dry_run"
            result.shares = shares
            result.total_cost = signal.entry_price * shares
            cost = signal.entry_price * shares

            if not self.tracker.has_paper_capital(cost):
                logger.info(f"[DRY RUN] [{coin}] Swing skipped — insufficient paper capital")
                result.status = "failed"
                return result

            trade_id = self.tracker.log_paper_trade(
                market.slug, market.coin, "swing_leg", signal.side,
                signal.entry_price, signal.target_sell_price, shares,
                signal.fee_total * shares,
                market_end_time=market.end_time.isoformat(),
            )
            logger.info(
                f"[DRY RUN] [{coin}] SWING LEG 1: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"| {shares} shares, cost=${cost:.2f} | watching for {second_side.upper()} "
                f"<={self.config.swing_leg.second_leg_max:.2f}"
            )
            position = SwingPosition(
                market=market,
                first_side=signal.side,
                first_token_id=token_id,
                second_token_id=second_token_id,
                first_entry_price=signal.entry_price,
                first_shares=shares,
                first_paper_trade_id=trade_id,
                second_side=second_side,
            )
            self._open_swing_positions[market.coin] = position
            asyncio.create_task(self._monitor_swing_leg(position))
            return result

        # Live mode — place real GTC buy for first leg
        if shares < 5:
            result.status = "failed"
            result.error = f"Shares too small: {shares}"
            return result

        try:
            buy_order = await asyncio.to_thread(
                self.client.create_order,
                {"token_id": token_id, "price": signal.entry_price,
                 "size": shares, "side": "BUY"},
                {"tick_size": market.tick_size, "neg_risk": market.neg_risk},
            )
            buy_resp = await asyncio.to_thread(self.client.post_order, buy_order, "GTC")
            buy_order_id = buy_resp.get("orderID", buy_resp.get("id", ""))

            if not buy_order_id:
                result.status = "failed"
                result.error = f"Swing buy rejected: {buy_resp}"
                logger.warning(f"[{coin}] {result.error}")
                return result

            result.buy_order_id = buy_order_id
            result.total_cost = signal.entry_price * shares
            result.shares = shares

            logger.info(
                f"[{coin}] SWING LEG 1 placed: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"({shares} shares, order {buy_order_id})"
            )
            position = SwingPosition(
                market=market,
                first_side=signal.side,
                first_token_id=token_id,
                second_token_id=second_token_id,
                first_entry_price=signal.entry_price,
                first_shares=shares,
                first_buy_order_id=buy_order_id,
                second_side=second_side,
            )
            self._open_swing_positions[market.coin] = position
            asyncio.create_task(self._monitor_swing_leg(position))

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            logger.error(f"[{coin}] Swing execution error: {e}")

        finally:
            if decision_id and result.status not in ("dry_run", "failed"):
                self.tracker.log_trade(decision_id, result)
            if result.status not in ("dry_run", "failed"):
                self.risk_manager.record_trade(result)

        return result

    async def _monitor_swing_leg(self, position: SwingPosition):
        """
        Monitor a swing position with smart exit logic.

        Phase 1 — first leg only (second_bought=False):
          Priority 1: Second leg dips → buy it (arb complete, hold both to resolution)
          Priority 2: High-confidence bid → hold first leg to resolution (don't sell at target)
          Priority 3: Progressive loss cut → exit based on dynamic time-based threshold
          Priority 4: Target hit → sell first leg at profit

        Phase 2 — both legs held (second_bought=True):
          Dead leg detection: if one leg's bid falls below dead_leg_threshold,
          sell it immediately to recover anything, hold the survivor to resolution.
        """
        cfg = self.config.swing_leg
        coin = position.market.coin.upper()
        is_dry = self.config.execution.dry_run

        while True:
            await self._wait_book_update()

            now = datetime.now(timezone.utc)
            remaining = (position.market.end_time - now).total_seconds()

            if remaining <= 0:
                self._open_swing_positions.pop(position.market.coin, None)
                logger.info(f"[{coin}] Swing monitor ended — market closed")
                return

            try:
                first_book = await self._fetch_book_price(position.first_token_id)
                second_book = await self._fetch_book_price(position.second_token_id)
            except Exception as e:
                logger.debug(f"[{coin}] Swing book fetch error: {e}")
                continue

            if not position.second_bought:
                # ─── Phase 1: First leg only ───
                bid = first_book.best_bid
                loss_pct = (
                    (position.first_entry_price - bid) / position.first_entry_price
                    if bid > 0 else 1.0
                )

                # Priority 1: Second leg cheap → complete the arb
                if second_book.best_ask <= cfg.second_leg_max:
                    combined = position.first_entry_price + second_book.best_ask
                    if combined < 1.0:  # True arb edge
                        second_shares = math.floor(cfg.order_size_usd / second_book.best_ask)
                        fee = (1.0 - second_book.best_ask) * position.market.fee_rate

                        if is_dry:
                            if self.tracker.has_paper_capital(second_book.best_ask * second_shares):
                                trade_id = self.tracker.log_paper_trade(
                                    position.market.slug, position.market.coin, "swing_leg",
                                    position.second_side, second_book.best_ask,
                                    1.0, second_shares, fee * second_shares,
                                    market_end_time=position.market.end_time.isoformat(),
                                )
                                position.second_bought = True
                                position.second_entry_price = second_book.best_ask
                                position.second_shares = second_shares
                                position.second_paper_trade_id = trade_id
                                logger.info(
                                    f"[DRY RUN] [{coin}] SWING LEG 2: BUY "
                                    f"{position.second_side.upper()}@{second_book.best_ask:.3f} "
                                    f"| {second_shares} shares — ARB COMPLETE 🎯 "
                                    f"combined={combined:.3f}"
                                )
                        else:
                            try:
                                buy_order = await asyncio.to_thread(
                                    self.client.create_order,
                                    {"token_id": position.second_token_id,
                                     "price": second_book.best_ask,
                                     "size": second_shares, "side": "BUY"},
                                    {"tick_size": position.market.tick_size,
                                     "neg_risk": position.market.neg_risk},
                                )
                                buy_resp = await asyncio.to_thread(
                                    self.client.post_order, buy_order, "GTC"
                                )
                                buy_order_id = buy_resp.get("orderID", buy_resp.get("id", ""))
                                if buy_order_id:
                                    position.second_bought = True
                                    position.second_entry_price = second_book.best_ask
                                    position.second_shares = second_shares
                                    position.second_buy_order_id = buy_order_id
                                    logger.info(
                                        f"[{coin}] SWING LEG 2 placed: "
                                        f"{position.second_side.upper()}@{second_book.best_ask:.3f} "
                                        f"({second_shares} shares) — ARB COMPLETE 🎯"
                                    )
                            except Exception as e:
                                logger.error(f"[{coin}] Swing leg 2 order failed: {e}")
                        continue  # Keep monitoring with both legs

                # Priority 2: High-confidence — first leg is nearly won, hold to resolution
                if bid >= cfg.high_confidence_bid:
                    logger.info(
                        f"[{coin}] SWING HIGH-CONFIDENCE @{bid:.3f} — "
                        f"holding {position.first_side.upper()} to resolution ({remaining:.0f}s)"
                    )
                    # Paper: leave trade open — outcome tracker resolves at $1
                    # Live: no pending sell order to cancel on first leg in swing mode
                    self._open_swing_positions.pop(position.market.coin, None)
                    return

                # Priority 3: Progressive loss cut
                # Threshold = loss_cut_pct when plenty of time, shrinks to 0 at close
                time_factor = min(1.0, remaining / cfg.time_pressure_s)
                dynamic_loss_cut = cfg.loss_cut_pct * time_factor

                if loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                    exit_bid = max(bid, 0.01)
                    fee_val = (1.0 - exit_bid) * position.market.fee_rate * position.first_shares
                    pnl = (exit_bid - position.first_entry_price) * position.first_shares - fee_val
                    status = "closed_win" if pnl > 0 else "closed_loss"
                    logger.info(
                        f"[{coin}] SWING LOSS CUT @{exit_bid:.3f} "
                        f"(loss={loss_pct:.0%} ≥ {dynamic_loss_cut:.0%}, {remaining:.0f}s) "
                        f"pnl=${pnl:+.4f}"
                    )
                    if is_dry and position.first_paper_trade_id:
                        self.tracker.close_paper_trade_early(
                            position.first_paper_trade_id, exit_bid, pnl, status,
                            exit_reason="loss_cut", time_remaining_s=remaining, bid_at_exit=exit_bid,
                        )
                    else:
                        try:
                            sell_order = await asyncio.to_thread(
                                self.client.create_order,
                                {"token_id": position.first_token_id,
                                 "price": max(0.01, exit_bid - 0.02),
                                 "size": position.first_shares, "side": "SELL"},
                                {"tick_size": position.market.tick_size,
                                 "neg_risk": position.market.neg_risk},
                            )
                            await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                        except Exception as e:
                            logger.error(f"[{coin}] Swing loss cut sell failed: {e}")
                    self._open_swing_positions.pop(position.market.coin, None)
                    return

                # Priority 4: Any net-positive exit after fees → sell first leg
                fee_buy = (1.0 - position.first_entry_price) * position.market.fee_rate
                fee_sell = (1.0 - bid) * position.market.fee_rate
                net_pnl = (bid - position.first_entry_price - fee_buy - fee_sell) * position.first_shares
                if net_pnl > 0:
                    fee_val = fee_sell * position.first_shares
                    pnl = net_pnl
                    logger.info(
                        f"[{coin}] SWING NET-POSITIVE EXIT @{bid:.3f} pnl=${pnl:+.4f}"
                    )
                    if is_dry and position.first_paper_trade_id:
                        self.tracker.close_paper_trade_early(
                            position.first_paper_trade_id, bid, pnl,
                            "closed_win" if pnl > 0 else "closed_loss",
                            exit_reason="profit_target", time_remaining_s=remaining, bid_at_exit=bid,
                        )
                    else:
                        try:
                            sell_order = await asyncio.to_thread(
                                self.client.create_order,
                                {"token_id": position.first_token_id,
                                 "price": bid, "size": position.first_shares, "side": "SELL"},
                                {"tick_size": position.market.tick_size,
                                 "neg_risk": position.market.neg_risk},
                            )
                            await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                        except Exception as e:
                            logger.error(f"[{coin}] Swing target sell failed: {e}")
                    self._open_swing_positions.pop(position.market.coin, None)
                    return

            else:
                # ─── Phase 2: Both legs held ───
                # Dead leg detection: one leg's bid has collapsed → sell it, hold the other.
                # This converts the dual position into a cleaner single-leg hold-to-resolution.
                first_bid = first_book.best_bid
                second_bid = second_book.best_bid

                # Guard prevents re-triggering after leg already sold:
                # Paper: first_paper_trade_id is set to None after selling
                # Live:  first_buy_order_id is set to "" after selling
                first_open = (
                    (is_dry and position.first_paper_trade_id is not None) or
                    (not is_dry and bool(position.first_buy_order_id))
                )
                second_open = (
                    (is_dry and position.second_paper_trade_id is not None) or
                    (not is_dry and bool(position.second_buy_order_id))
                )

                first_dead = first_bid <= cfg.dead_leg_threshold and first_open
                second_dead = second_bid <= cfg.dead_leg_threshold and second_open

                if first_dead:
                    fee_val = (
                        (1.0 - first_bid) * position.market.fee_rate * position.first_shares
                    )
                    pnl = (
                        (first_bid - position.first_entry_price) * position.first_shares - fee_val
                    )
                    logger.info(
                        f"[{coin}] DEAD LEG (first@{first_bid:.3f}) — "
                        f"selling, holding {position.second_side.upper()} to resolution"
                    )
                    if is_dry:
                        self.tracker.close_paper_trade_early(
                            position.first_paper_trade_id, first_bid, pnl, "closed_loss",
                            exit_reason="dead_leg", time_remaining_s=remaining, bid_at_exit=first_bid,
                        )
                        position.first_paper_trade_id = None  # prevent re-trigger
                    else:
                        try:
                            sell_order = await asyncio.to_thread(
                                self.client.create_order,
                                {"token_id": position.first_token_id,
                                 "price": max(0.01, first_bid),
                                 "size": position.first_shares, "side": "SELL"},
                                {"tick_size": position.market.tick_size,
                                 "neg_risk": position.market.neg_risk},
                            )
                            await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                        except Exception as e:
                            logger.error(f"[{coin}] Dead leg (first) sell failed: {e}")
                        position.first_buy_order_id = ""  # prevent re-trigger

                elif second_dead:
                    fee_val = (
                        (1.0 - second_bid) * position.market.fee_rate
                        * (position.second_shares or 0.0)
                    )
                    pnl = (
                        (second_bid - position.second_entry_price)
                        * (position.second_shares or 0.0) - fee_val
                    )
                    logger.info(
                        f"[{coin}] DEAD LEG (second@{second_bid:.3f}) — "
                        f"selling, holding {position.first_side.upper()} to resolution"
                    )
                    if is_dry:
                        self.tracker.close_paper_trade_early(
                            position.second_paper_trade_id, second_bid, pnl, "closed_loss",
                            exit_reason="dead_leg", time_remaining_s=remaining, bid_at_exit=second_bid,
                        )
                        position.second_paper_trade_id = None  # prevent re-trigger
                    else:
                        try:
                            sell_order = await asyncio.to_thread(
                                self.client.create_order,
                                {"token_id": position.second_token_id,
                                 "price": max(0.01, second_bid),
                                 "size": position.second_shares, "side": "SELL"},
                                {"tick_size": position.market.tick_size,
                                 "neg_risk": position.market.neg_risk},
                            )
                            await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                        except Exception as e:
                            logger.error(f"[{coin}] Dead leg (second) sell failed: {e}")
                        position.second_buy_order_id = ""  # prevent re-trigger

                # Both legs healthy (or one just sold) — hold to resolution

    # -----------------------------------------------------------------------
    # Paper position profit monitor (dry-run only)
    # -----------------------------------------------------------------------

    async def _monitor_paper_single_leg(self, trade_id: int, market,
                                         token_id: str, entry_price: float,
                                         target_sell: float, shares: float):
        """Watch a paper trade's live bid — progressive loss cutting and smart profit taking.

        Priority order each cycle:
        1. High-confidence bid (≥ high_confidence_bid) → hold to resolution for $1
        2. Any net-positive exit (fee-adjusted) → sell now, don't wait for a big move
        3. Progressive loss cut → exit based on dynamic time-based threshold
        4. Near-close fallback → exit if any profit at ≤30s remaining
        """
        cfg = self.config.single_leg
        coin = market.coin.upper()

        while True:
            await self._wait_book_update()

            now = datetime.now(timezone.utc)
            remaining = (market.end_time - now).total_seconds()

            if remaining <= 0:
                return  # Market ended — outcome tracker resolves it at $1 or $0

            try:
                book = await self._fetch_book_price(token_id)
                bid = book.best_bid
                if bid <= 0:
                    continue

                loss_pct = (entry_price - bid) / entry_price      # positive = losing

                # ── 1. High-confidence: nearly decided — let resolution pay $1 ──
                if bid >= cfg.high_confidence_bid:
                    logger.info(
                        f"[{coin}] Paper HIGH-CONFIDENCE @{bid:.3f} — "
                        f"holding to resolution ({remaining:.0f}s)"
                    )
                    return  # outcome tracker closes this at $1 (or $0 if surprised)

                # ── 2. Any net-positive exit — sell as soon as we're ahead after fees ──
                # Don't wait for the fixed target_sell price. A small confirmed profit
                # is always better than risking a loss by holding longer.
                fee_buy = (1.0 - entry_price) * market.fee_rate
                fee_sell = (1.0 - bid) * market.fee_rate
                net_pnl = (bid - entry_price - fee_buy - fee_sell) * shares
                if net_pnl > 0:
                    self.tracker.close_paper_trade_early(
                        trade_id, bid, net_pnl, "closed_win",
                        exit_reason="profit_target", time_remaining_s=remaining, bid_at_exit=bid,
                    )
                    logger.info(
                        f"[{coin}] Paper SELL @{bid:.3f} (net pnl=${net_pnl:+.4f}, {remaining:.0f}s left)"
                    )
                    return

                # ── 3. Progressive loss cut ──
                # Full threshold (loss_cut_pct) applies when time > time_pressure_s.
                # Shrinks linearly to 0 at close, forcing exit on increasingly smaller losses.
                time_factor = min(1.0, remaining / cfg.time_pressure_s)
                dynamic_loss_cut = cfg.loss_cut_pct * time_factor

                if loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                    fee = (1.0 - bid) * market.fee_rate
                    pnl = (bid - entry_price) * shares - fee * shares
                    status = "closed_win" if pnl > 0 else "closed_loss"
                    self.tracker.close_paper_trade_early(
                        trade_id, bid, pnl, status,
                        exit_reason="loss_cut", time_remaining_s=remaining, bid_at_exit=bid,
                    )
                    logger.info(
                        f"[{coin}] Paper LOSS CUT @{bid:.3f} "
                        f"(loss={loss_pct:.0%} >= {dynamic_loss_cut:.0%}, {remaining:.0f}s) "
                        f"pnl=${pnl:+.4f}"
                    )
                    return

                # ── 4. Near-close fallback: exit any position at ≤30s to avoid unknown outcome ──
                if remaining <= 30:
                    fee = (1.0 - bid) * market.fee_rate
                    pnl = (bid - entry_price) * shares - fee * shares
                    status = "closed_win" if pnl > 0 else "closed_loss"
                    self.tracker.close_paper_trade_early(
                        trade_id, bid, pnl, status,
                        exit_reason="near_close", time_remaining_s=remaining, bid_at_exit=bid,
                    )
                    logger.info(
                        f"[{coin}] Paper NEAR-CLOSE exit @{bid:.3f} pnl=${pnl:+.4f}"
                    )
                    return

            except Exception as e:
                logger.debug(f"[{coin}] Paper monitor error: {e}")

    async def _wait_book_update(self):
        """Suspend until the next WebSocket book push — falls back to 1s sleep if no scanner."""
        if self.scanner and self.scanner._ws_feed.is_connected():
            await self.scanner._ws_feed.wait_any_update()
        else:
            await asyncio.sleep(1)

    async def _fetch_book_price(self, token_id: str) -> OrderBookSnapshot:
        """Return book for a token — WS cache first, HTTP fallback if not yet populated."""
        if self.scanner:
            book = self.scanner.get_book_for_token(token_id)
            if book is not None:
                return book

        # HTTP fallback (only fires until WS cache is warm)
        url = f"{self.config.polymarket.clob_base_url}/book"
        resp = await self._http.get(url, params={"token_id": token_id})
        resp.raise_for_status()
        data = resp.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = float(bids[-1]["price"]) if bids else 0.0
        best_ask = float(asks[-1]["price"]) if asks else 1.0
        return OrderBookSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_depth_usd=0.0,
            ask_depth_usd=0.0,
            timestamp=time.time(),
        )

    # -----------------------------------------------------------------------
    # Shared helpers
    # -----------------------------------------------------------------------

    async def _abort_sell(self, token_id: str, shares: float, entry_price: float,
                          tick_size: str, neg_risk: bool) -> float:
        """Emergency sell to exit a one-legged dual-leg position."""
        sell_price = max(0.01, entry_price - 0.02)
        try:
            sell_order = await asyncio.to_thread(
                self.client.create_order,
                {"token_id": token_id, "price": sell_price, "size": shares, "side": "SELL"},
                {"tick_size": tick_size, "neg_risk": neg_risk},
            )
            resp = await asyncio.to_thread(self.client.post_order, sell_order, "FOK")

            if self._is_filled(resp):
                abort_cost = (entry_price - sell_price) * shares
                fee = (1.0 - sell_price) * 0.072 * shares
                abort_cost += fee
                logger.info(f"Abort sell filled. Cost: ${abort_cost:.4f}")
                return abort_cost
            else:
                logger.warning("Abort sell rejected. Naked position remains — will resolve at close.")
                return entry_price * shares
        except Exception as e:
            logger.critical(f"Abort sell exception: {e}")
            return entry_price * shares

    @property
    def order_size_usd(self) -> float:
        return self._order_size_usd if self._order_size_usd is not None else self.config.execution.order_size_usd

    def set_order_size(self, usd: float):
        self._order_size_usd = usd
        logger.info(f"Order size set to ${usd:.2f}")

    def _calc_shares(self, price: float) -> float:
        if price <= 0:
            return 0
        return math.floor(self.order_size_usd / price)

    @staticmethod
    def _is_filled(response: dict) -> bool:
        if not response:
            return False
        return response.get("status", "").lower() in ("matched", "filled", "live")

    def get_open_positions(self) -> dict[str, SingleLegPosition]:
        return dict(self._open_positions)
