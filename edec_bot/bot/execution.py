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
            result.shares_requested = result.shares
            result.shares_filled = result.shares
            cost = signal.combined_cost * result.shares
            if self.tracker.has_paper_capital(cost):
                self.tracker.log_paper_trade(
                    signal.market.slug, signal.market.coin, "dual_leg", "both",
                    signal.combined_cost, 1.0, result.shares, signal.fee_total,
                    decision_id=signal.decision_id or decision_id,
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
            result.shares_requested = shares
            if shares < 5:
                result.status = "failed"
                result.blocked_min_5_shares = True
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
        """Buy one side with a GTC limit and monitor it with strategy-specific exits."""
        result = TradeResult(signal=signal, strategy_type=signal.strategy_type,
                             side=signal.side, status="open")

        market = signal.market
        coin = market.coin.upper()
        token_id = market.up_token_id if signal.side == "up" else market.down_token_id

        order_size = self.order_size_usd
        shares = math.floor(order_size / signal.entry_price)
        result.shares_requested = shares
        result.shares_filled = shares

        if self.config.execution.dry_run:
            result.status = "dry_run"
            result.shares = shares
            result.shares_filled = shares
            result.total_cost = signal.entry_price * shares
            result.fee_total = signal.fee_total
            cost = signal.entry_price * shares
            if self.tracker.has_paper_capital(cost):
                trade_id = self.tracker.log_paper_trade(
                    market.slug, market.coin, signal.strategy_type, signal.side,
                    signal.entry_price, signal.target_sell_price, shares, signal.fee_total,
                    decision_id=signal.decision_id or decision_id,
                    market_end_time=market.end_time.isoformat(),
                    market_start_time=market.start_time.isoformat(),
                    signal_context=signal.signal_context,
                    signal_overlap_count=signal.signal_overlap_count,
                    order_size_usd=order_size,
                    shares_requested=shares,
                    shares_filled=shares,
                    blocked_min_5_shares=False,
                    entry_bid=signal.entry_bid,
                    entry_ask=signal.entry_ask or signal.entry_price,
                    entry_spread=signal.entry_spread,
                    entry_depth_side_usd=signal.entry_depth_side_usd,
                    opposite_depth_usd=signal.opposite_depth_usd,
                    depth_ratio=signal.depth_ratio,
                    window_id=market.slug,
                    signal_score=signal.signal_score,
                    score_velocity=signal.score_velocity,
                    score_entry=signal.score_entry,
                    score_depth=signal.score_depth,
                    score_spread=signal.score_spread,
                    score_time=signal.score_time,
                    score_balance=signal.score_balance,
                    target_delta=signal.target_delta,
                    hard_stop_delta=signal.hard_stop_delta,
                )
                logger.info(
                    f"[DRY RUN] [{coin}] Paper trade ({signal.strategy_type}): "
                    f"BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                    f"-> EXIT@{signal.target_sell_price:.3f} | score={signal.signal_score:.1f} "
                    f"| {shares} shares, cost=${cost:.2f}"
                )
                asyncio.create_task(self._monitor_paper_single_leg(
                    trade_id=trade_id,
                    market=market,
                    token_id=token_id,
                    entry_price=signal.entry_price,
                    target_sell=signal.target_sell_price,
                    shares=shares,
                    strategy_type=signal.strategy_type,
                ))
            else:
                logger.info(f"[DRY RUN] [{coin}] Skipped - insufficient paper capital (need ${cost:.2f})")
            return result

        if shares < 5:
            result.status = "failed"
            result.blocked_min_5_shares = True
            result.error = f"Shares too small: {shares} (min 5)"
            logger.warning(f"[{coin}] {result.error}")
            return result

        result.shares = shares
        result.shares_filled = shares

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
                result.error = f"Buy order rejected: {buy_resp}"
                logger.warning(f"[{coin}] {result.error}")
                return result

            result.buy_order_id = buy_order_id
            result.total_cost = signal.entry_price * shares

            if signal.strategy_type == "lead_lag":
                logger.info(
                    f"[{coin}] LEAD-LAG BUY placed: {shares} {signal.side.upper()} "
                    f"@ {signal.entry_price:.3f} (order {buy_order_id}) - target@{signal.target_sell_price:.3f}"
                )
            else:
                logger.info(
                    f"[{coin}] SINGLE-LEG BUY placed: {shares} {signal.side.upper()} "
                    f"@ {signal.entry_price:.3f} (order {buy_order_id}) - scalp@"
                    f"{signal.target_sell_price:.2f}, runner@{self.config.single_leg.high_confidence_bid:.2f}"
                )

            position = SingleLegPosition(
                market=market,
                side=signal.side,
                token_id=token_id,
                entry_price=signal.entry_price,
                target_price=signal.target_sell_price,
                shares=shares,
                buy_order_id=buy_order_id,
                sell_order_id=None,
                strategy_type=signal.strategy_type,
                decision_id=decision_id,
            )
            self._open_positions[buy_order_id] = position
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

    def _lead_lag_params(self, coin: str) -> dict[str, float]:
        cfg = self.config.lead_lag
        coin_key = (coin or "").lower()
        override = cfg.coin_overrides.get(coin_key)
        return {
            "profit_take_delta": cfg.profit_take_delta,
            "profit_take_cap": cfg.profit_take_cap,
            "stall_window_s": cfg.stall_window_s,
            "min_progress_delta": cfg.min_progress_delta,
            "hard_stop_loss_pct": cfg.hard_stop_loss_pct,
            "min_velocity_30s": override.min_velocity_30s if override and override.min_velocity_30s is not None else cfg.min_velocity_30s,
            "min_entry": override.min_entry if override and override.min_entry is not None else cfg.min_entry,
            "max_entry": override.max_entry if override and override.max_entry is not None else cfg.max_entry,
            "min_book_depth_usd": override.min_book_depth_usd if override and override.min_book_depth_usd is not None else cfg.min_book_depth_usd,
        }

    @staticmethod
    def _net_pnl(entry_price: float, bid: float, fee_rate: float, shares: float) -> float:
        fee_buy = ExecutionEngine._per_share_fee(entry_price, fee_rate)
        fee_sell = ExecutionEngine._per_share_fee(bid, fee_rate)
        return (bid - entry_price - fee_buy - fee_sell) * shares

    def _dynamic_single_leg_loss_cut(self, remaining: float) -> float:
        cfg = self.config.single_leg
        time_factor = min(cfg.loss_cut_max_factor, remaining / cfg.time_pressure_s)
        return cfg.loss_cut_pct * time_factor

    def _lead_lag_exit_reason(
        self,
        *,
        coin: str,
        entry_price: float,
        target_price: float,
        bid: float,
        remaining: float,
        elapsed_s: float,
        max_bid_seen: float | None,
        ever_profitable: bool,
        shares: float,
        fee_rate: float,
    ) -> tuple[str | None, float, float]:
        params = self._lead_lag_params(coin)
        net_pnl = self._net_pnl(entry_price, bid, fee_rate, shares)
        favorable_excursion = max(0.0, (max_bid_seen if max_bid_seen is not None else bid) - entry_price)
        loss_pct = max(0.0, (entry_price - bid) / max(entry_price, 1e-9))
        if bid >= target_price and net_pnl > 0:
            return "profit_target", net_pnl, loss_pct
        if elapsed_s >= params["stall_window_s"] and (favorable_excursion < params["min_progress_delta"] or not ever_profitable):
            return "stall_exit", net_pnl, loss_pct
        if loss_pct >= params["hard_stop_loss_pct"]:
            return "loss_cut", net_pnl, loss_pct
        if remaining <= 30:
            return "near_close", net_pnl, loss_pct
        return None, net_pnl, loss_pct

    @staticmethod
    def _live_exit_price(bid: float, exit_reason: str) -> float:
        discount = 0.02 if exit_reason == "loss_cut" else 0.01
        return max(0.01, bid - discount)

    async def _monitor_single_leg(self, position: SingleLegPosition, result: TradeResult):
        """Monitor an open single-leg or lead-lag live position with strategy-specific exits."""
        coin = position.market.coin.upper()
        cfg = self.config.single_leg
        high_confidence_held = False
        monitor_started_at = datetime.now(timezone.utc)
        max_bid_seen = None
        ever_profitable = False

        while True:
            await self._wait_book_update()

            now = datetime.now(timezone.utc)
            remaining = (position.market.end_time - now).total_seconds()
            if remaining <= 0:
                self._open_positions.pop(position.buy_order_id, None)
                return

            if position.sell_order_id and not high_confidence_held:
                try:
                    order_status = await asyncio.to_thread(self.client.get_order, position.sell_order_id)
                    status = (order_status or {}).get("status", "").lower()
                    if status in ("matched", "filled"):
                        result.status = "success"
                        logger.info(f"[{coin}] {position.strategy_type.upper()} SELL FILLED @ {position.target_price:.3f}")
                        self._open_positions.pop(position.buy_order_id, None)
                        return
                except Exception as e:
                    logger.warning(f"[{coin}] Order status check failed: {e}")

            try:
                book = await self._fetch_book_price(position.token_id)
                bid = book.best_bid
            except Exception:
                continue

            if bid <= 0:
                continue

            elapsed_s = max(0.0, (datetime.now(timezone.utc) - monitor_started_at).total_seconds())
            if max_bid_seen is None or bid > max_bid_seen:
                max_bid_seen = bid

            if position.strategy_type == "lead_lag":
                exit_reason, net_pnl, loss_pct = self._lead_lag_exit_reason(
                    coin=position.market.coin,
                    entry_price=position.entry_price,
                    target_price=position.target_price,
                    bid=bid,
                    remaining=remaining,
                    elapsed_s=elapsed_s,
                    max_bid_seen=max_bid_seen,
                    ever_profitable=ever_profitable,
                    shares=position.shares,
                    fee_rate=position.market.fee_rate,
                )
                ever_profitable = ever_profitable or net_pnl > 0
                if exit_reason:
                    try:
                        exit_price = self._live_exit_price(bid, exit_reason)
                        sell_order = await asyncio.to_thread(
                            self.client.create_order,
                            {"token_id": position.token_id, "price": exit_price,
                             "size": position.shares, "side": "SELL"},
                            {"tick_size": position.market.tick_size,
                             "neg_risk": position.market.neg_risk},
                        )
                        sell_resp = await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                        position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                        result.sell_order_id = position.sell_order_id
                        logger.info(
                            f"[{coin}] LEAD-LAG {exit_reason.upper()} @{exit_price:.3f} "
                            f"(bid={bid:.3f}, pnl=${net_pnl:+.4f}, loss={loss_pct:.1%}, {remaining:.0f}s left)"
                        )
                    except Exception as e:
                        logger.error(f"[{coin}] Lead-lag sell failed: {e}")
                    self._open_positions.pop(position.buy_order_id, None)
                    return
                continue

            loss_pct = (position.entry_price - bid) / position.entry_price
            net_pnl = self._net_pnl(position.entry_price, bid, position.market.fee_rate, position.shares)
            dynamic_loss_cut = self._dynamic_single_leg_loss_cut(remaining)

            if (
                bid >= cfg.scalp_take_profit_bid
                and bid < cfg.high_confidence_bid
                and net_pnl >= cfg.scalp_min_profit_usd
            ):
                try:
                    scalp_price = self._live_exit_price(bid, "profit_target")
                    sell_order = await asyncio.to_thread(
                        self.client.create_order,
                        {"token_id": position.token_id, "price": scalp_price,
                         "size": position.shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size,
                         "neg_risk": position.market.neg_risk},
                    )
                    await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                    logger.info(
                        f"[{coin}] SCALP EXIT @{scalp_price:.3f} "
                        f"(net pnl=${net_pnl:+.4f}, target>={cfg.scalp_take_profit_bid:.2f})"
                    )
                except Exception as e:
                    logger.error(f"[{coin}] Scalp sell failed: {e}")
                self._open_positions.pop(position.buy_order_id, None)
                return

            if bid >= cfg.high_confidence_bid and not high_confidence_held:
                if position.sell_order_id:
                    try:
                        await asyncio.to_thread(self.client.cancel, position.sell_order_id)
                    except Exception as e:
                        logger.warning(f"[{coin}] Cancel sell failed: {e}")
                high_confidence_held = True
                logger.info(
                    f"[{coin}] HIGH-CONFIDENCE @{bid:.3f} - "
                    f"holding {position.side.upper()} to resolution ({remaining:.0f}s left)"
                )
                continue

            if not high_confidence_held and loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                if position.sell_order_id:
                    try:
                        await asyncio.to_thread(self.client.cancel, position.sell_order_id)
                    except Exception:
                        pass
                try:
                    emergency_price = self._live_exit_price(bid, "loss_cut")
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
                        f"(loss={loss_pct:.0%} >= {dynamic_loss_cut:.0%}, {remaining:.0f}s left)"
                    )
                except Exception as e:
                    logger.error(f"[{coin}] Loss cut sell failed: {e}")
                self._open_positions.pop(position.buy_order_id, None)
                return

            if not high_confidence_held and remaining <= 30:
                if position.sell_order_id:
                    try:
                        await asyncio.to_thread(self.client.cancel, position.sell_order_id)
                    except Exception:
                        pass
                try:
                    emergency_price = self._live_exit_price(bid, "near_close")
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
        shares = math.floor(self.config.swing_leg.order_size_usd / signal.entry_price)
        result.shares_requested = shares
        result.shares_filled = shares

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
                decision_id=signal.decision_id or decision_id,
                market_end_time=market.end_time.isoformat(),
                market_start_time=market.start_time.isoformat(),
                signal_context=signal.signal_context,
                signal_overlap_count=signal.signal_overlap_count,
                order_size_usd=self.config.swing_leg.order_size_usd,
                shares_requested=shares,
                shares_filled=shares,
                entry_bid=signal.entry_bid,
                entry_ask=signal.entry_ask or signal.entry_price,
                entry_spread=signal.entry_spread,
                entry_depth_side_usd=signal.entry_depth_side_usd,
                opposite_depth_usd=signal.opposite_depth_usd,
                depth_ratio=signal.depth_ratio,
                window_id=market.slug,
                signal_score=signal.signal_score,
                score_velocity=signal.score_velocity,
                score_entry=signal.score_entry,
                score_depth=signal.score_depth,
                score_spread=signal.score_spread,
                score_time=signal.score_time,
                score_balance=signal.score_balance,
                target_delta=signal.target_delta,
                hard_stop_delta=signal.hard_stop_delta,
            )
            logger.info(
                f"[DRY RUN] [{coin}] SWING: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"| {shares} shares, cost=${cost:.2f} | exit target@{signal.target_sell_price:.2f}"
            )
            position = SwingPosition(
                market=market,
                first_side=signal.side,
                first_token_id=token_id,
                first_entry_price=signal.entry_price,
                first_shares=shares,
                first_paper_trade_id=trade_id,
            )
            self._open_swing_positions[market.coin] = position
            asyncio.create_task(self._monitor_swing_leg(position))
            return result

        # Live mode — place real GTC buy for first leg
        if shares < 5:
            result.status = "failed"
            result.blocked_min_5_shares = True
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
            result.shares_filled = shares

            logger.info(
                f"[{coin}] SWING LEG 1 placed: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"({shares} shares, order {buy_order_id})"
            )
            position = SwingPosition(
                market=market,
                first_side=signal.side,
                first_token_id=token_id,
                first_entry_price=signal.entry_price,
                first_shares=shares,
                first_buy_order_id=buy_order_id,
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
        Monitor a swing mean-reversion position with smart exit logic.

        Buy one cheap side in a calm market → sell when it bounces.
        Priority order each cycle:
          Priority 1: High-confidence bid (≥ high_confidence_bid) → hold to resolution
          Priority 2: Progressive loss cut → dynamic threshold based on time remaining
          Priority 3: Any net-positive exit after fees → sell now
          Priority 4: Near-close (≤30s) → exit regardless of P&L
        """
        cfg = self.config.swing_leg
        coin = position.market.coin.upper()
        is_dry = self.config.execution.dry_run
        monitor_started_at = datetime.now(timezone.utc)
        max_bid_seen = None
        min_bid_seen = None
        time_to_max_bid_s = None
        time_to_min_bid_s = None
        first_profit_time_s = None
        high_confidence_hit = False

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
            except Exception as e:
                logger.debug(f"[{coin}] Swing book fetch error: {e}")
                continue

            # ─── Mean-reversion monitoring ───
            bid = first_book.best_bid
            ask = first_book.best_ask
            loss_pct = (
                (position.first_entry_price - bid) / position.first_entry_price
                if bid > 0 else 1.0
            )
            elapsed_s = max(0.0, (datetime.now(timezone.utc) - monitor_started_at).total_seconds())
            changed = False
            if bid > 0 and (max_bid_seen is None or bid > max_bid_seen):
                max_bid_seen = bid
                time_to_max_bid_s = elapsed_s
                changed = True
            if bid > 0 and (min_bid_seen is None or bid < min_bid_seen):
                min_bid_seen = bid
                time_to_min_bid_s = elapsed_s
                changed = True

            # Priority 1: High-confidence — first leg is nearly won, hold to resolution
            if bid >= cfg.high_confidence_bid:
                high_confidence_hit = True
                if is_dry and position.first_paper_trade_id:
                    self.tracker.record_paper_trade_path(
                        position.first_paper_trade_id,
                        max_bid_seen=max_bid_seen,
                        min_bid_seen=min_bid_seen,
                        time_to_max_bid_s=time_to_max_bid_s,
                        time_to_min_bid_s=time_to_min_bid_s,
                        first_profit_time_s=first_profit_time_s,
                        high_confidence_hit=True,
                    )
                logger.info(
                    f"[{coin}] SWING HIGH-CONFIDENCE @{bid:.3f} — "
                    f"holding {position.first_side.upper()} to resolution ({remaining:.0f}s)"
                )
                self._open_swing_positions.pop(position.market.coin, None)
                return

            # Priority 2: Dynamic loss cut — wider early, tightens to 0 at close
            # time_factor = remaining/time_pressure_s, capped at loss_cut_max_factor
            # e.g. at 180s with defaults: factor=2.0 → cut=50%; at 90s: cut=25%; at 0s: 0
            time_factor = min(cfg.loss_cut_max_factor, remaining / cfg.time_pressure_s)
            dynamic_loss_cut = cfg.loss_cut_pct * time_factor

            if first_profit_time_s is None:
                fee_buy = self._per_share_fee(position.first_entry_price, position.market.fee_rate)
                fee_sell = self._per_share_fee(bid, position.market.fee_rate)
                net_pnl_probe = (bid - position.first_entry_price - fee_buy - fee_sell) * position.first_shares
                if net_pnl_probe > 0:
                    first_profit_time_s = elapsed_s
                    changed = True

            if is_dry and position.first_paper_trade_id and changed:
                self.tracker.record_paper_trade_path(
                    position.first_paper_trade_id,
                    max_bid_seen=max_bid_seen,
                    min_bid_seen=min_bid_seen,
                    time_to_max_bid_s=time_to_max_bid_s,
                    time_to_min_bid_s=time_to_min_bid_s,
                    first_profit_time_s=first_profit_time_s,
                    high_confidence_hit=high_confidence_hit,
                )

            if loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                exit_bid = max(bid, 0.01)
                fee_val = self._per_share_fee(exit_bid, position.market.fee_rate) * position.first_shares
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
                        exit_reason="loss_cut", time_remaining_s=remaining,
                        bid_at_exit=exit_bid, ask_at_exit=ask,
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

            # Priority 3: Any net-positive exit after fees → sell first leg
            fee_buy = self._per_share_fee(position.first_entry_price, position.market.fee_rate)
            fee_sell = self._per_share_fee(bid, position.market.fee_rate)
            net_pnl = (bid - position.first_entry_price - fee_buy - fee_sell) * position.first_shares
            if net_pnl > 0:
                pnl = net_pnl
                logger.info(
                    f"[{coin}] SWING NET-POSITIVE EXIT @{bid:.3f} pnl=${pnl:+.4f}"
                )
                if is_dry and position.first_paper_trade_id:
                    self.tracker.close_paper_trade_early(
                        position.first_paper_trade_id, bid, pnl,
                        "closed_win" if pnl > 0 else "closed_loss",
                        exit_reason="profit_target", time_remaining_s=remaining,
                        bid_at_exit=bid, ask_at_exit=ask,
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

            # Priority 4: Near-close (≤30s) — exit regardless of P&L
            if remaining <= 30:
                fee_val = self._per_share_fee(bid, position.market.fee_rate) * position.first_shares
                pnl = (bid - position.first_entry_price) * position.first_shares - fee_val
                status = "closed_win" if pnl > 0 else "closed_loss"
                logger.info(
                    f"[{coin}] SWING NEAR-CLOSE exit @{bid:.3f} pnl=${pnl:+.4f} ({remaining:.0f}s)"
                )
                if is_dry and position.first_paper_trade_id:
                    self.tracker.close_paper_trade_early(
                        position.first_paper_trade_id, bid, pnl, status,
                        exit_reason="near_close", time_remaining_s=remaining,
                        bid_at_exit=bid, ask_at_exit=ask,
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
                        logger.error(f"[{coin}] Swing near-close sell failed: {e}")
                self._open_swing_positions.pop(position.market.coin, None)
                return

    # -----------------------------------------------------------------------
    # Paper position profit monitor (dry-run only)
    # -----------------------------------------------------------------------

    async def _monitor_paper_single_leg(self, trade_id: int, market,
                                         token_id: str, entry_price: float,
                                         target_sell: float, shares: float,
                                         strategy_type: str = "single_leg"):
        """Watch a paper trade's live bid and apply the same exit structure as runtime logic."""
        cfg = self.config.single_leg
        coin = market.coin.upper()
        monitor_started_at = datetime.now(timezone.utc)
        max_bid_seen = None
        min_bid_seen = None
        time_to_max_bid_s = None
        time_to_min_bid_s = None
        first_profit_time_s = None
        scalp_hit = False
        high_confidence_hit = False
        peak_net_pnl = None
        trough_net_pnl = None
        mfe = None
        mae = None
        ever_profitable = False

        while True:
            await self._wait_book_update()

            now = datetime.now(timezone.utc)
            remaining = (market.end_time - now).total_seconds()
            if remaining <= 0:
                return

            try:
                book = await self._fetch_book_price(token_id)
                bid = book.best_bid
                ask = book.best_ask
                if bid <= 0:
                    continue

                elapsed_s = max(0.0, (datetime.now(timezone.utc) - monitor_started_at).total_seconds())
                changed = False
                if max_bid_seen is None or bid > max_bid_seen:
                    max_bid_seen = bid
                    time_to_max_bid_s = elapsed_s
                    changed = True
                if min_bid_seen is None or bid < min_bid_seen:
                    min_bid_seen = bid
                    time_to_min_bid_s = elapsed_s
                    changed = True

                net_pnl = self._net_pnl(entry_price, bid, market.fee_rate, shares)
                ever_profitable = ever_profitable or net_pnl > 0
                if peak_net_pnl is None or net_pnl > peak_net_pnl:
                    peak_net_pnl = net_pnl
                    changed = True
                if trough_net_pnl is None or net_pnl < trough_net_pnl:
                    trough_net_pnl = net_pnl
                    changed = True
                mfe = max(0.0, (max_bid_seen or bid) - entry_price)
                mae = max(0.0, entry_price - (min_bid_seen or bid))
                if first_profit_time_s is None and net_pnl > 0:
                    first_profit_time_s = elapsed_s
                    changed = True
                if bid >= cfg.scalp_take_profit_bid and not scalp_hit:
                    scalp_hit = True
                    changed = True
                if bid >= cfg.high_confidence_bid and not high_confidence_hit:
                    high_confidence_hit = True
                    changed = True
                if changed:
                    self.tracker.record_paper_trade_path(
                        trade_id,
                        max_bid_seen=max_bid_seen,
                        min_bid_seen=min_bid_seen,
                        time_to_max_bid_s=time_to_max_bid_s,
                        time_to_min_bid_s=time_to_min_bid_s,
                        first_profit_time_s=first_profit_time_s,
                        scalp_hit=scalp_hit,
                        high_confidence_hit=high_confidence_hit,
                        mfe=mfe,
                        mae=mae,
                        peak_net_pnl=peak_net_pnl,
                        trough_net_pnl=trough_net_pnl,
                    )

                if strategy_type == "lead_lag":
                    exit_reason, net_pnl, loss_pct = self._lead_lag_exit_reason(
                        coin=market.coin,
                        entry_price=entry_price,
                        target_price=target_sell,
                        bid=bid,
                        remaining=remaining,
                        elapsed_s=elapsed_s,
                        max_bid_seen=max_bid_seen,
                        ever_profitable=ever_profitable,
                        shares=shares,
                        fee_rate=market.fee_rate,
                    )
                    if exit_reason:
                        status = "closed_win" if net_pnl > 0 else "closed_loss"
                        self.tracker.close_paper_trade_early(
                            trade_id, bid, net_pnl, status,
                            exit_reason=exit_reason,
                            time_remaining_s=remaining,
                            bid_at_exit=bid,
                            ask_at_exit=ask,
                            stall_exit_triggered=(exit_reason == "stall_exit"),
                        )
                        logger.info(
                            f"[{coin}] Paper LEAD-LAG {exit_reason.upper()} @{bid:.3f} "
                            f"(pnl=${net_pnl:+.4f}, loss={loss_pct:.1%}, {remaining:.0f}s left)"
                        )
                        return
                    continue

                if bid >= cfg.high_confidence_bid:
                    self.tracker.record_paper_trade_path(
                        trade_id,
                        max_bid_seen=max_bid_seen,
                        min_bid_seen=min_bid_seen,
                        time_to_max_bid_s=time_to_max_bid_s,
                        time_to_min_bid_s=time_to_min_bid_s,
                        first_profit_time_s=first_profit_time_s,
                        scalp_hit=scalp_hit,
                        high_confidence_hit=True,
                        mfe=mfe,
                        mae=mae,
                        peak_net_pnl=peak_net_pnl,
                        trough_net_pnl=trough_net_pnl,
                    )
                    logger.info(
                        f"[{coin}] Paper HIGH-CONFIDENCE @{bid:.3f} - holding to resolution ({remaining:.0f}s)"
                    )
                    return

                if (
                    bid >= cfg.scalp_take_profit_bid
                    and bid < cfg.high_confidence_bid
                    and net_pnl >= cfg.scalp_min_profit_usd
                ):
                    self.tracker.close_paper_trade_early(
                        trade_id, bid, net_pnl, "closed_win",
                        exit_reason="profit_target", time_remaining_s=remaining,
                        bid_at_exit=bid, ask_at_exit=ask,
                    )
                    logger.info(
                        f"[{coin}] Paper SCALP EXIT @{bid:.3f} "
                        f"(net pnl=${net_pnl:+.4f}, target>={cfg.scalp_take_profit_bid:.2f})"
                    )
                    return

                loss_pct = (entry_price - bid) / entry_price
                dynamic_loss_cut = self._dynamic_single_leg_loss_cut(remaining)
                if loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                    status = "closed_win" if net_pnl > 0 else "closed_loss"
                    self.tracker.close_paper_trade_early(
                        trade_id, bid, net_pnl, status,
                        exit_reason="loss_cut", time_remaining_s=remaining,
                        bid_at_exit=bid, ask_at_exit=ask,
                    )
                    logger.info(
                        f"[{coin}] Paper LOSS CUT @{bid:.3f} "
                        f"(loss={loss_pct:.0%} >= {dynamic_loss_cut:.0%}, {remaining:.0f}s) "
                        f"pnl=${net_pnl:+.4f}"
                    )
                    return

                if remaining <= 30:
                    status = "closed_win" if net_pnl > 0 else "closed_loss"
                    self.tracker.close_paper_trade_early(
                        trade_id, bid, net_pnl, status,
                        exit_reason="near_close", time_remaining_s=remaining,
                        bid_at_exit=bid, ask_at_exit=ask,
                    )
                    logger.info(f"[{coin}] Paper NEAR-CLOSE exit @{bid:.3f} pnl=${net_pnl:+.4f}")
                    return

            except Exception as e:
                logger.debug(f"[{coin}] Paper monitor error: {e}")

    async def _wait_book_update(self):
        """Suspend until the next WebSocket book push, or at most 1 second.

        The 1s cap is critical: when a market closes the WS feed unsubscribes
        and no further events fire. Without a timeout the monitor would block
        forever and never reach the near-close or loss-cut checks.
        """
        if self.scanner and self.scanner._ws_feed.is_connected():
            try:
                await asyncio.wait_for(
                    self.scanner._ws_feed.wait_any_update(), timeout=1.0
                )
            except asyncio.TimeoutError:
                pass
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
                fee = self._per_share_fee(sell_price, 0.072) * shares
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
    def _per_share_fee(price: float, fee_rate: float) -> float:
        return fee_rate * price * (1.0 - price)

    @staticmethod
    def _is_filled(response: dict) -> bool:
        if not response:
            return False
        return response.get("status", "").lower() in ("matched", "filled", "live")

    def get_open_positions(self) -> dict[str, SingleLegPosition]:
        return dict(self._open_positions)





