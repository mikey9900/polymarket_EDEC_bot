"""Execution engine — dual-leg FOK state machine + single-leg GTC buy/sell."""

import asyncio
import logging
import math
from datetime import datetime, timezone

from bot.config import Config
from bot.models import DualOrderState, SingleLegPosition, TradeResult, TradeSignal
from bot.risk_manager import RiskManager
from bot.tracker import DecisionTracker

logger = logging.getLogger(__name__)


class ExecutionEngine:
    def __init__(self, config: Config, clob_client, risk_manager: RiskManager,
                 tracker: DecisionTracker):
        self.config = config
        self.client = clob_client
        self.risk_manager = risk_manager
        self.tracker = tracker
        self.state = DualOrderState.IDLE

        # Open single-leg positions, keyed by buy_order_id
        self._open_positions: dict[str, SingleLegPosition] = {}

    async def execute(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Dispatch to the appropriate execution path based on strategy type."""
        if signal.strategy_type == "single_leg":
            return await self.execute_single_leg(signal, decision_id)
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
            logger.info(
                f"[DRY RUN] Would buy UP@{signal.up_price:.3f} + DOWN@{signal.down_price:.3f} "
                f"({result.shares:.0f} shares) [{signal.market.coin.upper()}]"
            )
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

        order_size = self.config.single_leg.order_size_usd
        shares = math.floor(order_size / signal.entry_price)

        if self.config.execution.dry_run:
            result.status = "dry_run"
            result.shares = shares
            result.total_cost = signal.entry_price * shares
            result.fee_total = signal.fee_total
            logger.info(
                f"[DRY RUN] [{coin}] SINGLE-LEG: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"→ SELL@{signal.target_sell_price:.3f} | {shares} shares"
            )
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
        """Monitor an open single-leg position until sell fills or market closes."""
        coin = position.market.coin.upper()
        hold = self.config.single_leg.hold_if_unfilled

        while True:
            await asyncio.sleep(5)

            now = datetime.now(timezone.utc)
            remaining = (position.market.end_time - now).total_seconds()

            # Check if sell order is filled
            if position.sell_order_id:
                try:
                    order_status = await asyncio.to_thread(
                        self.client.get_order, position.sell_order_id
                    )
                    status = (order_status or {}).get("status", "").lower()

                    if status in ("matched", "filled"):
                        result.status = "success"
                        logger.info(
                            f"[{coin}] SINGLE-LEG SELL FILLED @ {position.target_price:.3f} | "
                            f"Position closed."
                        )
                        self._open_positions.pop(position.buy_order_id, None)
                        return
                except Exception as e:
                    logger.warning(f"[{coin}] Order status check failed: {e}")

            # Near market close: decide whether to hold or exit
            if remaining <= 30:
                if hold:
                    # Cancel sell order, hold to resolution
                    if position.sell_order_id:
                        try:
                            await asyncio.to_thread(
                                self.client.cancel, position.sell_order_id
                            )
                            logger.info(f"[{coin}] Holding {position.side.upper()} to resolution.")
                        except Exception as e:
                            logger.warning(f"[{coin}] Cancel sell failed: {e}")
                    result.status = "open"
                else:
                    # Market-sell before close (limit at a low price to force fill)
                    try:
                        emergency_price = max(0.01, position.entry_price - 0.05)
                        sell_order = await asyncio.to_thread(
                            self.client.create_order,
                            {"token_id": position.token_id, "price": emergency_price,
                             "size": position.shares, "side": "SELL"},
                            {"tick_size": position.market.tick_size,
                             "neg_risk": position.market.neg_risk},
                        )
                        await asyncio.to_thread(self.client.post_order, sell_order, "GTC")
                        logger.info(f"[{coin}] Emergency sell placed @ {emergency_price:.3f}")
                    except Exception as e:
                        logger.error(f"[{coin}] Emergency sell failed: {e}")

                self._open_positions.pop(position.buy_order_id, None)
                return

            # Market already ended
            if remaining <= 0:
                self._open_positions.pop(position.buy_order_id, None)
                return

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

    def _calc_shares(self, price: float) -> float:
        if price <= 0:
            return 0
        return math.floor(self.config.execution.order_size_usd / price)

    @staticmethod
    def _is_filled(response: dict) -> bool:
        if not response:
            return False
        return response.get("status", "").lower() in ("matched", "filled", "live")

    def get_open_positions(self) -> dict[str, SingleLegPosition]:
        return dict(self._open_positions)
