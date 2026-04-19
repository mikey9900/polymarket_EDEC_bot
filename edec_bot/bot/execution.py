"""Execution engine — dual-leg FOK state machine + single-leg GTC buy/sell."""

import asyncio
import logging
import math
import time

import httpx

from bot.config import Config, resolve_lead_lag_params
from bot.models import DualOrderState, OrderBookSnapshot, SingleLegPosition, SwingPosition, TradeResult, TradeSignal
from bot.execution_flows import dual_leg as dual_leg_execution
from bot.execution_flows import single_leg as single_leg_execution
from bot.execution_flows import swing_leg as swing_leg_execution
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
        self._pending_single_entries: dict[str, SingleLegPosition] = {}

        # Open swing positions, keyed by coin (one per coin at a time)
        self._open_swing_positions: dict[str, SwingPosition] = {}
        self._pending_swing_entries: dict[str, SwingPosition] = {}

        # HTTP client for dry-run book price monitoring
        self._http = httpx.AsyncClient(timeout=5.0)

    async def execute(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Dispatch to the appropriate execution path based on strategy type."""
        if signal.strategy_type in ("single_leg", "lead_lag"):
            return await self.execute_single_leg(signal, decision_id)
        if signal.strategy_type == "swing_leg":
            return await self.execute_swing_leg(signal, decision_id)
        return await self._execute_dual_leg(signal, decision_id)

    async def aclose(self):
        """Close owned HTTP resources."""
        await self._http.aclose()

    # -----------------------------------------------------------------------
    # Dual-leg: FOK both sides atomically
    # -----------------------------------------------------------------------

    async def _execute_dual_leg(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Execute a dual-leg arb trade using FOK orders."""
        return await dual_leg_execution.execute(self, signal, decision_id)

    async def execute_single_leg(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Buy one side with a GTC limit and monitor it with strategy-specific exits."""
        return await single_leg_execution.execute(self, signal, decision_id)

    async def _monitor_single_leg_entry(self, position: SingleLegPosition, result: TradeResult):
        """Wait for a resting live entry order to actually fill before opening the position."""
        await single_leg_execution.monitor_entry(self, position, result)

    async def _monitor_single_leg(self, position: SingleLegPosition, result: TradeResult):
        """Monitor an open single-leg or lead-lag live position with strategy-specific exits."""
        await single_leg_execution.monitor_position(self, position, result)

    async def execute_swing_leg(self, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
        """Buy first leg of a swing trade and start monitoring for the second leg."""
        return await swing_leg_execution.execute(self, signal, decision_id)

    async def _monitor_swing_entry(self, position: SwingPosition, result: TradeResult):
        """Wait for a resting swing entry to fill before opening the live position."""
        await swing_leg_execution.monitor_entry(self, position, result)

    async def _monitor_swing_leg(self, position: SwingPosition, result: TradeResult | None = None):
        """Monitor a swing mean-reversion position with smart exit logic."""
        await swing_leg_execution.monitor_position(self, position, result)

    async def _monitor_paper_single_leg(self, trade_id: int, market,
                                         token_id: str, entry_price: float,
                                         target_sell: float, shares: float,
                                         strategy_type: str = "single_leg"):
        """Watch a paper trade's live bid and apply the same exit structure as runtime logic."""
        await single_leg_execution.monitor_paper_position(
            self,
            trade_id,
            market,
            token_id,
            entry_price,
            target_sell,
            shares,
            strategy_type,
        )

    def _lead_lag_params(self, coin: str) -> dict[str, float]:
        return resolve_lead_lag_params(self.config.lead_lag, coin)

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

    async def _wait_book_update(self):
        """Suspend until the next WebSocket book push, or at most 1 second."""
        if self.scanner and self.scanner._ws_feed.is_connected():
            try:
                await asyncio.wait_for(self.scanner._ws_feed.wait_any_update(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(1)

    async def _fetch_book_price(self, token_id: str) -> OrderBookSnapshot:
        """Return book for a token - WS cache first, HTTP fallback if not yet populated."""
        if self.scanner:
            book = self.scanner.get_book_for_token(token_id)
            if book is not None:
                return book

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

    async def _abort_sell(
        self,
        token_id: str,
        shares: float,
        entry_price: float,
        fee_rate: float,
        tick_size: str,
        neg_risk: bool,
    ) -> float:
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
                fee = self._per_share_fee(sell_price, fee_rate) * shares
                abort_cost += fee
                logger.info(f"Abort sell filled. Cost: ${abort_cost:.4f}")
                return abort_cost
            logger.warning("Abort sell rejected. Naked position remains - will resolve at close.")
            return entry_price * shares
        except Exception as exc:
            logger.critical(f"Abort sell exception: {exc}")
            return entry_price * shares

    @property
    def order_size_usd(self) -> float:
        return self._order_size_usd if self._order_size_usd is not None else self.config.execution.order_size_usd

    def _strategy_order_size_usd(self, strategy_type: str) -> float:
        if self._order_size_usd is not None:
            return self._order_size_usd
        if strategy_type == "single_leg":
            return self.config.single_leg.order_size_usd
        if strategy_type == "lead_lag":
            return self.config.lead_lag.order_size_usd
        if strategy_type == "swing_leg":
            return self.config.swing_leg.order_size_usd
        return self.order_size_usd

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
    def _response_status(response: dict | None) -> str:
        if not response:
            return ""
        nested_order = response.get("order")
        containers = [response]
        if isinstance(nested_order, dict):
            containers.append(nested_order)
        for container in containers:
            for key in ("status", "orderStatus"):
                status = container.get(key)
                if status:
                    return str(status).strip().lower()
        return ""

    @staticmethod
    def _safe_float(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _filled_shares(cls, response: dict | None, fallback: float = 0.0) -> float:
        if not response:
            return 0.0
        nested_order = response.get("order")
        containers = [response]
        if isinstance(nested_order, dict):
            containers.append(nested_order)
        keys = (
            "size_matched",
            "matched_size",
            "matchedSize",
            "filled_size",
            "filledSize",
            "size_filled",
            "filled",
            "filled_amount",
            "filledAmount",
            "makerAmountFilled",
            "maker_amount_filled",
        )
        for container in containers:
            for key in keys:
                filled = cls._safe_float(container.get(key))
                if filled > 0:
                    return filled
        if cls._response_status(response) in ("matched", "filled") and fallback > 0:
            return fallback
        return 0.0

    @classmethod
    def _filled_price(cls, response: dict | None, fallback: float = 0.0) -> float:
        if not response:
            return fallback
        nested_order = response.get("order")
        containers = [response]
        if isinstance(nested_order, dict):
            containers.append(nested_order)
        keys = (
            "avg_price",
            "avgPrice",
            "average_price",
            "averagePrice",
            "matched_price",
            "matchedPrice",
            "fill_price",
            "fillPrice",
            "price",
        )
        for container in containers:
            for key in keys:
                price = cls._safe_float(container.get(key))
                if price > 0:
                    return price
        return fallback

    @classmethod
    def _has_any_fill(cls, response: dict | None) -> bool:
        return cls._filled_shares(response) > 0 or cls._response_status(response) in ("matched", "filled")

    @classmethod
    def _is_order_filled(cls, response: dict | None, expected_shares: float = 0.0) -> bool:
        status = cls._response_status(response)
        if status in ("matched", "filled"):
            return True
        filled = cls._filled_shares(response)
        return expected_shares > 0 and filled >= expected_shares

    @staticmethod
    def _is_terminal_order_state(status: str | None) -> bool:
        return (status or "").strip().lower() in (
            "matched",
            "filled",
            "canceled",
            "cancelled",
            "expired",
            "rejected",
            "failed",
            "unmatched",
        )

    @staticmethod
    def _is_filled(response: dict) -> bool:
        if not response:
            return False
        return ExecutionEngine._response_status(response) in ("matched", "filled")

    @staticmethod
    def _exclude_market_slug(mapping: dict, market_slug: str, *, attr: str = "market") -> dict:
        return {
            key: value
            for key, value in mapping.items()
            if getattr(getattr(value, attr), "slug", None) != market_slug
        }

    def resolve_market_positions(self, market_slug: str, winner: str) -> float:
        """Resolve risk state and clear any in-memory positions for a settled market."""
        matching = [p for p in self.risk_manager.open_positions if p.signal.market.slug == market_slug]
        pnl = 0.0
        for result in matching:
            actual_profit = self.risk_manager._resolution_profit(result, winner)
            pnl += actual_profit
            result.realized_pnl = actual_profit
            strategy_type = result.strategy_type or result.signal.strategy_type
            exit_price = 1.0 if strategy_type == "dual_leg" or actual_profit >= 0 else 0.0
            if result.trade_id and getattr(self.tracker, "close_live_trade", None):
                self.tracker.close_live_trade(
                    result.trade_id,
                    status="resolved_win" if actual_profit >= 0 else "resolved_loss",
                    exit_reason="resolution",
                    exit_price=exit_price,
                    pnl=actual_profit,
                    time_remaining_s=0.0,
                    exit_limit_price=exit_price,
                    exit_fill_price=exit_price,
                    max_bid_seen=result.max_bid_seen or None,
                    min_bid_seen=result.min_bid_seen or None,
                    time_to_max_bid_s=result.time_to_max_bid_s or None,
                    time_to_min_bid_s=result.time_to_min_bid_s or None,
                    first_profit_time_s=result.first_profit_time_s or None,
                    scalp_hit=result.scalp_hit,
                    high_confidence_hit=result.high_confidence_hit,
                    hold_to_resolution=result.hold_to_resolution,
                    mfe=result.mfe or None,
                    mae=result.mae or None,
                    peak_net_pnl=result.peak_net_pnl or None,
                    trough_net_pnl=result.trough_net_pnl or None,
                    dynamic_loss_cut_pct=result.dynamic_loss_cut_pct or None,
                    favorable_excursion=result.favorable_excursion or None,
                    ever_profitable=result.ever_profitable,
                    cancel_repost_count=result.cancel_repost_count or None,
                )
            self.risk_manager.close_position(result, actual_profit)
        self._open_positions = self._exclude_market_slug(self._open_positions, market_slug)
        self._pending_single_entries = self._exclude_market_slug(self._pending_single_entries, market_slug)
        self._open_swing_positions = self._exclude_market_slug(self._open_swing_positions, market_slug)
        self._pending_swing_entries = self._exclude_market_slug(self._pending_swing_entries, market_slug)
        return pnl

    def get_open_positions(self) -> dict[str, SingleLegPosition]:
        return dict(self._open_positions)
