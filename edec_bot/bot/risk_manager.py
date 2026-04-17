"""Risk manager â€” P&L tracking, position limits, kill switch."""

import logging
from collections import deque
from datetime import datetime, timedelta, timezone

from bot.config import Config
from bot.models import TradeResult

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, config: Config):
        self.config = config
        self.daily_pnl: float = 0.0
        self.session_pnl: float = 0.0
        self.open_positions: list[TradeResult] = []
        self.trades_this_hour: deque[datetime] = deque()
        self.kill_switch_active: bool = False
        self._paused: bool = False

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _prune_hourly_window(self) -> None:
        cutoff = self._now_utc() - timedelta(hours=1)
        while self.trades_this_hour and self.trades_this_hour[0] < cutoff:
            self.trades_this_hour.popleft()

    def can_trade(self) -> bool:
        """Check all risk limits. Returns False if any are breached."""
        if self.kill_switch_active:
            return False
        if self._paused:
            return False
        if self.daily_pnl <= -self.config.risk.max_daily_loss_usd:
            logger.warning(f"Daily loss limit hit: ${self.daily_pnl:.2f}")
            self.activate_kill_switch("Daily loss limit reached")
            return False
        if len(self.open_positions) >= self.config.risk.max_open_positions:
            return False
        self._prune_hourly_window()
        if len(self.trades_this_hour) >= self.config.risk.max_trades_per_hour:
            return False
        if self.config.risk.session_profit_target > 0:
            if self.session_pnl >= self.config.risk.session_profit_target:
                logger.info(f"Session profit target reached: ${self.session_pnl:.2f}")
                return False
        return True

    def record_attempt(self):
        """Count a live order attempt toward the hourly guardrail."""
        self._prune_hourly_window()
        self.trades_this_hour.append(self._now_utc())

    def open_position(self, result: TradeResult):
        """Track a confirmed filled live position."""
        if any(existing is result for existing in self.open_positions):
            return
        self.open_positions.append(result)

    def record_abort(self, abort_cost: float):
        """Abort cost is a realized loss."""
        self.daily_pnl -= abort_cost
        self.session_pnl -= abort_cost

    @staticmethod
    def _per_share_fee(price: float, fee_rate: float) -> float:
        return fee_rate * price * (1.0 - price)

    def _resolution_profit(self, result: TradeResult, winner: str) -> float:
        strategy_type = result.strategy_type or result.signal.strategy_type
        shares = result.shares_filled or result.shares or result.shares_requested
        if shares <= 0:
            return 0.0

        if strategy_type == "dual_leg":
            combined_cost = result.total_cost or result.signal.combined_cost
            fee_total = result.fee_total or result.signal.fee_total
            return (1.0 - combined_cost - fee_total) * shares

        side = (result.side or result.signal.side or "").lower()
        winner_side = (winner or "").lower()
        entry_price = result.signal.entry_price
        fee_rate = result.signal.market.fee_rate
        buy_fee_total = result.fee_total or (self._per_share_fee(entry_price, fee_rate) * shares)
        won = (
            (side == "up" and winner_side == "up")
            or (side == "down" and winner_side == "down")
        )
        if won:
            return (1.0 - entry_price) * shares - buy_fee_total
        return -((entry_price * shares) + buy_fee_total)

    def close_position(self, result: TradeResult, actual_profit: float):
        """Close an open live position and realize P&L."""
        self.daily_pnl += actual_profit
        self.session_pnl += actual_profit
        self.open_positions = [p for p in self.open_positions if p is not result]
        logger.info(
            f"Position closed: {result.signal.market.slug} "
            f"| P&L: ${actual_profit:+.4f} "
            f"| Daily: ${self.daily_pnl:+.2f} | Session: ${self.session_pnl:+.2f}"
        )

    def resolve_market(self, market_slug: str, winner: str) -> float:
        """Resolve every still-open live position for a market."""
        matching = [p for p in self.open_positions if p.signal.market.slug == market_slug]
        total_profit = 0.0
        for result in matching:
            actual_profit = self._resolution_profit(result, winner)
            total_profit += actual_profit
            self.close_position(result, actual_profit)
        return total_profit

    def record_trade(self, result: TradeResult):
        """Backward-compatible wrapper for immediate executions."""
        self.record_attempt()
        if result.status in ("success", "open"):
            self.open_position(result)
        elif result.status in ("aborted", "partial_abort"):
            self.record_abort(result.abort_cost)

    def record_resolution(self, result: TradeResult, actual_profit: float):
        """Backward-compatible wrapper for closing a specific live position."""
        self.close_position(result, actual_profit)

    def activate_kill_switch(self, reason: str):
        """Emergency stop â€” halt all trading."""
        self.kill_switch_active = True
        logger.critical(f"KILL SWITCH ACTIVATED: {reason}")

    def deactivate_kill_switch(self):
        self.kill_switch_active = False
        logger.info("Kill switch deactivated")

    def reset_daily_stats(self):
        """Reset in-memory P&L and risk counters â€” called when user resets paper stats."""
        self.daily_pnl = 0.0
        self.session_pnl = 0.0
        self.trades_this_hour.clear()
        self.open_positions.clear()
        self.kill_switch_active = False
        self._paused = False
        logger.info("Risk manager daily stats reset")

    def pause(self):
        self._paused = True
        logger.info("Trading paused")

    def resume(self):
        self._paused = False
        logger.info("Trading resumed")

    @property
    def is_paused(self) -> bool:
        return self._paused

    def get_status(self) -> dict:
        """Current risk manager state."""
        return {
            "daily_pnl": round(self.daily_pnl, 4),
            "session_pnl": round(self.session_pnl, 4),
            "open_positions": len(self.open_positions),
            "trades_this_hour": len(self.trades_this_hour),
            "kill_switch": self.kill_switch_active,
            "paused": self._paused,
        }
