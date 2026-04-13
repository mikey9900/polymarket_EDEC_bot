"""Risk manager — P&L tracking, position limits, kill switch."""

import logging
from collections import deque
from datetime import datetime, timedelta

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
        # Clean old entries from hourly window
        cutoff = datetime.utcnow() - timedelta(hours=1)
        while self.trades_this_hour and self.trades_this_hour[0] < cutoff:
            self.trades_this_hour.popleft()
        if len(self.trades_this_hour) >= self.config.risk.max_trades_per_hour:
            return False
        # Session profit target
        if self.config.risk.session_profit_target > 0:
            if self.session_pnl >= self.config.risk.session_profit_target:
                logger.info(f"Session profit target reached: ${self.session_pnl:.2f}")
                return False
        return True

    def record_trade(self, result: TradeResult):
        """Record a trade attempt."""
        self.trades_this_hour.append(datetime.utcnow())
        if result.status == "success":
            self.open_positions.append(result)
        elif result.status in ("aborted", "partial_abort"):
            # Abort cost is a realized loss
            self.daily_pnl -= result.abort_cost
            self.session_pnl -= result.abort_cost

    def record_resolution(self, result: TradeResult, actual_profit: float):
        """Called when a market resolves for an open position."""
        self.daily_pnl += actual_profit
        self.session_pnl += actual_profit
        # Remove from open positions
        self.open_positions = [
            p for p in self.open_positions
            if p.signal.market.slug != result.signal.market.slug
        ]
        logger.info(
            f"Resolution P&L: ${actual_profit:+.4f} | "
            f"Daily: ${self.daily_pnl:+.2f} | Session: ${self.session_pnl:+.2f}"
        )

    def activate_kill_switch(self, reason: str):
        """Emergency stop — halt all trading."""
        self.kill_switch_active = True
        logger.critical(f"KILL SWITCH ACTIVATED: {reason}")

    def deactivate_kill_switch(self):
        self.kill_switch_active = False
        logger.info("Kill switch deactivated")

    def reset_daily_stats(self):
        """Reset in-memory P&L and risk counters — called when user resets paper stats."""
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
