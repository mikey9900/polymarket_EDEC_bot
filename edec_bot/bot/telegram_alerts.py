"""Alert publishing for the backup Telegram surface."""

from __future__ import annotations

import logging


logger = logging.getLogger("edec.telegram")


class TelegramAlertPublisher:
    def __init__(self, bot):
        self.bot = bot

    async def send_alert(self, text: str, *, parse_mode: str = "Markdown"):
        if not self.bot._app or not self.bot.chat_id:
            return None
        try:
            msg = await self.bot._app.bot.send_message(
                chat_id=self.bot.chat_id,
                text=text,
                parse_mode=parse_mode,
            )
        except Exception:
            logger.exception("Failed to send Telegram alert")
            return None
        self.bot._track(msg)
        return msg

    async def alert_archive_complete(self, archive_result: dict):
        row_counts = archive_result.get("row_counts", {})
        msg = (
            "*Archive Completed*\n"
            f"24h paper/live/decisions: {row_counts.get('paper_trades_24h', 0)}/"
            f"{row_counts.get('live_trades_24h', 0)}/{row_counts.get('decisions_24h', 0)}\n"
            f"Recent trades/signals rows: {row_counts.get('recent_trades_rows', 0)}/"
            f"{row_counts.get('recent_signals_rows', 0)}\n"
            "Use the HA dashboard for archive files and follow-up actions."
        )
        await self.send_alert(msg)

    async def alert_dual_leg(
        self,
        slug: str,
        coin: str,
        up_price: float,
        down_price: float,
        combined_cost: float,
        expected_profit: float,
        shares: float,
    ):
        await self.send_alert(
            "*Dual-Leg Opened*\n"
            f"{coin.upper()} `{slug}`\n"
            f"UP {up_price:.3f} / DOWN {down_price:.3f}\n"
            f"Cost: ${combined_cost:.3f} | Expected: ${expected_profit:.3f} | Shares: {shares:.2f}"
        )

    async def alert_single_leg(
        self,
        slug: str,
        coin: str,
        side: str,
        entry_price: float,
        target_sell_price: float,
        shares: float,
        expected_profit: float,
        *,
        strategy_type: str = "single_leg",
    ):
        await self.send_alert(
            "*Repricing Position Opened*\n"
            f"{coin.upper()} `{slug}`\n"
            f"{strategy_type}: {side.upper()} @ {entry_price:.3f} -> {target_sell_price:.3f}\n"
            f"Shares: {shares:.2f} | Expected: ${expected_profit:.3f}"
        )

    async def alert_trade(self, text: str):
        await self.send_alert(text)

    async def alert_abort(self, slug: str, error: str, abort_cost: float):
        await self.send_alert(
            "*Trade Aborted*\n"
            f"`{slug}`\n"
            f"Reason: {error or 'unknown'}\n"
            f"Abort cost: ${abort_cost:.3f}"
        )

    async def alert_resolution(self, slug: str, outcome: str, live_pnl: float):
        await self.send_alert(
            "*Market Resolved*\n"
            f"`{slug}` -> `{outcome}`\n"
            f"Live P&L: ${live_pnl:+.3f}"
        )

    async def alert_kill_switch(self, reason: str):
        await self.send_alert(
            "*Kill Switch Activated*\n"
            f"{reason or 'Manual kill'}"
        )
