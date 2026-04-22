"""Backup Telegram command handling."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.control_plane import ControlPlane, ControlRequest
from bot import telegram_dashboard as dashboard_ui

logger = logging.getLogger("edec.telegram")

_DEPRECATED_SURFACES = {
    "export": "Exports now live in the HA dashboard control plane.",
    "latest_export": "Latest archive delivery moved to the HA dashboard.",
    "sync_repo_latest": "Dropbox sync is managed from the HA dashboard now.",
    "fetch_github": "GitHub export fetch moved to the HA dashboard.",
    "config": "Configuration inspection now lives in the HA dashboard and config files.",
    "set": "Runtime config edits from Telegram are deprecated; use the HA dashboard or config files.",
    "filters": "Filter analysis moved to the HA dashboard.",
    "clean": "Chat cleanup moved out of Telegram; use the HA/dashboard surfaces instead.",
    "export_recent": "Recent export download moved to the HA dashboard.",
    "archive_health": "Archive health now lives in the HA dashboard.",
    "session_export": "Session export is available from the HA dashboard control plane.",
}


class TelegramBackupController:
    def __init__(
        self,
        *,
        config,
        tracker,
        risk_manager,
        strategy_engine,
        executor,
        polymarket_cli,
        control_plane: ControlPlane,
    ):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.polymarket_cli = polymarket_cli
        self.control_plane = control_plane

    def deprecation_text(self, key: str) -> str:
        return f"Telegram backup surface notice: {_DEPRECATED_SURFACES.get(key, 'This workflow moved to the HA dashboard.')}"

    def build_help_text(self) -> str:
        return (
            "*Telegram Backup Commands*\n"
            "/status, /mode, /start, /stop, /kill\n"
            "/trades, /stats\n"
            "/pmaccount, /pmorders, /pmtrades, /pmcancelall\n"
            "/help\n\n"
            "Archive/export/sync workflows now live in the HA dashboard control plane."
        )

    def build_status_text(self) -> str:
        risk_status = self.risk_manager.get_status() if self.risk_manager else {
            "kill_switch": False,
            "paused": False,
            "daily_pnl": 0.0,
            "open_positions": 0,
        }
        return dashboard_ui.build_status_panel_text(
            risk_status,
            getattr(self.strategy_engine, "mode", "unknown"),
            float(self.executor.order_size_usd) if self.executor is not None else float(self.config.execution.order_size_usd),
        )

    def build_main_keyboard(self) -> InlineKeyboardMarkup:
        is_running = self.control_plane.build_controls_payload().get("state") == "running"
        is_dry = bool(getattr(self.config.execution, "dry_run", True))
        capital_balance = 0.0
        if self.tracker and hasattr(self.tracker, "get_paper_capital"):
            capital_balance = float(self.tracker.get_paper_capital()[1])
        return dashboard_ui.build_main_keyboard(
            is_running=is_running,
            is_dry=is_dry,
            order_size=float(self.executor.order_size_usd) if self.executor is not None else float(self.config.execution.order_size_usd),
            capital_balance=capital_balance,
        )

    def build_stats_text(self, *, args: list[str] | None = None) -> str:
        args = args or []
        if args and args[0] == "7d":
            lines = ["*Last 7 Days*\n"]
            for i in range(7):
                date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")
                stats = self.tracker.get_daily_stats(date)
                lines.append(
                    f"`{date}` | Evals: {stats['total_evaluations']} | "
                    f"Signals: {stats['signals']} | Trades: {stats['trades_executed']} | "
                    f"OK: {stats['successful']}"
                )
            return "\n".join(lines)
        stats = self.tracker.get_daily_stats()
        return (
            f"*Today's Stats ({stats['date']})*\n"
            f"Evaluations: {stats['total_evaluations']}\n"
            f"Signals: {stats['signals']}\n"
            f"Skips: {stats['skips']}\n"
            f"Trades Executed: {stats['trades_executed']}\n"
            f"Successful: {stats['successful']}\n"
            f"Aborted: {stats['aborted']}"
        )

    def build_recent_trades_text(self) -> str:
        return dashboard_ui.build_recent_trades_panel_text(
            self.tracker.get_recent_trades(limit=5) if self.tracker else []
        )

    def build_mode_help_lines(self) -> list[str]:
        return [
            "`/mode both` - all enabled strategies",
            "`/mode dual` - dual-leg only",
            "`/mode single` - single-leg only",
            "`/mode lead` - lead-lag only",
            "`/mode swing` - swing-leg only",
            "`/mode off` - pause all trading",
        ]

    def _pm_cli_unavailable_text(self) -> str:
        if not self.polymarket_cli:
            return "Polymarket CLI is not configured for this runtime."
        reason = getattr(self.polymarket_cli, "unavailable_reason", lambda: "Polymarket CLI unavailable.")()
        return f"Polymarket CLI unavailable.\n{reason}"

    def _pm_cli_is_available(self) -> bool:
        return bool(self.polymarket_cli and getattr(self.polymarket_cli, "is_available", True))

    def _format_allowances(self, allowances: dict | None) -> str:
        if not allowances:
            return "none"
        return ", ".join(f"`{key}`={value}" for key, value in allowances.items())

    def _pm_cancel_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("Confirm cancel all", callback_data="pmcancelall_confirm"),
                InlineKeyboardButton("Abort", callback_data="pmcancelall_abort"),
            ]]
        )

    def format_pm_account_text(self, wallet, account_status, balance) -> str:
        if isinstance(wallet, Exception) or isinstance(account_status, Exception) or isinstance(balance, Exception):
            return "Polymarket CLI failed to read account state."
        if not wallet.configured:
            return "*Polymarket Account*\nWallet not configured."
        mode = "Closed-only" if getattr(account_status, "closed_only", False) else "Active"
        return (
            "*Polymarket Account*\n"
            f"Address: `{wallet.address}`\n"
            f"Proxy: `{wallet.proxy_address}`\n"
            f"Mode: `{mode}`\n"
            f"Collateral: `{balance.balance}` USDC\n"
            f"Allowances: {self._format_allowances(getattr(balance, 'allowances', {}))}"
        )

    def format_pm_orders_text(self, orders) -> str:
        if isinstance(orders, Exception):
            return "Polymarket CLI failed to fetch orders."
        rows = ["*Polymarket Open Orders*"]
        for order in getattr(orders, "data", [])[:10]:
            rows.append(
                f"{str(order.get('side', '')).upper()} `{order.get('id', '?')}` @ "
                f"{order.get('price', '?')} x {order.get('original_size', '?')}"
            )
        if len(rows) == 1:
            rows.append("No open orders.")
        return "\n".join(rows)

    def format_pm_trades_text(self, trades) -> str:
        if isinstance(trades, Exception):
            return "Polymarket CLI failed to fetch recent trades."
        rows = ["*Polymarket Recent Trades*"]
        for trade in getattr(trades, "data", [])[:10]:
            rows.append(
                f"{str(trade.get('side', '')).upper()} `{trade.get('id', '?')}` @ "
                f"{trade.get('price', '?')} x {trade.get('size', '?')}"
            )
        if len(rows) == 1:
            rows.append("No recent trades.")
        return "\n".join(rows)

    def format_pmcancelall_result_text(self, result) -> str:
        if isinstance(result, Exception):
            return "Cancel all orders failed."
        canceled = getattr(result, "canceled", []) or []
        failures = getattr(result, "failed", []) or []
        lines = [f"*Cancel All Complete*\nCanceled: `{len(canceled)}`"]
        if failures:
            lines.append(f"Failed: `{len(failures)}`")
        return "\n".join(lines)

    def apply_control(self, request: ControlRequest):
        return self.control_plane.apply_sync(request)
