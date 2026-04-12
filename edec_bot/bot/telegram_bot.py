"""Telegram interface — commands, alerts, status updates, and data export."""

import asyncio
import logging
import os
from datetime import datetime

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from bot.config import Config
from bot.tracker import DecisionTracker
from bot.risk_manager import RiskManager

logger = logging.getLogger(__name__)

# Mode labels for display
MODE_LABELS = {
    "both": "🟢 BOTH (dual + single)",
    "dual": "🔵 DUAL-LEG only",
    "single": "🟡 SINGLE-LEG only",
    "off": "🔴 OFF (no trading)",
}


class TelegramBot:
    def __init__(self, config: Config, tracker: DecisionTracker,
                 risk_manager: RiskManager, export_fn=None,
                 scanner=None, strategy_engine=None):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.export_fn = export_fn
        self.scanner = scanner              # MarketScanner (for per-coin book prices)
        self.strategy_engine = strategy_engine  # StrategyEngine (for mode control)
        self.chat_id = config.telegram_chat_id
        self._app: Application | None = None

    async def start(self):
        """Initialize and start the Telegram bot."""
        if not self.config.telegram_bot_token:
            logger.warning("No Telegram bot token configured — Telegram disabled")
            return

        self._app = (
            Application.builder()
            .token(self.config.telegram_bot_token)
            .build()
        )

        handlers = [
            ("status", self._cmd_status),
            ("mode", self._cmd_mode),
            ("start", self._cmd_start),
            ("stop", self._cmd_stop),
            ("kill", self._cmd_kill),
            ("trades", self._cmd_trades),
            ("stats", self._cmd_stats),
            ("export", self._cmd_export),
            ("config", self._cmd_config),
            ("set", self._cmd_set),
            ("filters", self._cmd_filters),
            ("help", self._cmd_help),
        ]
        for cmd, handler in handlers:
            self._app.add_handler(CommandHandler(cmd, handler))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram bot started")

    async def stop(self):
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

    def _auth(self, update: Update) -> bool:
        return str(update.effective_chat.id) == str(self.chat_id)

    # -----------------------------------------------------------------------
    # Alert methods (called by other components)
    # -----------------------------------------------------------------------

    async def send_alert(self, message: str):
        if not self._app or not self.chat_id:
            return
        try:
            await self._app.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

    async def alert_dual_leg(self, market_slug: str, coin: str, up_price: float,
                             down_price: float, combined: float, profit: float,
                             shares: float, dry_run: bool = False):
        prefix = "👀 DRY RUN" if dry_run else "✅ TRADE"
        msg = (
            f"{prefix} DUAL-LEG `{coin.upper()}`\n"
            f"`{market_slug}`\n"
            f"UP@{up_price:.3f} + DOWN@{down_price:.3f} = {combined:.3f}\n"
            f"Shares: {shares:.0f} | Est. profit: ${profit:.4f}"
        )
        await self.send_alert(msg)

    async def alert_single_leg(self, market_slug: str, coin: str, side: str,
                               entry_price: float, target_price: float,
                               shares: float, profit: float, dry_run: bool = False):
        prefix = "👀 DRY RUN" if dry_run else "✅ TRADE"
        msg = (
            f"{prefix} SINGLE-LEG `{coin.upper()}` → {side.upper()}\n"
            f"`{market_slug}`\n"
            f"BUY@{entry_price:.3f} → SELL@{target_price:.3f}\n"
            f"Shares: {shares:.0f} | Est. profit: ${profit:.4f}"
        )
        await self.send_alert(msg)

    # Keep old name for backward compat
    async def alert_trade(self, market_slug: str, up_price: float, down_price: float,
                          combined: float, profit: float, shares: float, dry_run: bool = False):
        await self.alert_dual_leg(market_slug, "", up_price, down_price, combined,
                                  profit, shares, dry_run)

    async def alert_abort(self, market_slug: str, reason: str, abort_cost: float):
        msg = (
            f"⚠️ ABORT: `{market_slug}`\n"
            f"Reason: {reason}\n"
            f"Abort cost: ${abort_cost:.4f}"
        )
        await self.send_alert(msg)

    async def alert_resolution(self, market_slug: str, winner: str, pnl: float):
        emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "📊"
        msg = (
            f"{emoji} RESULT: `{market_slug}`\n"
            f"Winner: {winner} | P&L: ${pnl:+.4f}"
        )
        await self.send_alert(msg)

    async def alert_kill_switch(self, reason: str, daily_pnl: float):
        msg = (
            f"🛑 KILL SWITCH ACTIVATED\n"
            f"Reason: {reason}\n"
            f"Daily P&L: ${daily_pnl:.2f}"
        )
        await self.send_alert(msg)

    async def alert_feed_status(self, feed_name: str, connected: bool, total_feeds: int):
        if connected:
            msg = f"✅ {feed_name} feed reconnected ({total_feeds} feeds active)"
        else:
            msg = f"⚠️ {feed_name} feed disconnected ({total_feeds} feeds remaining)"
        await self.send_alert(msg)

    # -----------------------------------------------------------------------
    # Command handlers
    # -----------------------------------------------------------------------

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        status = self.risk_manager.get_status()
        dry_run = "ON" if self.config.execution.dry_run else "OFF"
        mode = self.strategy_engine.mode if self.strategy_engine else "unknown"
        mode_label = MODE_LABELS.get(mode, mode)

        bot_state = (
            "🔴 KILLED" if status["kill_switch"]
            else "⏸ PAUSED" if status["paused"]
            else "🟢 RUNNING"
        )

        lines = [
            f"*EDEC Bot Status*",
            f"State: {bot_state} | Dry Run: {dry_run}",
            f"Mode: {mode_label}",
            f"Daily P&L: ${status['daily_pnl']:+.2f} | Session: ${status['session_pnl']:+.2f}",
            f"Open Positions: {status['open_positions']} | Trades/hr: {status['trades_this_hour']}",
            "",
            "*Per-Coin Order Books*",
        ]

        # Per-coin book snapshot
        if self.scanner:
            snapshot = self.scanner.get_status_snapshot()
            cfg_dual = self.config.dual_leg
            cfg_single = self.config.single_leg

            for coin in self.config.coins:
                coin_data = snapshot.get(coin)
                if coin_data:
                    up_ask = coin_data["up_ask"]
                    down_ask = coin_data["down_ask"]
                    combined = up_ask + down_ask

                    # Signal indicators
                    indicators = []
                    if up_ask <= cfg_dual.price_threshold and down_ask <= cfg_dual.price_threshold and combined <= cfg_dual.max_combined_cost:
                        indicators.append("🔵 DUAL?")
                    if up_ask <= cfg_single.entry_max and down_ask >= cfg_single.opposite_min:
                        indicators.append("🟡 SL↑")
                    elif down_ask <= cfg_single.entry_max and up_ask >= cfg_single.opposite_min:
                        indicators.append("🟡 SL↓")

                    signal_str = " " + " ".join(indicators) if indicators else ""
                    lines.append(
                        f"`{coin.upper():>4}`: UP@{up_ask:.3f} DN@{down_ask:.3f}{signal_str}"
                    )
                else:
                    lines.append(f"`{coin.upper():>4}`: — no market —")
        else:
            lines.append("_(scanner not attached)_")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return

        if not self.strategy_engine:
            await update.message.reply_text("Strategy engine not available.")
            return

        args = context.args
        if not args:
            # Show current mode
            mode = self.strategy_engine.mode
            msg = (
                f"*Strategy Mode*\n"
                f"Current: {MODE_LABELS.get(mode, mode)}\n\n"
                f"Change with:\n"
                f"`/mode both` — dual-leg + single-leg\n"
                f"`/mode dual` — dual-leg arb only\n"
                f"`/mode single` — single-leg momentum only\n"
                f"`/mode off` — pause all trading"
            )
            await update.message.reply_text(msg, parse_mode="Markdown")
            return

        new_mode = args[0].lower()
        if self.strategy_engine.set_mode(new_mode):
            label = MODE_LABELS.get(new_mode, new_mode)
            await update.message.reply_text(f"✅ Mode set to: {label}")
        else:
            await update.message.reply_text(
                f"❌ Unknown mode `{new_mode}`. Use: both, dual, single, off",
                parse_mode="Markdown",
            )

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self.risk_manager.resume()
        self.risk_manager.deactivate_kill_switch()
        await update.message.reply_text("▶️ Trading resumed")

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self.risk_manager.pause()
        await update.message.reply_text("⏸ Trading paused (monitoring continues)")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self.risk_manager.activate_kill_switch("Manual kill via Telegram")
        await update.message.reply_text("🛑 Kill switch activated — all trading stopped")

    async def _cmd_trades(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        trades = self.tracker.get_recent_trades(limit=10)
        if not trades:
            await update.message.reply_text("No trades yet.")
            return

        lines = ["*Recent Trades*\n"]
        for t in trades:
            pnl = t.get("actual_profit")
            pnl_str = f"${pnl:+.4f}" if pnl is not None else "pending"
            strategy = t.get("strategy_type", "dual_leg")
            coin = t.get("coin", "btc").upper()
            lines.append(
                f"`{t['timestamp'][:16]}` [{coin}] {strategy} {t['status']}\n"
                f"  UP@{t.get('up_price', 0):.3f} DN@{t.get('down_price', 0):.3f} → {pnl_str}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        args = context.args
        if args and args[0] == "7d":
            from datetime import timedelta
            lines = ["*Last 7 Days*\n"]
            for i in range(7):
                date = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
                stats = self.tracker.get_daily_stats(date)
                lines.append(
                    f"`{date}` | Evals: {stats['total_evaluations']} | "
                    f"Signals: {stats['signals']} | Trades: {stats['trades_executed']} | "
                    f"OK: {stats['successful']}"
                )
            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            stats = self.tracker.get_daily_stats()
            msg = (
                f"*Today's Stats ({stats['date']})*\n"
                f"Evaluations: {stats['total_evaluations']}\n"
                f"Signals: {stats['signals']}\n"
                f"Skips: {stats['skips']}\n"
                f"Trades Executed: {stats['trades_executed']}\n"
                f"Successful: {stats['successful']}\n"
                f"Aborted: {stats['aborted']}"
            )
            await update.message.reply_text(msg, parse_mode="Markdown")

    async def _cmd_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        if not self.export_fn:
            await update.message.reply_text("Export not available")
            return

        await update.message.reply_text("⏳ Generating Excel export...")
        try:
            today_only = context.args and context.args[0] == "today"
            path = self.export_fn(today_only=today_only)
            with open(path, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=os.path.basename(path),
                    caption="📊 EDEC Bot Decision Export",
                )
        except Exception as e:
            await update.message.reply_text(f"Export error: {e}")

    async def _cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        cfg = self.config
        dl = cfg.dual_leg
        sl = cfg.single_leg
        msg = (
            f"*Configuration*\n"
            f"Coins: {', '.join(cfg.coins)}\n"
            f"Dry run: {cfg.execution.dry_run}\n"
            f"Order size: ${cfg.execution.order_size_usd}\n"
            f"Daily loss limit: ${cfg.risk.max_daily_loss_usd}\n\n"
            f"*Dual-Leg*\n"
            f"  Price threshold: {dl.price_threshold}\n"
            f"  Max combined cost: {dl.max_combined_cost}\n"
            f"  Min edge after fees: {dl.min_edge_after_fees}\n"
            f"  Max velocity 30s: {dl.max_velocity_30s}%\n"
            f"  Min time remaining: {dl.min_time_remaining_s}s\n\n"
            f"*Single-Leg*\n"
            f"  Entry max: {sl.entry_max}\n"
            f"  Opposite min: {sl.opposite_min}\n"
            f"  Target sell: {sl.target_sell}\n"
            f"  Order size: ${sl.order_size_usd}\n"
            f"  Min time remaining: {sl.min_time_remaining_s}s\n"
            f"  Hold if unfilled: {sl.hold_if_unfilled}"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def _cmd_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "Usage: `/set <param> <value>`\n"
                "Params: threshold, max\\_cost, min\\_edge, size, dry\\_run\n\n"
                "Note: Most changes require restart. Use `/mode` for live strategy switching.",
                parse_mode="Markdown",
            )
            return

        param = context.args[0].lower()
        value = context.args[1]
        await update.message.reply_text(
            f"⚠️ Config changes require restart.\n"
            f"Edit `config.yaml` and restart the bot.\n"
            f"Requested: {param} = {value}\n\n"
            f"💡 Tip: Use `/mode` to switch strategies without restarting."
        )

    async def _cmd_filters(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        stats = self.tracker.get_filter_stats()
        if not stats:
            await update.message.reply_text("No filter data yet.")
            return

        lines = ["*Filter Performance*\n"]
        for s in stats:
            total = s["passed"] + s["failed"]
            fail_pct = (s["failed"] / total * 100) if total > 0 else 0
            lines.append(
                f"`{s['filter']:20s}` ✅{s['passed']} ❌{s['failed']} ({fail_pct:.0f}% reject)"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        msg = (
            "*EDEC Bot Commands*\n"
            "/status — Per-coin book prices + bot state\n"
            "/mode — Show current strategy mode\n"
            "/mode both|dual|single|off — Switch mode live\n"
            "/start — Resume trading\n"
            "/stop — Pause trading\n"
            "/kill — Emergency stop\n"
            "/trades — Last 10 trades\n"
            "/stats — Today's summary\n"
            "/stats 7d — Last 7 days\n"
            "/export — Send Excel file\n"
            "/export today — Today only\n"
            "/config — Show all settings\n"
            "/filters — Filter pass/fail rates\n"
            "/help — This message"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
