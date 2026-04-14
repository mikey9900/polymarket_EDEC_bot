"""Telegram interface — commands, alerts, status updates, and data export."""

import asyncio
import logging
import os
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from bot.config import Config
from bot.tracker import DecisionTracker
from bot.risk_manager import RiskManager
from version import __version__

logger = logging.getLogger(__name__)

# Mode labels for display
MODE_LABELS = {
    "both": "🟢 ALL (dual + single + lead-lag + swing)",
    "dual": "🔵 DUAL-LEG only",
    "single": "🟡 SINGLE-LEG only",
    "lead": "🟠 LEAD-LAG only",
    "swing": "🟣 SWING LEG only",
    "off": "🔴 OFF",
}


BUDGET_OPTIONS = [1, 2, 5, 10, 15, 20]
CAPITAL_OPTIONS = [5, 10, 20, 50, 100]


class TelegramBot:
    def __init__(self, config: Config, tracker: DecisionTracker,
                 risk_manager: RiskManager, export_fn=None, export_recent_fn=None,
                 scanner=None, strategy_engine=None, executor=None, aggregator=None):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.export_fn = export_fn
        self.export_recent_fn = export_recent_fn
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.aggregator = aggregator
        self.chat_id = config.telegram_chat_id
        self._app: Application | None = None
        self._dashboard_message_id: int | None = None  # live dashboard message
        self._dashboard_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._refresh_lock = asyncio.Lock()
        self._ephemeral_msgs: list[int] = []  # message IDs to delete on next cleanup

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
            ("clean", self._cmd_clean),
        ]
        for cmd, handler in handlers:
            self._app.add_handler(CommandHandler(cmd, handler))

        # Inline button callbacks
        self._app.add_handler(CallbackQueryHandler(self._handle_button))

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

    # -----------------------------------------------------------------------
    # Live dashboard
    # -----------------------------------------------------------------------

    @staticmethod
    def _format_usd(price: float) -> str:
        """Format a USD price appropriately for its magnitude."""
        if price >= 10000:
            return f"${price:,.0f}"
        elif price >= 1000:
            return f"${price:,.0f}"
        elif price >= 100:
            return f"${price:.1f}"
        elif price >= 10:
            return f"${price:.2f}"
        elif price >= 1:
            return f"${price:.3f}"
        else:
            return f"${price:.4f}"

    def _build_dashboard_text(self) -> str:
        """Build the live dashboard message text with live prices and resolution history."""
        now_str = datetime.utcnow().strftime("%H:%M:%S UTC")
        is_active = self.strategy_engine.is_active if self.strategy_engine else False
        paper = self.tracker.get_paper_stats() if self.tracker else {}

        state_icon = "🟢" if is_active else "⏹"
        state_label = "SCANNING" if is_active else "STOPPED"

        pnl = paper.get("total_pnl", 0)
        pnl_emoji = "📈" if pnl >= 0 else "📉"
        wins = paper.get("wins", 0)
        losses = paper.get("losses", 0)
        open_pos = paper.get("open_positions", 0)
        balance = paper.get("current_balance", 0)
        total_cap = paper.get("total_capital", 0)
        win_rate = f"{paper.get('win_rate', 0):.0f}%" if (wins + losses) > 0 else "—"

        lines = [
            f"📡 *EDEC Bot v{__version__}*  {state_icon} {state_label}  🧪 Dry  _{now_str}_",
            "─────────────────────────────",
        ]

        # Per-coin row: COIN  $PRICE  ⬆️⬇️⬆️⬆️  UP/DN
        if self.scanner or self.aggregator:
            snapshot = self.scanner.get_status_snapshot() if self.scanner else {}
            for coin in self.config.coins:
                # Live USD price from aggregator
                usd_str = "—"
                if self.aggregator:
                    agg = self.aggregator.get_aggregated_price(coin)
                    if agg:
                        usd_str = self._format_usd(agg.price)

                # Last 4 resolution outcomes
                history_icons = ""
                if self.tracker:
                    outcomes = self.tracker.get_coin_recent_outcomes(coin, limit=4)
                    icons = []
                    for o in outcomes:
                        icons.append("✅" if o == "UP" else "❌")
                    # Pad to 4 with dots if fewer than 4
                    while len(icons) < 4:
                        icons.insert(0, "·")
                    history_icons = "".join(icons)

                # Order book prices
                data = snapshot.get(coin)
                signal_icon = ""
                if data:
                    up_ask = data.get("up_ask", 0)
                    dn_ask = data.get("down_ask", 0)
                    book_str = f"↑{up_ask:.2f} ↓{dn_ask:.2f}"
                    # Signal indicator (kept outside the box)
                    cfg_dl = self.config.dual_leg
                    cfg_sl = self.config.single_leg
                    cfg_ll = self.config.lead_lag
                    cfg_sw = self.config.swing_leg
                    combined = up_ask + dn_ask
                    if combined <= cfg_dl.max_combined_cost:
                        signal_icon = " 🔵"
                    elif up_ask <= cfg_sl.entry_max and dn_ask >= cfg_sl.opposite_min:
                        signal_icon = " 🟡"
                    elif dn_ask <= cfg_sl.entry_max and up_ask >= cfg_sl.opposite_min:
                        signal_icon = " 🟡"
                    elif cfg_ll.min_entry <= up_ask <= cfg_ll.max_entry:
                        signal_icon = " 🟠"
                    elif cfg_ll.min_entry <= dn_ask <= cfg_ll.max_entry:
                        signal_icon = " 🟠"
                    elif up_ask <= cfg_sw.first_leg_max or dn_ask <= cfg_sw.first_leg_max:
                        signal_icon = " 🟣"
                else:
                    book_str = "no market"

                coin_label = f"`{coin.upper():<4}`"
                price_col = f"`{usd_str:>10}`"
                history_col = history_icons if history_icons else "····"
                lines.append(f"{coin_label} {price_col}  {history_col}  `{book_str}`{signal_icon}")

        buys = paper.get("buys", 0)
        sells = paper.get("sells", 0)
        avg_buy = paper.get("avg_buy_price", 0)
        avg_sell = paper.get("avg_sell_price", 0)

        lines += [
            "─────────────────────────────",
            f"💰 `${balance:.2f}` / `${total_cap:.0f}`  {pnl_emoji} P&L: `${pnl:+.2f}`",
            f"✅ {wins}  ❌ {losses}  📦 {open_pos}  🎯 {win_rate}",
            f"🛒 Buys: {buys} avg `${avg_buy:.3f}`  💸 Sells: {sells} avg `${avg_sell:.3f}`",
        ]

        return "\n".join(lines)

    _MSG_ID_FILE = "data/dashboard_msg_id.txt"
    _EPHEMERAL_LOG = "data/ephemeral_msgs.txt"

    def _save_msg_id(self) -> None:
        try:
            with open(self._MSG_ID_FILE, "w") as f:
                f.write(str(self._dashboard_message_id or ""))
        except Exception:
            pass

    def _load_msg_id(self) -> int | None:
        try:
            val = open(self._MSG_ID_FILE).read().strip()
            return int(val) if val else None
        except Exception:
            return None

    def _persist_ephemeral(self, msg_id: int) -> None:
        """Append a sent message ID to the persistent log so it survives restarts."""
        try:
            with open(self._EPHEMERAL_LOG, "a") as f:
                f.write(f"{msg_id}\n")
        except Exception:
            pass

    def _load_persisted_ephemerals(self) -> list[int]:
        try:
            with open(self._EPHEMERAL_LOG) as f:
                return [int(line.strip()) for line in f if line.strip().isdigit()]
        except Exception:
            return []

    def _clear_ephemeral_log(self) -> None:
        try:
            open(self._EPHEMERAL_LOG, "w").close()
        except Exception:
            pass

    async def start_dashboard(self):
        """Send the live dashboard message and start the refresh + cleanup loops."""
        if not self._app or not self.chat_id:
            return

        # Delete all messages from the previous run immediately on startup
        old_msgs = self._load_persisted_ephemerals()
        self._clear_ephemeral_log()
        for msg_id in old_msgs:
            try:
                await self._app.bot.delete_message(chat_id=self.chat_id, message_id=msg_id)
            except Exception as e:
                logger.debug(f"Could not delete old message {msg_id}: {e}")

        # Delete leftover dashboard from previous run
        old_id = self._load_msg_id()
        if old_id:
            try:
                await self._app.bot.delete_message(chat_id=self.chat_id, message_id=old_id)
            except Exception:
                pass

        try:
            msg = await self._app.bot.send_message(
                chat_id=self.chat_id,
                text=self._build_dashboard_text(),
                parse_mode="Markdown",
                reply_markup=self._main_keyboard(),
            )
            self._dashboard_message_id = msg.message_id
            self._save_msg_id()
            self._dashboard_task = asyncio.create_task(self._dashboard_loop())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Live dashboard started")
        except Exception as e:
            logger.error(f"Failed to start dashboard: {e}")

    async def stop_dashboard(self):
        """Stop the dashboard refresh and cleanup loops."""
        for task in (self._dashboard_task, self._cleanup_task):
            if task:
                task.cancel()
        self._dashboard_task = None
        self._cleanup_task = None

    async def _dashboard_loop(self):
        """Refresh the dashboard message every 10 seconds."""
        while True:
            await asyncio.sleep(10)
            await self._refresh_dashboard()

    async def _cleanup_loop(self):
        """Every 60 seconds: delete ephemeral messages and refresh the dashboard in-place."""
        while True:
            await asyncio.sleep(60)
            await self._do_cleanup()

    async def _repost_dashboard(self):
        """Delete old dashboard and re-post at the bottom (only used after sending new alerts)."""
        if not self._app or not self.chat_id:
            return
        if self._dashboard_message_id:
            try:
                await self._app.bot.delete_message(
                    chat_id=self.chat_id, message_id=self._dashboard_message_id
                )
            except Exception:
                pass
            self._dashboard_message_id = None
        try:
            text = self._build_dashboard_text()
            msg = await self._app.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=self._main_keyboard(),
            )
            self._dashboard_message_id = msg.message_id
            self._save_msg_id()
        except Exception as e:
            logger.error(f"Failed to re-post dashboard: {e}")

    async def _do_cleanup(self):
        """Delete tracked ephemeral messages in parallel, then refresh the dashboard."""
        if not self._app or not self.chat_id:
            return
        msgs_to_delete = self._ephemeral_msgs[:]
        self._ephemeral_msgs.clear()
        self._clear_ephemeral_log()
        if msgs_to_delete:
            await asyncio.gather(
                *[self._app.bot.delete_message(chat_id=self.chat_id, message_id=mid)
                  for mid in msgs_to_delete],
                return_exceptions=True,
            )
        await self._refresh_dashboard()

    async def _deep_clean(self):
        """Sweep the last 250 message IDs before the dashboard and delete any bot messages.
        Catches old messages whose IDs were never tracked (e.g. from previous runs)."""
        if not self._app or not self.chat_id:
            return

        # Collect tracked IDs + range sweep around current dashboard
        ids: set[int] = set(self._ephemeral_msgs)
        self._ephemeral_msgs.clear()
        self._clear_ephemeral_log()

        if self._dashboard_message_id:
            lo = max(1, self._dashboard_message_id - 250)
            hi = self._dashboard_message_id  # don't delete the dashboard itself
            ids.update(range(lo, hi))

        if ids:
            results = await asyncio.gather(
                *[self._app.bot.delete_message(chat_id=self.chat_id, message_id=mid)
                  for mid in ids],
                return_exceptions=True,
            )
            deleted = sum(1 for r in results if not isinstance(r, Exception))
            logger.info(f"Deep clean: deleted {deleted}/{len(ids)} messages")

        await self._refresh_dashboard()

    async def _refresh_dashboard(self):
        """Edit the existing dashboard message with fresh data.
        Falls back to re-posting if the message ID is lost."""
        if not self._app or not self.chat_id:
            return

        # No known message — re-create from scratch (no loop: calls repost, not cleanup)
        if not self._dashboard_message_id:
            await self._repost_dashboard()
            return

        # Build text first — surface content errors before touching Telegram
        try:
            text = self._build_dashboard_text()
        except Exception as e:
            logger.error(f"Dashboard build error: {e}", exc_info=True)
            return

        # Capture the ID we intend to edit before any await
        msg_id = self._dashboard_message_id
        retry_after: float = 0.0

        async with self._refresh_lock:
            try:
                await self._app.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=msg_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=self._main_keyboard(),
                )
            except RetryAfter as e:
                retry_after = e.retry_after
                logger.warning(f"Telegram rate limit — retry in {retry_after}s")
            except Exception as e:
                err_str = str(e).lower()
                if "message is not modified" in err_str:
                    pass
                elif "message to edit not found" in err_str:
                    # Only nullify if nobody else already updated _dashboard_message_id
                    if self._dashboard_message_id == msg_id:
                        self._dashboard_message_id = None
                else:
                    logger.warning(f"Dashboard refresh failed: {e}")

        # Sleep outside the lock so other refreshes aren't blocked
        if retry_after:
            await asyncio.sleep(retry_after)

    async def send_alert(self, message: str, reply_markup=None):
        if not self._app or not self.chat_id:
            logger.warning(f"Telegram not configured — skipping message: {message[:50]}")
            return
        try:
            sent = await self._app.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
                reply_markup=reply_markup,
            )
            self._ephemeral_msgs.append(sent.message_id)
            self._persist_ephemeral(sent.message_id)
        except Exception as e:
            logger.error(f"Telegram send error: {e} | chat_id={self.chat_id}")

    def _track(self, msg) -> None:
        """Track a sent message for cleanup."""
        if msg:
            self._ephemeral_msgs.append(msg.message_id)
            self._persist_ephemeral(msg.message_id)

    def _main_keyboard(self) -> InlineKeyboardMarkup:
        """Main control keyboard."""
        status = self.risk_manager.get_status()
        is_running = self.strategy_engine.is_active if self.strategy_engine else False
        order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
        is_dry = self.config.execution.dry_run
        _, capital_balance = self.tracker.get_paper_capital() if self.tracker else (0, 0)

        return InlineKeyboardMarkup([
            # Row 1: Run control
            [
                InlineKeyboardButton(
                    "⏸ Pause" if is_running else "▶️ Resume",
                    callback_data="stop" if is_running else "start",
                ),
                InlineKeyboardButton("🛑 Kill Switch", callback_data="kill"),
            ],
            # Row 2: Mode toggle
            [
                InlineKeyboardButton(
                    "📋 Dry Run ✅" if is_dry else "📋 Dry Run",
                    callback_data="noop",
                ),
                InlineKeyboardButton(
                    "🌊 Wet Run 🔒",
                    callback_data="wet_disabled",
                ),
            ],
            # Row 3: Data
            [
                InlineKeyboardButton("📊 Stats", callback_data="stats"),
                InlineKeyboardButton("📈 Status", callback_data="status"),
            ],
            [
                InlineKeyboardButton("📋 Trades", callback_data="trades"),
                InlineKeyboardButton("🔍 Filters", callback_data="filters"),
            ],
            # Row 4: Capital & Budget
            [
                InlineKeyboardButton(f"🏦 Capital: ${capital_balance:.2f}", callback_data="capital"),
                InlineKeyboardButton(f"💰 Budget: ${order_size:.0f}", callback_data="budget"),
            ],
            # Row 5: Refresh / Reset
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="refresh"),
                InlineKeyboardButton("🗑 Reset Stats", callback_data="reset_stats"),
            ],
            # Row 6: Export
            [
                InlineKeyboardButton("📤 Export Today", callback_data="export_today"),
                InlineKeyboardButton("📤 Export All", callback_data="export_all"),
            ],
            # Row 7: Quick AI export
            [
                InlineKeyboardButton("📊 Last 50 Trades (CSV)", callback_data="export_recent"),
            ],
        ])

    def _budget_keyboard(self) -> InlineKeyboardMarkup:
        """Budget per-trade selection."""
        order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
        buttons = [
            InlineKeyboardButton(
                f"✅ ${amt}" if amt == order_size else f"${amt}",
                callback_data=f"budget_{amt}",
            )
            for amt in BUDGET_OPTIONS
        ]
        rows = [buttons[i:i+3] for i in range(0, len(buttons), 3)]
        rows.append([InlineKeyboardButton("« Back", callback_data="back")])
        return InlineKeyboardMarkup(rows)

    def _capital_keyboard(self) -> InlineKeyboardMarkup:
        """Paper capital selection."""
        _, balance = self.tracker.get_paper_capital()
        buttons = [
            InlineKeyboardButton(f"${amt}", callback_data=f"capital_{amt}")
            for amt in CAPITAL_OPTIONS
        ]
        rows = [buttons[i:i+3] for i in range(0, len(buttons), 3)]
        rows.append([InlineKeyboardButton("« Back", callback_data="back")])
        return InlineKeyboardMarkup(rows)

    async def _handle_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline keyboard button presses."""
        query = update.callback_query

        if not self._auth(update):
            return

        data = query.data

        # --- No-op / disabled buttons ---
        if data in ("noop", "wet_disabled"):
            await query.answer(
                "🌊 Wet Run coming soon — currently disabled for safety." if data == "wet_disabled"
                else "Already in Dry Run mode.",
                show_alert=True,
            )
            return

        # --- Budget selection ---
        if data.startswith("budget_"):
            amt = float(data.split("_")[1])
            await query.answer(f"✅ Budget set to ${amt:.0f}", show_alert=False)
            if self.executor:
                self.executor.set_order_size(amt)
            await self._do_cleanup()
            return

        if data == "budget":
            await query.answer()
            order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
            await query.edit_message_text(
                f"💰 *Budget Per Trade*\n"
                f"Current: *${order_size:.0f}*\n"
                f"_Amount spent per order leg._",
                parse_mode="Markdown",
                reply_markup=self._budget_keyboard(),
            )
            return

        # --- Capital selection ---
        if data.startswith("capital_"):
            amt = float(data.split("_")[1])
            await query.answer(f"✅ Capital set to ${amt:.0f}", show_alert=False)
            if self.tracker:
                self.tracker.set_paper_capital(amt)
            await self._do_cleanup()
            return

        if data == "capital":
            await query.answer()
            _, balance = self.tracker.get_paper_capital() if self.tracker else (0, 0)
            await query.edit_message_text(
                f"🏦 *Paper Capital*\n"
                f"Current balance: *${balance:.2f}*\n\n"
                f"_Select a new bankroll to start fresh:_",
                parse_mode="Markdown",
                reply_markup=self._capital_keyboard(),
            )
            return

        if data == "back":
            await query.answer()
            await self._refresh_dashboard()
            return

        # --- Start / Stop / Kill ---
        if data == "start":
            await query.answer("▶️ Scanning started", show_alert=False)
            if self.strategy_engine:
                self.strategy_engine.start_scanning()
            self.risk_manager.resume()
            self.risk_manager.deactivate_kill_switch()
            await self._do_cleanup()
            return

        if data == "stop":
            await query.answer("⏸ Bot stopped", show_alert=False)
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            self.risk_manager.pause()
            await self._do_cleanup()
            return

        if data == "kill":
            await query.answer("🛑 Kill switch activated!", show_alert=True)
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            self.risk_manager.activate_kill_switch("Manual kill via Telegram")
            await self._do_cleanup()
            return

        if data == "refresh":
            await query.answer("🧹 Cleaning up...", show_alert=False)
            await self._deep_clean()
            return

        if data == "reset_stats":
            await query.answer("🗑 Stats reset!", show_alert=False)
            if self.tracker:
                self.tracker.reset_paper_stats()
            self.risk_manager.reset_daily_stats()
            await self._do_cleanup()
            return

        # --- Info buttons (edit dashboard in-place — no new messages, no chat clutter) ---
        _back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="back")]])
        await query.answer()

        if data == "stats":
            stats = self.tracker.get_daily_stats()
            paper = self.tracker.get_paper_stats()
            pnl_emoji = "📈" if paper["total_pnl"] >= 0 else "📉"
            win_rate = f"{paper['win_rate']:.0f}%" if paper["total_trades"] > 0 else "—"
            text = (
                f"📊 *Today's Stats ({stats['date']})*\n"
                f"Evaluations: {stats['total_evaluations']}\n"
                f"Signals: {stats['signals']} | Skips: {stats['skips']}\n\n"
                f"{pnl_emoji} *Paper Trading*\n"
                f"Capital: ${paper['current_balance']:.2f} / ${paper['total_capital']:.2f}\n"
                f"P&L: ${paper['total_pnl']:+.2f} | Win rate: {win_rate}\n"
                f"Trades: {paper['total_trades']} ✅{paper['wins']} ❌{paper['losses']} 🔄{paper['open_positions']}"
            )
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_back_kb)

        elif data == "status":
            status = self.risk_manager.get_status()
            mode = self.strategy_engine.mode if self.strategy_engine else "unknown"
            order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
            text = (
                f"📈 *Status*\n"
                f"State: {'🔴 KILLED' if status['kill_switch'] else '⏸ PAUSED' if status['paused'] else '🟢 RUNNING'}\n"
                f"Mode: {MODE_LABELS.get(mode, mode)}\n"
                f"Budget: ${order_size:.0f}/trade\n"
                f"Daily P&L: ${status['daily_pnl']:+.2f} | Open: {status['open_positions']}"
            )
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_back_kb)

        elif data == "trades":
            trades = self.tracker.get_recent_trades(limit=5)
            if not trades:
                text = "No trades yet."
            else:
                lines = ["📋 *Recent Trades*\n"]
                for t in trades:
                    pnl = t.get("actual_profit")
                    pnl_str = f"${pnl:+.4f}" if pnl is not None else "pending"
                    emoji = "✅" if t["status"] == "success" else "❌"
                    lines.append(f"{emoji} `{t['timestamp'][:16]}` {t['coin'].upper()} → {pnl_str}")
                text = "\n".join(lines)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_back_kb)

        elif data in ("export_today", "export_all"):
            # Export sends a document — run in thread pool so it never blocks the event loop
            if not self.export_fn:
                self._track(await query.message.reply_text("Export not available."))
                await self._repost_dashboard()
                return
            wait_msg = await query.message.reply_text("⏳ Generating spreadsheet...")
            self._track(wait_msg)
            try:
                loop = asyncio.get_event_loop()
                today_only = (data == "export_today")
                path = await loop.run_in_executor(None, lambda: self.export_fn(today_only=today_only))
                import os
                with open(path, "rb") as f:
                    self._track(await query.message.reply_document(
                        document=f,
                        filename=os.path.basename(path),
                        caption="📊 EDEC Bot Export — Paper Trades, Decisions, Filter Performance",
                    ))
            except Exception as e:
                self._track(await query.message.reply_text(f"Export error: {e}"))
            await self._repost_dashboard()

        elif data == "export_recent":
            if not self.export_recent_fn:
                self._track(await query.message.reply_text("Recent export not available."))
                await self._repost_dashboard()
                return
            wait_msg = await query.message.reply_text("⏳ Building last 50 trades CSV...")
            self._track(wait_msg)
            try:
                loop = asyncio.get_event_loop()
                path = await loop.run_in_executor(None, self.export_recent_fn)
                import os
                with open(path, "rb") as f:
                    self._track(await query.message.reply_document(
                        document=f,
                        filename=os.path.basename(path),
                        caption="📊 Last 50 Trades CSV — compact export for AI analysis",
                    ))
            except Exception as e:
                self._track(await query.message.reply_text(f"Export error: {e}"))
            await self._repost_dashboard()

        elif data == "filters":
            stats = self.tracker.get_filter_stats()
            if not stats:
                text = "No filter data yet."
            else:
                lines = ["🔍 *Filter Performance*\n"]
                for s in stats:
                    total = s["passed"] + s["failed"]
                    fail_pct = (s["failed"] / total * 100) if total > 0 else 0
                    bar = "🟩" * int((100 - fail_pct) / 20) + "🟥" * int(fail_pct / 20)
                    lines.append(f"`{s['filter']:18s}` {bar} {fail_pct:.0f}% fail")
                text = "\n".join(lines)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=_back_kb)

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

    def _track_cmd(self, update: Update) -> None:
        """Track the user's command message for cleanup."""
        if update.message:
            self._ephemeral_msgs.append(update.message.message_id)

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)

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

        self._track_cmd(update)
        self._track(await update.message.reply_text("\n".join(lines), parse_mode="Markdown"))

    async def _cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)

        if not self.strategy_engine:
            self._track(await update.message.reply_text("Strategy engine not available."))
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
            self._track(await update.message.reply_text(msg, parse_mode="Markdown"))
            return

        new_mode = args[0].lower()
        if self.strategy_engine.set_mode(new_mode):
            label = MODE_LABELS.get(new_mode, new_mode)
            self._track(await update.message.reply_text(f"✅ Mode set to: {label}"))
        else:
            self._track(await update.message.reply_text(
                f"❌ Unknown mode `{new_mode}`. Use: both, dual, single, off",
                parse_mode="Markdown",
            ))

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        self.risk_manager.resume()
        self.risk_manager.deactivate_kill_switch()
        self._track(await update.message.reply_text("▶️ Trading resumed"))

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        self.risk_manager.pause()
        self._track(await update.message.reply_text("⏸ Trading paused (monitoring continues)"))

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        self.risk_manager.activate_kill_switch("Manual kill via Telegram")
        self._track(await update.message.reply_text("🛑 Kill switch activated — all trading stopped"))

    async def _cmd_trades(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        trades = self.tracker.get_recent_trades(limit=10)
        if not trades:
            self._track(await update.message.reply_text("No trades yet."))
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
        self._track(await update.message.reply_text("\n".join(lines), parse_mode="Markdown"))

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
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
            self._track(await update.message.reply_text("\n".join(lines), parse_mode="Markdown"))
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
            self._track(await update.message.reply_text(msg, parse_mode="Markdown"))

    async def _cmd_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not self.export_fn:
            self._track(await update.message.reply_text("Export not available"))
            return

        wait_msg = await update.message.reply_text("⏳ Generating Excel export...")
        self._track(wait_msg)
        try:
            today_only = context.args and context.args[0] == "today"
            path = self.export_fn(today_only=today_only)
            with open(path, "rb") as f:
                self._track(await update.message.reply_document(
                    document=f,
                    filename=os.path.basename(path),
                    caption="📊 EDEC Bot Decision Export",
                ))
        except Exception as e:
            self._track(await update.message.reply_text(f"Export error: {e}"))

    async def _cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
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
        self._track(await update.message.reply_text(msg, parse_mode="Markdown"))

    async def _cmd_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not context.args or len(context.args) < 2:
            self._track(await update.message.reply_text(
                "Usage: `/set <param> <value>`\n"
                "Params: threshold, max\\_cost, min\\_edge, size, dry\\_run\n\n"
                "Note: Most changes require restart. Use `/mode` for live strategy switching.",
                parse_mode="Markdown",
            ))
            return

        param = context.args[0].lower()
        value = context.args[1]
        self._track(await update.message.reply_text(
            f"⚠️ Config changes require restart.\n"
            f"Edit `config.yaml` and restart the bot.\n"
            f"Requested: {param} = {value}\n\n"
            f"💡 Tip: Use `/mode` to switch strategies without restarting."
        ))

    async def _cmd_filters(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        stats = self.tracker.get_filter_stats()
        if not stats:
            self._track(await update.message.reply_text("No filter data yet."))
            return

        lines = ["*Filter Performance*\n"]
        for s in stats:
            total = s["passed"] + s["failed"]
            fail_pct = (s["failed"] / total * 100) if total > 0 else 0
            lines.append(
                f"`{s['filter']:20s}` ✅{s['passed']} ❌{s['failed']} ({fail_pct:.0f}% reject)"
            )
        self._track(await update.message.reply_text("\n".join(lines), parse_mode="Markdown"))

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
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
            "/clean — Delete old chat messages\n"
            "/help — This message"
        )
        self._track(await update.message.reply_text(msg, parse_mode="Markdown"))

    async def _cmd_clean(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Delete old bot messages by sweeping recent message IDs."""
        if not self._auth(update):
            return
        # Delete the user's /clean command too
        try:
            await update.message.delete()
        except Exception:
            pass
        await self._deep_clean()
