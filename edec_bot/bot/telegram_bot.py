"""Telegram interface — commands, alerts, status updates, and data export."""

import asyncio
import io
import json
import logging
import mimetypes
import os
import zipfile
from datetime import datetime
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest
from uuid import uuid4

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.request import HTTPXRequest

from bot.config import Config
from bot.tracker import DecisionTracker
from bot.risk_manager import RiskManager
from version import __version__

logger = logging.getLogger(__name__)

# Mode labels for display
MODE_LABELS = {
    "both": "🟢 ALL enabled strategies",
    "dual": "🔵 DUAL-LEG only",
    "single": "🟡 SINGLE-LEG only",
    "lead": "🟠 LEAD-LAG only",
    "swing": "🟣 SWING LEG only",
    "off": "🔴 OFF",
}


BUDGET_OPTIONS = [1, 2, 5, 10, 15, 20]
CAPITAL_OPTIONS = [5, 10, 20, 50, 100, 5000, 25000, 50000]


class TelegramBot:
    _SEND_FILE_ATTEMPTS = max(1, int(os.getenv("EDEC_TG_SEND_FILE_ATTEMPTS", "3")))
    _SEND_FILE_CONNECT_TIMEOUT = float(os.getenv("EDEC_TG_SEND_FILE_CONNECT_TIMEOUT", "30"))
    _SEND_FILE_READ_TIMEOUT = float(os.getenv("EDEC_TG_SEND_FILE_READ_TIMEOUT", "120"))
    _SEND_FILE_WRITE_TIMEOUT = float(os.getenv("EDEC_TG_SEND_FILE_WRITE_TIMEOUT", "120"))
    _SEND_FILE_POOL_TIMEOUT = float(os.getenv("EDEC_TG_SEND_FILE_POOL_TIMEOUT", "30"))
    _GET_UPDATES_READ_TIMEOUT = float(os.getenv("EDEC_TG_GET_UPDATES_READ_TIMEOUT", "35"))
    _TELEGRAM_HTTP_POOL_SIZE = max(8, int(os.getenv("EDEC_TG_HTTP_POOL_SIZE", "32")))
    _TELEGRAM_HTTP_TRUST_ENV = os.getenv("EDEC_TG_HTTP_TRUST_ENV", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    _TELEGRAM_PROXY_URL = (os.getenv("EDEC_TG_PROXY_URL") or "").strip() or None

    def __init__(self, config: Config, tracker: DecisionTracker,
                 risk_manager: RiskManager, export_fn=None, export_recent_fn=None,
                 scanner=None, strategy_engine=None, executor=None, aggregator=None,
                 archive_fn=None, archive_latest_fn=None, archive_health_fn=None,
                 repo_sync_fn=None):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.export_fn = export_fn
        self.export_recent_fn = export_recent_fn
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.aggregator = aggregator
        self.archive_fn = archive_fn
        self.archive_latest_fn = archive_latest_fn
        self.archive_health_fn = archive_health_fn
        self.repo_sync_fn = repo_sync_fn
        self.chat_id = config.telegram_chat_id
        self._app: Application | None = None
        self._dashboard_message_id: int | None = None  # live dashboard message
        self._dashboard_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._refresh_lock = asyncio.Lock()
        self._ephemeral_msgs: list[int] = []  # message IDs to delete on next cleanup
        self._dashboard_view: str = "main"

    def _enabled_mode_summary(self) -> str:
        enabled = []
        if self.config.dual_leg.enabled:
            enabled.append("dual-leg")
        if self.config.single_leg.enabled:
            enabled.append("single-leg")
        if self.config.lead_lag.enabled:
            enabled.append("lead-lag")
        if self.config.swing_leg.enabled:
            enabled.append("swing-leg")
        return " + ".join(enabled) if enabled else "none"

    def _mode_help_lines(self) -> list[str]:
        return [
            f"`/mode both` - all enabled strategies ({self._enabled_mode_summary()})",
            "`/mode dual` - dual-leg arb only",
            "`/mode single` - single-leg repricing only",
            "`/mode lead` - lead-lag repricing only",
            "`/mode swing` - swing-leg mean reversion only",
            "`/mode off` - pause all trading",
        ]

    def _build_request(self, *, for_updates: bool) -> HTTPXRequest:
        read_timeout = self._GET_UPDATES_READ_TIMEOUT if for_updates else self._SEND_FILE_READ_TIMEOUT
        return HTTPXRequest(
            connection_pool_size=self._TELEGRAM_HTTP_POOL_SIZE,
            read_timeout=read_timeout,
            write_timeout=self._SEND_FILE_WRITE_TIMEOUT,
            connect_timeout=self._SEND_FILE_CONNECT_TIMEOUT,
            pool_timeout=self._SEND_FILE_POOL_TIMEOUT,
            media_write_timeout=self._SEND_FILE_WRITE_TIMEOUT,
            proxy=self._TELEGRAM_PROXY_URL,
            httpx_kwargs={"trust_env": self._TELEGRAM_HTTP_TRUST_ENV},
        )

    async def start(self):
        """Initialize and start the Telegram bot."""
        if not self.config.telegram_bot_token:
            logger.warning("No Telegram bot token configured — Telegram disabled")
            return

        logger.info(
            "Telegram HTTP client configured (trust_env=%s, proxy=%s)",
            self._TELEGRAM_HTTP_TRUST_ENV,
            "set" if self._TELEGRAM_PROXY_URL else "disabled",
        )
        self._app = (
            Application.builder()
            .token(self.config.telegram_bot_token)
            .request(self._build_request(for_updates=False))
            .get_updates_request(self._build_request(for_updates=True))
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
            ("latest_export", self._cmd_latest_export),
            ("sync_repo_latest", self._cmd_sync_repo_latest),
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
    _DEEP_CLEAN_WINDOW = int(os.getenv("EDEC_TG_DEEP_CLEAN_WINDOW", "15000"))
    _DEEP_CLEAN_BATCH = int(os.getenv("EDEC_TG_DEEP_CLEAN_BATCH", "200"))
    _DEEP_CLEAN_PAUSE_S = float(os.getenv("EDEC_TG_DEEP_CLEAN_PAUSE_S", "0.15"))
    _STARTUP_DEEP_CLEAN = os.getenv("EDEC_TG_STARTUP_DEEP_CLEAN", "false").lower() not in ("0", "false", "no")
    _AUTO_EPHEMERAL_CLEAN = os.getenv("EDEC_TG_AUTO_EPHEMERAL_CLEAN", "false").lower() not in ("0", "false", "no")

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
            self._dashboard_view = "main"
            self._save_msg_id()
            self._dashboard_task = asyncio.create_task(self._dashboard_loop())
            if self._AUTO_EPHEMERAL_CLEAN:
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Live dashboard started")
            if self._STARTUP_DEEP_CLEAN:
                await self._deep_clean()
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
            self._dashboard_view = "main"
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

    async def _delete_ids_batched(self, to_delete: list[int]) -> dict:
        stats = {"attempted": len(to_delete), "deleted": 0, "undeletable": 0}
        if not to_delete:
            return stats
        batch = max(1, self._DEEP_CLEAN_BATCH)
        for i in range(0, len(to_delete), batch):
            chunk = to_delete[i:i + batch]
            results = await asyncio.gather(
                *[
                    self._app.bot.delete_message(chat_id=self.chat_id, message_id=mid)
                    for mid in chunk
                ],
                return_exceptions=True,
            )
            for r in results:
                if not isinstance(r, Exception):
                    stats["deleted"] += 1
                else:
                    msg = str(r).lower()
                    if "can't be deleted" in msg or "can not be deleted" in msg:
                        stats["undeletable"] += 1
            await asyncio.sleep(max(0.0, self._DEEP_CLEAN_PAUSE_S))
        return stats

    async def _deep_clean(self) -> dict:
        """Sweep tracked + recent message IDs before the dashboard and delete bot clutter."""
        stats = {"attempted": 0, "deleted": 0, "undeletable": 0}
        if not self._app or not self.chat_id:
            return stats

        # Collect tracked IDs + range sweep around current dashboard
        ids: set[int] = set(self._ephemeral_msgs)
        self._ephemeral_msgs.clear()
        self._clear_ephemeral_log()

        if self._dashboard_message_id:
            lo = max(1, self._dashboard_message_id - self._DEEP_CLEAN_WINDOW)
            hi = self._dashboard_message_id  # don't delete the dashboard itself
            ids.update(range(lo, hi))

        to_delete = sorted(ids, reverse=True)
        stats = await self._delete_ids_batched(to_delete)

        logger.info(
            "Deep clean: deleted %s/%s (undeletable=%s)",
            stats["deleted"],
            stats["attempted"],
            stats["undeletable"],
        )
        await self._refresh_dashboard()
        return stats

    async def _clear_chat_history(self) -> dict:
        """Attempt to clear as much prior chat history as allowed, then restore dashboard."""
        stats = {"attempted": 0, "deleted": 0, "undeletable": 0}
        if not self._app or not self.chat_id:
            return stats

        ids: set[int] = set(self._ephemeral_msgs)
        ids.update(self._load_persisted_ephemerals())
        self._ephemeral_msgs.clear()
        self._clear_ephemeral_log()

        if self._dashboard_message_id and self._dashboard_message_id > 1:
            ids.update(range(1, self._dashboard_message_id))

        to_delete = sorted(ids, reverse=True)
        stats = await self._delete_ids_batched(to_delete)
        logger.info(
            "Clear chat: deleted %s/%s (undeletable=%s)",
            stats["deleted"],
            stats["attempted"],
            stats["undeletable"],
        )
        await self._repost_dashboard()
        return stats

    async def _refresh_dashboard(self, force: bool = False):
        """Edit the existing dashboard message with fresh data.
        Falls back to re-posting if the message ID is lost."""
        if not self._app or not self.chat_id:
            return

        if self._dashboard_view != "main" and not force:
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

    async def _send_document_via_raw_bot_api(
        self,
        file_bytes: bytes,
        filename: str,
        caption: str,
    ) -> tuple[bool, str | None]:
        if not self.config.telegram_bot_token or not self.chat_id:
            return False, "Telegram bot token/chat is not configured"
        loop = asyncio.get_running_loop()
        ok, error, message_id = await loop.run_in_executor(
            None,
            self._send_document_via_raw_bot_api_sync,
            file_bytes,
            filename,
            caption,
        )
        if ok and message_id:
            self._track_message_id(message_id)
        return ok, error

    def _send_document_via_raw_bot_api_sync(
        self,
        file_bytes: bytes,
        filename: str,
        caption: str,
    ) -> tuple[bool, str | None, int | None]:
        boundary = f"----EDECBotBoundary{uuid4().hex}"
        mime_type = mimetypes.guess_type(filename, strict=False)[0] or "application/octet-stream"
        body_parts: list[bytes] = []

        def _add_text(name: str, value: str) -> None:
            body_parts.extend(
                [
                    f"--{boundary}\r\n".encode("utf-8"),
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"),
                    value.encode("utf-8"),
                    b"\r\n",
                ]
            )

        _add_text("chat_id", str(self.chat_id))
        if caption:
            _add_text("caption", caption)

        body_parts.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                (
                    f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'
                ).encode("utf-8"),
                f"Content-Type: {mime_type}\r\n\r\n".encode("utf-8"),
                file_bytes,
                b"\r\n",
                f"--{boundary}--\r\n".encode("utf-8"),
            ]
        )
        payload = b"".join(body_parts)
        request = urlrequest.Request(
            url=f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendDocument",
            data=payload,
            method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
        timeout = max(self._SEND_FILE_CONNECT_TIMEOUT, self._SEND_FILE_READ_TIMEOUT)

        try:
            with opener.open(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
        except urlerror.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            compact = " ".join(raw.split()) or str(exc)
            return False, f"raw bot api http {exc.code}: {compact}", None
        except Exception as exc:
            return False, f"raw bot api {type(exc).__name__}: {exc}", None

        try:
            result = json.loads(raw)
        except Exception:
            compact = " ".join(raw.split())
            return False, f"raw bot api invalid response: {compact}", None

        if not result.get("ok"):
            compact = " ".join(str(result.get("description") or result).split())
            return False, f"raw bot api: {compact}", None

        message = result.get("result") or {}
        message_id = message.get("message_id")
        return True, None, message_id if isinstance(message_id, int) else None

    async def _send_excel_with_fallbacks(
        self,
        path: str,
        caption: str,
        prior_error: str | None = None,
    ) -> tuple[bool, str | None]:
        filename = os.path.basename(path)
        with open(path, "rb") as f:
            file_bytes = f.read()
        ok, error = await self._send_document_via_raw_bot_api(file_bytes, filename, caption)
        if ok:
            logger.info("Telegram raw Bot API fallback sent %s", path)
            return True, None
        combined = prior_error
        if error:
            combined = f"{prior_error}; raw bot api fallback failed: {error}" if prior_error else error
        return await self._send_excel_zip_fallback(path, caption, combined)

    async def _send_file_path(self, path: str, caption: str) -> tuple[bool, str | None]:
        if not self._app or not self.chat_id or not path:
            return False, "Telegram app/chat is not configured"
        if not os.path.exists(path):
            return False, "File does not exist"
        filename = os.path.basename(path)
        is_excel = path.lower().endswith(".xlsx")
        use_in_memory_upload = is_excel or os.path.getsize(path) <= 1024 * 1024
        attempts = max(1, self._SEND_FILE_ATTEMPTS)
        for attempt in range(1, attempts + 1):
            try:
                with open(path, "rb") as f:
                    document = (
                        InputFile(f.read(), filename=filename)
                        if use_in_memory_upload
                        else f
                    )
                    sent = await self._app.bot.send_document(
                        chat_id=self.chat_id,
                        document=document,
                        filename=filename,
                        caption=caption,
                        disable_content_type_detection=True if is_excel else None,
                        connect_timeout=self._SEND_FILE_CONNECT_TIMEOUT,
                        read_timeout=self._SEND_FILE_READ_TIMEOUT,
                        write_timeout=self._SEND_FILE_WRITE_TIMEOUT,
                        pool_timeout=self._SEND_FILE_POOL_TIMEOUT,
                    )
                self._track(sent)
                return True, None
            except RetryAfter as e:
                delay = float(e.retry_after)
                logger.warning(
                    "Telegram send_document rate-limited for %s on attempt %s/%s; retry in %.1fs",
                    path,
                    attempt,
                    attempts,
                    delay,
                )
                if attempt >= attempts:
                    return False, f"RetryAfter {delay:.1f}s"
                await asyncio.sleep(delay)
            except (TimedOut, TimeoutError) as e:
                delay = min(5.0 * attempt, 15.0)
                logger.warning(
                    "Telegram send_document timed out for %s on attempt %s/%s; retry in %.1fs: %s",
                    path,
                    attempt,
                    attempts,
                    delay,
                    e,
                )
                if attempt >= attempts:
                    return False, str(e)
                await asyncio.sleep(delay)
            except NetworkError as e:
                err_text = str(e)
                delay = min(5.0 * attempt, 15.0)
                retryable_markers = (
                    "timed out",
                    "readerror",
                    "writeerror",
                    "connection reset",
                    "server disconnected",
                    "connection aborted",
                    "connection refused",
                )
                if any(marker in err_text.lower() for marker in retryable_markers) and attempt < attempts:
                    logger.warning(
                        "Telegram send_document network error for %s on attempt %s/%s; retry in %.1fs: %s",
                        path,
                        attempt,
                        attempts,
                        delay,
                        err_text,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error("Telegram send_document network error for %s: %s", path, err_text)
                if is_excel and attempt >= attempts:
                    return await self._send_excel_with_fallbacks(path, caption, err_text)
                return False, err_text
            except Exception as e:
                logger.error("Telegram send_document error for %s: %s", path, e)
                if is_excel and attempt >= attempts:
                    return await self._send_excel_with_fallbacks(path, caption, str(e))
                return False, str(e)
        if is_excel:
            return await self._send_excel_with_fallbacks(path, caption, "Unknown Telegram send failure")
        return False, "Unknown Telegram send failure"

    async def _send_excel_zip_fallback(
        self, path: str, caption: str, prior_error: str | None = None
    ) -> tuple[bool, str | None]:
        if not self._app or not self.chat_id:
            return False, prior_error or "Telegram app/chat is not configured"
        filename = os.path.basename(path)
        zip_name = f"{os.path.splitext(filename)[0]}.zip"
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(path, arcname=filename)
        zip_bytes = zip_buffer.getvalue()
        attempts = max(1, self._SEND_FILE_ATTEMPTS)
        fallback_error = prior_error

        for attempt in range(1, attempts + 1):
            try:
                sent = await self._app.bot.send_document(
                    chat_id=self.chat_id,
                    document=InputFile(zip_bytes, filename=zip_name),
                    filename=zip_name,
                    caption=f"{caption} (zipped fallback)",
                    connect_timeout=self._SEND_FILE_CONNECT_TIMEOUT,
                    read_timeout=self._SEND_FILE_READ_TIMEOUT,
                    write_timeout=self._SEND_FILE_WRITE_TIMEOUT,
                    pool_timeout=self._SEND_FILE_POOL_TIMEOUT,
                )
                self._track(sent)
                return True, None
            except RetryAfter as e:
                delay = float(e.retry_after)
                logger.warning(
                    "Telegram zipped Excel fallback rate-limited for %s on attempt %s/%s; retry in %.1fs",
                    path,
                    attempt,
                    attempts,
                    delay,
                )
                if attempt >= attempts:
                    suffix = f"zip fallback failed: RetryAfter {delay:.1f}s"
                    fallback_error = f"{prior_error}; {suffix}" if prior_error else suffix
                    break
                await asyncio.sleep(delay)
            except (TimedOut, TimeoutError) as e:
                delay = min(5.0 * attempt, 15.0)
                logger.warning(
                    "Telegram zipped Excel fallback timed out for %s on attempt %s/%s; retry in %.1fs: %s",
                    path,
                    attempt,
                    attempts,
                    delay,
                    e,
                )
                if attempt >= attempts:
                    suffix = f"zip fallback failed: {e}"
                    fallback_error = f"{prior_error}; {suffix}" if prior_error else suffix
                    break
                await asyncio.sleep(delay)
            except NetworkError as e:
                err_text = str(e)
                delay = min(5.0 * attempt, 15.0)
                retryable_markers = (
                    "timed out",
                    "readerror",
                    "writeerror",
                    "connection reset",
                    "server disconnected",
                    "connection aborted",
                    "connection refused",
                )
                if any(marker in err_text.lower() for marker in retryable_markers) and attempt < attempts:
                    logger.warning(
                        "Telegram zipped Excel fallback network error for %s on attempt %s/%s; retry in %.1fs: %s",
                        path,
                        attempt,
                        attempts,
                        delay,
                        err_text,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error("Telegram zipped Excel fallback network error for %s: %s", path, err_text)
                suffix = f"zip fallback failed: {err_text}"
                fallback_error = f"{prior_error}; {suffix}" if prior_error else suffix
                break
            except Exception as e:
                logger.error("Telegram zipped Excel fallback failed for %s: %s", path, e)
                suffix = f"zip fallback failed: {e}"
                fallback_error = f"{prior_error}; {suffix}" if prior_error else suffix
                break

        ok, error = await self._send_document_via_raw_bot_api(
            zip_bytes,
            zip_name,
            f"{caption} (zipped fallback)",
        )
        if ok:
            logger.info("Telegram raw Bot API zipped fallback sent %s", path)
            return True, None

        suffix_detail = error or "Unknown Telegram send failure"
        suffix = f"raw zip fallback failed: {suffix_detail}"
        return False, f"{fallback_error}; {suffix}" if fallback_error else suffix

    async def send_repo_sync_files(self, sync_result: dict, include_index: bool = True) -> dict[str, Any]:
        downloads = (sync_result or {}).get("downloads", {})
        results: dict[str, Any] = {}
        file_specs = [
            ("latest_last24h_xlsx", "Dropbox latest 24h Excel export"),
            ("latest_trades_csv_gz", "Dropbox latest compressed trades export"),
            ("latest_signals_csv_gz", "Dropbox latest compressed signals export"),
        ]
        if include_index:
            file_specs.append(("latest_index_json", "Dropbox latest index pointer"))

        for key, caption in file_specs:
            item = downloads.get(key, {})
            path = item.get("path")
            if item.get("ok") and path and os.path.exists(path):
                ok, error = await self._send_file_path(path, caption)
                results[key] = {"sent": ok, "error": error, "path": path}
            else:
                results[key] = {"sent": False, "error": "File not available after Dropbox sync", "path": path}
        return results

    def _repo_sync_message_lines(self, result: dict, heading_ok: str, heading_fail: str) -> list[str]:
        ok = bool((result or {}).get("ok"))
        downloads = (result or {}).get("downloads", {})
        lines = [
            heading_ok if ok else heading_fail,
            f"Output dir: `{result.get('output_dir', 'unknown')}`",
            f"Expanded CSV: `{result.get('expanded_trades_csv') or 'none'}`",
            f"Expanded Signals CSV: `{result.get('expanded_signals_csv') or 'none'}`",
        ]
        for key in ("latest_last24h_xlsx", "latest_trades_csv_gz", "latest_signals_csv_gz", "latest_index_json"):
            d = downloads.get(key, {})
            status_txt = f"`{key}`: {'ok' if d.get('ok') else 'error'} (status={d.get('status')})"
            if not d.get("ok") and d.get("remote_path"):
                status_txt += f"\n  path: `{d.get('remote_path')}`"
            friendly = ((d.get("error_details") or {}).get("friendly") or "").strip()
            if not d.get("ok") and friendly:
                status_txt += f"\n  fix: `{friendly}`"
            err = d.get("error")
            if not d.get("ok") and err:
                err_compact = " ".join(str(err).split())
                if len(err_compact) > 180:
                    err_compact = f"{err_compact[:177]}..."
                status_txt += f"\n  error: `{err_compact}`"
            lines.append(status_txt)
        return lines

    @staticmethod
    def _file_send_summary(send_result: dict[str, Any], label_map: dict[str, str]) -> list[str]:
        lines: list[str] = []
        for key, label in label_map.items():
            info = send_result.get(key, {}) if send_result else {}
            if info.get("sent"):
                continue
            error = " ".join(str(info.get("error") or "unknown error").split())
            if len(error) > 180:
                error = f"{error[:177]}..."
            lines.append(f"{label}: `{error}`")
        return lines

    async def send_latest_archive_files(self, include_index: bool = True) -> dict[str, Any]:
        if not self.archive_latest_fn:
            return {"available": False, "files": {}, "sent_any": False}
        paths = self.archive_latest_fn() or {}
        files: dict[str, Any] = {}

        def _path_set():
            return (
                paths.get("latest_excel"),
                paths.get("latest_trades"),
                paths.get("latest_signals"),
                paths.get("latest_index"),
            )

        excel, trades, signals, index = _path_set()
        if not ((excel and os.path.exists(excel)) or (trades and os.path.exists(trades)) or (signals and os.path.exists(signals))):
            if self.archive_fn:
                loop = asyncio.get_event_loop()
                try:
                    await loop.run_in_executor(None, self.archive_fn)
                except Exception:
                    pass
                paths = self.archive_latest_fn() or {}
                excel, trades, signals, index = _path_set()

        if excel and os.path.exists(excel):
            ok, error = await self._send_file_path(excel, "EDEC latest 24h Excel export")
            files["latest_excel"] = {"sent": ok, "error": error, "path": excel}
        else:
            files["latest_excel"] = {"sent": False, "error": "Latest Excel file not found", "path": excel}
        if trades and os.path.exists(trades):
            ok, error = await self._send_file_path(trades, "EDEC latest compressed trades export (500)")
            files["latest_trades"] = {"sent": ok, "error": error, "path": trades}
        else:
            files["latest_trades"] = {"sent": False, "error": "Latest trades file not found", "path": trades}
        if signals and os.path.exists(signals):
            ok, error = await self._send_file_path(signals, "EDEC latest compressed signals export (500)")
            files["latest_signals"] = {"sent": ok, "error": error, "path": signals}
        else:
            files["latest_signals"] = {"sent": False, "error": "Latest signals file not found", "path": signals}
        if include_index and index and os.path.exists(index):
            ok, error = await self._send_file_path(index, "EDEC latest index pointer (most-recent metadata)")
            files["latest_index"] = {"sent": ok, "error": error, "path": index}
        elif include_index:
            files["latest_index"] = {"sent": False, "error": "Latest index file not found", "path": index}
        sent_any = any(bool(info.get("sent")) for info in files.values())
        available = any((info.get("path") and os.path.exists(info["path"])) for info in files.values() if info.get("path"))
        return {"available": available, "files": files, "sent_any": sent_any}

    async def alert_archive_complete(self, archive_result: dict):
        row_counts = archive_result.get("row_counts", {})
        msg = (
            "*Archive Completed*\n"
            f"24h paper/live/decisions: {row_counts.get('paper_trades_24h', 0)}/"
            f"{row_counts.get('live_trades_24h', 0)}/{row_counts.get('decisions_24h', 0)}\n"
            f"Recent trades/signals rows: {row_counts.get('recent_trades_rows', 0)}/"
            f"{row_counts.get('recent_signals_rows', 0)}"
        )
        await self.send_alert(msg)

    async def _build_archive_health_text(self) -> str:
        if self.archive_health_fn:
            loop = asyncio.get_event_loop()
            try:
                health = await loop.run_in_executor(None, self.archive_health_fn)
            except Exception as e:
                return f"Archive health check failed: {e}"
        else:
            if not self.archive_latest_fn:
                return "Archive health unavailable (archive not configured)."
            paths = self.archive_latest_fn() or {}
            index_path = paths.get("latest_index")
            if not index_path or not os.path.exists(index_path):
                return "No archive index found yet. Run /latest_export or wait for the daily archive run."
            try:
                with open(index_path, "r", encoding="utf-8") as f:
                    idx = json.load(f)
            except Exception as e:
                return f"Archive index unreadable: {e}"
            health = {
                "label": idx.get("label", "EDEC-BOT"),
                "checked_at_utc": "unknown",
                "index": idx,
                "dropbox_live": None,
            }

        idx = health.get("index") or {}
        rows = idx.get("row_counts", {})
        label = health.get("label", idx.get("label", "EDEC-BOT"))
        exported = idx.get("exported_at_utc", "unknown")
        checked_at = health.get("checked_at_utc", "unknown")
        upload_results = idx.get("dropbox_uploads") or {}

        live = health.get("dropbox_live")
        if live is None:
            dropbox_line = "Dropbox live check: disabled"
        else:
            ok = bool(live.get("ok"))
            files = live.get("files", {})
            missing = [k for k, v in files.items() if not v.get("exists")]
            auth_failed = [
                k for k, v in files.items()
                if ((v.get("error_details") or {}).get("reason") in ("expired_access_token", "invalid_access_token"))
            ]
            if ok:
                dropbox_line = "Dropbox live check: ok"
            elif auth_failed:
                dropbox_line = "Dropbox live check: token expired/invalid"
            else:
                miss = ", ".join(missing) if missing else "unknown"
                dropbox_line = f"Dropbox live check: missing ({miss})"

        upload_failures = [k for k, v in upload_results.items() if not v.get("ok")]
        upload_line = (
            f"Last upload result: failed ({', '.join(upload_failures)})"
            if upload_failures
            else "Last upload result: ok/unknown"
        )

        return (
            f"Archive Health ({label})\n"
            f"Last export (UTC): {exported}\n"
            f"Live check (UTC): {checked_at}\n"
            f"{dropbox_line}\n"
            f"{upload_line}\n"
            f"Rows 24h P/L/D: {rows.get('paper_trades_24h', 0)}/"
            f"{rows.get('live_trades_24h', 0)}/{rows.get('decisions_24h', 0)}\n"
            f"Recent trades rows: {rows.get('recent_trades_rows', 0)}"
        )

    def _track(self, msg) -> None:
        """Track a sent message for cleanup."""
        if msg:
            self._track_message_id(getattr(msg, "message_id", None))

    def _track_message_id(self, message_id: int | None) -> None:
        if message_id:
            self._ephemeral_msgs.append(message_id)
            self._persist_ephemeral(message_id)

    def _main_keyboard(self) -> InlineKeyboardMarkup:
        """Main control keyboard."""
        is_running = self.strategy_engine.is_active if self.strategy_engine else False
        order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
        is_dry = self.config.execution.dry_run
        _, capital_balance = self.tracker.get_paper_capital() if self.tracker else (0, 0)

        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "\u23F8 Pause" if is_running else "\u25B6 Resume",
                    callback_data="stop" if is_running else "start",
                ),
                InlineKeyboardButton("\U0001F6D1 Kill Switch", callback_data="kill"),
            ],
            [
                InlineKeyboardButton(
                    "\U0001F4CB Dry Run \u2705" if is_dry else "\U0001F4CB Dry Run",
                    callback_data="noop",
                ),
                InlineKeyboardButton("\U0001F30A Wet Run \U0001F512", callback_data="wet_disabled"),
            ],
            [
                InlineKeyboardButton("\U0001F4CA Stats", callback_data="stats"),
                InlineKeyboardButton("\U0001F4C8 Status", callback_data="status"),
            ],
            [
                InlineKeyboardButton("\U0001F50D Filters", callback_data="filters"),
                InlineKeyboardButton("\u2139\uFE0F Commands", callback_data="help_panel"),
            ],
            [
                InlineKeyboardButton(f"\U0001F3E6 Capital: ${capital_balance:,.2f}", callback_data="capital"),
                InlineKeyboardButton(f"\U0001F4B0 Budget: ${order_size:.0f}", callback_data="budget"),
            ],
            [
                InlineKeyboardButton("\U0001F504 Refresh", callback_data="refresh"),
                InlineKeyboardButton("\U0001F5D1 Reset Stats", callback_data="reset_stats"),
            ],
            [
                InlineKeyboardButton("\U0001F9F9 Clear Chat", callback_data="clear_chat"),
                InlineKeyboardButton("\U0001F9ED Archive Health", callback_data="archive_health"),
            ],
            [
                InlineKeyboardButton("\U0001F4CA Last 500 Trades", callback_data="export_recent"),
                InlineKeyboardButton("\U0001F5C4 Latest Archive", callback_data="export_latest"),
                InlineKeyboardButton("\U0001F4E5 Sync Dropbox", callback_data="sync_repo_latest"),
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
            InlineKeyboardButton(f"${amt:,}", callback_data=f"capital_{amt}")
            for amt in CAPITAL_OPTIONS
        ]
        rows = [buttons[i:i+3] for i in range(0, len(buttons), 3)]
        rows.append([InlineKeyboardButton("« Back", callback_data="back")])
        return InlineKeyboardMarkup(rows)

    def _set_dashboard_view(self, view: str) -> None:
        self._dashboard_view = view

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
            self._set_dashboard_view("main")
            amt = float(data.split("_")[1])
            await query.answer(f"✅ Budget set to ${amt:.0f}", show_alert=False)
            if self.executor:
                self.executor.set_order_size(amt)
            await self._do_cleanup()
            return

        if data == "budget":
            self._set_dashboard_view("budget")
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
            self._set_dashboard_view("main")
            amt = float(data.split("_")[1])
            await query.answer(f"✅ Capital set to ${amt:,.0f}", show_alert=False)
            if self.tracker:
                self.tracker.set_paper_capital(amt)
            await self._do_cleanup()
            return

        if data == "capital":
            self._set_dashboard_view("capital")
            await query.answer()
            _, balance = self.tracker.get_paper_capital() if self.tracker else (0, 0)
            await query.edit_message_text(
                f"🏦 *Paper Capital*\n"
                f"Current balance: *${balance:,.2f}*\n\n"
                f"_Select a new bankroll to start fresh:_",
                parse_mode="Markdown",
                reply_markup=self._capital_keyboard(),
            )
            return

        if data == "back":
            self._set_dashboard_view("main")
            await query.answer()
            await self._refresh_dashboard(force=True)
            return

        # --- Start / Stop / Kill ---
        if data == "start":
            self._set_dashboard_view("main")
            await query.answer("▶️ Scanning started", show_alert=False)
            if self.strategy_engine:
                self.strategy_engine.start_scanning()
            self.risk_manager.resume()
            self.risk_manager.deactivate_kill_switch()
            await self._do_cleanup()
            return

        if data == "stop":
            self._set_dashboard_view("main")
            await query.answer("⏸ Bot stopped", show_alert=False)
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            self.risk_manager.pause()
            await self._do_cleanup()
            return

        if data == "kill":
            self._set_dashboard_view("main")
            await query.answer("🛑 Kill switch activated!", show_alert=True)
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            self.risk_manager.activate_kill_switch("Manual kill via Telegram")
            await self._do_cleanup()
            return

        if data == "refresh":
            self._set_dashboard_view("main")
            await query.answer("Refreshing dashboard...", show_alert=False)
            await self._refresh_dashboard(force=True)
            return

        if data == "clear_chat":
            self._set_dashboard_view("main")
            await query.answer("Clearing chat history...", show_alert=True)
            stats = await self._clear_chat_history()
            note = (
                f"Chat clear done. Deleted {stats.get('deleted', 0)}/"
                f"{stats.get('attempted', 0)} messages."
            )
            if stats.get("undeletable", 0):
                note += " Some old messages may be undeletable due to Telegram limits."
            self._track(await self._app.bot.send_message(chat_id=self.chat_id, text=note))
            await self._repost_dashboard()
            return

        if data == "reset_stats":
            self._set_dashboard_view("main")
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
            self._set_dashboard_view("stats")
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
            self._set_dashboard_view("status")
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
            self._set_dashboard_view("trades")
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
            self._set_dashboard_view("main")
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
            self._set_dashboard_view("main")
            if not self.export_recent_fn:
                self._track(await query.message.reply_text("Recent export not available."))
                await self._repost_dashboard()
                return
            wait_msg = await query.message.reply_text("⏳ Building Last 500 Trades CSV (Dropbox sync first)...")
            self._track(wait_msg)
            try:
                loop = asyncio.get_event_loop()
                path = None
                if self.repo_sync_fn:
                    try:
                        sync_result = await loop.run_in_executor(None, self.repo_sync_fn)
                        synced_csv = sync_result.get("expanded_trades_csv")
                        if synced_csv and os.path.exists(synced_csv):
                            path = synced_csv
                    except Exception:
                        # Fall back to local DB export if Dropbox sync fails.
                        path = None
                if not path:
                    path = await loop.run_in_executor(None, self.export_recent_fn)
                with open(path, "rb") as f:
                    self._track(await query.message.reply_document(
                        document=f,
                        filename=os.path.basename(path),
                        caption="📊 Last 500 Trades CSV — compact export for AI analysis",
                    ))
            except Exception as e:
                self._track(await query.message.reply_text(f"Export error: {e}"))
            await self._repost_dashboard()


        elif data == "export_latest":
            self._set_dashboard_view("main")
            wait_msg = await query.message.reply_text("⏳ Sending latest archive files...")
            self._track(wait_msg)
            try:
                send_result = await self.send_latest_archive_files(include_index=True)
                if not send_result.get("available"):
                    self._track(await query.message.reply_text("Latest archive files not found yet."))
                failed_lines = self._file_send_summary(
                    send_result.get("files", {}),
                    {
                        "latest_excel": "Latest Excel send failed",
                        "latest_trades": "Latest trades send failed",
                        "latest_signals": "Latest signals send failed",
                        "latest_index": "Latest index send failed",
                    },
                )
                if failed_lines:
                    self._track(await query.message.reply_text("\n".join(failed_lines), parse_mode="Markdown"))
            except Exception as e:
                self._track(await query.message.reply_text(f"Latest archive error: {e}"))
            await self._repost_dashboard()

        elif data == "archive_health":
            self._set_dashboard_view("archive_health")
            text = await self._build_archive_health_text()
            await query.edit_message_text(text, reply_markup=_back_kb)
        elif data == "sync_repo_latest":
            self._set_dashboard_view("main")
            if not self.repo_sync_fn:
                self._track(await query.message.reply_text("Repo sync is not configured."))
                await self._repost_dashboard()
                return
            wait_msg = await query.message.reply_text("⏳ Syncing latest Dropbox files to local repo folder...")
            self._track(wait_msg)
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, self.repo_sync_fn)
                lines = self._repo_sync_message_lines(
                    result,
                    "✅ *Repo Sync Complete*",
                    "⚠️ *Repo Sync Partial/Failed*",
                )
                self._track(await query.message.reply_text("\n".join(lines), parse_mode="Markdown"))
                send_result = await self.send_repo_sync_files(result, include_index=True)
                failed_lines = self._file_send_summary(
                    send_result,
                    {
                        "latest_last24h_xlsx": "Synced Excel send failed",
                        "latest_trades_csv_gz": "Synced trades send failed",
                        "latest_signals_csv_gz": "Synced signals send failed",
                        "latest_index_json": "Synced index send failed",
                    },
                )
                if failed_lines:
                    self._track(await query.message.reply_text("\n".join(failed_lines), parse_mode="Markdown"))
            except Exception as e:
                self._track(await query.message.reply_text(f"Repo sync error: {e}"))
            await self._repost_dashboard()
        elif data == "help_panel":
            self._set_dashboard_view("help_panel")
            text = self._commands_text()
            await query.edit_message_text(text, reply_markup=_back_kb)
        elif data == "filters":
            self._set_dashboard_view("filters")
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
            mode = self.strategy_engine.mode
            msg = (
                f"*Strategy Mode*\n"
                f"Current: {MODE_LABELS.get(mode, mode)}\n\n"
                f"Enabled in config: `{self._enabled_mode_summary()}`\n\n"
                f"Change with:\n" + "\n".join(self._mode_help_lines())
            )
            self._track(await update.message.reply_text(msg, parse_mode="Markdown"))
            return

        new_mode = args[0].lower()
        if self.strategy_engine.set_mode(new_mode):
            label = MODE_LABELS.get(new_mode, new_mode)
            self._track(await update.message.reply_text(f"Mode set to: {label}"))
        else:
            self._track(await update.message.reply_text(
                f"Unknown mode `{new_mode}`. Use: both, dual, single, lead, swing, off",
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

    async def _cmd_latest_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        wait_msg = await update.message.reply_text("Sending latest archive files...")
        self._track(wait_msg)
        try:
            send_result = await self.send_latest_archive_files(include_index=True)
            if not send_result.get("available"):
                self._track(await update.message.reply_text("Latest archive files not found yet."))
            failed_lines = self._file_send_summary(
                send_result.get("files", {}),
                {
                    "latest_excel": "Latest Excel send failed",
                    "latest_trades": "Latest trades send failed",
                    "latest_signals": "Latest signals send failed",
                    "latest_index": "Latest index send failed",
                },
            )
            if failed_lines:
                self._track(await update.message.reply_text("\n".join(failed_lines), parse_mode="Markdown"))
        except Exception as e:
            self._track(await update.message.reply_text(f"Latest archive error: {e}"))

    async def _cmd_sync_repo_latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not self.repo_sync_fn:
            self._track(await update.message.reply_text("Repo sync is not configured."))
            return
        wait_msg = await update.message.reply_text("Syncing latest Dropbox files to local repo folder...")
        self._track(wait_msg)
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.repo_sync_fn)
            lines = self._repo_sync_message_lines(
                result,
                "*Repo Sync Complete*",
                "*Repo Sync Failed*",
            )
            self._track(await update.message.reply_text("\n".join(lines), parse_mode="Markdown"))
            send_result = await self.send_repo_sync_files(result, include_index=True)
            failed_lines = self._file_send_summary(
                send_result,
                {
                    "latest_last24h_xlsx": "Synced Excel send failed",
                    "latest_trades_csv_gz": "Synced trades send failed",
                    "latest_signals_csv_gz": "Synced signals send failed",
                    "latest_index_json": "Synced index send failed",
                },
            )
            if failed_lines:
                self._track(await update.message.reply_text("\n".join(failed_lines), parse_mode="Markdown"))
        except Exception as e:
            self._track(await update.message.reply_text(f"Repo sync error: {e}"))

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
            f"  Scalp take-profit bid: {sl.scalp_take_profit_bid}\n"
            f"  High-confidence bid: {sl.high_confidence_bid}\n"
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

    def _commands_text(self) -> str:
        return (
            "EDEC Bot Commands\n"
            "/status - Per-coin book prices + bot state\n"
            "/mode - Show current strategy mode\n"
            "/mode both|dual|single|lead|swing|off - Switch mode live\n"
            "/start - Resume trading\n"
            "/stop - Pause trading\n"
            "/kill - Emergency stop\n"
            "/trades - Last 10 trades\n"
            "/stats - Today's summary\n"
            "/stats 7d - Last 7 days\n"
            "/export - Send Excel file\n"
            "/export today - Today only\n"
            "/latest_export - Send latest archive files\n"
            "/sync_repo_latest - Sync Dropbox latest files into local repo folder\n"
            "/config - Show all settings\n"
            "/filters - Filter pass/fail rates\n"
            "/clean - Delete old chat messages\n"
            "/help - This message"
        )

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        msg = self._commands_text()
        self._track(await update.message.reply_text(msg))

    async def _cmd_clean(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Delete old bot messages by sweeping recent message IDs."""
        if not self._auth(update):
            return
        # Delete the user's /clean command too
        try:
            await update.message.delete()
        except Exception:
            pass
        stats = await self._deep_clean()
        note = (
            "Cleanup done. Deleted "
            f"{stats.get('deleted', 0)}/{stats.get('attempted', 0)} messages."
        )
        if stats.get("undeletable", 0):
            note += " Some older Telegram messages may be undeletable due to Telegram limits."
        self._track(await self._app.bot.send_message(chat_id=self.chat_id, text=note))
