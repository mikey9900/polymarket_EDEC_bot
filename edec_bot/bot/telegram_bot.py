"""Telegram interface — commands, alerts, status updates, and data export."""

import asyncio
import io
import json
import logging
import mimetypes
import os
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest
from uuid import uuid4

from telegram import Update, InlineKeyboardMarkup, InputFile
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.request import HTTPXRequest

from bot.config import Config
from bot import telegram_buttons
from bot import telegram_dashboard as dashboard_ui
from bot import telegram_exports as export_workflows
from bot.tracker import DecisionTracker
from bot.risk_manager import RiskManager
from version import __version__

logger = logging.getLogger(__name__)

MODE_LABELS = dashboard_ui.MODE_LABELS


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
                 repo_sync_fn=None, session_export_fn=None, excel_dropbox_link_fn=None,
                 fetch_github_fn=None):
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
        self.session_export_fn = session_export_fn
        self.excel_dropbox_link_fn = excel_dropbox_link_fn
        self.fetch_github_fn = fetch_github_fn
        self.chat_id = config.telegram_chat_id
        self._app: Application | None = None
        self._dashboard_message_id: int | None = None  # live dashboard message
        self._dashboard_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._refresh_lock = asyncio.Lock()
        self._repost_lock = asyncio.Lock()  # serialize dashboard re-posts to avoid duplicates
        self._ephemeral_msgs: list[int] = []  # message IDs to delete on next cleanup
        self._dashboard_view: str = "main"
        # Monotonic timestamp of last user-initiated refresh; used to debounce the 10s loop
        self._last_manual_refresh: float = 0.0
        # Marker used to detect whether any ephemeral/alert messages have been sent since
        # the last dashboard post — lets us skip pointless reposts.
        self._msgs_since_dashboard_post: int = 0

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

    async def _run_blocking(self, func):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func)

    def _background_task_done(self, task: asyncio.Task[Any], label: str) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception:
            logger.exception("Failed inspecting Telegram background task %s", label)
            return
        if exc:
            logger.error(
                "Telegram background task failed (%s)",
                label,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    def _spawn_background_task(self, coro, *, label: str) -> asyncio.Task[Any]:
        if self._app:
            try:
                task = self._app.create_task(coro, name=f"telegram:{label}")
            except TypeError:
                task = self._app.create_task(coro)
        else:
            try:
                task = asyncio.create_task(coro, name=f"telegram:{label}")
            except TypeError:
                task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(lambda done: self._background_task_done(done, label))
        return task

    async def _reply_tracked(self, message, text: str, parse_mode: str | None = None):
        self._track(await message.reply_text(text, parse_mode=parse_mode))

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
            ("fetch_github", self._cmd_fetch_github),
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
        for task in list(self._background_tasks):
            task.cancel()
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
        return dashboard_ui.format_usd(price)

    def _build_dashboard_text(self) -> str:
        return dashboard_ui.build_dashboard_text(
            version=__version__,
            config=self.config,
            tracker=self.tracker,
            scanner=self.scanner,
            aggregator=self.aggregator,
            strategy_engine=self.strategy_engine,
        )

    def _build_dashboard_payload_sync(self) -> tuple[str, InlineKeyboardMarkup]:
        """Build both the dashboard text and its keyboard in one sync call."""
        return self._build_dashboard_text(), self._main_keyboard()

    async def _build_dashboard_payload(self) -> tuple[str, InlineKeyboardMarkup]:
        # Run the SQLite-heavy build in a worker thread so it can't block WS
        # heartbeats. tracker.conn is now check_same_thread=False; tracker._io_lock
        # serializes against main-thread writes.
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._locked_build_dashboard_payload)

    def _locked_build_dashboard_payload(self) -> tuple[str, InlineKeyboardMarkup]:
        with self.tracker._io_lock:
            return self._build_dashboard_payload_sync()

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
            text, keyboard = await self._build_dashboard_payload()
            msg = await self._app.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
            self._dashboard_message_id = msg.message_id
            self._dashboard_view = "main"
            self._msgs_since_dashboard_post = 0
            self._save_msg_id()
            self._dashboard_task = asyncio.create_task(self._dashboard_loop())
            if self._AUTO_EPHEMERAL_CLEAN:
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Live dashboard started")
            if self._STARTUP_DEEP_CLEAN:
                await self._deep_clean()
        except Exception as e:
            logger.error(f"Failed to start dashboard: {e}", exc_info=True)

    async def stop_dashboard(self):
        """Stop the dashboard refresh and cleanup loops."""
        for task in (self._dashboard_task, self._cleanup_task):
            if task:
                task.cancel()
        self._dashboard_task = None
        self._cleanup_task = None

    async def _dashboard_loop(self):
        """Refresh the dashboard message every 10 seconds.

        Skips the tick if the user manually refreshed within the last 3s so the
        auto-loop doesn't contend with the refresh lock right after a button press.
        """
        while True:
            await asyncio.sleep(10)
            loop = asyncio.get_running_loop()
            if (loop.time() - self._last_manual_refresh) < 3.0:
                continue
            await self._refresh_dashboard()

    async def _cleanup_loop(self):
        """Every 60 seconds: delete ephemeral messages and refresh the dashboard in-place."""
        while True:
            await asyncio.sleep(60)
            await self._do_cleanup()

    async def _repost_dashboard(self, *, only_if_buried: bool = False):
        """Delete old dashboard and re-post at the bottom.

        Serialized via `_repost_lock` so concurrent callers can't produce duplicate
        dashboard messages. If `only_if_buried` is True, the repost is skipped when
        no ephemeral/alert messages have been sent since the last post (meaning the
        existing dashboard is already at the bottom of the chat).
        """
        if not self._app or not self.chat_id:
            return
        async with self._repost_lock:
            if only_if_buried and self._msgs_since_dashboard_post == 0 and self._dashboard_message_id:
                # Dashboard is already the most recent message — no need to repost.
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
                text, keyboard = await self._build_dashboard_payload()
                msg = await self._app.bot.send_message(
                    chat_id=self.chat_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=keyboard,
                )
                self._dashboard_message_id = msg.message_id
                self._dashboard_view = "main"
                self._msgs_since_dashboard_post = 0
                self._save_msg_id()
            except Exception as e:
                logger.error(f"Failed to re-post dashboard: {e}", exc_info=True)

    async def _do_cleanup(self):
        """Delete tracked ephemeral messages in parallel, then refresh the dashboard."""
        await self._delete_ephemeral_messages()
        await self._refresh_dashboard()

    async def _delete_ephemeral_messages(self):
        """Delete tracked ephemeral messages without touching dashboard state."""
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

    async def _refresh_then_cleanup(self):
        """Refresh dashboard promptly, then clean up old ephemeral messages."""
        await self._refresh_dashboard(force=True)
        await self._delete_ephemeral_messages()

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

        if force:
            # Stamp the time so the 10s loop briefly backs off and doesn't fight us.
            self._last_manual_refresh = asyncio.get_running_loop().time()

        # No known message — re-create from scratch (no loop: calls repost, not cleanup)
        if not self._dashboard_message_id:
            await self._repost_dashboard()
            return

        # Build text + keyboard on the main thread because tracker reads use a
        # thread-bound SQLite connection.
        try:
            text, keyboard = await self._build_dashboard_payload()
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
                    reply_markup=keyboard,
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
            self._track_message_id(sent.message_id)
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

        if is_excel and self.excel_dropbox_link_fn:
            try:
                loop = asyncio.get_running_loop()
                url, dbx_err = await loop.run_in_executor(None, self.excel_dropbox_link_fn, path)
                if url:
                    is_link = url.startswith("https://")
                    display = url if is_link else url.replace("//", "/")
                    text = (
                        f"📊 Excel export on Dropbox:\n{display}"
                        if is_link
                        else f"📊 Excel saved to Dropbox (enable `sharing.write` scope for a clickable link):\n`{display}`"
                    )
                    sent_msg = await self._app.bot.send_message(
                        chat_id=self.chat_id, text=text, parse_mode="Markdown",
                    )
                    self._track(sent_msg)
                    logger.info("Sent Dropbox ref for Excel %s: %s", path, url)
                    return True, None
                reason = dbx_err or "unknown error"
                logger.warning("Dropbox Excel link failed for %s: %s", path, reason)
                return False, f"Excel Dropbox failed: {reason}"
            except Exception as exc:
                logger.warning("Dropbox Excel link error for %s: %s", path, exc)
                return False, f"Excel Dropbox error: {exc}"

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
        return await export_workflows.send_repo_sync_files(
            sync_result,
            self._send_file_path,
            include_index=include_index,
        )

    def _repo_sync_message_lines(self, result: dict, heading_ok: str, heading_fail: str) -> list[str]:
        return export_workflows.repo_sync_message_lines(result, heading_ok, heading_fail)

    @staticmethod
    def _file_send_summary(send_result: dict[str, Any], label_map: dict[str, str]) -> list[str]:
        return export_workflows.file_send_summary(send_result, label_map)

    async def send_latest_archive_files(self, include_index: bool = True) -> dict[str, Any]:
        return await export_workflows.send_latest_archive_files(
            self.archive_latest_fn,
            self.archive_fn,
            send_file_path=self._send_file_path,
            run_blocking=self._run_blocking,
            include_index=include_index,
        )

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
        return await export_workflows.build_archive_health_text(
            self.archive_latest_fn,
            self.archive_health_fn,
            run_blocking=self._run_blocking,
        )

    async def _handle_export_request(
        self,
        reply_message,
        *,
        today_only: bool,
        wait_text: str,
        unavailable_text: str,
        caption: str,
    ) -> None:
        if not self.export_fn:
            await self._reply_tracked(reply_message, unavailable_text)
            return
        await self._reply_tracked(reply_message, wait_text)
        try:
            result = await export_workflows.send_spreadsheet_export(
                self.export_fn,
                today_only=today_only,
                caption=caption,
                send_file_path=self._send_file_path,
                run_blocking=self._run_blocking,
            )
            if not result.get("sent"):
                await self._reply_tracked(
                    reply_message,
                    f"Export error: {result.get('error') or 'unknown error'}",
                )
        except Exception as e:
            await self._reply_tracked(reply_message, f"Export error: {e}")

    async def _handle_recent_export_request(self, reply_message) -> None:
        if not self.export_recent_fn:
            await self._reply_tracked(reply_message, "Recent export not available.")
            return
        await self._reply_tracked(
            reply_message,
            "⏳ Building Last 100 trades/signals CSVs (archive + Dropbox sync)...",
        )
        try:
            result = await export_workflows.send_recent_export_files(
                self.export_recent_fn,
                archive_fn=self.archive_fn,
                archive_latest_fn=self.archive_latest_fn,
                repo_sync_fn=self.repo_sync_fn,
                send_file_path=self._send_file_path,
                run_blocking=self._run_blocking,
            )
            failed_lines = self._file_send_summary(
                result.get("files", {}),
                {
                    "trades": "Last 100 trades send failed",
                    "signals": "Signals export send failed",
                },
            )
            if failed_lines:
                await self._reply_tracked(reply_message, "\n".join(failed_lines), parse_mode="Markdown")
            if result.get("archive_error"):
                await self._reply_tracked(
                    reply_message,
                    f"⚠️ Dropbox archive refresh failed; used fallback.\n`{result['archive_error']}`",
                    parse_mode="Markdown",
                )
            if result.get("repo_sync_error"):
                await self._reply_tracked(
                    reply_message,
                    f"⚠️ Dropbox repo sync failed; used local fallback.\n`{result['repo_sync_error']}`",
                    parse_mode="Markdown",
                )
        except Exception as e:
            await self._reply_tracked(reply_message, f"Export error: {e}")

    async def _handle_session_export_request(self, reply_message) -> None:
        if not self.session_export_fn:
            await self._reply_tracked(reply_message, "Session export not configured.")
            return
        await self._reply_tracked(
            reply_message,
            "\u23f3 Exporting session trades/signals to Dropbox + GitHub...",
        )
        try:
            result = await self._run_blocking(self.session_export_fn)
            trade_count = result.get("trade_count", 0)
            signal_count = result.get("signal_count", 0)
            since = result.get("session_since_utc", "unknown")
            lines = [
                f"\u2705 *Session Export Complete*",
                f"Trades: {trade_count} | Signals: {signal_count}",
                f"Since: `{since}`",
            ]
            uploads = result.get("dropbox_uploads") or {}
            failed_dbx = [k for k, v in uploads.items() if not v.get("ok")]
            if failed_dbx:
                lines.append(f"\u26a0\ufe0f Dropbox failed: {', '.join(failed_dbx)}")
            pushes = result.get("github_pushes")
            if pushes is None:
                lines.append("\u2139\ufe0f GitHub push skipped (EDEC_GITHUB_TOKEN / EDEC_GITHUB_REPO not set)")
            else:
                failed_gh = [k for k, v in pushes.items() if not v.get("ok")]
                if failed_gh:
                    lines.append(f"\u26a0\ufe0f GitHub failed: {', '.join(failed_gh)}")
            await self._reply_tracked(reply_message, "\n".join(lines), parse_mode="Markdown")
        except Exception as e:
            await self._reply_tracked(reply_message, f"Session export error: {e}")

    async def _handle_latest_export_request(self, reply_message, *, wait_text: str) -> None:
        await self._reply_tracked(reply_message, wait_text)
        try:
            send_result = await self.send_latest_archive_files(include_index=True)
            if not send_result.get("available"):
                await self._reply_tracked(reply_message, "Latest archive files not found yet.")
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
                await self._reply_tracked(reply_message, "\n".join(failed_lines), parse_mode="Markdown")
        except Exception as e:
            await self._reply_tracked(reply_message, f"Latest archive error: {e}")

    async def _handle_repo_sync_request(
        self,
        reply_message,
        *,
        wait_text: str,
        heading_ok: str,
        heading_fail: str,
    ) -> None:
        if not self.repo_sync_fn:
            await self._reply_tracked(reply_message, "Repo sync is not configured.")
            return
        await self._reply_tracked(reply_message, wait_text)
        try:
            result = await self._run_blocking(self.repo_sync_fn)
            lines = self._repo_sync_message_lines(result, heading_ok, heading_fail)
            await self._reply_tracked(reply_message, "\n".join(lines), parse_mode="Markdown")
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
                await self._reply_tracked(reply_message, "\n".join(failed_lines), parse_mode="Markdown")
        except Exception as e:
            await self._reply_tracked(reply_message, f"Repo sync error: {e}")

    def _track(self, msg) -> None:
        """Track a sent message for cleanup."""
        if msg:
            self._track_message_id(getattr(msg, "message_id", None))

    def _track_message_id(self, message_id: int | None) -> None:
        if message_id:
            self._ephemeral_msgs.append(message_id)
            self._persist_ephemeral(message_id)
            self._msgs_since_dashboard_post += 1

    def _main_keyboard(self) -> InlineKeyboardMarkup:
        is_running = self.strategy_engine.is_active if self.strategy_engine else False
        order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
        is_dry = self.config.execution.dry_run
        _, capital_balance = self.tracker.get_paper_capital() if self.tracker else (0, 0)
        return dashboard_ui.build_main_keyboard(
            is_running=is_running,
            is_dry=is_dry,
            order_size=order_size,
            capital_balance=capital_balance,
        )

    def _budget_keyboard(self) -> InlineKeyboardMarkup:
        order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd
        return dashboard_ui.build_budget_keyboard(order_size)

    def _capital_keyboard(self) -> InlineKeyboardMarkup:
        return dashboard_ui.build_capital_keyboard()

    def _set_dashboard_view(self, view: str) -> None:
        self._dashboard_view = view

    async def _handle_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline keyboard button presses."""
        await telegram_buttons.handle_button(self, update, context)

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
                               shares: float, profit: float, dry_run: bool = False,
                               strategy_type: str = "single_leg"):
        prefix = "[DRY RUN]" if dry_run else "TRADE"
        strategy_key = (strategy_type or "single_leg").lower()
        strategy_label = {
            "single_leg": "SINGLE-LEG",
            "lead_lag": "LEAD-LAG",
            "swing_leg": "SWING LEG",
        }.get(strategy_key, strategy_key.replace("_", " ").upper())
        exit_label = "EXIT" if strategy_key == "swing_leg" else "SELL"
        msg = (
            f"{prefix} {strategy_label} `{coin.upper()}` -> {side.upper()}\n"
            f"`{market_slug}`\n"
            f"BUY@{entry_price:.3f} -> {exit_label}@{target_price:.3f}\n"
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
            self._msgs_since_dashboard_post += 1

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        status_text = dashboard_ui.build_status_command_text(
            config=self.config,
            risk_status=self.risk_manager.get_status(),
            scanner=self.scanner,
            strategy_engine=self.strategy_engine,
        )
        self._track(await update.message.reply_text(status_text, parse_mode="Markdown"))

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
        if self.strategy_engine:
            self.strategy_engine.start_scanning()
        self.risk_manager.resume()
        self.risk_manager.deactivate_kill_switch()
        self._track(await update.message.reply_text("▶️ Trading resumed"))

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if self.strategy_engine:
            self.strategy_engine.stop_scanning()
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
            lines = ["*Last 7 Days*\n"]
            for i in range(7):
                date = (datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")
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
        await self._handle_export_request(
            update.message,
            today_only=bool(context and context.args and context.args[0] == "today"),
            wait_text="? Generating Excel export...",
            unavailable_text="Export not available",
            caption="?? EDEC Bot Decision Export",
        )

    async def _cmd_latest_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._handle_latest_export_request(
            update.message,
            wait_text="Sending latest archive files...",
        )

    async def _cmd_sync_repo_latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._handle_repo_sync_request(
            update.message,
            wait_text="Syncing latest Dropbox files to local repo folder...",
            heading_ok="*Repo Sync Complete*",
            heading_fail="*Repo Sync Failed*",
        )

    async def _handle_fetch_github_request(self, reply_message, limit: int = 3) -> None:
        if not self.fetch_github_fn:
            await self._reply_tracked(reply_message, "GitHub fetch not configured (missing EDEC_GITHUB_TOKEN / EDEC_GITHUB_REPO).")
            return
        await self._reply_tracked(
            reply_message,
            f"\u23f3 Fetching last {limit} session export folder(s) from GitHub...",
        )
        try:
            result = await self._run_blocking(lambda: self.fetch_github_fn(limit=limit))
            if not result.get("ok"):
                await self._reply_tracked(
                    reply_message,
                    f"\u274c GitHub fetch failed: {result.get('error', 'unknown error')}",
                )
                return

            fetched = result.get("folders", [])
            count = result.get("fetched_count", 0)
            note = result.get("note", "")
            output_dir = result.get("output_dir", "")

            if count == 0:
                msg = f"\u2139\ufe0f No export folders found.\n{note}"
            else:
                lines = [f"\u2705 *GitHub Fetch Complete*", f"Folders: {count} | Saved to: `{output_dir}`", ""]
                for f in fetched:
                    folder = f.get("folder", "?")
                    files = f.get("files", [])
                    errs = f.get("errors", [])
                    csv_files = [n for n in files if n.endswith(".csv") and not n.endswith(".csv.gz")]
                    gz_files = [n for n in files if n.endswith(".csv.gz")]
                    lines.append(f"\U0001f4c1 `{folder}`")
                    lines.append(f"  CSV: {len(csv_files)} | GZ: {len(gz_files)} | Total files: {len(files)}")
                    if errs:
                        lines.append(f"  \u26a0\ufe0f Errors: {', '.join(errs[:3])}")
                msg = "\n".join(lines)

            await self._reply_tracked(reply_message, msg, parse_mode="Markdown")
        except Exception as e:
            await self._reply_tracked(reply_message, f"GitHub fetch error: {e}")

    async def _cmd_fetch_github(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        limit = 3
        if context.args:
            try:
                limit = max(1, min(20, int(context.args[0])))
            except ValueError:
                pass
        await self._handle_fetch_github_request(update.message, limit=limit)

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
            "/fetch_github [N] - Download last N session exports from GitHub data repo\n"
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
