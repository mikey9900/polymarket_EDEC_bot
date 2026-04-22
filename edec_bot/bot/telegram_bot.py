"""Telegram interface - backup ops, inspection, and alerts."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

from bot.control_plane import ControlPlane, ControlRequest
from bot.telegram_alerts import TelegramAlertPublisher
from bot.telegram_backup import TelegramBackupController

logger = logging.getLogger("edec.telegram")


class TelegramBot:
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

    def __init__(
        self,
        config,
        tracker,
        risk_manager,
        export_fn=None,
        export_recent_fn=None,
        scanner=None,
        strategy_engine=None,
        executor=None,
        aggregator=None,
        archive_fn=None,
        archive_latest_fn=None,
        archive_health_fn=None,
        repo_sync_fn=None,
        session_export_fn=None,
        excel_dropbox_link_fn=None,
        fetch_github_fn=None,
        polymarket_cli=None,
        control_plane: ControlPlane | None = None,
    ):
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
        self.polymarket_cli = polymarket_cli
        self.chat_id = getattr(config, "telegram_chat_id", "")
        self._app: Application | None = None
        self._tracked_message_ids: list[int] = []
        self.control_plane = control_plane or ControlPlane(
            config=config,
            tracker=tracker,
            risk_manager=risk_manager,
            strategy_engine=strategy_engine,
            executor=executor,
            session_export_fn=session_export_fn,
        )
        self.alerts = TelegramAlertPublisher(self)
        self.backup = TelegramBackupController(
            config=config,
            tracker=tracker,
            risk_manager=risk_manager,
            strategy_engine=strategy_engine,
            executor=executor,
            polymarket_cli=polymarket_cli,
            control_plane=self.control_plane,
        )

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

    def _auth(self, update: Update) -> bool:
        return str(update.effective_chat.id) == str(self.chat_id)

    def _track(self, msg) -> None:
        if msg:
            self._track_message_id(getattr(msg, "message_id", None))

    def _track_message_id(self, message_id: int | None) -> None:
        if message_id is not None:
            self._tracked_message_ids.append(int(message_id))

    def _track_cmd(self, update: Update) -> None:
        message = getattr(update, "message", None)
        if message is not None:
            self._track_message_id(getattr(message, "message_id", None))
            return
        query = getattr(update, "callback_query", None)
        if query is not None:
            self._track_message_id(getattr(getattr(query, "message", None), "message_id", None))

    async def _reply_tracked(self, message, text: str, parse_mode: str | None = None, **kwargs):
        self._track(await message.reply_text(text, parse_mode=parse_mode, **kwargs))

    async def start(self):
        if not getattr(self.config, "telegram_bot_token", ""):
            logger.warning("No Telegram bot token configured - Telegram disabled")
            return
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
            ("pmaccount", self._cmd_pmaccount),
            ("pmorders", self._cmd_pmorders),
            ("pmtrades", self._cmd_pmtrades),
            ("pmcancelall", self._cmd_pmcancelall),
            ("help", self._cmd_help),
            ("clean", self._cmd_clean),
        ]
        for cmd, handler in handlers:
            self._app.add_handler(CommandHandler(cmd, handler))
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
            self._app = None

    async def start_dashboard(self):
        return None

    async def stop_dashboard(self):
        return None

    async def send_alert(self, text: str, *, parse_mode: str = "Markdown"):
        return await self.alerts.send_alert(text, parse_mode=parse_mode)

    async def alert_archive_complete(self, archive_result: dict):
        await self.alerts.alert_archive_complete(archive_result)

    async def alert_dual_leg(self, *args, **kwargs):
        await self.alerts.alert_dual_leg(*args, **kwargs)

    async def alert_single_leg(self, *args, **kwargs):
        await self.alerts.alert_single_leg(*args, **kwargs)

    async def alert_trade(self, text: str):
        await self.alerts.alert_trade(text)

    async def alert_abort(self, *args, **kwargs):
        await self.alerts.alert_abort(*args, **kwargs)

    async def alert_resolution(self, *args, **kwargs):
        await self.alerts.alert_resolution(*args, **kwargs)

    async def alert_kill_switch(self, reason: str):
        await self.alerts.alert_kill_switch(reason)

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._reply_tracked(
            update.message,
            self.backup.build_status_text(),
            parse_mode="Markdown",
            reply_markup=self.backup.build_main_keyboard(),
        )

    async def _cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not context or not context.args:
            await self._reply_tracked(
                update.message,
                "*Mode Control*\n" + "\n".join(self.backup.build_mode_help_lines()),
                parse_mode="Markdown",
            )
            return
        result = self.backup.apply_control(ControlRequest("mode", context.args[0]))
        await self._reply_tracked(update.message, result.message)

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        result = self.backup.apply_control(ControlRequest("start"))
        await self._reply_tracked(update.message, f"Trading resumed. {result.message}")

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        result = self.backup.apply_control(ControlRequest("stop"))
        await self._reply_tracked(update.message, f"Trading paused. {result.message}")

    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        result = self.backup.apply_control(ControlRequest("kill"))
        await self._reply_tracked(update.message, result.message)

    async def _cmd_trades(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._reply_tracked(update.message, self.backup.build_recent_trades_text(), parse_mode="Markdown")

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        args = list(getattr(context, "args", []) or [])
        await self._reply_tracked(update.message, self.backup.build_stats_text(args=args), parse_mode="Markdown")

    async def _send_deprecation_notice(self, reply_message, key: str) -> None:
        await self._reply_tracked(reply_message, self.backup.deprecation_text(key))

    async def _cmd_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "export")

    async def _cmd_latest_export(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "latest_export")

    async def _cmd_sync_repo_latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "sync_repo_latest")

    async def _cmd_fetch_github(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "fetch_github")

    async def _cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "config")

    async def _cmd_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "set")

    async def _cmd_filters(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "filters")

    async def _cmd_pmaccount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not self.backup._pm_cli_is_available():
            await self._reply_tracked(update.message, self.backup._pm_cli_unavailable_text())
            return
        wallet, account_status, balance = await asyncio.gather(
            self.polymarket_cli.get_wallet_info(),
            self.polymarket_cli.get_account_status(),
            self.polymarket_cli.get_collateral_balance(),
            return_exceptions=True,
        )
        await self._reply_tracked(
            update.message,
            self.backup.format_pm_account_text(wallet, account_status, balance),
            parse_mode="Markdown",
        )

    async def _cmd_pmorders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not self.backup._pm_cli_is_available():
            await self._reply_tracked(update.message, self.backup._pm_cli_unavailable_text())
            return
        orders = await self.polymarket_cli.get_open_orders()
        await self._reply_tracked(update.message, self.backup.format_pm_orders_text(orders), parse_mode="Markdown")

    async def _cmd_pmtrades(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        if not self.backup._pm_cli_is_available():
            await self._reply_tracked(update.message, self.backup._pm_cli_unavailable_text())
            return
        trades = await self.polymarket_cli.get_trades()
        await self._reply_tracked(update.message, self.backup.format_pm_trades_text(trades), parse_mode="Markdown")

    async def _cmd_pmcancelall(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        allow_mutating = bool(getattr(getattr(self.config, "cli", None), "allow_mutating_commands", False))
        if not allow_mutating:
            await self._reply_tracked(update.message, "Mutating CLI commands are disabled.")
            return
        await self._reply_tracked(
            update.message,
            "Confirm cancel-all against Polymarket?",
            reply_markup=self.backup._pm_cancel_keyboard(),
        )

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._reply_tracked(update.message, self.backup.build_help_text(), parse_mode="Markdown")

    async def _cmd_clean(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        self._track_cmd(update)
        await self._send_deprecation_notice(update.message, "clean")

    async def _handle_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update):
            return
        query = update.callback_query
        data = str(getattr(query, "data", "") or "")
        if data == "start":
            result = self.backup.apply_control(ControlRequest("start"))
            await query.answer(text="▶ Scanning started")
            return
        if data == "stop":
            result = self.backup.apply_control(ControlRequest("stop"))
            await query.answer(text="⏸ Trading paused")
            return
        if data == "kill":
            result = self.backup.apply_control(ControlRequest("kill"))
            await query.answer(text="🛑 Kill switch activated", show_alert=True)
            return
        if data == "status":
            await query.answer()
            await query.edit_message_text(
                self.backup.build_status_text(),
                parse_mode="Markdown",
                reply_markup=self.backup.build_main_keyboard(),
            )
            return
        if data == "stats":
            await query.answer()
            await query.edit_message_text(
                self.backup.build_stats_text(),
                parse_mode="Markdown",
                reply_markup=self.backup.build_main_keyboard(),
            )
            return
        if data == "help_panel":
            await query.answer()
            await query.edit_message_text(
                self.backup.build_help_text(),
                parse_mode="Markdown",
                reply_markup=self.backup.build_main_keyboard(),
            )
            return
        if data == "pmcancelall_confirm":
            if not self.backup._pm_cli_is_available():
                await query.answer(text="CLI unavailable", show_alert=True)
                return
            result = await self.polymarket_cli.cancel_all_orders()
            await query.edit_message_text(
                self.backup.format_pmcancelall_result_text(result),
                parse_mode="Markdown",
            )
            return
        if data == "pmcancelall_abort":
            await query.edit_message_text("Cancel-all aborted.")
            return
        if data in ("export_recent", "session_export", "sync_repo_latest", "archive_health", "export_latest", "fetch_github"):
            await query.answer(text="Moved to HA dashboard", show_alert=True)
            await query.edit_message_text(self.backup.deprecation_text(data if data != "export_latest" else "latest_export"))
            return
        await query.answer(text="Unsupported backup action", show_alert=True)
