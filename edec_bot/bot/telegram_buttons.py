"""Telegram dashboard button controller."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot import telegram_dashboard as dashboard_ui


def _back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("\u00ab Back", callback_data="back")]])


async def _handle_disabled_buttons(bot: Any, query: Any, data: str) -> bool:
    if data not in ("noop", "wet_disabled"):
        return False
    await query.answer(
        "\U0001F30A Wet Run coming soon - currently disabled for safety."
        if data == "wet_disabled"
        else "Already in Dry Run mode.",
        show_alert=True,
    )
    return True


async def _handle_budget_buttons(bot: Any, query: Any, data: str) -> bool:
    if data.startswith("budget_"):
        bot._set_dashboard_view("main")
        amount = float(data.split("_")[1])
        await query.answer(f"\u2705 Budget set to ${amount:.0f}", show_alert=False)
        if bot.executor:
            bot.executor.set_order_size(amount)
        await bot._do_cleanup()
        return True

    if data != "budget":
        return False

    bot._set_dashboard_view("budget")
    await query.answer()
    order_size = bot.executor.order_size_usd if bot.executor else bot.config.execution.order_size_usd
    await query.edit_message_text(
        dashboard_ui.build_budget_panel_text(order_size),
        parse_mode="Markdown",
        reply_markup=bot._budget_keyboard(),
    )
    return True


async def _handle_capital_buttons(bot: Any, query: Any, data: str) -> bool:
    if data.startswith("capital_"):
        bot._set_dashboard_view("main")
        amount = float(data.split("_")[1])
        await query.answer(f"\u2705 Capital set to ${amount:,.0f}", show_alert=False)
        if bot.tracker:
            bot.tracker.set_paper_capital(amount)
        await bot._do_cleanup()
        return True

    if data != "capital":
        return False

    bot._set_dashboard_view("capital")
    await query.answer()
    _, balance = bot.tracker.get_paper_capital() if bot.tracker else (0, 0)
    await query.edit_message_text(
        dashboard_ui.build_capital_panel_text(balance),
        parse_mode="Markdown",
        reply_markup=bot._capital_keyboard(),
    )
    return True


async def _handle_control_buttons(bot: Any, query: Any, data: str) -> bool:
    if data == "back":
        bot._set_dashboard_view("main")
        await query.answer()
        await bot._refresh_dashboard(force=True)
        return True

    if data == "start":
        bot._set_dashboard_view("main")
        await query.answer("\u25b6 Scanning started", show_alert=False)
        if bot.strategy_engine:
            bot.strategy_engine.start_scanning()
        bot.risk_manager.resume()
        bot.risk_manager.deactivate_kill_switch()
        await bot._do_cleanup()
        return True

    if data == "stop":
        bot._set_dashboard_view("main")
        await query.answer("\u23f8 Bot stopped", show_alert=False)
        if bot.strategy_engine:
            bot.strategy_engine.stop_scanning()
        bot.risk_manager.pause()
        await bot._do_cleanup()
        return True

    if data == "kill":
        bot._set_dashboard_view("main")
        await query.answer("\U0001F6D1 Kill switch activated!", show_alert=True)
        if bot.strategy_engine:
            bot.strategy_engine.stop_scanning()
        bot.risk_manager.activate_kill_switch("Manual kill via Telegram")
        await bot._do_cleanup()
        return True

    if data == "refresh":
        bot._set_dashboard_view("main")
        await query.answer("Refreshing dashboard...", show_alert=False)
        await bot._refresh_dashboard(force=True)
        return True

    if data == "clear_chat":
        bot._set_dashboard_view("main")
        await query.answer("Clearing chat history...", show_alert=True)
        stats = await bot._clear_chat_history()
        note = (
            f"Chat clear done. Deleted {stats.get('deleted', 0)}/"
            f"{stats.get('attempted', 0)} messages."
        )
        if stats.get("undeletable", 0):
            note += " Some old messages may be undeletable due to Telegram limits."
        bot._track(await bot._app.bot.send_message(chat_id=bot.chat_id, text=note))
        await bot._repost_dashboard()
        return True

    if data == "reset_stats":
        bot._set_dashboard_view("main")
        await query.answer("\U0001F5D1 Stats reset!", show_alert=False)
        if bot.tracker:
            bot.tracker.reset_paper_stats()
        bot.risk_manager.reset_daily_stats()
        await bot._do_cleanup()
        return True

    return False


async def _handle_panel_buttons(bot: Any, query: Any, data: str) -> bool:
    back_kb = _back_keyboard()

    if data == "stats":
        bot._set_dashboard_view("stats")
        stats = bot.tracker.get_daily_stats()
        paper = bot.tracker.get_paper_stats()
        text = dashboard_ui.build_stats_panel_text(stats, paper)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_kb)
        return True

    if data == "status":
        bot._set_dashboard_view("status")
        status = bot.risk_manager.get_status()
        mode = bot.strategy_engine.mode if bot.strategy_engine else "unknown"
        order_size = bot.executor.order_size_usd if bot.executor else bot.config.execution.order_size_usd
        text = dashboard_ui.build_status_panel_text(status, mode, order_size)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_kb)
        return True

    if data == "trades":
        bot._set_dashboard_view("trades")
        trades = bot.tracker.get_recent_trades(limit=5)
        text = dashboard_ui.build_recent_trades_panel_text(trades)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_kb)
        return True

    if data in ("export_today", "export_all", "export_recent", "export_latest", "sync_repo_latest"):
        bot._set_dashboard_view("main")
        if data in ("export_today", "export_all"):
            await bot._handle_export_request(
                query.message,
                today_only=(data == "export_today"),
                wait_text="\u23f3 Generating spreadsheet...",
                unavailable_text="Export not available.",
                caption="\U0001F4CA EDEC Bot Export - Paper Trades, Decisions, Filter Performance",
            )
        elif data == "export_recent":
            await bot._handle_recent_export_request(query.message)
        elif data == "export_latest":
            await bot._handle_latest_export_request(
                query.message,
                wait_text="\u23f3 Sending latest archive files...",
            )
        else:
            await bot._handle_repo_sync_request(
                query.message,
                wait_text="\u23f3 Syncing latest Dropbox files to local repo folder...",
                heading_ok="\u2705 *Repo Sync Complete*",
                heading_fail="\u26a0\ufe0f *Repo Sync Partial/Failed*",
            )
        await bot._repost_dashboard()
        return True

    if data == "archive_health":
        bot._set_dashboard_view("archive_health")
        text = await bot._build_archive_health_text()
        await query.edit_message_text(text, reply_markup=back_kb)
        return True

    if data == "help_panel":
        bot._set_dashboard_view("help_panel")
        text = bot._commands_text()
        await query.edit_message_text(text, reply_markup=back_kb)
        return True

    if data == "filters":
        bot._set_dashboard_view("filters")
        stats = bot.tracker.get_filter_stats()
        if not stats:
            text = "No filter data yet."
        else:
            lines = ["\U0001F50D *Filter Performance*\n"]
            for stat in stats:
                total = stat["passed"] + stat["failed"]
                fail_pct = (stat["failed"] / total * 100) if total > 0 else 0
                bar = "\U0001F7E9" * int((100 - fail_pct) / 20) + "\U0001F7E5" * int(fail_pct / 20)
                lines.append(f"`{stat['filter']:18s}` {bar} {fail_pct:.0f}% fail")
            text = "\n".join(lines)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_kb)
        return True

    return False


async def handle_button(bot: Any, update: Any, context: Any) -> None:
    """Route Telegram inline button interactions."""
    query = update.callback_query
    if not bot._auth(update):
        return

    data = query.data

    if await _handle_disabled_buttons(bot, query, data):
        return
    if await _handle_budget_buttons(bot, query, data):
        return
    if await _handle_capital_buttons(bot, query, data):
        return
    if await _handle_control_buttons(bot, query, data):
        return

    await query.answer()
    await _handle_panel_buttons(bot, query, data)
