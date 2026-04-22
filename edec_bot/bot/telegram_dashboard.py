"""Telegram backup UI helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


MODE_LABELS = {
    "both": "ALL enabled strategies",
    "dual": "DUAL-LEG only",
    "single": "SINGLE-LEG only",
    "lead": "LEAD-LAG only",
    "swing": "SWING LEG only",
    "off": "OFF",
}

BUDGET_OPTIONS = [1, 2, 5, 10, 15, 20]
CAPITAL_OPTIONS = [5, 10, 20, 50, 100, 5000, 25000, 50000]


def format_usd(price: float) -> str:
    if price >= 1000:
        return f"${price:,.0f}"
    if price >= 100:
        return f"${price:.1f}"
    if price >= 10:
        return f"${price:.2f}"
    if price >= 1:
        return f"${price:.3f}"
    return f"${price:.4f}"


def build_dashboard_text(
    *,
    version: str,
    config,
    tracker=None,
    scanner=None,
    aggregator=None,
    strategy_engine=None,
) -> str:
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    is_active = strategy_engine.is_active if strategy_engine else False
    paper = tracker.get_paper_stats() if tracker else {}
    state_label = "SCANNING" if is_active else "STOPPED"
    pnl = paper.get("total_pnl", 0)
    wins = paper.get("wins", 0)
    losses = paper.get("losses", 0)
    open_pos = paper.get("open_positions", 0)
    balance = paper.get("current_balance", 0)
    total_cap = paper.get("total_capital", 0)
    win_rate = f"{paper.get('win_rate', 0):.0f}%" if (wins + losses) > 0 else "-"

    lines = [
        f"EDEC Bot v{version}  {state_label}  Dry  {now_str}",
        "-----------------------------",
    ]
    if scanner or aggregator:
        snapshot = scanner.get_status_snapshot() if scanner else {}
        for coin in config.coins:
            usd_str = "-"
            if aggregator:
                agg = aggregator.get_aggregated_price(coin)
                if agg:
                    usd_str = format_usd(agg.price)
            history_icons = ""
            if tracker:
                outcomes = tracker.get_coin_recent_outcomes(coin, limit=4)
                icons = ["W" if outcome == "UP" else "L" for outcome in outcomes]
                while len(icons) < 4:
                    icons.insert(0, ".")
                history_icons = "".join(icons)
            coin_snapshot = snapshot.get(coin)
            book_str = "no market"
            signal_icon = ""
            if coin_snapshot:
                up_ask = coin_snapshot.get("up_ask", 0)
                dn_ask = coin_snapshot.get("down_ask", 0)
                book_str = f"UP@{up_ask:.2f} DN@{dn_ask:.2f}"
                cfg_dl = config.dual_leg
                cfg_sl = config.single_leg
                cfg_ll = config.lead_lag
                cfg_sw = config.swing_leg
                combined = up_ask + dn_ask
                if combined <= cfg_dl.max_combined_cost:
                    signal_icon = " DUAL"
                elif up_ask <= cfg_sl.entry_max and dn_ask >= cfg_sl.opposite_min:
                    signal_icon = " SINGLE"
                elif dn_ask <= cfg_sl.entry_max and up_ask >= cfg_sl.opposite_min:
                    signal_icon = " SINGLE"
                elif cfg_ll.min_entry <= up_ask <= cfg_ll.max_entry:
                    signal_icon = " LEAD"
                elif cfg_ll.min_entry <= dn_ask <= cfg_ll.max_entry:
                    signal_icon = " LEAD"
                elif up_ask <= cfg_sw.first_leg_max or dn_ask <= cfg_sw.first_leg_max:
                    signal_icon = " SWING"
            lines.append(f"{coin.upper():<4} {usd_str:>10}  {history_icons or '....'}  {book_str}{signal_icon}")
    lines.extend(
        [
            "-----------------------------",
            f"Balance ${balance:.2f} / ${total_cap:.0f}  P&L ${pnl:+.2f}",
            f"W {wins}  L {losses}  Open {open_pos}  Win {win_rate}",
        ]
    )
    return "\n".join(lines)


def build_main_keyboard(*, is_running: bool, is_dry: bool, order_size: float, capital_balance: float) -> InlineKeyboardMarkup:
    del order_size, capital_balance
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Pause" if is_running else "Resume", callback_data="stop" if is_running else "start"),
            InlineKeyboardButton("Kill", callback_data="kill"),
        ],
        [
            InlineKeyboardButton("Status", callback_data="status"),
            InlineKeyboardButton("Stats", callback_data="stats"),
        ],
        [
            InlineKeyboardButton("Commands", callback_data="help_panel"),
            InlineKeyboardButton("Dry Run" if is_dry else "Wet Run", callback_data="status"),
        ],
    ])


def build_budget_keyboard(order_size: float) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(
            f"${amt}" if amt != order_size else f"* ${amt}",
            callback_data=f"budget_{amt}",
        )
        for amt in BUDGET_OPTIONS
    ]
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([InlineKeyboardButton("Back", callback_data="back")])
    return InlineKeyboardMarkup(rows)


def build_capital_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(f"${amt:,}", callback_data=f"capital_{amt}")
        for amt in CAPITAL_OPTIONS
    ]
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([InlineKeyboardButton("Back", callback_data="back")])
    return InlineKeyboardMarkup(rows)


def build_budget_panel_text(order_size: float) -> str:
    return f"Budget per trade: ${order_size:.0f}"


def build_capital_panel_text(balance: float) -> str:
    return f"Paper capital balance: ${balance:,.2f}"


def build_stats_panel_text(stats: dict, paper: dict) -> str:
    return (
        f"Today's Stats ({stats['date']})\n"
        f"Evaluations: {stats['total_evaluations']}\n"
        f"Signals: {stats['signals']} | Skips: {stats['skips']}\n"
        f"Capital: ${paper['current_balance']:.2f} / ${paper['total_capital']:.2f}\n"
        f"P&L: ${paper['total_pnl']:+.2f}"
    )


def build_status_panel_text(status: dict, mode: str, order_size: float) -> str:
    state = "KILLED" if status["kill_switch"] else "PAUSED" if status["paused"] else "RUNNING"
    return (
        f"*Status*\n"
        f"State: {state}\n"
        f"Mode: {MODE_LABELS.get(mode, mode)}\n"
        f"Budget: ${order_size:.0f}/trade\n"
        f"Daily P&L: ${status['daily_pnl']:+.2f} | Open: {status['open_positions']}"
    )


def build_recent_trades_panel_text(trades: list[dict]) -> str:
    if not trades:
        return "No trades yet."
    lines = ["*Recent Trades*\n"]
    for trade in trades:
        pnl = trade.get("actual_profit")
        pnl_str = f"${pnl:+.4f}" if pnl is not None else "pending"
        lines.append(f"`{trade['timestamp'][:16]}` {trade['coin'].upper()} -> {pnl_str}")
    return "\n".join(lines)


def build_status_command_text(*, config, risk_status: dict, scanner=None, strategy_engine=None) -> str:
    dry_run = "ON" if config.execution.dry_run else "OFF"
    mode = strategy_engine.mode if strategy_engine else "unknown"
    mode_label = MODE_LABELS.get(mode, mode)
    bot_state = "KILLED" if risk_status["kill_switch"] else "PAUSED" if risk_status["paused"] else "RUNNING"

    lines = [
        "*EDEC Bot Status*",
        f"State: {bot_state} | Dry Run: {dry_run}",
        f"Mode: {mode_label}",
        f"Daily P&L: ${risk_status['daily_pnl']:+.2f} | Session: ${risk_status['session_pnl']:+.2f}",
        f"Open Positions: {risk_status['open_positions']} | Trades/hr: {risk_status['trades_this_hour']}",
        "",
        "*Per-Coin Order Books*",
    ]
    if scanner:
        snapshot = scanner.get_status_snapshot()
        cfg_dual = config.dual_leg
        cfg_single = config.single_leg
        for coin in config.coins:
            coin_data = snapshot.get(coin)
            if coin_data:
                up_ask = coin_data["up_ask"]
                down_ask = coin_data["down_ask"]
                combined = up_ask + down_ask
                indicators = []
                if up_ask <= cfg_dual.price_threshold and down_ask <= cfg_dual.price_threshold and combined <= cfg_dual.max_combined_cost:
                    indicators.append("DUAL?")
                if up_ask <= cfg_single.entry_max and down_ask >= cfg_single.opposite_min:
                    indicators.append("SL↑")
                elif down_ask <= cfg_single.entry_max and up_ask >= cfg_single.opposite_min:
                    indicators.append("SL↓")
                signal_str = " " + " ".join(indicators) if indicators else ""
                lines.append(f"`{coin.upper():>4}`: UP@{up_ask:.3f} DN@{down_ask:.3f}{signal_str}")
            else:
                lines.append(f"`{coin.upper():>4}`: - no market -")
    else:
        lines.append("_(scanner not attached)_")
    return "\n".join(lines)
