"""Paper-trade helpers for DecisionTracker."""

from __future__ import annotations

import logging
from typing import Any

from bot.tracker_support.schema import utc_now

logger = logging.getLogger(__name__)


def set_paper_capital(tracker: Any, amount: float) -> None:
    """Set (or reset) the paper trading bankroll."""
    tracker.conn.execute("DELETE FROM paper_capital WHERE id = 1")
    tracker.conn.execute(
        "INSERT INTO paper_capital (id, total_capital, current_balance) VALUES (1, ?, ?)",
        (amount, amount),
    )
    tracker.conn.commit()
    logger.info(f"Paper capital set to ${amount:.2f}")


def get_paper_capital(tracker: Any) -> tuple[float, float]:
    """Return (total_capital, current_balance)."""
    row = tracker.conn.execute("SELECT total_capital, current_balance FROM paper_capital WHERE id = 1").fetchone()
    return (row[0], row[1]) if row else (0.0, 0.0)


def has_paper_capital(tracker: Any, cost: float) -> bool:
    _, balance = tracker.get_paper_capital()
    return balance >= cost


def log_paper_trade(
    tracker: Any,
    market_slug: str,
    coin: str,
    strategy_type: str,
    side: str,
    entry_price: float,
    target_price: float,
    shares: float,
    fee_total: float,
    decision_id: int | None = None,
    market_end_time: str | None = None,
    market_start_time: str | None = None,
    signal_context: str = "",
    signal_overlap_count: int = 0,
    order_size_usd: float | None = None,
    shares_requested: float | None = None,
    shares_filled: float | None = None,
    blocked_min_5_shares: bool = False,
    entry_bid: float | None = None,
    entry_ask: float | None = None,
    entry_spread: float | None = None,
    entry_depth_side_usd: float | None = None,
    opposite_depth_usd: float | None = None,
    depth_ratio: float | None = None,
    window_id: str | None = None,
    signal_score: float | None = None,
    score_velocity: float | None = None,
    score_entry: float | None = None,
    score_depth: float | None = None,
    score_spread: float | None = None,
    score_time: float | None = None,
    score_balance: float | None = None,
    target_delta: float | None = None,
    hard_stop_delta: float | None = None,
) -> int:
    """Open a paper trade and deduct cost from balance."""
    cost = entry_price * shares
    paper_total, _ = tracker.get_paper_capital()
    values = (
        decision_id,
        utc_now().isoformat(),
        tracker._runtime_value("run_id"),
        tracker._runtime_value("app_version"),
        tracker._runtime_value("strategy_version"),
        tracker._runtime_value("config_path"),
        tracker._runtime_value("config_hash"),
        tracker._runtime_value("mode"),
        int(bool(tracker._runtime_value("dry_run", True))),
        order_size_usd if order_size_usd is not None else tracker._runtime_value("order_size_usd"),
        paper_total,
        market_slug,
        window_id or market_slug,
        coin,
        strategy_type,
        signal_context,
        signal_overlap_count,
        side,
        entry_price,
        target_price,
        shares,
        shares_requested if shares_requested is not None else shares,
        shares_filled if shares_filled is not None else shares,
        int(bool(blocked_min_5_shares)),
        cost,
        fee_total,
        "open",
        market_end_time,
        market_start_time,
        entry_bid,
        entry_ask if entry_ask is not None else entry_price,
        entry_spread,
        entry_depth_side_usd,
        opposite_depth_usd,
        depth_ratio,
        signal_score,
        score_velocity,
        score_entry,
        score_depth,
        score_spread,
        score_time,
        score_balance,
        target_delta,
        hard_stop_delta,
    )
    placeholders = ", ".join("?" for _ in values)
    cursor = tracker.conn.execute(
        f"""INSERT INTO paper_trades
           (decision_id, timestamp, run_id, app_version, strategy_version, config_path, config_hash,
            mode, dry_run, order_size_usd, paper_capital_total,
            market_slug, window_id, coin, strategy_type, signal_context, signal_overlap_count,
            side, entry_price, target_price, shares, shares_requested, shares_filled,
            blocked_min_5_shares, cost, fee_total, status, market_end_time, market_start_time,
            entry_bid, entry_ask, entry_spread, entry_depth_side_usd, opposite_depth_usd, depth_ratio,
            signal_score, score_velocity, score_entry, score_depth, score_spread, score_time, score_balance,
            target_delta, hard_stop_delta)
           VALUES ({placeholders})""",
        values,
    )
    tracker.conn.execute(
        "UPDATE paper_capital SET current_balance = current_balance - ? WHERE id = 1",
        (cost,),
    )
    tracker.conn.commit()
    return cursor.lastrowid


def record_paper_trade_path(
    tracker: Any,
    trade_id: int,
    *,
    max_bid_seen: float | None = None,
    min_bid_seen: float | None = None,
    time_to_max_bid_s: float | None = None,
    time_to_min_bid_s: float | None = None,
    first_profit_time_s: float | None = None,
    scalp_hit: bool | None = None,
    high_confidence_hit: bool | None = None,
    mfe: float | None = None,
    mae: float | None = None,
    peak_net_pnl: float | None = None,
    trough_net_pnl: float | None = None,
) -> None:
    assignments: list[str] = []
    values: list[object] = []
    mapping = {
        "max_bid_seen": max_bid_seen,
        "min_bid_seen": min_bid_seen,
        "time_to_max_bid_s": time_to_max_bid_s,
        "time_to_min_bid_s": time_to_min_bid_s,
        "first_profit_time_s": first_profit_time_s,
        "mfe": mfe,
        "mae": mae,
        "peak_net_pnl": peak_net_pnl,
        "trough_net_pnl": trough_net_pnl,
    }
    for col, value in mapping.items():
        if value is not None:
            assignments.append(f"{col} = ?")
            values.append(value)
    if scalp_hit is not None:
        assignments.append("scalp_hit = ?")
        values.append(int(bool(scalp_hit)))
    if high_confidence_hit is not None:
        assignments.append("high_confidence_hit = ?")
        values.append(int(bool(high_confidence_hit)))
    if not assignments:
        return
    values.append(trade_id)
    tracker.conn.execute(f"UPDATE paper_trades SET {', '.join(assignments)} WHERE id = ?", tuple(values))
    tracker.conn.commit()


def close_paper_trades(
    tracker: Any,
    market_slug: str,
    winner: str,
    exit_bid: float | None = None,
    exit_ask: float | None = None,
) -> None:
    """Resolve all open paper trades for a market and update balance."""
    trades = tracker.conn.execute(
        """SELECT id, strategy_type, side, entry_price, shares, cost, fee_total
           FROM paper_trades WHERE market_slug = ? AND status = 'open'""",
        (market_slug,),
    ).fetchall()

    for trade_id, strategy_type, side, entry_price, shares, cost, fee_total in trades:
        if strategy_type == "dual_leg":
            pnl = (1.0 - entry_price) * shares - fee_total
            exit_price = 1.0
            status = "closed_win" if pnl > 0 else "closed_loss"
        else:
            won = (side == "up" and winner.upper() == "UP") or (side == "down" and winner.upper() == "DOWN")
            if won:
                pnl = (1.0 - entry_price) * shares - fee_total
                exit_price = 1.0
                status = "closed_win"
            else:
                pnl = -cost
                exit_price = 0.0
                status = "closed_loss"

        now = utc_now().isoformat()
        tracker.conn.execute(
            """UPDATE paper_trades
               SET status=?, exit_price=?, pnl=?,
                   exit_reason='resolution', exit_timestamp=?,
                   bid_at_exit=COALESCE(?, bid_at_exit),
                   ask_at_exit=COALESCE(?, ask_at_exit),
                   exit_spread=COALESCE(?, exit_spread)
               WHERE id=?""",
            (
                status,
                exit_price,
                pnl,
                now,
                exit_bid,
                exit_ask,
                (exit_ask - exit_bid) if (exit_ask is not None and exit_bid is not None) else None,
                trade_id,
            ),
        )
        tracker.conn.execute(
            "UPDATE paper_capital SET current_balance = current_balance + ? WHERE id = 1",
            (cost + pnl,),
        )

    if trades:
        tracker.conn.commit()
        logger.info(f"Closed {len(trades)} paper trades for {market_slug} -> winner: {winner}")


def close_paper_trade_early(
    tracker: Any,
    trade_id: int,
    exit_price: float,
    pnl: float,
    status: str,
    exit_reason: str = "manual",
    time_remaining_s: float | None = None,
    bid_at_exit: float | None = None,
    ask_at_exit: float | None = None,
    stall_exit_triggered: bool | None = None,
) -> None:
    """Close a paper trade early before market resolution."""
    row = tracker.conn.execute(
        "SELECT cost FROM paper_trades WHERE id = ? AND status = 'open'",
        (trade_id,),
    ).fetchone()
    if not row:
        return
    cost = row[0]
    now = utc_now().isoformat()
    tracker.conn.execute(
        """UPDATE paper_trades
           SET status=?, exit_price=?, pnl=?,
               exit_reason=?, exit_timestamp=?, time_remaining_s=?, bid_at_exit=?,
               ask_at_exit=?, exit_spread=?, stall_exit_triggered=COALESCE(?, stall_exit_triggered)
           WHERE id=?""",
        (
            status,
            exit_price,
            pnl,
            exit_reason,
            now,
            time_remaining_s,
            bid_at_exit,
            ask_at_exit,
            (ask_at_exit - bid_at_exit) if (ask_at_exit is not None and bid_at_exit is not None) else None,
            int(bool(stall_exit_triggered)) if stall_exit_triggered is not None else None,
            trade_id,
        ),
    )
    tracker.conn.execute(
        "UPDATE paper_capital SET current_balance = current_balance + ? WHERE id = 1",
        (cost + pnl,),
    )
    tracker.conn.commit()
    logger.info(
        f"Paper trade {trade_id} closed early: exit@{exit_price:.3f} pnl=${pnl:+.4f} "
        f"[{status}] reason={exit_reason} t_remaining={time_remaining_s}"
    )


def reset_paper_stats(tracker: Any) -> None:
    """Reset displayed stats to zero without deleting trade history."""
    now = utc_now().isoformat()
    tracker.conn.execute(
        """UPDATE paper_capital
           SET current_balance = total_capital, reset_at = ?
           WHERE id = 1""",
        (now,),
    )
    tracker.conn.commit()
    logger.info(f"Paper stats reset at {now}")


def get_paper_stats(tracker: Any) -> dict:
    """Return paper trading summary (only trades since last reset)."""
    total, balance = tracker.get_paper_capital()
    reset_at = tracker.conn.execute("SELECT reset_at FROM paper_capital WHERE id = 1").fetchone()
    reset_at = reset_at[0] if reset_at and reset_at[0] else "1970-01-01"

    row = tracker.conn.execute(
        """SELECT COUNT(*),
                  SUM(CASE WHEN status='closed_win' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='closed_loss' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='open' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN pnl IS NOT NULL THEN pnl ELSE 0 END),
                  AVG(entry_price),
                  SUM(CASE WHEN status IN ('closed_win','closed_loss') THEN 1 ELSE 0 END),
                  AVG(CASE WHEN status IN ('closed_win','closed_loss') THEN exit_price END)
           FROM paper_trades WHERE timestamp >= ?""",
        (reset_at,),
    ).fetchone()
    total_trades, wins, losses, open_pos, realized_pnl, avg_buy, sells, avg_sell = row
    wins = wins or 0
    losses = losses or 0
    return {
        "total_capital": total,
        "current_balance": balance,
        "total_pnl": balance - total,
        "realized_pnl": realized_pnl or 0.0,
        "total_trades": total_trades or 0,
        "wins": wins,
        "losses": losses,
        "open_positions": open_pos or 0,
        "win_rate": (wins / max(wins + losses, 1)) * 100,
        "buys": total_trades or 0,
        "sells": sells or 0,
        "avg_buy_price": avg_buy or 0.0,
        "avg_sell_price": avg_sell or 0.0,
    }
