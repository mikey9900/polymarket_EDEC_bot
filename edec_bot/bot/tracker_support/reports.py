"""Read/query helpers for DecisionTracker."""

from __future__ import annotations

from typing import Any

from bot.tracker_support.schema import utc_now


def get_daily_stats(tracker: Any, date: str | None = None) -> dict:
    """Get aggregated stats for a day."""
    if date is None:
        date = utc_now().strftime("%Y-%m-%d")

    trades = tracker.conn.execute(
        "SELECT status, combined_cost, fee_total, abort_cost FROM trades WHERE timestamp LIKE ?",
        (f"{date}%",),
    ).fetchall()

    decisions = tracker.conn.execute(
        "SELECT action, COUNT(*) FROM decisions WHERE timestamp LIKE ? GROUP BY action",
        (f"{date}%",),
    ).fetchall()

    total_trades = len(trades)
    successful = sum(1 for trade in trades if trade[0] in ("success", "closed_win", "resolved_win"))
    aborted = sum(1 for trade in trades if trade[0] in ("aborted", "partial_abort"))
    decision_counts = {action: count for action, count in decisions}

    return {
        "date": date,
        "total_evaluations": sum(decision_counts.values()),
        "signals": decision_counts.get("TRADE", 0) + decision_counts.get("DRY_RUN_SIGNAL", 0),
        "skips": decision_counts.get("SKIP", 0),
        "trades_executed": total_trades,
        "successful": successful,
        "aborted": aborted,
    }


def get_recent_trades(tracker: Any, limit: int = 10) -> list[dict]:
    """Get the most recent trades."""
    rows = tracker.conn.execute(
        """SELECT t.timestamp, t.market_slug, t.coin, t.strategy_type, t.side,
                  t.up_price, t.down_price, t.entry_price, t.target_price,
                  t.combined_cost, t.fee_total, t.shares, t.status, t.abort_cost,
                  COALESCE(t.pnl, do.actual_profit), t.exit_reason
           FROM trades t
           LEFT JOIN decision_outcomes do ON do.decision_id = t.decision_id
           ORDER BY t.id DESC LIMIT ?""",
        (limit,),
    ).fetchall()

    return [
        {
            "timestamp": row[0],
            "market": row[1],
            "coin": row[2],
            "strategy_type": row[3],
            "side": row[4],
            "up_price": row[5] or 0,
            "down_price": row[6] or 0,
            "entry_price": row[7],
            "target_price": row[8],
            "combined_cost": row[9],
            "fee_total": row[10],
            "shares": row[11],
            "status": row[12],
            "abort_cost": row[13],
            "actual_profit": row[14],
            "exit_reason": row[15],
        }
        for row in rows
    ]


def get_filter_stats(tracker: Any) -> list[dict]:
    """Get pass/fail rates for each filter."""
    rows = tracker.conn.execute("SELECT filter_passed, filter_failed FROM decisions").fetchall()

    filter_counts: dict[str, dict] = {}
    for passed_str, failed_str in rows:
        for name in (passed_str or "").split(","):
            name = name.strip()
            if name:
                filter_counts.setdefault(name, {"passed": 0, "failed": 0})
                filter_counts[name]["passed"] += 1
        for name in (failed_str or "").split(","):
            name = name.strip()
            if name:
                filter_counts.setdefault(name, {"passed": 0, "failed": 0})
                filter_counts[name]["failed"] += 1

    return [{"filter": name, **counts} for name, counts in sorted(filter_counts.items())]


def get_recent_paper_trades(tracker: Any, limit: int = 3) -> list[dict]:
    rows = tracker.conn.execute(
        """SELECT timestamp, coin, strategy_type, side, entry_price,
                  target_price, shares, cost, status, pnl
           FROM paper_trades ORDER BY id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "timestamp": row[0],
            "coin": row[1],
            "strategy_type": row[2],
            "side": row[3],
            "entry_price": row[4],
            "target_price": row[5],
            "shares": row[6],
            "cost": row[7],
            "status": row[8],
            "pnl": row[9],
        }
        for row in rows
    ]


def get_coin_recent_outcomes(tracker: Any, coin: str, limit: int = 4) -> list[str]:
    rows = tracker.conn.execute(
        """SELECT o.winner
           FROM outcomes o
           JOIN decisions d ON d.market_slug = o.market_slug
           WHERE d.coin = ?
           GROUP BY o.market_slug
           ORDER BY o.resolved_at DESC
           LIMIT ?""",
        (coin, limit),
    ).fetchall()
    return [row[0] for row in rows]


def get_coin_recent_outcome_details(tracker: Any, coin: str, limit: int = 6) -> list[dict]:
    rows = tracker.conn.execute(
        """SELECT o.market_slug, o.resolved_at, o.winner, o.btc_open_price, o.btc_close_price
           FROM outcomes o
           JOIN decisions d ON d.market_slug = o.market_slug
           WHERE d.coin = ?
           GROUP BY o.market_slug
           ORDER BY o.resolved_at DESC
           LIMIT ?""",
        (coin, limit),
    ).fetchall()
    return [
        {
            "market_slug": row[0],
            "resolved_at": row[1],
            "winner": row[2],
            "btc_open_price": row[3],
            "btc_close_price": row[4],
        }
        for row in rows
    ]
