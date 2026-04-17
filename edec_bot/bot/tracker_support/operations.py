"""Runtime metadata and core write operations for DecisionTracker."""

from __future__ import annotations

import logging
from typing import Any

from bot.models import Decision, TradeResult
from bot.tracker_support.schema import utc_now

logger = logging.getLogger(__name__)


def set_runtime_context(tracker: Any, context: dict[str, object]) -> None:
    tracker._runtime_context = dict(context)
    run_id = str(context.get("run_id") or "")
    if not run_id:
        return
    tracker.conn.execute(
        """INSERT OR REPLACE INTO runs (
            run_id, started_at, app_version, strategy_version, config_path, config_hash,
            dry_run, initial_mode, default_order_size_usd, initial_paper_capital
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            context.get("started_at") or utc_now().isoformat(),
            context.get("app_version"),
            context.get("strategy_version"),
            context.get("config_path"),
            context.get("config_hash"),
            int(bool(context.get("dry_run", True))),
            context.get("mode"),
            context.get("order_size_usd"),
            context.get("paper_capital_total"),
        ),
    )
    tracker.conn.commit()


def latest_run_metadata(tracker: Any) -> dict | None:
    row = tracker.conn.execute(
        """SELECT run_id, started_at, app_version, strategy_version, config_path,
                  config_hash, dry_run, initial_mode, default_order_size_usd,
                  initial_paper_capital
           FROM runs ORDER BY started_at DESC LIMIT 1"""
    ).fetchone()
    if not row:
        return None
    return {
        "run_id": row[0],
        "started_at": row[1],
        "app_version": row[2],
        "strategy_version": row[3],
        "config_path": row[4],
        "config_hash": row[5],
        "dry_run": bool(row[6]),
        "mode": row[7],
        "order_size_usd": row[8],
        "paper_capital_total": row[9],
    }


def log_decision(tracker: Any, decision: Decision) -> int:
    """Log a strategy evaluation. Returns the decision ID."""
    passed = [item.name for item in decision.filter_results if item.passed]
    failed = [item.name for item in decision.filter_results if not item.passed]

    cursor = tracker.conn.execute(
        """INSERT INTO decisions (
            timestamp, run_id, app_version, strategy_version, config_path, config_hash,
            mode, dry_run, order_size_usd, paper_capital_total, signal_context,
            signal_overlap_count, suppressed_reason,
            market_slug, window_id, coin, strategy_type, market_end_time, market_start_time,
            up_best_ask, down_best_ask, combined_cost,
            btc_price, coin_velocity_30s, coin_velocity_60s,
            up_depth_usd, down_depth_usd, time_remaining_s,
            feed_count, filter_passed, filter_failed, action, reason,
            entry_price, target_price, expected_profit_per_share,
            entry_bid, entry_ask, entry_spread,
            entry_depth_side_usd, opposite_depth_usd, depth_ratio,
            signal_score, score_velocity, score_entry, score_depth,
            score_spread, score_time, score_balance,
            resignal_cooldown_s, min_price_improvement, last_signal_age_s
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            decision.timestamp.isoformat(),
            decision.run_id or tracker._runtime_value("run_id"),
            decision.app_version or tracker._runtime_value("app_version"),
            decision.strategy_version or tracker._runtime_value("strategy_version"),
            decision.config_path or tracker._runtime_value("config_path"),
            decision.config_hash or tracker._runtime_value("config_hash"),
            decision.mode or tracker._runtime_value("mode"),
            int(decision.dry_run),
            decision.order_size_usd,
            decision.paper_capital_total,
            decision.signal_context,
            decision.signal_overlap_count,
            decision.suppressed_reason,
            decision.market_slug,
            decision.window_id,
            decision.coin,
            decision.strategy_type,
            decision.market_end_time.isoformat(),
            decision.market_start_time.isoformat(),
            decision.up_best_ask,
            decision.down_best_ask,
            decision.combined_cost,
            decision.btc_price,
            decision.coin_velocity_30s,
            decision.coin_velocity_60s,
            decision.up_depth_usd,
            decision.down_depth_usd,
            decision.time_remaining_s,
            decision.feed_count,
            ",".join(passed),
            ",".join(failed),
            decision.action,
            decision.reason,
            decision.entry_price,
            decision.target_price,
            decision.expected_profit_per_share,
            decision.entry_bid,
            decision.entry_ask,
            decision.entry_spread,
            decision.entry_depth_side_usd,
            decision.opposite_depth_usd,
            decision.depth_ratio,
            decision.signal_score,
            decision.score_velocity,
            decision.score_entry,
            decision.score_depth,
            decision.score_spread,
            decision.score_time,
            decision.score_balance,
            decision.resignal_cooldown_s,
            decision.min_price_improvement,
            decision.last_signal_age_s,
        ),
    )
    tracker.conn.commit()
    return cursor.lastrowid


def log_trade(tracker: Any, decision_id: int, result: TradeResult) -> int:
    """Log an executed trade."""
    market = result.signal.market
    paper_total, _ = tracker.get_paper_capital()
    cursor = tracker.conn.execute(
        """INSERT INTO trades (
            decision_id, timestamp, run_id, app_version, strategy_version, config_path,
            config_hash, mode, dry_run, order_size_usd, paper_capital_total,
            market_slug, window_id, coin, strategy_type, side,
            up_price, down_price, entry_price, target_price,
            combined_cost, fee_total, shares, shares_requested, shares_filled, blocked_min_5_shares,
            up_order_id, down_order_id, buy_order_id, sell_order_id,
            status, abort_cost, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            decision_id,
            utc_now().isoformat(),
            tracker._runtime_value("run_id"),
            tracker._runtime_value("app_version"),
            tracker._runtime_value("strategy_version"),
            tracker._runtime_value("config_path"),
            tracker._runtime_value("config_hash"),
            tracker._runtime_value("mode"),
            int(bool(tracker._runtime_value("dry_run", True))),
            tracker._runtime_value("order_size_usd"),
            paper_total,
            market.slug,
            market.slug,
            market.coin,
            result.strategy_type or "dual_leg",
            result.side or None,
            result.up_fill_price or None,
            result.down_fill_price or None,
            result.signal.entry_price or None,
            result.signal.target_sell_price or None,
            result.total_cost,
            result.fee_total,
            result.shares,
            result.shares_requested,
            result.shares_filled,
            int(bool(result.blocked_min_5_shares)),
            result.up_order_id,
            result.down_order_id,
            result.buy_order_id,
            result.sell_order_id,
            result.status,
            result.abort_cost,
            result.error,
        ),
    )
    tracker.conn.commit()
    return cursor.lastrowid


def log_outcome(tracker: Any, market_slug: str, winner: str, btc_open: float, btc_close: float) -> None:
    """Log a market resolution and backfill decision outcomes."""
    cursor = tracker.conn.execute(
        """INSERT OR IGNORE INTO outcomes (
            market_slug, resolved_at, winner, btc_open_price, btc_close_price
        ) VALUES (?, ?, ?, ?, ?)""",
        (market_slug, utc_now().isoformat(), winner, btc_open, btc_close),
    )
    tracker.conn.commit()
    outcome_id = cursor.lastrowid
    if outcome_id == 0:
        row = tracker.conn.execute("SELECT id FROM outcomes WHERE market_slug = ?", (market_slug,)).fetchone()
        outcome_id = row[0] if row else 0

    if not outcome_id:
        return

    decisions = tracker.conn.execute(
        "SELECT id, up_best_ask, down_best_ask, combined_cost, action FROM decisions WHERE market_slug = ?",
        (market_slug,),
    ).fetchall()

    for dec_id, up_ask, down_ask, combined, action in decisions:
        if up_ask is None or down_ask is None:
            continue
        fee_rate = 0.072
        fee_up = fee_rate * up_ask * (1.0 - up_ask)
        fee_down = fee_rate * down_ask * (1.0 - down_ask)
        total_cost = combined + fee_up + fee_down
        hyp_profit = 1.0 - total_cost if total_cost < 1.0 else -(total_cost - 1.0)
        would_profit = hyp_profit > 0

        actual = None
        if action == "TRADE":
            trade = tracker.conn.execute(
                "SELECT combined_cost, fee_total, status, abort_cost FROM trades WHERE decision_id = ?",
                (dec_id,),
            ).fetchone()
            if trade:
                t_cost, t_fee, t_status, t_abort = trade
                if t_status == "success":
                    actual = 1.0 - (t_cost + t_fee)
                elif t_status in ("aborted", "partial_abort"):
                    actual = -t_abort
                else:
                    actual = 0.0

        tracker.conn.execute(
            """INSERT OR REPLACE INTO decision_outcomes (
                decision_id, outcome_id, would_have_profited,
                hypothetical_profit, actual_profit
            ) VALUES (?, ?, ?, ?, ?)""",
            (dec_id, outcome_id, int(would_profit), hyp_profit, actual),
        )

    tracker.conn.commit()
    logger.info(f"Outcome logged: {market_slug} -> {winner}, backfilled {len(decisions)} decisions")


def update_decision_signal_context(
    tracker: Any,
    decision_id: int,
    signal_context: str,
    signal_overlap_count: int = 0,
) -> None:
    tracker.conn.execute(
        """UPDATE decisions
           SET signal_context = ?, signal_overlap_count = ?
           WHERE id = ?""",
        (signal_context, signal_overlap_count, decision_id),
    )
    tracker.conn.commit()


def suppress_decision(
    tracker: Any,
    decision_id: int,
    reason: str,
    *,
    resignal_cooldown_s: float | None = None,
    min_price_improvement: float | None = None,
    last_signal_age_s: float | None = None,
) -> None:
    assignments = ["action = 'SUPPRESSED'", "suppressed_reason = ?"]
    values: list[object] = [reason]
    if resignal_cooldown_s is not None:
        assignments.append("resignal_cooldown_s = ?")
        values.append(resignal_cooldown_s)
    if min_price_improvement is not None:
        assignments.append("min_price_improvement = ?")
        values.append(min_price_improvement)
    if last_signal_age_s is not None:
        assignments.append("last_signal_age_s = ?")
        values.append(last_signal_age_s)
    values.append(decision_id)
    tracker.conn.execute(
        f"""UPDATE decisions
           SET {', '.join(assignments)}
           WHERE id = ?""",
        tuple(values),
    )
    tracker.conn.commit()
