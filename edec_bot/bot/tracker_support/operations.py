"""Runtime metadata and core write operations for DecisionTracker."""

from __future__ import annotations

import logging
from typing import Any

from bot.models import Decision, TradeResult
from bot.tracker_support import paper as tracker_paper
from bot.tracker_support.schema import utc_now

logger = logging.getLogger(__name__)


def _require_linked_decision(decision_id: int | None, label: str) -> int:
    resolved = int(decision_id or 0)
    if resolved <= 0:
        raise ValueError(f"{label} requires a valid decision_id for export-safe attribution")
    return resolved


def _trade_update(tracker: Any, trade_id: int, updates: dict[str, object]) -> None:
    assignments: list[str] = []
    values: list[object] = []
    bool_columns = {
        "scalp_hit",
        "high_confidence_hit",
        "hold_to_resolution",
        "stall_exit_triggered",
        "ever_profitable",
    }
    for column, value in updates.items():
        if value is None:
            continue
        assignments.append(f"{column} = ?")
        if column in bool_columns:
            values.append(int(bool(value)))
        else:
            values.append(value)
    if not assignments:
        return
    values.append(trade_id)
    tracker.conn.execute(f"UPDATE trades SET {', '.join(assignments)} WHERE id = ?", tuple(values))
    tracker.conn.commit()


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
    columns = [
        "timestamp", "run_id", "app_version", "strategy_version", "config_path", "config_hash",
        "mode", "dry_run", "order_size_usd", "paper_capital_total", "signal_context",
        "signal_overlap_count", "suppressed_reason",
        "market_slug", "window_id", "coin", "strategy_type", "market_end_time", "market_start_time",
        "up_best_ask", "down_best_ask", "combined_cost",
        "btc_price", "coin_velocity_30s", "coin_velocity_60s",
        "up_depth_usd", "down_depth_usd", "time_remaining_s",
        "feed_count", "filter_passed", "filter_failed", "action", "reason",
        "entry_price", "target_price", "expected_profit_per_share",
        "entry_bid", "entry_ask", "entry_spread",
        "entry_depth_side_usd", "opposite_depth_usd", "depth_ratio",
        "signal_score", "score_velocity", "score_entry", "score_depth",
        "score_spread", "score_time", "score_balance", "score_research_flow", "score_research_crowding",
        "resignal_cooldown_s", "min_price_improvement", "last_signal_age_s",
        "research_cluster_id", "research_cluster_n", "research_cluster_win_pct", "research_cluster_avg_pnl",
        "research_policy_action", "research_market_regime_1d", "research_liquidity_score_1d",
        "research_crowding_score_1d", "research_score_flow_1d", "research_score_crowding_1d",
        "research_signal_score_adjustment",
        "source_prices_json", "source_ages_json", "source_dispersion_pct",
        "source_staleness_max_s", "source_staleness_avg_s",
    ]
    values = (
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
        decision.score_research_flow,
        decision.score_research_crowding,
        decision.resignal_cooldown_s,
        decision.min_price_improvement,
        decision.last_signal_age_s,
        decision.research_cluster_id,
        decision.research_cluster_n,
        decision.research_cluster_win_pct,
        decision.research_cluster_avg_pnl,
        decision.research_policy_action,
        decision.research_market_regime_1d,
        decision.research_liquidity_score_1d,
        decision.research_crowding_score_1d,
        decision.research_score_flow_1d,
        decision.research_score_crowding_1d,
        decision.research_signal_score_adjustment,
        decision.source_prices_json,
        decision.source_ages_json,
        decision.source_dispersion_pct,
        decision.source_staleness_max_s,
        decision.source_staleness_avg_s,
    )
    placeholders = ", ".join("?" for _ in columns)
    cursor = tracker.conn.execute(
        f"INSERT INTO decisions ({', '.join(columns)}) VALUES ({placeholders})",
        values,
    )
    tracker.conn.commit()
    return cursor.lastrowid


def log_trade(tracker: Any, decision_id: int, result: TradeResult) -> int:
    """Log an executed trade."""
    resolved_decision_id = _require_linked_decision(decision_id or result.signal.decision_id, "log_trade")
    market = result.signal.market
    paper_total, _ = tracker.get_paper_capital()
    timestamp = utc_now().isoformat()
    entry_fill_ratio = result.entry_fill_ratio
    if entry_fill_ratio <= 0 and result.shares_requested:
        entry_fill_ratio = (result.shares_filled or 0.0) / max(result.shares_requested, 1e-9)
    has_entry_fill = (result.shares_filled or 0.0) > 0
    columns = [
        "decision_id", "timestamp", "run_id", "app_version", "strategy_version", "config_path",
        "config_hash", "mode", "dry_run", "order_size_usd", "paper_capital_total",
        "market_slug", "window_id", "coin", "strategy_type", "side",
        "up_price", "down_price", "entry_price", "target_price",
        "combined_cost", "fee_total", "shares", "shares_requested", "shares_filled", "blocked_min_5_shares",
        "up_order_id", "down_order_id", "buy_order_id", "sell_order_id",
        "status", "abort_cost", "error",
        "entry_order_submitted_at", "entry_filled_at", "entry_time_to_fill_s",
        "entry_limit_price", "entry_fill_price", "entry_slippage", "entry_fill_ratio",
        "exit_order_submitted_at", "exit_filled_at", "exit_limit_price", "exit_fill_price",
        "exit_slippage", "exit_reason", "exit_price", "pnl", "time_remaining_s",
        "bid_at_exit", "ask_at_exit", "exit_spread",
        "max_bid_seen", "min_bid_seen", "time_to_max_bid_s", "time_to_min_bid_s",
        "first_profit_time_s", "scalp_hit", "high_confidence_hit", "hold_to_resolution",
        "mfe", "mae", "peak_net_pnl", "trough_net_pnl", "stall_exit_triggered",
        "dynamic_loss_cut_pct", "loss_pct_at_exit", "favorable_excursion",
        "ever_profitable", "cancel_repost_count",
    ]
    values = (
        resolved_decision_id,
        timestamp,
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
        result.entry_order_submitted_at or timestamp,
        result.entry_filled_at or (timestamp if has_entry_fill else None),
        result.entry_time_to_fill_s if has_entry_fill else None,
        result.entry_limit_price or result.signal.entry_price or result.signal.combined_cost,
        result.entry_fill_price or (result.signal.entry_price if has_entry_fill else None),
        result.entry_slippage if has_entry_fill else None,
        entry_fill_ratio if has_entry_fill else 0.0,
        result.exit_order_submitted_at or None,
        result.exit_filled_at or None,
        result.exit_limit_price or None,
        result.exit_fill_price or None,
        result.exit_slippage,
        result.exit_reason or None,
        result.exit_price or None,
        result.realized_pnl if result.exit_reason else None,
        result.time_remaining_s if result.exit_reason else None,
        result.bid_at_exit if result.exit_reason else None,
        result.ask_at_exit if result.exit_reason else None,
        result.exit_spread if result.exit_reason else None,
        result.max_bid_seen if result.max_bid_seen != 0.0 else None,
        result.min_bid_seen if result.min_bid_seen != 0.0 else None,
        result.time_to_max_bid_s if result.time_to_max_bid_s != 0.0 else None,
        result.time_to_min_bid_s if result.time_to_min_bid_s != 0.0 else None,
        result.first_profit_time_s if result.first_profit_time_s != 0.0 else None,
        int(bool(result.scalp_hit)),
        int(bool(result.high_confidence_hit)),
        int(bool(result.hold_to_resolution)),
        result.mfe if result.mfe != 0.0 else None,
        result.mae if result.mae != 0.0 else None,
        result.peak_net_pnl if result.peak_net_pnl != 0.0 else None,
        result.trough_net_pnl if result.trough_net_pnl != 0.0 else None,
        int(bool(result.stall_exit_triggered)),
        result.dynamic_loss_cut_pct if result.dynamic_loss_cut_pct != 0.0 else None,
        result.loss_pct_at_exit if result.loss_pct_at_exit != 0.0 else None,
        result.favorable_excursion if result.favorable_excursion != 0.0 else None,
        int(bool(result.ever_profitable)),
        result.cancel_repost_count,
    )
    placeholders = ", ".join("?" for _ in columns)
    cursor = tracker.conn.execute(
        f"INSERT INTO trades ({', '.join(columns)}) VALUES ({placeholders})",
        values,
    )
    tracker.conn.commit()
    return cursor.lastrowid


def log_outcome(tracker: Any, market_slug: str, winner: str, btc_open: float, btc_close: float) -> None:
    """Log a market resolution and backfill decision outcomes."""
    normalized_winner = (winner or "").upper()
    resolved_at = utc_now().isoformat()
    cursor = tracker.conn.execute(
        """INSERT OR IGNORE INTO outcomes (
            market_slug, resolved_at, winner, btc_open_price, btc_close_price
        ) VALUES (?, ?, ?, ?, ?)""",
        (market_slug, resolved_at, normalized_winner, btc_open, btc_close),
    )
    tracker.conn.commit()
    outcome_id = cursor.lastrowid
    if outcome_id == 0:
        row = tracker.conn.execute(
            "SELECT id, resolved_at FROM outcomes WHERE market_slug = ?",
            (market_slug,),
        ).fetchone()
        outcome_id = row[0] if row else 0
        if row and row[1]:
            resolved_at = row[1]

    if not outcome_id:
        return

    tracker.conn.execute(
        """UPDATE trades
           SET resolution_winner = ?,
               resolution_side_match = CASE
                   WHEN lower(COALESCE(strategy_type, '')) = 'dual_leg' THEN NULL
                   WHEN lower(COALESCE(side, '')) NOT IN ('up', 'down') THEN NULL
                   WHEN upper(side) = ? THEN 1
                   ELSE 0
               END
           WHERE market_slug = ?""",
        (normalized_winner, normalized_winner, market_slug),
    )
    tracker.conn.execute(
        """UPDATE paper_trades
           SET resolution_winner = ?,
               resolution_side_match = CASE
                   WHEN lower(COALESCE(strategy_type, '')) = 'dual_leg' THEN NULL
                   WHEN lower(COALESCE(side, '')) NOT IN ('up', 'down') THEN NULL
                   WHEN upper(side) = ? THEN 1
                   ELSE 0
               END
           WHERE market_slug = ?""",
        (normalized_winner, normalized_winner, market_slug),
    )
    tracker_paper.apply_resolution_outcome(tracker, market_slug, normalized_winner, resolved_at)

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
                """SELECT combined_cost, fee_total, status, abort_cost, pnl
                   FROM trades
                   WHERE decision_id = ?
                   ORDER BY id DESC
                   LIMIT 1""",
                (dec_id,),
            ).fetchone()
            if trade:
                t_cost, t_fee, t_status, t_abort, t_pnl = trade
                if t_status == "success":
                    actual = 1.0 - (t_cost + t_fee)
                elif t_status in ("closed_win", "closed_loss", "resolved_win", "resolved_loss") and t_pnl is not None:
                    actual = t_pnl
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
    logger.info(f"Outcome logged: {market_slug} -> {normalized_winner}, backfilled {len(decisions)} decisions")


def update_live_trade(
    tracker: Any,
    trade_id: int,
    *,
    sell_order_id: str | None = None,
    status: str | None = None,
    error: str | None = None,
    exit_order_submitted_at: str | None = None,
    exit_limit_price: float | None = None,
    exit_reason: str | None = None,
    dynamic_loss_cut_pct: float | None = None,
    cancel_repost_count: int | None = None,
    hold_to_resolution: bool | None = None,
    stall_exit_triggered: bool | None = None,
) -> None:
    _trade_update(
        tracker,
        trade_id,
        {
            "sell_order_id": sell_order_id,
            "status": status,
            "error": error,
            "exit_order_submitted_at": exit_order_submitted_at,
            "exit_limit_price": exit_limit_price,
            "exit_reason": exit_reason,
            "dynamic_loss_cut_pct": dynamic_loss_cut_pct,
            "cancel_repost_count": cancel_repost_count,
            "hold_to_resolution": hold_to_resolution,
            "stall_exit_triggered": stall_exit_triggered,
        },
    )


def record_live_trade_path(
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
    hold_to_resolution: bool | None = None,
    mfe: float | None = None,
    mae: float | None = None,
    peak_net_pnl: float | None = None,
    trough_net_pnl: float | None = None,
    favorable_excursion: float | None = None,
    ever_profitable: bool | None = None,
    cancel_repost_count: int | None = None,
) -> None:
    _trade_update(
        tracker,
        trade_id,
        {
            "max_bid_seen": max_bid_seen,
            "min_bid_seen": min_bid_seen,
            "time_to_max_bid_s": time_to_max_bid_s,
            "time_to_min_bid_s": time_to_min_bid_s,
            "first_profit_time_s": first_profit_time_s,
            "scalp_hit": scalp_hit,
            "high_confidence_hit": high_confidence_hit,
            "hold_to_resolution": hold_to_resolution,
            "mfe": mfe,
            "mae": mae,
            "peak_net_pnl": peak_net_pnl,
            "trough_net_pnl": trough_net_pnl,
            "favorable_excursion": favorable_excursion,
            "ever_profitable": ever_profitable,
            "cancel_repost_count": cancel_repost_count,
        },
    )


def close_live_trade(
    tracker: Any,
    trade_id: int,
    *,
    status: str,
    exit_reason: str,
    exit_price: float,
    pnl: float,
    time_remaining_s: float | None = None,
    bid_at_exit: float | None = None,
    ask_at_exit: float | None = None,
    exit_limit_price: float | None = None,
    exit_fill_price: float | None = None,
    max_bid_seen: float | None = None,
    min_bid_seen: float | None = None,
    time_to_max_bid_s: float | None = None,
    time_to_min_bid_s: float | None = None,
    first_profit_time_s: float | None = None,
    scalp_hit: bool | None = None,
    high_confidence_hit: bool | None = None,
    hold_to_resolution: bool | None = None,
    mfe: float | None = None,
    mae: float | None = None,
    peak_net_pnl: float | None = None,
    trough_net_pnl: float | None = None,
    stall_exit_triggered: bool | None = None,
    dynamic_loss_cut_pct: float | None = None,
    loss_pct_at_exit: float | None = None,
    favorable_excursion: float | None = None,
    ever_profitable: bool | None = None,
    cancel_repost_count: int | None = None,
) -> None:
    exit_filled_at = utc_now().isoformat()
    exit_spread = None
    if bid_at_exit is not None and ask_at_exit is not None:
        exit_spread = ask_at_exit - bid_at_exit
    _trade_update(
        tracker,
        trade_id,
        {
            "status": status,
            "exit_reason": exit_reason,
            "exit_price": exit_price,
            "pnl": pnl,
            "exit_filled_at": exit_filled_at,
            "time_remaining_s": time_remaining_s,
            "bid_at_exit": bid_at_exit,
            "ask_at_exit": ask_at_exit,
            "exit_spread": exit_spread,
            "exit_limit_price": exit_limit_price,
            "exit_fill_price": exit_fill_price if exit_fill_price is not None else exit_price,
            "max_bid_seen": max_bid_seen,
            "min_bid_seen": min_bid_seen,
            "time_to_max_bid_s": time_to_max_bid_s,
            "time_to_min_bid_s": time_to_min_bid_s,
            "first_profit_time_s": first_profit_time_s,
            "scalp_hit": scalp_hit,
            "high_confidence_hit": high_confidence_hit,
            "hold_to_resolution": hold_to_resolution,
            "mfe": mfe,
            "mae": mae,
            "peak_net_pnl": peak_net_pnl,
            "trough_net_pnl": trough_net_pnl,
            "stall_exit_triggered": stall_exit_triggered,
            "dynamic_loss_cut_pct": dynamic_loss_cut_pct,
            "loss_pct_at_exit": loss_pct_at_exit,
            "favorable_excursion": favorable_excursion,
            "ever_profitable": ever_profitable,
            "cancel_repost_count": cancel_repost_count,
        },
    )


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
