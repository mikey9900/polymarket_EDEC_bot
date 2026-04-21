"""Compact recent-trade CSV export helpers."""

from __future__ import annotations

import csv
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


HEADERS = [
    "id", "d", "t", "c", "st", "sd",
    "rid", "av", "sv", "ch", "md", "dr", "os", "cap", "wid", "ctx", "ov",
    "ep", "tp", "eb", "ea", "es", "sh", "srq", "sfl", "b5", "cs", "fee",
    "te", "ts", "v30", "v60", "du", "dd", "eds", "ods", "drt", "ma",
    "sg", "sgv", "sge", "sgd", "sgs", "sgt", "sgb", "td", "hsd",
    "fp", "ff", "why", "rcid", "rcn", "rcw", "rcp", "rpa", "rrg", "rliq", "rcrd", "rsa", "sgf", "sgc",
    "er", "xp", "xb", "xa", "xs", "tx", "hd",
    "pnl", "pp", "maxb", "minb", "ttmax", "ttmin", "tfp", "sc", "hc",
    "mfe", "mae", "pnp", "tnp", "sx", "rw", "rm",
    "status",
]


def fetch_recent_trade_rows(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        f"""
        SELECT
            pt.id,
            pt.timestamp,
            pt.coin,
            pt.strategy_type,
            pt.side,
            pt.run_id,
            pt.app_version,
            pt.strategy_version,
            pt.config_hash,
            pt.mode,
            pt.dry_run,
            pt.order_size_usd,
            pt.paper_capital_total,
            pt.window_id,
            pt.signal_context,
            pt.signal_overlap_count,
            pt.entry_price,
            pt.target_price,
            pt.entry_bid,
            pt.entry_ask,
            pt.entry_spread,
            pt.shares,
            pt.shares_requested,
            pt.shares_filled,
            pt.blocked_min_5_shares,
            pt.cost,
            pt.fee_total,
            pt.market_start_time,
            pt.exit_reason,
            pt.exit_price,
            pt.bid_at_exit,
            pt.ask_at_exit,
            pt.exit_spread,
            pt.pnl,
            pt.exit_timestamp,
            pt.time_remaining_s,
            pt.max_bid_seen,
            pt.min_bid_seen,
            pt.time_to_max_bid_s,
            pt.time_to_min_bid_s,
            pt.first_profit_time_s,
            pt.scalp_hit,
            pt.high_confidence_hit,
            pt.status,
            pt.entry_depth_side_usd,
            pt.opposite_depth_usd,
            pt.depth_ratio,
            pt.signal_score,
            pt.score_velocity,
            pt.score_entry,
            pt.score_depth,
            pt.score_spread,
            pt.score_time,
            pt.score_balance,
            pt.target_delta,
            pt.hard_stop_delta,
            d.filter_passed,
            d.filter_failed,
            d.research_cluster_id,
            d.research_cluster_n,
            d.research_cluster_win_pct,
            d.research_cluster_avg_pnl,
            d.research_policy_action,
            d.research_market_regime_1d,
            d.research_liquidity_score_1d,
            d.research_crowding_score_1d,
            d.research_signal_score_adjustment,
            d.score_research_flow,
            d.score_research_crowding,
            d.coin_velocity_30s,
            d.coin_velocity_60s,
            d.up_depth_usd,
            d.down_depth_usd,
            d.time_remaining_s AS entry_remaining,
            d.reason AS decision_reason,
            pt.mfe,
            pt.mae,
            pt.peak_net_pnl,
            pt.trough_net_pnl,
            pt.stall_exit_triggered
            ,
            pt.resolution_winner,
            pt.resolution_side_match
        FROM paper_trades pt
        LEFT JOIN decisions d ON d.id = pt.decision_id
        ORDER BY pt.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def export_recent_to_csv(db_path: str = "data/decisions.db", output_dir: str = "data", limit: int = 50) -> str:
    """Generate a compact CSV export with the last N trades for AI analysis."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = fetch_recent_trade_rows(conn, limit)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts = _utc_now().strftime("%Y%m%d_%H%M%S")
    filepath = str(Path(output_dir) / f"edec_recent{limit}_{ts}.csv")
    latest = str(Path(output_dir) / f"edec_recent{limit}_latest.csv")

    with open(filepath, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADERS)
        for row in rows:
            writer.writerow(format_recent_trade_row(row))

    with open(latest, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADERS)
        for row in rows:
            writer.writerow(format_recent_trade_row(row))

    conn.close()
    logger.info(f"Recent {limit} trades CSV export saved: {filepath} (latest={latest})")
    return filepath


def format_recent_trade_row(row):
    def _as_float(value):
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    ts_str = str(row["timestamp"] or "")
    date_part = ts_str[:10] if len(ts_str) >= 10 else ts_str
    time_part = ts_str[11:19] if len(ts_str) >= 19 else ""

    hold_s = None
    if row["exit_timestamp"] and row["timestamp"]:
        try:
            t_in = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            t_out = datetime.fromisoformat(str(row["exit_timestamp"]).replace("Z", "+00:00"))
            hold_s = round((t_out - t_in).total_seconds(), 1)
        except Exception:
            pass

    pnl_pct = None
    if row["cost"] and row["cost"] > 0 and row["pnl"] is not None:
        pnl_pct = round(row["pnl"] / row["cost"] * 100, 2)

    side = str(row["side"] or "").lower()
    vel30 = _as_float(row["coin_velocity_30s"])
    vel60 = _as_float(row["coin_velocity_60s"])
    up_depth = _as_float(row["up_depth_usd"])
    down_depth = _as_float(row["down_depth_usd"])
    if vel30 is None or side not in ("up", "down"):
        momentum_align = None
    else:
        aligned = (side == "up" and vel30 > 0) or (side == "down" and vel30 < 0)
        momentum_align = "aligned" if aligned else "counter"

    return [
        row["id"], date_part, time_part, row["coin"], row["strategy_type"], row["side"],
        row["run_id"], row["app_version"], row["strategy_version"], row["config_hash"], row["mode"],
        int(bool(row["dry_run"])) if row["dry_run"] is not None else None,
        row["order_size_usd"], row["paper_capital_total"], row["window_id"], row["signal_context"], row["signal_overlap_count"],
        row["entry_price"], row["target_price"], row["entry_bid"], row["entry_ask"], row["entry_spread"], row["shares"],
        row["shares_requested"], row["shares_filled"], row["blocked_min_5_shares"], row["cost"], row["fee_total"],
        row["entry_remaining"], row["market_start_time"],
        round(vel30, 4) if vel30 is not None else None,
        round(vel60, 4) if vel60 is not None else None,
        round(up_depth, 2) if up_depth is not None else None,
        round(down_depth, 2) if down_depth is not None else None,
        row["entry_depth_side_usd"], row["opposite_depth_usd"], row["depth_ratio"], momentum_align,
        row["signal_score"], row["score_velocity"], row["score_entry"], row["score_depth"], row["score_spread"],
        row["score_time"], row["score_balance"], row["target_delta"], row["hard_stop_delta"],
        row["filter_passed"], row["filter_failed"], row["decision_reason"],
        row["research_cluster_id"], row["research_cluster_n"], row["research_cluster_win_pct"],
        row["research_cluster_avg_pnl"], row["research_policy_action"], row["research_market_regime_1d"],
        row["research_liquidity_score_1d"], row["research_crowding_score_1d"],
        row["research_signal_score_adjustment"], row["score_research_flow"], row["score_research_crowding"],
        row["exit_reason"], row["exit_price"], row["bid_at_exit"], row["ask_at_exit"], row["exit_spread"], row["time_remaining_s"], hold_s,
        row["pnl"], pnl_pct, row["max_bid_seen"], row["min_bid_seen"], row["time_to_max_bid_s"], row["time_to_min_bid_s"], row["first_profit_time_s"],
        int(bool(row["scalp_hit"])) if row["scalp_hit"] is not None else None,
        int(bool(row["high_confidence_hit"])) if row["high_confidence_hit"] is not None else None,
        row["mfe"], row["mae"], row["peak_net_pnl"], row["trough_net_pnl"],
        int(bool(row["stall_exit_triggered"])) if row["stall_exit_triggered"] is not None else None,
        row["resolution_winner"], row["resolution_side_match"], row["status"],
    ]
