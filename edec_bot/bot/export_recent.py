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
    "fp", "ff", "why",
    "er", "xp", "xb", "xa", "xs", "tx", "hd",
    "pnl", "pp", "maxb", "minb", "ttmax", "ttmin", "tfp", "sc", "hc",
    "mfe", "mae", "pnp", "tnp", "sx",
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

    ts_str = str(row[1] or "")
    date_part = ts_str[:10] if len(ts_str) >= 10 else ts_str
    time_part = ts_str[11:19] if len(ts_str) >= 19 else ""

    hold_s = None
    if row[34] and row[1]:
        try:
            t_in = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            t_out = datetime.fromisoformat(str(row[34]).replace("Z", "+00:00"))
            hold_s = round((t_out - t_in).total_seconds(), 1)
        except Exception:
            pass

    pnl_pct = None
    if row[25] and row[25] > 0 and row[33] is not None:
        pnl_pct = round(row[33] / row[25] * 100, 2)

    side = str(row[4] or "").lower()
    vel30 = _as_float(row[58])
    vel60 = _as_float(row[59])
    up_depth = _as_float(row[60])
    down_depth = _as_float(row[61])
    if vel30 is None or side not in ("up", "down"):
        momentum_align = None
    else:
        aligned = (side == "up" and vel30 > 0) or (side == "down" and vel30 < 0)
        momentum_align = "aligned" if aligned else "counter"

    return [
        row[0], date_part, time_part, row[2], row[3], row[4],
        row[5], row[6], row[7], row[8], row[9], int(bool(row[10])) if row[10] is not None else None,
        row[11], row[12], row[13], row[14], row[15],
        row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26],
        row[62], row[27],
        round(vel30, 4) if vel30 is not None else None,
        round(vel60, 4) if vel60 is not None else None,
        round(up_depth, 2) if up_depth is not None else None,
        round(down_depth, 2) if down_depth is not None else None,
        row[44], row[45], row[46], momentum_align,
        row[47], row[48], row[49], row[50], row[51], row[52], row[53], row[54], row[55],
        row[56], row[57], row[63],
        row[28], row[29], row[30], row[31], row[32], row[33], hold_s,
        row[33], pnl_pct, row[36], row[37], row[38], row[39], row[40],
        int(bool(row[41])) if row[41] is not None else None,
        int(bool(row[42])) if row[42] is not None else None,
        row[64], row[65], row[66], row[67], int(bool(row[68])) if row[68] is not None else None,
        row[43],
    ]
