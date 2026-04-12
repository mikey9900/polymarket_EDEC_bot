"""Decision tracker — logs ALL strategy evaluations and outcomes to SQLite."""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from bot.models import Decision, FilterResult, TradeResult

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    coin TEXT NOT NULL DEFAULT 'btc',
    strategy_type TEXT NOT NULL DEFAULT 'dual_leg',
    market_end_time TEXT NOT NULL,
    up_best_ask REAL,
    down_best_ask REAL,
    combined_cost REAL,
    btc_price REAL,
    coin_velocity_30s REAL,
    coin_velocity_60s REAL,
    up_depth_usd REAL,
    down_depth_usd REAL,
    time_remaining_s REAL,
    feed_count INTEGER,
    filter_passed TEXT,
    filter_failed TEXT,
    action TEXT NOT NULL,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER REFERENCES decisions(id),
    timestamp TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    coin TEXT NOT NULL DEFAULT 'btc',
    strategy_type TEXT NOT NULL DEFAULT 'dual_leg',
    side TEXT,
    up_price REAL,
    down_price REAL,
    entry_price REAL,
    target_price REAL,
    combined_cost REAL,
    fee_total REAL,
    shares REAL,
    up_order_id TEXT,
    down_order_id TEXT,
    buy_order_id TEXT,
    sell_order_id TEXT,
    status TEXT NOT NULL,
    abort_cost REAL DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_slug TEXT NOT NULL UNIQUE,
    resolved_at TEXT NOT NULL,
    winner TEXT NOT NULL,
    btc_open_price REAL,
    btc_close_price REAL
);

CREATE TABLE IF NOT EXISTS decision_outcomes (
    decision_id INTEGER REFERENCES decisions(id),
    outcome_id INTEGER REFERENCES outcomes(id),
    would_have_profited INTEGER,
    hypothetical_profit REAL,
    actual_profit REAL,
    PRIMARY KEY (decision_id, outcome_id)
);

CREATE INDEX IF NOT EXISTS idx_decisions_market ON decisions(market_slug);
CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_outcomes_market ON outcomes(market_slug);
"""


class DecisionTracker:
    def __init__(self, db_path: str = "data/decisions.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def log_decision(self, decision: Decision) -> int:
        """Log a strategy evaluation. Returns the decision ID."""
        passed = [f.name for f in decision.filter_results if f.passed]
        failed = [f.name for f in decision.filter_results if not f.passed]

        cursor = self.conn.execute(
            """INSERT INTO decisions (
                timestamp, market_slug, coin, strategy_type, market_end_time,
                up_best_ask, down_best_ask, combined_cost,
                btc_price, coin_velocity_30s, coin_velocity_60s,
                up_depth_usd, down_depth_usd, time_remaining_s,
                feed_count, filter_passed, filter_failed, action, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                decision.timestamp.isoformat(),
                decision.market_slug,
                decision.coin,
                decision.strategy_type,
                decision.market_end_time.isoformat(),
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
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def log_trade(self, decision_id: int, result: TradeResult) -> int:
        """Log an executed trade."""
        market = result.signal.market
        cursor = self.conn.execute(
            """INSERT INTO trades (
                decision_id, timestamp, market_slug, coin, strategy_type, side,
                up_price, down_price, entry_price, target_price,
                combined_cost, fee_total, shares,
                up_order_id, down_order_id, buy_order_id, sell_order_id,
                status, abort_cost, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                decision_id,
                datetime.utcnow().isoformat(),
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
                result.up_order_id,
                result.down_order_id,
                result.buy_order_id,
                result.sell_order_id,
                result.status,
                result.abort_cost,
                result.error,
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def log_outcome(self, market_slug: str, winner: str,
                    btc_open: float, btc_close: float):
        """Log a market resolution and backfill decision outcomes."""
        # Insert outcome
        cursor = self.conn.execute(
            """INSERT OR IGNORE INTO outcomes (
                market_slug, resolved_at, winner, btc_open_price, btc_close_price
            ) VALUES (?, ?, ?, ?, ?)""",
            (market_slug, datetime.utcnow().isoformat(), winner, btc_open, btc_close),
        )
        self.conn.commit()
        outcome_id = cursor.lastrowid
        if outcome_id == 0:
            # Already existed
            row = self.conn.execute(
                "SELECT id FROM outcomes WHERE market_slug = ?", (market_slug,)
            ).fetchone()
            outcome_id = row[0] if row else 0

        if not outcome_id:
            return

        # Backfill decision outcomes for all decisions in this market
        decisions = self.conn.execute(
            "SELECT id, up_best_ask, down_best_ask, combined_cost, action FROM decisions WHERE market_slug = ?",
            (market_slug,),
        ).fetchall()

        for dec_id, up_ask, down_ask, combined, action in decisions:
            if up_ask is None or down_ask is None:
                continue
            # Calculate hypothetical profit
            # Fee for each side: (1 - price) * fee_rate
            # We use 0.072 as default fee rate
            fee_rate = 0.072
            fee_up = (1.0 - up_ask) * fee_rate
            fee_down = (1.0 - down_ask) * fee_rate
            total_cost = combined + fee_up + fee_down
            hyp_profit = 1.0 - total_cost if total_cost < 1.0 else -(total_cost - 1.0)
            would_profit = hyp_profit > 0

            # Actual profit (from trades table if executed)
            actual = None
            if action == "TRADE":
                trade = self.conn.execute(
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

            self.conn.execute(
                """INSERT OR REPLACE INTO decision_outcomes (
                    decision_id, outcome_id, would_have_profited,
                    hypothetical_profit, actual_profit
                ) VALUES (?, ?, ?, ?, ?)""",
                (dec_id, outcome_id, int(would_profit), hyp_profit, actual),
            )

        self.conn.commit()
        logger.info(
            f"Outcome logged: {market_slug} -> {winner}, "
            f"backfilled {len(decisions)} decisions"
        )

    def get_daily_stats(self, date: str | None = None) -> dict:
        """Get aggregated stats for a day."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")

        trades = self.conn.execute(
            "SELECT status, combined_cost, fee_total, abort_cost FROM trades WHERE timestamp LIKE ?",
            (f"{date}%",),
        ).fetchall()

        decisions = self.conn.execute(
            "SELECT action, COUNT(*) FROM decisions WHERE timestamp LIKE ? GROUP BY action",
            (f"{date}%",),
        ).fetchall()

        total_trades = len(trades)
        successful = sum(1 for t in trades if t[0] == "success")
        aborted = sum(1 for t in trades if t[0] in ("aborted", "partial_abort"))

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

    def get_recent_trades(self, limit: int = 10) -> list[dict]:
        """Get the most recent trades."""
        rows = self.conn.execute(
            """SELECT t.timestamp, t.market_slug, t.coin, t.strategy_type, t.side,
                      t.up_price, t.down_price, t.entry_price, t.target_price,
                      t.combined_cost, t.fee_total, t.shares, t.status, t.abort_cost,
                      do.actual_profit
               FROM trades t
               LEFT JOIN decision_outcomes do ON do.decision_id = t.decision_id
               ORDER BY t.id DESC LIMIT ?""",
            (limit,),
        ).fetchall()

        return [
            {
                "timestamp": r[0],
                "market": r[1],
                "coin": r[2],
                "strategy_type": r[3],
                "side": r[4],
                "up_price": r[5] or 0,
                "down_price": r[6] or 0,
                "entry_price": r[7],
                "target_price": r[8],
                "combined_cost": r[9],
                "fee_total": r[10],
                "shares": r[11],
                "status": r[12],
                "abort_cost": r[13],
                "actual_profit": r[14],
            }
            for r in rows
        ]

    def get_filter_stats(self) -> list[dict]:
        """Get pass/fail rates for each filter."""
        rows = self.conn.execute(
            "SELECT filter_passed, filter_failed FROM decisions"
        ).fetchall()

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

        return [
            {"filter": name, **counts}
            for name, counts in sorted(filter_counts.items())
        ]

    def close(self):
        self.conn.close()
