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

CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    coin TEXT NOT NULL,
    strategy_type TEXT NOT NULL,
    side TEXT,
    entry_price REAL NOT NULL,
    target_price REAL,
    shares REAL NOT NULL,
    cost REAL NOT NULL,
    fee_total REAL DEFAULT 0,
    status TEXT DEFAULT 'open',
    exit_price REAL,
    pnl REAL,
    exit_reason TEXT,
    exit_timestamp TEXT,
    time_remaining_s REAL,
    bid_at_exit REAL,
    market_end_time TEXT
);

CREATE TABLE IF NOT EXISTS paper_capital (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_capital REAL NOT NULL,
    current_balance REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decisions_market ON decisions(market_slug);
CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_outcomes_market ON outcomes(market_slug);
CREATE INDEX IF NOT EXISTS idx_paper_market ON paper_trades(market_slug);
"""


class DecisionTracker:
    def __init__(self, db_path: str = "data/decisions.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self):
        """Add any columns/tables that didn't exist in older DB versions."""
        # Ensure paper tables exist (added in v1.0.8)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                coin TEXT NOT NULL,
                strategy_type TEXT NOT NULL,
                side TEXT,
                entry_price REAL NOT NULL,
                target_price REAL,
                shares REAL NOT NULL,
                cost REAL NOT NULL,
                fee_total REAL DEFAULT 0,
                status TEXT DEFAULT 'open',
                exit_price REAL,
                pnl REAL,
                exit_reason TEXT,
                exit_timestamp TEXT,
                time_remaining_s REAL,
                bid_at_exit REAL,
                market_end_time TEXT
            );
            CREATE TABLE IF NOT EXISTS paper_capital (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_capital REAL NOT NULL,
                current_balance REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_paper_market ON paper_trades(market_slug);
        """)
        # Add coin/strategy_type columns to decisions if missing (added in v1.0.5)
        existing = {row[1] for row in self.conn.execute("PRAGMA table_info(decisions)")}
        if "coin" not in existing:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN coin TEXT NOT NULL DEFAULT 'btc'")
        if "strategy_type" not in existing:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN strategy_type TEXT NOT NULL DEFAULT 'dual_leg'")
        if "coin_velocity_30s" not in existing:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN coin_velocity_30s REAL")
        if "coin_velocity_60s" not in existing:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN coin_velocity_60s REAL")
        # Add reset_at to paper_capital if missing (added in v1.2.7)
        cap_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(paper_capital)")}
        if "reset_at" not in cap_cols:
            self.conn.execute("ALTER TABLE paper_capital ADD COLUMN reset_at TEXT")
        # Add new paper_trades columns (added in v1.3.3)
        pt_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(paper_trades)")}
        new_pt_cols = {
            "exit_reason": "TEXT",
            "exit_timestamp": "TEXT",
            "time_remaining_s": "REAL",
            "bid_at_exit": "REAL",
            "market_end_time": "TEXT",
        }
        for col, col_type in new_pt_cols.items():
            if col not in pt_cols:
                self.conn.execute(f"ALTER TABLE paper_trades ADD COLUMN {col} {col_type}")
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

    # -----------------------------------------------------------------------
    # Paper trading
    # -----------------------------------------------------------------------

    def get_recent_paper_trades(self, limit: int = 3) -> list[dict]:
        rows = self.conn.execute(
            """SELECT timestamp, coin, strategy_type, side, entry_price,
                      target_price, shares, cost, status, pnl
               FROM paper_trades ORDER BY id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [
            {"timestamp": r[0], "coin": r[1], "strategy_type": r[2], "side": r[3],
             "entry_price": r[4], "target_price": r[5], "shares": r[6],
             "cost": r[7], "status": r[8], "pnl": r[9]}
            for r in rows
        ]

    def set_paper_capital(self, amount: float):
        """Set (or reset) the paper trading bankroll."""
        self.conn.execute("DELETE FROM paper_capital WHERE id = 1")
        self.conn.execute(
            "INSERT INTO paper_capital (id, total_capital, current_balance) VALUES (1, ?, ?)",
            (amount, amount),
        )
        self.conn.commit()
        logger.info(f"Paper capital set to ${amount:.2f}")

    def get_paper_capital(self) -> tuple[float, float]:
        """Return (total_capital, current_balance)."""
        row = self.conn.execute(
            "SELECT total_capital, current_balance FROM paper_capital WHERE id = 1"
        ).fetchone()
        return (row[0], row[1]) if row else (0.0, 0.0)

    def has_paper_capital(self, cost: float) -> bool:
        _, balance = self.get_paper_capital()
        return balance >= cost

    def log_paper_trade(self, market_slug: str, coin: str, strategy_type: str,
                        side: str, entry_price: float, target_price: float,
                        shares: float, fee_total: float,
                        market_end_time: str | None = None) -> int:
        """Open a paper trade and deduct cost from balance."""
        cost = entry_price * shares
        cursor = self.conn.execute(
            """INSERT INTO paper_trades
               (timestamp, market_slug, coin, strategy_type, side,
                entry_price, target_price, shares, cost, fee_total, status, market_end_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
            (datetime.utcnow().isoformat(), market_slug, coin, strategy_type,
             side, entry_price, target_price, shares, cost, fee_total, market_end_time),
        )
        # Deduct from paper balance
        self.conn.execute(
            "UPDATE paper_capital SET current_balance = current_balance - ? WHERE id = 1",
            (cost,),
        )
        self.conn.commit()
        return cursor.lastrowid

    def close_paper_trades(self, market_slug: str, winner: str):
        """Resolve all open paper trades for a market and update balance."""
        trades = self.conn.execute(
            """SELECT id, strategy_type, side, entry_price, shares, cost, fee_total
               FROM paper_trades WHERE market_slug = ? AND status = 'open'""",
            (market_slug,),
        ).fetchall()

        for row in trades:
            trade_id, strategy_type, side, entry_price, shares, cost, fee_total = row

            if strategy_type == "dual_leg":
                # Both sides always pay out $1 combined; win if net pnl > 0
                pnl = (1.0 - entry_price) * shares - fee_total
                exit_price = 1.0
                status = "closed_win" if pnl > 0 else "closed_loss"
            else:
                # Single leg — check if our side won
                won = (side == "up" and winner.upper() == "UP") or \
                      (side == "down" and winner.upper() == "DOWN")
                if won:
                    pnl = (1.0 - entry_price) * shares - fee_total
                    exit_price = 1.0
                    status = "closed_win"
                else:
                    pnl = -cost
                    exit_price = 0.0
                    status = "closed_loss"

            now = datetime.utcnow().isoformat()
            self.conn.execute(
                """UPDATE paper_trades
                   SET status=?, exit_price=?, pnl=?,
                       exit_reason='resolution', exit_timestamp=?
                   WHERE id=?""",
                (status, exit_price, pnl, now, trade_id),
            )
            # Return cost + profit/loss to balance
            self.conn.execute(
                "UPDATE paper_capital SET current_balance = current_balance + ? WHERE id = 1",
                (cost + pnl,),
            )

        if trades:
            self.conn.commit()
            logger.info(f"Closed {len(trades)} paper trades for {market_slug} → winner: {winner}")

    def close_paper_trade_early(self, trade_id: int, exit_price: float,
                                pnl: float, status: str,
                                exit_reason: str = "manual",
                                time_remaining_s: float | None = None,
                                bid_at_exit: float | None = None):
        """Close a paper trade early (profit-take or stop-loss) before market resolution."""
        row = self.conn.execute(
            "SELECT cost FROM paper_trades WHERE id = ? AND status = 'open'",
            (trade_id,),
        ).fetchone()
        if not row:
            return  # Already closed or not found
        cost = row[0]
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """UPDATE paper_trades
               SET status=?, exit_price=?, pnl=?,
                   exit_reason=?, exit_timestamp=?, time_remaining_s=?, bid_at_exit=?
               WHERE id=?""",
            (status, exit_price, pnl, exit_reason, now, time_remaining_s, bid_at_exit, trade_id),
        )
        self.conn.execute(
            "UPDATE paper_capital SET current_balance = current_balance + ? WHERE id = 1",
            (cost + pnl,),
        )
        self.conn.commit()
        logger.info(
            f"Paper trade {trade_id} closed early: exit@{exit_price:.3f} pnl=${pnl:+.4f} "
            f"[{status}] reason={exit_reason} t_remaining={time_remaining_s}"
        )

    def reset_paper_stats(self):
        """Reset displayed stats to zero without deleting trade history."""
        now = datetime.utcnow().isoformat()
        # Restore balance to total_capital and record the reset timestamp
        self.conn.execute(
            """UPDATE paper_capital
               SET current_balance = total_capital, reset_at = ?
               WHERE id = 1""",
            (now,),
        )
        self.conn.commit()
        logger.info(f"Paper stats reset at {now}")

    def get_paper_stats(self) -> dict:
        """Return paper trading summary (only trades since last reset)."""
        total, balance = self.get_paper_capital()
        reset_at = self.conn.execute(
            "SELECT reset_at FROM paper_capital WHERE id = 1"
        ).fetchone()
        reset_at = reset_at[0] if reset_at and reset_at[0] else "1970-01-01"

        row = self.conn.execute(
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

    def get_coin_recent_outcomes(self, coin: str, limit: int = 4) -> list[str]:
        """Get last N resolution winners for a coin (oldest first for display)."""
        rows = self.conn.execute(
            """SELECT winner FROM outcomes
               WHERE market_slug LIKE ?
               ORDER BY id DESC LIMIT ?""",
            (f"{coin}-updown-5m-%", limit),
        ).fetchall()
        # Reverse so oldest is on left, newest is on right
        return [r[0].upper() for r in reversed(rows)]

    def close(self):
        self.conn.close()
