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
    run_id TEXT,
    app_version TEXT,
    strategy_version TEXT,
    config_path TEXT,
    config_hash TEXT,
    mode TEXT,
    dry_run INTEGER DEFAULT 1,
    order_size_usd REAL,
    paper_capital_total REAL,
    signal_context TEXT,
    signal_overlap_count INTEGER DEFAULT 0,
    suppressed_reason TEXT,
    market_slug TEXT NOT NULL,
    window_id TEXT,
    coin TEXT NOT NULL DEFAULT 'btc',
    strategy_type TEXT NOT NULL DEFAULT 'dual_leg',
    market_end_time TEXT NOT NULL,
    market_start_time TEXT,
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
    reason TEXT,
    entry_price REAL,
    target_price REAL,
    expected_profit_per_share REAL,
    entry_bid REAL,
    entry_ask REAL,
    entry_spread REAL,
    entry_depth_side_usd REAL,
    opposite_depth_usd REAL,
    depth_ratio REAL,
    signal_score REAL,
    score_velocity REAL,
    score_entry REAL,
    score_depth REAL,
    score_spread REAL,
    score_time REAL,
    score_balance REAL,
    resignal_cooldown_s REAL,
    min_price_improvement REAL,
    last_signal_age_s REAL
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id INTEGER REFERENCES decisions(id),
    timestamp TEXT NOT NULL,
    run_id TEXT,
    app_version TEXT,
    strategy_version TEXT,
    config_path TEXT,
    config_hash TEXT,
    mode TEXT,
    dry_run INTEGER DEFAULT 1,
    order_size_usd REAL,
    paper_capital_total REAL,
    market_slug TEXT NOT NULL,
    window_id TEXT,
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
    shares_requested REAL,
    shares_filled REAL,
    blocked_min_5_shares INTEGER DEFAULT 0,
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
    run_id TEXT,
    app_version TEXT,
    strategy_version TEXT,
    config_path TEXT,
    config_hash TEXT,
    mode TEXT,
    dry_run INTEGER DEFAULT 1,
    order_size_usd REAL,
    paper_capital_total REAL,
    market_slug TEXT NOT NULL,
    window_id TEXT,
    coin TEXT NOT NULL,
    strategy_type TEXT NOT NULL,
    signal_context TEXT,
    signal_overlap_count INTEGER DEFAULT 0,
    side TEXT,
    entry_price REAL NOT NULL,
    target_price REAL,
    shares REAL NOT NULL,
    shares_requested REAL,
    shares_filled REAL,
    blocked_min_5_shares INTEGER DEFAULT 0,
    cost REAL NOT NULL,
    fee_total REAL DEFAULT 0,
    status TEXT DEFAULT 'open',
    exit_price REAL,
    pnl REAL,
    exit_reason TEXT,
    exit_timestamp TEXT,
    time_remaining_s REAL,
    bid_at_exit REAL,
    ask_at_exit REAL,
    exit_spread REAL,
    market_end_time TEXT
    ,
    market_start_time TEXT,
    entry_bid REAL,
    entry_ask REAL,
    entry_spread REAL,
    entry_depth_side_usd REAL,
    opposite_depth_usd REAL,
    depth_ratio REAL,
    max_bid_seen REAL,
    min_bid_seen REAL,
    time_to_max_bid_s REAL,
    time_to_min_bid_s REAL,
    first_profit_time_s REAL,
    scalp_hit INTEGER DEFAULT 0,
    high_confidence_hit INTEGER DEFAULT 0,
    signal_score REAL,
    score_velocity REAL,
    score_entry REAL,
    score_depth REAL,
    score_spread REAL,
    score_time REAL,
    score_balance REAL,
    target_delta REAL,
    hard_stop_delta REAL,
    mfe REAL,
    mae REAL,
    peak_net_pnl REAL,
    trough_net_pnl REAL,
    stall_exit_triggered INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS paper_capital (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_capital REAL NOT NULL,
    current_balance REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    app_version TEXT,
    strategy_version TEXT,
    config_path TEXT,
    config_hash TEXT,
    dry_run INTEGER DEFAULT 1,
    initial_mode TEXT,
    default_order_size_usd REAL,
    initial_paper_capital REAL
);
"""

INDEX_SCHEMA = """
CREATE INDEX IF NOT EXISTS idx_decisions_market ON decisions(market_slug);
CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp);
CREATE INDEX IF NOT EXISTS idx_decisions_run ON decisions(run_id);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_outcomes_market ON outcomes(market_slug);
CREATE INDEX IF NOT EXISTS idx_paper_market ON paper_trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_paper_run ON paper_trades(run_id);
"""


class DecisionTracker:
    def __init__(self, db_path: str = "data/decisions.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._runtime_context: dict[str, object] = {}
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self._ensure_indexes()
        self.conn.commit()

    def _migrate(self):
        """Add any columns/tables that didn't exist in older DB versions."""
        # Ensure paper tables exist (added in v1.0.8)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                run_id TEXT,
                app_version TEXT,
                strategy_version TEXT,
                config_path TEXT,
                config_hash TEXT,
                mode TEXT,
                dry_run INTEGER DEFAULT 1,
                order_size_usd REAL,
                paper_capital_total REAL,
                market_slug TEXT NOT NULL,
                window_id TEXT,
                coin TEXT NOT NULL,
                strategy_type TEXT NOT NULL,
                signal_context TEXT,
                signal_overlap_count INTEGER DEFAULT 0,
                side TEXT,
                entry_price REAL NOT NULL,
                target_price REAL,
                shares REAL NOT NULL,
                shares_requested REAL,
                shares_filled REAL,
                blocked_min_5_shares INTEGER DEFAULT 0,
                cost REAL NOT NULL,
                fee_total REAL DEFAULT 0,
                status TEXT DEFAULT 'open',
                exit_price REAL,
                pnl REAL,
                exit_reason TEXT,
                exit_timestamp TEXT,
                time_remaining_s REAL,
                bid_at_exit REAL,
                ask_at_exit REAL,
                exit_spread REAL,
                market_end_time TEXT,
                market_start_time TEXT,
                entry_bid REAL,
                entry_ask REAL,
                entry_spread REAL,
                entry_depth_side_usd REAL,
                opposite_depth_usd REAL,
                depth_ratio REAL,
                max_bid_seen REAL,
                min_bid_seen REAL,
                time_to_max_bid_s REAL,
                time_to_min_bid_s REAL,
                first_profit_time_s REAL,
                scalp_hit INTEGER DEFAULT 0,
                high_confidence_hit INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS paper_capital (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_capital REAL NOT NULL,
                current_balance REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                app_version TEXT,
                strategy_version TEXT,
                config_path TEXT,
                config_hash TEXT,
                dry_run INTEGER DEFAULT 1,
                initial_mode TEXT,
                default_order_size_usd REAL,
                initial_paper_capital REAL
            );
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
        decision_new_cols = {
            "run_id": "TEXT",
            "app_version": "TEXT",
            "strategy_version": "TEXT",
            "config_path": "TEXT",
            "config_hash": "TEXT",
            "mode": "TEXT",
            "dry_run": "INTEGER DEFAULT 1",
            "order_size_usd": "REAL",
            "paper_capital_total": "REAL",
            "signal_context": "TEXT",
            "signal_overlap_count": "INTEGER DEFAULT 0",
            "suppressed_reason": "TEXT",
            "window_id": "TEXT",
            "market_start_time": "TEXT",
            "entry_price": "REAL",
            "target_price": "REAL",
            "expected_profit_per_share": "REAL",
            "entry_bid": "REAL",
            "entry_ask": "REAL",
            "entry_spread": "REAL",
            "entry_depth_side_usd": "REAL",
            "opposite_depth_usd": "REAL",
            "depth_ratio": "REAL",
            "signal_score": "REAL",
            "score_velocity": "REAL",
            "score_entry": "REAL",
            "score_depth": "REAL",
            "score_spread": "REAL",
            "score_time": "REAL",
            "score_balance": "REAL",
            "resignal_cooldown_s": "REAL",
            "min_price_improvement": "REAL",
            "last_signal_age_s": "REAL",
        }
        for col, col_type in decision_new_cols.items():
            if col not in existing:
                self.conn.execute(f"ALTER TABLE decisions ADD COLUMN {col} {col_type}")
        trade_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(trades)")}
        trade_new_cols = {
            "run_id": "TEXT",
            "app_version": "TEXT",
            "strategy_version": "TEXT",
            "config_path": "TEXT",
            "config_hash": "TEXT",
            "mode": "TEXT",
            "dry_run": "INTEGER DEFAULT 1",
            "order_size_usd": "REAL",
            "paper_capital_total": "REAL",
            "window_id": "TEXT",
            "shares_requested": "REAL",
            "shares_filled": "REAL",
            "blocked_min_5_shares": "INTEGER DEFAULT 0",
        }
        for col, col_type in trade_new_cols.items():
            if col not in trade_cols:
                self.conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type}")
        # Add reset_at to paper_capital if missing (added in v1.2.7)
        cap_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(paper_capital)")}
        if "reset_at" not in cap_cols:
            self.conn.execute("ALTER TABLE paper_capital ADD COLUMN reset_at TEXT")
        # Add new paper_trades columns (added in v1.3.3)
        pt_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(paper_trades)")}
        new_pt_cols = {
            "run_id": "TEXT",
            "app_version": "TEXT",
            "strategy_version": "TEXT",
            "config_path": "TEXT",
            "config_hash": "TEXT",
            "mode": "TEXT",
            "dry_run": "INTEGER DEFAULT 1",
            "order_size_usd": "REAL",
            "paper_capital_total": "REAL",
            "window_id": "TEXT",
            "signal_context": "TEXT",
            "signal_overlap_count": "INTEGER DEFAULT 0",
            "exit_reason": "TEXT",
            "exit_timestamp": "TEXT",
            "time_remaining_s": "REAL",
            "bid_at_exit": "REAL",
            "market_end_time": "TEXT",
            "market_start_time": "TEXT",
            "entry_bid": "REAL",
            "entry_ask": "REAL",
            "entry_spread": "REAL",
            "ask_at_exit": "REAL",
            "exit_spread": "REAL",
            "entry_depth_side_usd": "REAL",
            "opposite_depth_usd": "REAL",
            "depth_ratio": "REAL",
            "shares_requested": "REAL",
            "shares_filled": "REAL",
            "blocked_min_5_shares": "INTEGER DEFAULT 0",
            "max_bid_seen": "REAL",
            "min_bid_seen": "REAL",
            "time_to_max_bid_s": "REAL",
            "time_to_min_bid_s": "REAL",
            "first_profit_time_s": "REAL",
            "scalp_hit": "INTEGER DEFAULT 0",
            "high_confidence_hit": "INTEGER DEFAULT 0",
            "signal_score": "REAL",
            "score_velocity": "REAL",
            "score_entry": "REAL",
            "score_depth": "REAL",
            "score_spread": "REAL",
            "score_time": "REAL",
            "score_balance": "REAL",
            "target_delta": "REAL",
            "hard_stop_delta": "REAL",
            "mfe": "REAL",
            "mae": "REAL",
            "peak_net_pnl": "REAL",
            "trough_net_pnl": "REAL",
            "stall_exit_triggered": "INTEGER DEFAULT 0",
        }
        for col, col_type in new_pt_cols.items():
            if col not in pt_cols:
                self.conn.execute(f"ALTER TABLE paper_trades ADD COLUMN {col} {col_type}")
        self.conn.commit()

    def _ensure_indexes(self) -> None:
        """Create indexes only after migrations have added any newly indexed columns."""
        self.conn.executescript(INDEX_SCHEMA)

    def set_runtime_context(self, context: dict[str, object]) -> None:
        """Store current bot/runtime metadata for subsequent decision/trade logs."""
        self._runtime_context = dict(context)
        run_id = str(context.get("run_id") or "")
        if not run_id:
            return
        self.conn.execute(
            """INSERT OR REPLACE INTO runs (
                run_id, started_at, app_version, strategy_version, config_path, config_hash,
                dry_run, initial_mode, default_order_size_usd, initial_paper_capital
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                context.get("started_at") or datetime.utcnow().isoformat(),
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
        self.conn.commit()

    def get_runtime_context(self) -> dict[str, object]:
        return dict(self._runtime_context)

    def latest_run_metadata(self) -> dict | None:
        row = self.conn.execute(
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

    def _runtime_value(self, key: str, default=None):
        return self._runtime_context.get(key, default)

    def log_decision(self, decision: Decision) -> int:
        """Log a strategy evaluation. Returns the decision ID."""
        passed = [f.name for f in decision.filter_results if f.passed]
        failed = [f.name for f in decision.filter_results if not f.passed]

        cursor = self.conn.execute(
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
                decision.run_id or self._runtime_value("run_id"),
                decision.app_version or self._runtime_value("app_version"),
                decision.strategy_version or self._runtime_value("strategy_version"),
                decision.config_path or self._runtime_value("config_path"),
                decision.config_hash or self._runtime_value("config_hash"),
                decision.mode or self._runtime_value("mode"),
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
        self.conn.commit()
        return cursor.lastrowid

    def log_trade(self, decision_id: int, result: TradeResult) -> int:
        """Log an executed trade."""
        market = result.signal.market
        paper_total, _ = self.get_paper_capital()
        cursor = self.conn.execute(
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
                datetime.utcnow().isoformat(),
                self._runtime_value("run_id"),
                self._runtime_value("app_version"),
                self._runtime_value("strategy_version"),
                self._runtime_value("config_path"),
                self._runtime_value("config_hash"),
                self._runtime_value("mode"),
                int(bool(self._runtime_value("dry_run", True))),
                self._runtime_value("order_size_usd"),
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
            # Polymarket fee per share: fee_rate * price * (1 - price)
            # We use 0.072 as default fee rate
            fee_rate = 0.072
            fee_up = fee_rate * up_ask * (1.0 - up_ask)
            fee_down = fee_rate * down_ask * (1.0 - down_ask)
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
                        hard_stop_delta: float | None = None) -> int:
        """Open a paper trade and deduct cost from balance."""
        cost = entry_price * shares
        paper_total, _ = self.get_paper_capital()
        cursor = self.conn.execute(
            """INSERT INTO paper_trades
               (timestamp, run_id, app_version, strategy_version, config_path, config_hash,
                mode, dry_run, order_size_usd, paper_capital_total,
                market_slug, window_id, coin, strategy_type, signal_context, signal_overlap_count,
                side, entry_price, target_price, shares, shares_requested, shares_filled,
                blocked_min_5_shares, cost, fee_total, status, market_end_time, market_start_time,
                entry_bid, entry_ask, entry_spread, entry_depth_side_usd, opposite_depth_usd, depth_ratio,
                signal_score, score_velocity, score_entry, score_depth, score_spread, score_time, score_balance,
                target_delta, hard_stop_delta)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                self._runtime_value("run_id"),
                self._runtime_value("app_version"),
                self._runtime_value("strategy_version"),
                self._runtime_value("config_path"),
                self._runtime_value("config_hash"),
                self._runtime_value("mode"),
                int(bool(self._runtime_value("dry_run", True))),
                order_size_usd if order_size_usd is not None else self._runtime_value("order_size_usd"),
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
            ),
        )
        # Deduct from paper balance
        self.conn.execute(
            "UPDATE paper_capital SET current_balance = current_balance - ? WHERE id = 1",
            (cost,),
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_decision_signal_context(
        self,
        decision_id: int,
        signal_context: str,
        signal_overlap_count: int = 0,
    ) -> None:
        self.conn.execute(
            """UPDATE decisions
               SET signal_context = ?, signal_overlap_count = ?
               WHERE id = ?""",
            (signal_context, signal_overlap_count, decision_id),
        )
        self.conn.commit()

    def suppress_decision(
        self,
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
        self.conn.execute(
            f"""UPDATE decisions
               SET {', '.join(assignments)}
               WHERE id = ?""",
            tuple(values),
        )
        self.conn.commit()

    def record_paper_trade_path(
        self,
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
        self.conn.execute(
            f"UPDATE paper_trades SET {', '.join(assignments)} WHERE id = ?",
            tuple(values),
        )
        self.conn.commit()

    def close_paper_trades(self, market_slug: str, winner: str,
                           exit_bid: float | None = None,
                           exit_ask: float | None = None):
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
                                bid_at_exit: float | None = None,
                                ask_at_exit: float | None = None,
                                stall_exit_triggered: bool | None = None):
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

    def get_coin_recent_outcome_details(self, coin: str, limit: int = 6) -> list[dict]:
        """Get last N resolution details for a coin (oldest first for display)."""
        rows = self.conn.execute(
            """SELECT market_slug, resolved_at, winner, btc_open_price, btc_close_price
               FROM outcomes
               WHERE market_slug LIKE ?
               ORDER BY id DESC LIMIT ?""",
            (f"{coin}-updown-5m-%", limit),
        ).fetchall()
        return [
            {
                "market_slug": row[0],
                "resolved_at": row[1],
                "winner": (row[2] or "").upper(),
                "open_price": row[3],
                "close_price": row[4],
            }
            for row in reversed(rows)
        ]

    def close(self):
        self.conn.close()


