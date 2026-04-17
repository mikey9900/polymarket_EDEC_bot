"""Decision tracker — logs ALL strategy evaluations and outcomes to SQLite."""

import sqlite3
from pathlib import Path

from bot.models import Decision, TradeResult
from bot.tracker_support import operations as tracker_ops
from bot.tracker_support import paper as tracker_paper
from bot.tracker_support import reports as tracker_reports
from bot.tracker_support import schema as tracker_schema




class DecisionTracker:
    def __init__(self, db_path: str = "data/decisions.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._runtime_context: dict[str, object] = {}
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(tracker_schema.SCHEMA)
        self._migrate()
        self._ensure_indexes()
        self.conn.commit()

    def _migrate(self):
        """Add any columns/tables that didn't exist in older DB versions."""
        tracker_schema.migrate(self.conn)

    def _ensure_indexes(self) -> None:
        """Create indexes only after migrations have added any newly indexed columns."""
        tracker_schema.ensure_indexes(self.conn)

    def set_runtime_context(self, context: dict[str, object]) -> None:
        tracker_ops.set_runtime_context(self, context)

    def get_runtime_context(self) -> dict[str, object]:
        return dict(self._runtime_context)

    def latest_run_metadata(self) -> dict | None:
        return tracker_ops.latest_run_metadata(self)

    def _runtime_value(self, key: str, default=None):
        return self._runtime_context.get(key, default)

    def log_decision(self, decision: Decision) -> int:
        return tracker_ops.log_decision(self, decision)

    def log_trade(self, decision_id: int, result: TradeResult) -> int:
        return tracker_ops.log_trade(self, decision_id, result)

    def log_outcome(self, market_slug: str, winner: str,
                    btc_open: float, btc_close: float):
        tracker_ops.log_outcome(self, market_slug, winner, btc_open, btc_close)

    def get_daily_stats(self, date: str | None = None) -> dict:
        return tracker_reports.get_daily_stats(self, date)

    def get_recent_trades(self, limit: int = 10) -> list[dict]:
        return tracker_reports.get_recent_trades(self, limit)

    def get_filter_stats(self) -> list[dict]:
        return tracker_reports.get_filter_stats(self)

    def get_recent_paper_trades(self, limit: int = 3) -> list[dict]:
        return tracker_reports.get_recent_paper_trades(self, limit)

    def set_paper_capital(self, amount: float):
        tracker_paper.set_paper_capital(self, amount)

    def get_paper_capital(self) -> tuple[float, float]:
        return tracker_paper.get_paper_capital(self)

    def has_paper_capital(self, cost: float) -> bool:
        return tracker_paper.has_paper_capital(self, cost)

    def log_paper_trade(self, market_slug: str, coin: str, strategy_type: str,
                        side: str, entry_price: float, target_price: float,
                        shares: float, fee_total: float,
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
                        hard_stop_delta: float | None = None) -> int:
        return tracker_paper.log_paper_trade(
            self, market_slug, coin, strategy_type, side, entry_price, target_price, shares, fee_total,
            decision_id, market_end_time, market_start_time, signal_context, signal_overlap_count,
            order_size_usd, shares_requested, shares_filled, blocked_min_5_shares, entry_bid, entry_ask,
            entry_spread, entry_depth_side_usd, opposite_depth_usd, depth_ratio, window_id, signal_score,
            score_velocity, score_entry, score_depth, score_spread, score_time, score_balance,
            target_delta, hard_stop_delta,
        )

    def update_decision_signal_context(
        self,
        decision_id: int,
        signal_context: str,
        signal_overlap_count: int = 0,
    ) -> None:
        tracker_ops.update_decision_signal_context(self, decision_id, signal_context, signal_overlap_count)

    def suppress_decision(
        self,
        decision_id: int,
        reason: str,
        *,
        resignal_cooldown_s: float | None = None,
        min_price_improvement: float | None = None,
        last_signal_age_s: float | None = None,
    ) -> None:
        tracker_ops.suppress_decision(
            self,
            decision_id,
            reason,
            resignal_cooldown_s=resignal_cooldown_s,
            min_price_improvement=min_price_improvement,
            last_signal_age_s=last_signal_age_s,
        )

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
        tracker_paper.record_paper_trade_path(
            self,
            trade_id,
            max_bid_seen=max_bid_seen,
            min_bid_seen=min_bid_seen,
            time_to_max_bid_s=time_to_max_bid_s,
            time_to_min_bid_s=time_to_min_bid_s,
            first_profit_time_s=first_profit_time_s,
            scalp_hit=scalp_hit,
            high_confidence_hit=high_confidence_hit,
            mfe=mfe,
            mae=mae,
            peak_net_pnl=peak_net_pnl,
            trough_net_pnl=trough_net_pnl,
        )

    def close_paper_trades(self, market_slug: str, winner: str,
                           exit_bid: float | None = None,
                           exit_ask: float | None = None):
        tracker_paper.close_paper_trades(self, market_slug, winner, exit_bid, exit_ask)

    def close_paper_trade_early(self, trade_id: int, exit_price: float,
                                pnl: float, status: str,
                                exit_reason: str = "manual",
                                time_remaining_s: float | None = None,
                                bid_at_exit: float | None = None,
                                ask_at_exit: float | None = None,
                                stall_exit_triggered: bool | None = None):
        tracker_paper.close_paper_trade_early(
            self,
            trade_id,
            exit_price,
            pnl,
            status,
            exit_reason,
            time_remaining_s,
            bid_at_exit,
            ask_at_exit,
            stall_exit_triggered,
        )

    def reset_paper_stats(self):
        tracker_paper.reset_paper_stats(self)

    def get_paper_stats(self) -> dict:
        return tracker_paper.get_paper_stats(self)

    def get_coin_recent_outcomes(self, coin: str, limit: int = 4) -> list[str]:
        return tracker_reports.get_coin_recent_outcomes(self, coin, limit)

    def get_coin_recent_outcome_details(self, coin: str, limit: int = 6) -> list[dict]:
        return tracker_reports.get_coin_recent_outcome_details(self, coin, limit)

    def close(self):
        self.conn.close()

