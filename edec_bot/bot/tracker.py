"""Decision tracker — logs ALL strategy evaluations and outcomes to SQLite."""

import sqlite3
import threading
from pathlib import Path

from bot.models import Decision, TradeResult
from bot.tracker_support import operations as tracker_ops
from bot.tracker_support import paper as tracker_paper
from bot.tracker_support import reports as tracker_reports
from bot.tracker_support import schema as tracker_schema


class ReadOnlyTrackerProxy:
    """Standalone SQLite reader for off-loop dashboard queries.

    Holds its own connection so heavy dashboard reads run against the WAL
    without blocking the writer connection used by the main asyncio loop.
    SQLite WAL allows concurrent readers + 1 writer with no lock contention.
    Exposes only the read methods the dashboard helpers need.
    """

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA query_only=ON")
        # Serialize access to this connection's cursor across executor workers.
        self._lock = threading.Lock()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def get_paper_capital(self):
        with self._lock:
            return tracker_paper.get_paper_capital(self)

    def get_paper_stats(self):
        with self._lock:
            return tracker_paper.get_paper_stats(self)

    def get_session_stats_by_coin(self):
        with self._lock:
            return tracker_paper.get_session_stats_by_coin(self)

    def get_recent_signals_by_coin(self, max_age_s: float = 30.0):
        with self._lock:
            return tracker_reports.get_recent_signals_by_coin(self, max_age_s)

    def get_coin_recent_outcomes(self, coin: str, limit: int = 4):
        with self._lock:
            return tracker_reports.get_coin_recent_outcomes(self, coin, limit)

    def get_coin_recent_resolutions(self, coin: str, limit: int = 4):
        with self._lock:
            return tracker_reports.get_coin_recent_resolutions(self, coin, limit)




class DecisionTracker:
    def __init__(self, db_path: str = "data/decisions.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        # check_same_thread=False lets background workers (dashboard slow-tier
        # refresh, telegram dashboard build) run heavy reads off the loop.
        # Writers serialize via _io_lock to avoid corrupting cursor state.
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._io_lock = threading.RLock()
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

    def save_runtime_state(self, state: dict[str, object], *, version: int = 1) -> None:
        tracker_ops.save_runtime_state(self, state, version=version)

    def load_runtime_state(self) -> dict[str, object] | None:
        return tracker_ops.load_runtime_state(self)

    def clear_runtime_state(self) -> None:
        tracker_ops.clear_runtime_state(self)

    def get_recoverable_live_trades(self) -> list[dict[str, object]]:
        return tracker_ops.get_recoverable_live_trades(self)

    def get_open_paper_trades(self) -> list[dict[str, object]]:
        return tracker_ops.get_open_paper_trades(self)

    def _runtime_value(self, key: str, default=None):
        return self._runtime_context.get(key, default)

    def log_decision(self, decision: Decision) -> int:
        return tracker_ops.log_decision(self, decision)

    def log_trade(self, decision_id: int, result: TradeResult) -> int:
        return tracker_ops.log_trade(self, decision_id, result)

    def update_live_trade(
        self,
        trade_id: int,
        *,
        buy_order_id: str | None = None,
        sell_order_id: str | None = None,
        status: str | None = None,
        error: str | None = None,
        total_cost: float | None = None,
        fee_total: float | None = None,
        shares: float | None = None,
        shares_requested: float | None = None,
        shares_filled: float | None = None,
        entry_order_submitted_at: str | None = None,
        entry_filled_at: str | None = None,
        entry_time_to_fill_s: float | None = None,
        entry_limit_price: float | None = None,
        entry_fill_price: float | None = None,
        entry_slippage: float | None = None,
        entry_fill_ratio: float | None = None,
        exit_order_submitted_at: str | None = None,
        exit_limit_price: float | None = None,
        exit_reason: str | None = None,
        dynamic_loss_cut_pct: float | None = None,
        cancel_repost_count: int | None = None,
        hold_to_resolution: bool | None = None,
        stall_exit_triggered: bool | None = None,
    ) -> None:
        tracker_ops.update_live_trade(
            self,
            trade_id,
            buy_order_id=buy_order_id,
            sell_order_id=sell_order_id,
            status=status,
            error=error,
            total_cost=total_cost,
            fee_total=fee_total,
            shares=shares,
            shares_requested=shares_requested,
            shares_filled=shares_filled,
            entry_order_submitted_at=entry_order_submitted_at,
            entry_filled_at=entry_filled_at,
            entry_time_to_fill_s=entry_time_to_fill_s,
            entry_limit_price=entry_limit_price,
            entry_fill_price=entry_fill_price,
            entry_slippage=entry_slippage,
            entry_fill_ratio=entry_fill_ratio,
            exit_order_submitted_at=exit_order_submitted_at,
            exit_limit_price=exit_limit_price,
            exit_reason=exit_reason,
            dynamic_loss_cut_pct=dynamic_loss_cut_pct,
            cancel_repost_count=cancel_repost_count,
            hold_to_resolution=hold_to_resolution,
            stall_exit_triggered=stall_exit_triggered,
        )

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
        hold_to_resolution: bool | None = None,
        mfe: float | None = None,
        mae: float | None = None,
        peak_net_pnl: float | None = None,
        trough_net_pnl: float | None = None,
        favorable_excursion: float | None = None,
        ever_profitable: bool | None = None,
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
            hold_to_resolution=hold_to_resolution,
            mfe=mfe,
            mae=mae,
            peak_net_pnl=peak_net_pnl,
            trough_net_pnl=trough_net_pnl,
            favorable_excursion=favorable_excursion,
            ever_profitable=ever_profitable,
        )

    def record_live_trade_path(
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
        hold_to_resolution: bool | None = None,
        mfe: float | None = None,
        mae: float | None = None,
        peak_net_pnl: float | None = None,
        trough_net_pnl: float | None = None,
        favorable_excursion: float | None = None,
        ever_profitable: bool | None = None,
        cancel_repost_count: int | None = None,
    ) -> None:
        tracker_ops.record_live_trade_path(
            self,
            trade_id,
            max_bid_seen=max_bid_seen,
            min_bid_seen=min_bid_seen,
            time_to_max_bid_s=time_to_max_bid_s,
            time_to_min_bid_s=time_to_min_bid_s,
            first_profit_time_s=first_profit_time_s,
            scalp_hit=scalp_hit,
            high_confidence_hit=high_confidence_hit,
            hold_to_resolution=hold_to_resolution,
            mfe=mfe,
            mae=mae,
            peak_net_pnl=peak_net_pnl,
            trough_net_pnl=trough_net_pnl,
            favorable_excursion=favorable_excursion,
            ever_profitable=ever_profitable,
            cancel_repost_count=cancel_repost_count,
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
                                stall_exit_triggered: bool | None = None,
                                loss_cut_threshold_pct: float | None = None,
                                loss_pct_at_exit: float | None = None,
                                favorable_excursion: float | None = None,
                                ever_profitable: bool | None = None):
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
            loss_cut_threshold_pct,
            loss_pct_at_exit,
            favorable_excursion,
            ever_profitable,
        )

    def close_live_trade(
        self,
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
        tracker_ops.close_live_trade(
            self,
            trade_id,
            status=status,
            exit_reason=exit_reason,
            exit_price=exit_price,
            pnl=pnl,
            time_remaining_s=time_remaining_s,
            bid_at_exit=bid_at_exit,
            ask_at_exit=ask_at_exit,
            exit_limit_price=exit_limit_price,
            exit_fill_price=exit_fill_price,
            max_bid_seen=max_bid_seen,
            min_bid_seen=min_bid_seen,
            time_to_max_bid_s=time_to_max_bid_s,
            time_to_min_bid_s=time_to_min_bid_s,
            first_profit_time_s=first_profit_time_s,
            scalp_hit=scalp_hit,
            high_confidence_hit=high_confidence_hit,
            hold_to_resolution=hold_to_resolution,
            mfe=mfe,
            mae=mae,
            peak_net_pnl=peak_net_pnl,
            trough_net_pnl=trough_net_pnl,
            stall_exit_triggered=stall_exit_triggered,
            dynamic_loss_cut_pct=dynamic_loss_cut_pct,
            loss_pct_at_exit=loss_pct_at_exit,
            favorable_excursion=favorable_excursion,
            ever_profitable=ever_profitable,
            cancel_repost_count=cancel_repost_count,
        )

    def reset_paper_stats(self):
        tracker_paper.reset_paper_stats(self)

    def get_paper_stats(self) -> dict:
        return tracker_paper.get_paper_stats(self)

    def get_coin_recent_outcomes(self, coin: str, limit: int = 4) -> list[str]:
        return tracker_reports.get_coin_recent_outcomes(self, coin, limit)

    def get_coin_recent_outcome_details(self, coin: str, limit: int = 6) -> list[dict]:
        return tracker_reports.get_coin_recent_outcome_details(self, coin, limit)

    def get_coin_recent_resolutions(self, coin: str, limit: int = 4) -> list[dict]:
        return tracker_reports.get_coin_recent_resolutions(self, coin, limit)

    def get_recent_signals_by_coin(self, max_age_s: float = 30.0) -> dict:
        return tracker_reports.get_recent_signals_by_coin(self, max_age_s)

    def get_session_stats_by_coin(self) -> dict:
        return tracker_paper.get_session_stats_by_coin(self)

    def close(self):
        self.conn.close()

