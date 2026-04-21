"""Shared data structures for the EDEC bot."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


@dataclass
class PriceTick:
    source: str
    price: float
    timestamp: float
    coin: str = "btc"


@dataclass
class AggregatedPrice:
    price: float
    timestamp: float
    velocity_30s: float
    velocity_60s: float
    is_trending: bool
    source_count: int
    sources: dict
    source_ages_s: dict = field(default_factory=dict)
    source_dispersion_pct: float = 0.0
    source_staleness_max_s: float = 0.0
    source_staleness_avg_s: float = 0.0
    coin: str = "btc"


@dataclass
class MarketInfo:
    event_id: str
    condition_id: str
    slug: str
    coin: str
    up_token_id: str
    down_token_id: str
    end_time: datetime
    start_time: datetime
    fee_rate: float
    tick_size: str
    neg_risk: bool
    accepting_orders: bool = True
    question: str = ""
    volume: Optional[float] = None
    reference_price: Optional[float] = None
    reference_label: str = ""


@dataclass
class OrderBookSnapshot:
    token_id: str
    best_bid: float
    best_ask: float
    bid_depth_usd: float
    ask_depth_usd: float
    timestamp: float


@dataclass
class FilterResult:
    name: str
    passed: bool
    value: str
    threshold: str


@dataclass
class TradeSignal:
    market: MarketInfo
    strategy_type: str
    decision_id: int = 0
    signal_context: str = ""
    signal_overlap_count: int = 0

    up_price: float = 0.0
    down_price: float = 0.0
    combined_cost: float = 0.0

    side: str = ""
    entry_price: float = 0.0
    target_sell_price: float = 0.0
    entry_bid: float = 0.0
    entry_ask: float = 0.0
    entry_spread: float = 0.0
    entry_depth_side_usd: float = 0.0
    opposite_depth_usd: float = 0.0
    depth_ratio: float = 0.0

    fee_total: float = 0.0
    expected_profit: float = 0.0
    time_remaining_s: float = 0.0
    up_book: Optional[OrderBookSnapshot] = None
    down_book: Optional[OrderBookSnapshot] = None
    filter_results: list = field(default_factory=list)

    signal_score: float = 0.0
    score_velocity: float = 0.0
    score_entry: float = 0.0
    score_depth: float = 0.0
    score_spread: float = 0.0
    score_time: float = 0.0
    score_balance: float = 0.0
    score_research_flow: float = 0.0
    score_research_crowding: float = 0.0
    target_delta: float = 0.0
    hard_stop_delta: float = 0.0
    order_size_usd: float = 0.0
    order_size_multiplier: float = 1.0
    resignal_cooldown_s: float = 0.0
    min_price_improvement: float = 0.0
    last_signal_age_s: Optional[float] = None


@dataclass
class SwingPosition:
    market: MarketInfo
    first_side: str
    first_token_id: str
    first_entry_price: float
    first_shares: float
    decision_id: int = 0
    trade_id: int = 0
    requested_shares: float = 0.0
    first_paper_trade_id: Optional[int] = None
    first_buy_order_id: str = ""
    sell_order_id: Optional[str] = None
    hold_to_resolution: bool = False
    pending_exit_reason: str = ""
    pending_exit_price: float = 0.0
    entry_order_submitted_at: str = ""
    entry_filled_at: str = ""
    cancel_repost_count: int = 0
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class SingleLegPosition:
    market: MarketInfo
    side: str
    token_id: str
    entry_price: float
    target_price: float
    shares: float
    buy_order_id: str
    sell_order_id: Optional[str] = None
    strategy_type: str = "single_leg"
    decision_id: int = 0
    trade_id: int = 0
    requested_shares: float = 0.0
    hold_to_resolution: bool = False
    pending_exit_reason: str = ""
    pending_exit_price: float = 0.0
    entry_order_submitted_at: str = ""
    entry_filled_at: str = ""
    cancel_repost_count: int = 0
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DualOrderState(Enum):
    IDLE = "idle"
    PLACING_FIRST = "placing_first"
    FIRST_PLACED = "first_placed"
    PLACING_SECOND = "placing_second"
    BOTH_PLACED = "both_placed"
    ABORTING = "aborting"
    DONE = "done"


@dataclass
class TradeResult:
    signal: TradeSignal
    strategy_type: str = ""
    trade_id: int = 0
    up_order_id: Optional[str] = None
    down_order_id: Optional[str] = None
    up_filled: bool = False
    down_filled: bool = False
    up_fill_price: float = 0.0
    down_fill_price: float = 0.0
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    side: str = ""
    total_cost: float = 0.0
    fee_total: float = 0.0
    shares: float = 0.0
    shares_requested: float = 0.0
    shares_filled: float = 0.0
    blocked_min_5_shares: bool = False
    status: str = "pending"
    abort_cost: float = 0.0
    error: str = ""
    entry_order_submitted_at: str = ""
    entry_filled_at: str = ""
    entry_time_to_fill_s: float = 0.0
    entry_limit_price: float = 0.0
    entry_fill_price: float = 0.0
    entry_slippage: float = 0.0
    entry_fill_ratio: float = 0.0
    exit_order_submitted_at: str = ""
    exit_filled_at: str = ""
    exit_limit_price: float = 0.0
    exit_fill_price: float = 0.0
    exit_slippage: float = 0.0
    exit_reason: str = ""
    exit_price: float = 0.0
    realized_pnl: float = 0.0
    time_remaining_s: float = 0.0
    bid_at_exit: float = 0.0
    ask_at_exit: float = 0.0
    exit_spread: float = 0.0
    max_bid_seen: float = 0.0
    min_bid_seen: float = 0.0
    time_to_max_bid_s: float = 0.0
    time_to_min_bid_s: float = 0.0
    first_profit_time_s: float = 0.0
    scalp_hit: bool = False
    high_confidence_hit: bool = False
    hold_to_resolution: bool = False
    mfe: float = 0.0
    mae: float = 0.0
    peak_net_pnl: float = 0.0
    trough_net_pnl: float = 0.0
    stall_exit_triggered: bool = False
    dynamic_loss_cut_pct: float = 0.0
    loss_pct_at_exit: float = 0.0
    favorable_excursion: float = 0.0
    ever_profitable: bool = False
    cancel_repost_count: int = 0


@dataclass
class Decision:
    timestamp: datetime
    market_slug: str
    coin: str
    market_end_time: datetime
    market_start_time: datetime
    window_id: str
    strategy_type: str
    up_best_ask: float
    down_best_ask: float
    combined_cost: float
    btc_price: float
    coin_velocity_30s: float
    coin_velocity_60s: float
    up_depth_usd: float
    down_depth_usd: float
    time_remaining_s: float
    feed_count: int
    filter_results: list
    action: str
    reason: str
    run_id: str = ""
    app_version: str = ""
    strategy_version: str = ""
    config_path: str = ""
    config_hash: str = ""
    mode: str = ""
    dry_run: bool = True
    order_size_usd: float = 0.0
    paper_capital_total: float = 0.0
    signal_context: str = ""
    signal_overlap_count: int = 0
    suppressed_reason: str = ""
    entry_price: float = 0.0
    target_price: float = 0.0
    expected_profit_per_share: float = 0.0
    entry_bid: float = 0.0
    entry_ask: float = 0.0
    entry_spread: float = 0.0
    entry_depth_side_usd: float = 0.0
    opposite_depth_usd: float = 0.0
    depth_ratio: float = 0.0
    signal_score: float = 0.0
    score_velocity: float = 0.0
    score_entry: float = 0.0
    score_depth: float = 0.0
    score_spread: float = 0.0
    score_time: float = 0.0
    score_balance: float = 0.0
    score_research_flow: float = 0.0
    score_research_crowding: float = 0.0
    resignal_cooldown_s: float = 0.0
    min_price_improvement: float = 0.0
    last_signal_age_s: Optional[float] = None
    research_cluster_id: str = ""
    research_cluster_n: int = 0
    research_cluster_win_pct: float = 0.0
    research_cluster_avg_pnl: float = 0.0
    research_policy_action: str = ""
    research_market_regime_1d: str = ""
    research_liquidity_score_1d: float = 0.0
    research_crowding_score_1d: float = 0.0
    research_score_flow_1d: float = 0.0
    research_score_crowding_1d: float = 0.0
    research_signal_score_adjustment: float = 0.0
    source_prices_json: str = ""
    source_ages_json: str = ""
    source_dispersion_pct: float = 0.0
    source_staleness_max_s: float = 0.0
    source_staleness_avg_s: float = 0.0
