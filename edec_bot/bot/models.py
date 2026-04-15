"""Shared data structures for the EDEC bot."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


@dataclass
class PriceTick:
    source: str
    price: float
    timestamp: float  # Unix timestamp
    coin: str = "btc"  # Which coin this tick is for


@dataclass
class AggregatedPrice:
    price: float
    timestamp: float
    velocity_30s: float  # % change over 30s
    velocity_60s: float  # % change over 60s
    is_trending: bool
    source_count: int
    sources: dict  # {source_name: price}
    coin: str = "btc"


@dataclass
class MarketInfo:
    event_id: str
    condition_id: str
    slug: str
    coin: str              # e.g. "btc", "eth", "sol"
    up_token_id: str
    down_token_id: str
    end_time: datetime
    start_time: datetime
    fee_rate: float        # e.g. 0.072
    tick_size: str
    neg_risk: bool
    accepting_orders: bool = True


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
    strategy_type: str        # "dual_leg" or "single_leg"
    decision_id: int = 0
    signal_context: str = ""
    signal_overlap_count: int = 0

    # Dual-leg fields
    up_price: float = 0.0
    down_price: float = 0.0
    combined_cost: float = 0.0

    # Single-leg fields
    side: str = ""            # "up" or "down"
    entry_price: float = 0.0
    target_sell_price: float = 0.0
    entry_bid: float = 0.0
    entry_ask: float = 0.0
    entry_spread: float = 0.0
    entry_depth_side_usd: float = 0.0
    opposite_depth_usd: float = 0.0
    depth_ratio: float = 0.0

    # Common
    fee_total: float = 0.0
    expected_profit: float = 0.0
    time_remaining_s: float = 0.0
    up_book: Optional[OrderBookSnapshot] = None
    down_book: Optional[OrderBookSnapshot] = None
    filter_results: list = field(default_factory=list)


@dataclass
class SwingPosition:
    """Tracks a swing mean-reversion position: one cheap side bought, monitoring for bounce exit."""
    market: MarketInfo
    first_side: str              # "up" or "down"
    first_token_id: str
    first_entry_price: float
    first_shares: float
    first_paper_trade_id: Optional[int] = None
    first_buy_order_id: str = ""
    opened_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SingleLegPosition:
    """Tracks an open single-leg position."""
    market: MarketInfo
    side: str                  # "up" or "down"
    token_id: str
    entry_price: float
    target_price: float
    shares: float
    buy_order_id: str
    sell_order_id: Optional[str] = None
    strategy_type: str = "single_leg"   # "single_leg" or "lead_lag" — controls exit behaviour
    opened_at: datetime = field(default_factory=datetime.utcnow)


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
    # Dual-leg
    up_order_id: Optional[str] = None
    down_order_id: Optional[str] = None
    up_filled: bool = False
    down_filled: bool = False
    up_fill_price: float = 0.0
    down_fill_price: float = 0.0
    # Single-leg
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    side: str = ""
    # Common
    total_cost: float = 0.0
    fee_total: float = 0.0
    shares: float = 0.0
    shares_requested: float = 0.0
    shares_filled: float = 0.0
    blocked_min_5_shares: bool = False
    status: str = "pending"   # success, aborted, partial_abort, failed, dry_run, open
    abort_cost: float = 0.0
    error: str = ""


@dataclass
class Decision:
    """One strategy evaluation cycle — logged to SQLite."""
    timestamp: datetime
    market_slug: str
    coin: str
    market_end_time: datetime
    market_start_time: datetime
    window_id: str
    strategy_type: str         # "dual_leg" or "single_leg"
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
    action: str               # "TRADE", "SKIP", "DRY_RUN_SIGNAL"
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
