"""Configuration loader — reads config.yaml + .env into typed dataclasses."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class DualLegConfig:
    enabled: bool
    price_threshold: float
    max_combined_cost: float
    min_edge_after_fees: float
    min_time_remaining_s: float
    max_velocity_30s: float
    max_velocity_60s: float
    min_book_depth_usd: float


@dataclass(frozen=True)
class SingleLegConfig:
    enabled: bool
    entry_max: float
    opposite_min: float
    target_sell: float
    min_time_remaining_s: float
    min_book_depth_usd: float
    hold_if_unfilled: bool
    order_size_usd: float
    min_velocity_30s: float = 0.08   # coin must be actually moving (not a thin-book artifact)
    profit_take_pct: float = 0.50    # close paper position when profit >= 50%
    min_profit_near_close: float = 0.20  # near expiry, take any >=20% profit
    loss_cut_pct: float = 0.40       # exit if loss exceeds this fraction of entry cost
    high_confidence_bid: float = 0.82  # hold to resolution if bid exceeds this (nearly resolved)
    time_pressure_s: float = 90.0    # loss threshold shrinks linearly below this seconds remaining


@dataclass(frozen=True)
class SwingLegConfig:
    enabled: bool
    first_leg_max: float       # Buy first leg when ask <= this (e.g., 0.40)
    second_leg_max: float      # Buy second leg when ask <= this (e.g., 0.45)
    first_leg_exit: float      # Sell first leg at bid >= this if no second leg (e.g., 0.52)
    max_velocity_30s: float    # Skip if coin trending too hard (reversal less likely)
    min_time_remaining_s: float
    min_book_depth_usd: float
    order_size_usd: float
    loss_cut_pct: float = 0.40       # exit first leg if loss exceeds this fraction
    high_confidence_bid: float = 0.82  # hold to resolution if first leg bid exceeds this
    time_pressure_s: float = 90.0    # loss threshold shrinks linearly below this seconds remaining
    dead_leg_threshold: float = 0.05  # sell a leg early if its bid drops below this (dual → single)


@dataclass(frozen=True)
class LeadLagConfig:
    enabled: bool
    min_velocity_30s: float
    min_entry: float
    max_entry: float
    target_sell: float
    min_time_remaining_s: float
    min_book_depth_usd: float
    order_size_usd: float


@dataclass(frozen=True)
class ExecutionConfig:
    order_size_usd: float
    abort_timeout_s: float
    max_slippage: float
    dry_run: bool


@dataclass(frozen=True)
class RiskConfig:
    max_daily_loss_usd: float
    max_open_positions: int
    max_trades_per_hour: int
    session_profit_target: float


@dataclass(frozen=True)
class FeedsConfig:
    binance_symbols: dict   # {coin: symbol or None}
    coinbase_product: str
    coingecko_poll_interval_s: int
    price_staleness_max_s: float


@dataclass(frozen=True)
class PolymarketConfig:
    clob_base_url: str
    gamma_base_url: str
    chain_id: int
    tick_size: str
    neg_risk: bool


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    file: str
    trade_log: str


@dataclass(frozen=True)
class Config:
    coins: tuple              # ("btc", "eth", "sol", ...)
    dual_leg: DualLegConfig
    single_leg: SingleLegConfig
    lead_lag: LeadLagConfig
    swing_leg: SwingLegConfig
    execution: ExecutionConfig
    risk: RiskConfig
    feeds: FeedsConfig
    polymarket: PolymarketConfig
    logging: LoggingConfig
    private_key: str
    telegram_bot_token: str
    telegram_chat_id: str


def _load_ha_options(ha_options_path: str = "/data/options.json") -> dict:
    """Read credentials from HA add-on options file if it exists."""
    import json
    try:
        with open(ha_options_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from YAML file, HA options, or .env."""
    # Load .env first (local dev fallback)
    env_path = Path(config_path).parent / ".env"
    load_dotenv(env_path)

    # HA add-on options override .env if present
    ha = _load_ha_options()

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    return Config(
        coins=tuple(raw.get("coins", ["btc"])),
        dual_leg=DualLegConfig(**raw["dual_leg"]),
        single_leg=SingleLegConfig(**raw["single_leg"]),
        lead_lag=LeadLagConfig(**raw["lead_lag"]),
        swing_leg=SwingLegConfig(**raw["swing_leg"]),
        execution=ExecutionConfig(**raw["execution"]),
        risk=RiskConfig(**raw["risk"]),
        feeds=FeedsConfig(**raw["feeds"]),
        polymarket=PolymarketConfig(**raw["polymarket"]),
        logging=LoggingConfig(**raw["logging"]),
        private_key=ha.get("private_key") or os.getenv("PRIVATE_KEY", ""),
        telegram_bot_token=ha.get("telegram_bot_token") or os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=ha.get("telegram_chat_id") or os.getenv("TELEGRAM_CHAT_ID", ""),
    )
