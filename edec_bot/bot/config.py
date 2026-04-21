"""Configuration loader - reads config.yaml + .env into typed dataclasses."""

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
    min_time_remaining_s: float
    min_book_depth_usd: float
    hold_if_unfilled: bool
    order_size_usd: float
    min_velocity_30s: float = 0.08
    loss_cut_pct: float = 0.25
    loss_cut_max_factor: float = 2.0
    high_confidence_bid: float = 0.82
    time_pressure_s: float = 90.0
    max_time_remaining_s: float = 200.0
    max_vel_divergence: float = 0.03
    entry_min: float = 0.15
    scalp_take_profit_bid: float = 0.56
    scalp_min_profit_usd: float = 0.05
    resignal_cooldown_s: float = 8.0
    min_price_improvement: float = 0.01
    max_entry_spread: float = 0.06
    max_source_dispersion_pct: float = 0.50
    max_source_staleness_s: float = 4.0
    disabled_coins: tuple = ()


@dataclass(frozen=True)
class SwingLegConfig:
    enabled: bool
    first_leg_max: float
    first_leg_exit: float
    max_velocity_30s: float
    min_time_remaining_s: float
    min_book_depth_usd: float
    order_size_usd: float
    loss_cut_pct: float = 0.25
    loss_cut_max_factor: float = 2.0
    high_confidence_bid: float = 0.82
    time_pressure_s: float = 90.0
    max_time_remaining_s: float = 300.0
    first_leg_min: float = 0.25
    max_vel_divergence: float = 0.03
    max_depth_ratio: float = 2.5
    disabled_coins: tuple = ()


@dataclass(frozen=True)
class LeadLagCoinOverride:
    min_velocity_30s: Optional[float] = None
    min_entry: Optional[float] = None
    max_entry: Optional[float] = None
    min_book_depth_usd: Optional[float] = None


@dataclass(frozen=True)
class LeadLagConfig:
    enabled: bool
    min_velocity_30s: float
    min_entry: float
    max_entry: float
    min_time_remaining_s: float
    min_book_depth_usd: float
    order_size_usd: float
    target_sell: float = 0.66
    resignal_cooldown_s: float = 6.0
    min_price_improvement: float = 0.01
    profit_take_delta: float = 0.06
    profit_take_cap: float = 0.68
    stall_window_s: float = 30.0
    min_progress_delta: float = 0.02
    hard_stop_loss_pct: float = 0.10
    max_entry_spread: float = 0.06
    max_source_dispersion_pct: float = 0.50
    max_source_staleness_s: float = 4.0
    disabled_coins: tuple = ()
    coin_overrides: dict[str, LeadLagCoinOverride] = field(default_factory=dict)


def resolve_lead_lag_params(cfg, coin: str) -> dict[str, float]:
    coin_key = (coin or "").lower()
    overrides = getattr(cfg, "coin_overrides", {}) or {}
    override = overrides.get(coin_key)
    return {
        "min_velocity_30s": override.min_velocity_30s if override and override.min_velocity_30s is not None else cfg.min_velocity_30s,
        "min_entry": override.min_entry if override and override.min_entry is not None else cfg.min_entry,
        "max_entry": override.max_entry if override and override.max_entry is not None else cfg.max_entry,
        "min_book_depth_usd": override.min_book_depth_usd if override and override.min_book_depth_usd is not None else cfg.min_book_depth_usd,
        "min_time_remaining_s": getattr(cfg, "min_time_remaining_s", 0.0),
        "order_size_usd": cfg.order_size_usd,
        "profit_take_delta": cfg.profit_take_delta,
        "profit_take_cap": cfg.profit_take_cap,
        "stall_window_s": cfg.stall_window_s,
        "min_progress_delta": cfg.min_progress_delta,
        "hard_stop_loss_pct": cfg.hard_stop_loss_pct,
        "resignal_cooldown_s": getattr(cfg, "resignal_cooldown_s", 0.0),
        "min_price_improvement": getattr(cfg, "min_price_improvement", 0.0),
    }


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
    binance_symbols: dict
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
class CliConfig:
    enabled: bool = True
    binary_path: str = "polymarket"
    timeout_s: float = 8.0
    signature_type: str = "proxy"
    allow_mutating_commands: bool = False
    startup_check: bool = True


@dataclass(frozen=True)
class ResearchConfig:
    enabled: bool = False
    artifact_path: str = "data/research/runtime_policy.json"
    paper_gate_enabled: bool = False
    execution_overlay_enabled: bool = True
    size_scaling_enabled: bool = True
    size_adjustment_per_score_point: float = 0.06
    size_floor_multiplier: float = 0.5
    size_ceiling_multiplier: float = 1.25
    thin_crowded_block_enabled: bool = True
    thin_crowded_block_live_enabled: bool = False
    thin_crowded_block_max_adjustment: float = -7.0


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    file: str
    trade_log: str


@dataclass(frozen=True)
class Config:
    coins: tuple
    dual_leg: DualLegConfig
    single_leg: SingleLegConfig
    lead_lag: LeadLagConfig
    swing_leg: SwingLegConfig
    execution: ExecutionConfig
    research: ResearchConfig
    risk: RiskConfig
    feeds: FeedsConfig
    polymarket: PolymarketConfig
    cli: CliConfig
    logging: LoggingConfig
    private_key: str
    telegram_bot_token: str
    telegram_chat_id: str


def _load_ha_options(ha_options_path: str = "/data/options.json") -> dict:
    """Read credentials from HA add-on options file if it exists."""
    import json

    try:
        with open(ha_options_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from YAML file, HA options, or .env."""
    env_path = Path(config_path).parent / ".env"
    load_dotenv(env_path)

    ha = _load_ha_options()

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    raw_lead_lag = dict(raw["lead_lag"])
    lead_lag_overrides = {
        str(coin): LeadLagCoinOverride(**(values or {}))
        for coin, values in (raw_lead_lag.pop("coin_overrides", {}) or {}).items()
    }
    cli_raw = raw.get("cli", {})
    research_raw = raw.get("research", {}) or {}

    return Config(
        coins=tuple(raw.get("coins", ["btc"])),
        dual_leg=DualLegConfig(**raw["dual_leg"]),
        single_leg=SingleLegConfig(**{
            **raw["single_leg"],
            "disabled_coins": tuple(raw["single_leg"].get("disabled_coins", [])),
        }),
        lead_lag=LeadLagConfig(**{
            **raw_lead_lag,
            "disabled_coins": tuple(raw_lead_lag.get("disabled_coins", [])),
            "coin_overrides": lead_lag_overrides,
        }),
        swing_leg=SwingLegConfig(**{
            **raw["swing_leg"],
            "disabled_coins": tuple(raw["swing_leg"].get("disabled_coins", [])),
        }),
        execution=ExecutionConfig(**raw["execution"]),
        research=ResearchConfig(**research_raw),
        risk=RiskConfig(**raw["risk"]),
        feeds=FeedsConfig(**raw["feeds"]),
        polymarket=PolymarketConfig(**raw["polymarket"]),
        cli=CliConfig(**cli_raw),
        logging=LoggingConfig(**raw["logging"]),
        private_key=ha.get("private_key") or os.getenv("PRIVATE_KEY", ""),
        telegram_bot_token=ha.get("telegram_bot_token") or os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=ha.get("telegram_chat_id") or os.getenv("TELEGRAM_CHAT_ID", ""),
    )
