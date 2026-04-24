"""Strategy engine — dual-leg and single-leg filter chains across all monitored coins."""

import asyncio
import json
import logging
import math
from datetime import datetime, timezone

from bot.config import Config, resolve_lead_lag_params
from bot.models import Decision, TradeSignal
from bot.price_aggregator import PriceAggregator
from bot.market_scanner import MarketScanner
from bot.strategies import dual_leg as dual_leg_strategy
from bot.strategies import lead_lag as lead_lag_strategy
from bot.strategies import single_leg as single_leg_strategy
from bot.strategies import swing_leg as swing_leg_strategy
from bot.tracker import DecisionTracker

logger = logging.getLogger(__name__)

# Valid runtime modes
VALID_MODES = {"dual", "single", "both", "lead", "swing", "off"}
RESEARCH_LIVE_AGGRESSIVENESS_DEFAULT = 5
RESEARCH_LIVE_OVERLAY_FACTORS = {
    1: 0.35,
    2: 0.50,
    3: 0.65,
    4: 0.85,
    5: 1.00,
    6: 1.15,
    7: 1.30,
    8: 1.50,
    9: 1.75,
    10: 2.00,
}
RESEARCH_LIVE_BLOCK_THRESHOLDS = {
    6: -7.0,
    7: -6.0,
    8: -5.0,
    9: -4.0,
    10: -3.0,
}


class StrategyEngine:
    def __init__(self, config: Config, aggregator: PriceAggregator,
                 scanner: MarketScanner, tracker: DecisionTracker,
                 risk_manager=None, research_provider=None):
        self.config = config
        self.aggregator = aggregator
        self.scanner = scanner
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.research_provider = research_provider
        self._running = False
        self._mode = "off"
        self._active = False
        # Key = (coin, strategy_type), value = (market_slug, last_entry_price, last_signal_at)
        self._last_signal: dict[tuple, tuple] = {}
        self.MIN_PRICE_IMPROVEMENT = {
            "dual_leg": 0.03,
            "swing_leg": 0.02,
        }
        self.RESIGNAL_COOLDOWN_S = {
            "dual_leg": 0.0,
            "swing_leg": 0.0,
        }

    def _sync_tracker_mode(self) -> None:
        if not self.tracker or not hasattr(self.tracker, "get_runtime_context") or not hasattr(self.tracker, "set_runtime_context"):
            return
        context = dict(self.tracker.get_runtime_context() or {})
        context["mode"] = self._mode
        self.tracker.set_runtime_context(context)

    @property
    def mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> bool:
        """Update active mode. Returns True if valid."""
        if mode not in VALID_MODES:
            return False
        self._mode = mode
        self._active = (mode != "off")
        self._sync_tracker_mode()
        logger.info(f"Strategy mode set to: {mode}")
        return True

    def start_scanning(self):
        """Start scanning - restores last mode or defaults to both repricing engines."""
        if self._mode == "off":
            self._mode = "both"
        self._active = True
        self._sync_tracker_mode()
        logger.info(f"Scanning started (mode={self._mode})")

    def stop_scanning(self):
        """Stop scanning - preserves mode setting for next start."""
        self._active = False
        logger.info("Scanning stopped")

    @property
    def is_active(self) -> bool:
        return self._active

    def dual_leg_enabled(self) -> bool:
        return self._active and self._mode in ("dual", "both") and self.config.dual_leg.enabled

    def single_leg_enabled(self) -> bool:
        return self._active and self._mode in ("single", "both") and self.config.single_leg.enabled

    def lead_lag_enabled(self) -> bool:
        return (self._active and self._mode in ("lead", "both")
                and self.config.lead_lag.enabled)

    def swing_leg_enabled(self) -> bool:
        return (self._active and self._mode in ("swing", "both")
                and self.config.swing_leg.enabled)

    async def run(self, signal_queue: asyncio.Queue):
        """Main strategy loop — evaluates when active, sleeps when stopped."""
        self._running = True
        while self._running:
            try:
                if self._active:
                    signals = self._evaluate_all()
                    for signal in signals:
                        await signal_queue.put(signal)
                    await asyncio.sleep(0.25)
                else:
                    await asyncio.sleep(1)  # idle — just wait for start command
            except Exception as e:
                logger.error(f"Strategy evaluation error: {e}")
                await asyncio.sleep(1)

    def stop(self):
        self._running = False

    def _evaluate_all(self) -> list[TradeSignal]:
        """Evaluate all active coin markets and return any signals."""
        signals = []
        active_markets = self.scanner.get_all_active()

        for coin, market in active_markets.items():
            up_book, down_book = self.scanner.get_books(coin)
            agg = self.aggregator.get_aggregated_price(coin)
            coin_signals: list[TradeSignal] = []

            if self.dual_leg_enabled():
                signal = self._evaluate_dual_leg(coin, market, up_book, down_book, agg)
                if signal is not None:
                    coin_signals.append(signal)

            if self.single_leg_enabled():
                signal = self._evaluate_single_leg(coin, market, up_book, down_book, agg)
                if signal is not None:
                    coin_signals.append(signal)

            if self.lead_lag_enabled():
                signal = self._evaluate_lead_lag(coin, market, up_book, down_book, agg)
                if signal is not None:
                    coin_signals.append(signal)

            if self.swing_leg_enabled():
                signal = self._evaluate_swing_leg(coin, market, up_book, down_book, agg)
                if signal is not None:
                    coin_signals.append(signal)

            if not coin_signals:
                continue

            strategy_names = [s.strategy_type for s in coin_signals]
            signal_context = "+".join(strategy_names)
            overlap_count = max(0, len(strategy_names) - 1)

            for signal in coin_signals:
                signal.signal_context = signal_context
                signal.signal_overlap_count = overlap_count
                if signal.decision_id:
                    self.tracker.update_decision_signal_context(
                        signal.decision_id,
                        signal_context=signal_context,
                        signal_overlap_count=overlap_count,
                    )

                gate = self._signal_gate_context(
                    signal.strategy_type,
                    coin,
                    market.slug,
                    signal.combined_cost if signal.strategy_type == "dual_leg" else signal.entry_price,
                )
                signal.resignal_cooldown_s = float(gate["resignal_cooldown_s"] or 0.0)
                signal.min_price_improvement = float(gate["min_price_improvement"] or 0.0)
                signal.last_signal_age_s = gate["last_signal_age_s"]
                if gate["reason"] is None:
                    self._record_signal(
                        signal.strategy_type,
                        coin,
                        market.slug,
                        signal.combined_cost if signal.strategy_type == "dual_leg" else signal.entry_price,
                    )
                    signals.append(signal)
                elif signal.decision_id:
                    self.tracker.suppress_decision(
                        signal.decision_id,
                        str(gate["reason"]),
                        resignal_cooldown_s=signal.resignal_cooldown_s,
                        min_price_improvement=signal.min_price_improvement,
                        last_signal_age_s=signal.last_signal_age_s,
                    )

        return signals

    def _is_price_improvement(self, strategy: str, coin: str, slug: str, price: float) -> bool:
        gate = self._signal_gate_context(strategy, coin, slug, price)
        return gate["reason"] is None

    def _signal_gate_reason(self, strategy: str, coin: str, slug: str, price: float) -> str | None:
        gate = self._signal_gate_context(strategy, coin, slug, price)
        return gate["reason"]

    def _strategy_gate_settings(self, strategy: str) -> tuple[float, float]:
        if strategy == "single_leg":
            return (
                self.config.single_leg.min_price_improvement,
                self.config.single_leg.resignal_cooldown_s,
            )
        if strategy == "lead_lag":
            return (
                self.config.lead_lag.min_price_improvement,
                self.config.lead_lag.resignal_cooldown_s,
            )
        return (
            self.MIN_PRICE_IMPROVEMENT.get(strategy, 0.03),
            self.RESIGNAL_COOLDOWN_S.get(strategy, 0.0),
        )

    def _signal_gate_context(self, strategy: str, coin: str, slug: str, price: float) -> dict[str, float | str | None]:
        key = (coin, strategy)
        last = self._last_signal.get(key)
        needed, cooldown_s = self._strategy_gate_settings(strategy)
        if last is None:
            return {
                "reason": None,
                "last_signal_age_s": None,
                "min_price_improvement": needed,
                "resignal_cooldown_s": cooldown_s,
            }

        last_slug, last_price, last_at = last
        now = datetime.now(timezone.utc)
        age_s = max(0.0, (now - last_at).total_seconds())
        if last_slug != slug:
            return {
                "reason": None,
                "last_signal_age_s": age_s,
                "min_price_improvement": needed,
                "resignal_cooldown_s": cooldown_s,
            }
        if (last_price - price) >= needed:
            return {
                "reason": None,
                "last_signal_age_s": age_s,
                "min_price_improvement": needed,
                "resignal_cooldown_s": cooldown_s,
            }
        if cooldown_s > 0 and age_s >= cooldown_s:
            return {
                "reason": None,
                "last_signal_age_s": age_s,
                "min_price_improvement": needed,
                "resignal_cooldown_s": cooldown_s,
            }
        return {
            "reason": (
                f"cooldown_active:{strategy}:age={age_s:.1f}s"
                f":need_price_improve={needed:.2f}:last={last_price:.3f}:now={price:.3f}"
            ),
            "last_signal_age_s": age_s,
            "min_price_improvement": needed,
            "resignal_cooldown_s": cooldown_s,
        }

    def _record_signal(self, strategy: str, coin: str, slug: str, price: float) -> None:
        self._last_signal[(coin, strategy)] = (
            slug,
            price,
            datetime.now(timezone.utc),
        )

    @staticmethod
    def _per_share_fee(price: float, fee_rate: float) -> float:
        return fee_rate * price * (1.0 - price)

    @staticmethod
    def _safe_ratio(numerator: float, denominator: float) -> float:
        if denominator <= 0:
            return 0.0
        return numerator / denominator

    def _lead_lag_params(self, coin: str) -> dict[str, float]:
        params = resolve_lead_lag_params(self.config.lead_lag, coin)
        overrides = self._research_filter_overrides("lead_lag", coin)
        for key in ("min_velocity_30s", "min_entry", "max_entry"):
            try:
                if key in overrides:
                    params[key] = float(overrides[key])
            except (TypeError, ValueError):
                continue
        return params

    def _single_leg_params(self, coin: str) -> dict[str, float | bool | tuple]:
        cfg = self.config.single_leg
        params: dict[str, float | bool | tuple] = {
            "entry_max": cfg.entry_max,
            "opposite_min": cfg.opposite_min,
            "min_time_remaining_s": cfg.min_time_remaining_s,
            "min_book_depth_usd": cfg.min_book_depth_usd,
            "hold_if_unfilled": cfg.hold_if_unfilled,
            "order_size_usd": cfg.order_size_usd,
            "min_velocity_30s": cfg.min_velocity_30s,
            "loss_cut_pct": cfg.loss_cut_pct,
            "loss_cut_max_factor": cfg.loss_cut_max_factor,
            "high_confidence_bid": cfg.high_confidence_bid,
            "time_pressure_s": cfg.time_pressure_s,
            "max_time_remaining_s": cfg.max_time_remaining_s,
            "max_vel_divergence": cfg.max_vel_divergence,
            "entry_min": cfg.entry_min,
            "scalp_take_profit_bid": cfg.scalp_take_profit_bid,
            "scalp_min_profit_usd": cfg.scalp_min_profit_usd,
            "resignal_cooldown_s": cfg.resignal_cooldown_s,
            "min_price_improvement": cfg.min_price_improvement,
            "max_entry_spread": cfg.max_entry_spread,
            "max_source_dispersion_pct": cfg.max_source_dispersion_pct,
            "max_source_staleness_s": cfg.max_source_staleness_s,
            "disabled_coins": tuple(cfg.disabled_coins),
        }
        overrides = self._research_filter_overrides("single_leg", coin)
        for key in ("entry_min", "entry_max", "min_velocity_30s", "high_confidence_bid"):
            try:
                if key in overrides:
                    params[key] = float(overrides[key])
            except (TypeError, ValueError):
                continue
        return params

    def _research_filter_overrides(self, strategy_type: str, coin: str) -> dict[str, object]:
        if not self.research_provider or not self.config.execution.dry_run:
            return {}
        provider = getattr(self.research_provider, "filter_overrides", None)
        if not callable(provider):
            return {}
        try:
            overrides = provider(strategy_type=strategy_type, coin=coin)
        except Exception as exc:
            logger.debug("Research filter override lookup failed for %s/%s: %s", strategy_type, coin, exc)
            return {}
        return dict(overrides or {}) if isinstance(overrides, dict) else {}

    def _lead_lag_target_price(self, entry_price: float, coin: str) -> float:
        params = self._lead_lag_params(coin)
        return min(entry_price + params["profit_take_delta"], params["profit_take_cap"])

    def _score_entry_component(self, entry_price: float, min_entry: float, max_entry: float) -> float:
        if max_entry <= min_entry:
            return 1.0
        relative = (entry_price - min_entry) / max(max_entry - min_entry, 1e-9)
        return max(0.0, min(1.0, 1.0 - relative))

    def _score_time_component(self, remaining: float, min_remaining: float, max_remaining: float) -> float:
        if remaining < min_remaining or remaining > max_remaining:
            return 0.0
        center = (min_remaining + max_remaining) / 2.0
        half_window = max((max_remaining - min_remaining) / 2.0, 1.0)
        return max(0.0, 1.0 - abs(remaining - center) / half_window)

    def _score_balance_component(self, depth_ratio: float) -> float:
        if depth_ratio <= 0:
            return 0.0
        symmetry_penalty = min(abs(math.log(depth_ratio, 2)), 1.0)
        return max(0.0, 1.0 - symmetry_penalty)

    def _repricing_score(
        self,
        *,
        velocity_30s: float,
        entry_price: float,
        min_entry: float,
        max_entry: float,
        entry_depth: float,
        min_depth: float,
        spread: float,
        remaining: float,
        min_remaining: float,
        max_remaining: float,
        depth_ratio: float,
    ) -> dict[str, float]:
        velocity_threshold = max(abs(min_entry - max_entry) / 2.0, 0.02)
        score_velocity = max(0.0, min(35.0, 35.0 * min(abs(velocity_30s) / max(velocity_threshold, 1e-9), 1.0)))
        score_entry = 20.0 * self._score_entry_component(entry_price, min_entry, max_entry)
        score_depth = max(0.0, min(15.0, 15.0 * min(entry_depth / max(min_depth * 2.0, 1e-9), 1.0)))
        score_spread = max(0.0, min(10.0, 10.0 * max(0.0, 1.0 - (spread / 0.04))))
        score_time = 10.0 * self._score_time_component(remaining, min_remaining, max_remaining)
        score_balance = 10.0 * self._score_balance_component(depth_ratio)
        total = score_velocity + score_entry + score_depth + score_spread + score_time + score_balance
        return {
            "signal_score": round(total, 2),
            "score_velocity": round(score_velocity, 2),
            "score_entry": round(score_entry, 2),
            "score_depth": round(score_depth, 2),
            "score_spread": round(score_spread, 2),
            "score_time": round(score_time, 2),
            "score_balance": round(score_balance, 2),
            "score_research_flow": 0.0,
            "score_research_crowding": 0.0,
        }

    def _research_annotation(
        self,
        *,
        strategy_type: str,
        coin: str,
        entry_price: float,
        agg,
        remaining: float,
    ) -> dict[str, object]:
        if not self.research_provider or entry_price <= 0:
            return {
                "research_cluster_id": "",
                "research_cluster_n": 0,
                "research_cluster_win_pct": 0.0,
                "research_cluster_avg_pnl": 0.0,
                "research_policy_action": "",
                "research_market_regime_1d": "",
                "research_liquidity_score_1d": 0.0,
                "research_crowding_score_1d": 0.0,
                "research_score_flow_1d": 0.0,
                "research_score_crowding_1d": 0.0,
                "research_signal_score_adjustment": 0.0,
            }
        try:
            return self.research_provider.lookup(
                strategy_type=strategy_type,
                coin=coin,
                entry_price=entry_price,
                velocity_30s=agg.velocity_30s if agg else 0.0,
                time_remaining_s=remaining,
            )
        except Exception as exc:
            logger.debug("Research lookup failed for %s/%s: %s", strategy_type, coin, exc)
            return {
                "research_cluster_id": "",
                "research_cluster_n": 0,
                "research_cluster_win_pct": 0.0,
                "research_cluster_avg_pnl": 0.0,
                "research_policy_action": "",
                "research_market_regime_1d": "",
                "research_liquidity_score_1d": 0.0,
                "research_crowding_score_1d": 0.0,
                "research_score_flow_1d": 0.0,
                "research_score_crowding_1d": 0.0,
                "research_signal_score_adjustment": 0.0,
            }

    def _apply_research_score(
        self,
        score_payload: dict[str, float],
        research_payload: dict[str, object],
        *,
        strategy_type: str,
    ) -> dict[str, float]:
        updated = dict(score_payload)
        strategy_multiplier = 1.1 if strategy_type == "lead_lag" else 1.0
        overlay_factor = self._research_live_overlay_factor()
        flow_score = float(research_payload.get("research_score_flow_1d") or 0.0) * strategy_multiplier * overlay_factor
        crowding_penalty = float(research_payload.get("research_score_crowding_1d") or 0.0) * strategy_multiplier * overlay_factor
        crowding_score = -abs(crowding_penalty)
        base_total = float(score_payload.get("signal_score") or 0.0)
        updated["score_research_flow"] = round(flow_score, 2)
        updated["score_research_crowding"] = round(crowding_score, 2)
        updated["signal_score"] = round(max(0.0, min(100.0, base_total + flow_score + crowding_score)), 2)
        return updated

    def _strategy_order_size_usd(self, strategy_type: str) -> float:
        context: dict[str, object] = {}
        try:
            context = self.tracker.get_runtime_context() or {}
        except Exception:
            context = {}
        if context.get("order_size_override_active"):
            try:
                override_size = float(context.get("order_size_usd") or 0.0)
            except (TypeError, ValueError):
                override_size = 0.0
            if override_size > 0:
                return override_size
        if strategy_type == "lead_lag":
            return self.config.lead_lag.order_size_usd
        if strategy_type == "swing_leg":
            return self.config.swing_leg.order_size_usd
        if strategy_type == "single_leg":
            return self.config.single_leg.order_size_usd
        return self.config.execution.order_size_usd

    def _research_live_aggressiveness_level(self) -> int:
        context: dict[str, object] = {}
        try:
            context = self.tracker.get_runtime_context() or {}
        except Exception:
            context = {}
        try:
            level = int(context.get("research_live_aggressiveness_level", RESEARCH_LIVE_AGGRESSIVENESS_DEFAULT))
        except (TypeError, ValueError):
            level = RESEARCH_LIVE_AGGRESSIVENESS_DEFAULT
        return max(1, min(10, level))

    def _research_live_overlay_factor(self) -> float:
        return float(RESEARCH_LIVE_OVERLAY_FACTORS[self._research_live_aggressiveness_level()])

    def _research_order_size(self, strategy_type: str, annotation: dict[str, object]) -> dict[str, float]:
        base_size = float(self._strategy_order_size_usd(strategy_type) or 0.0)
        multiplier = 1.0
        rcfg = self.config.research
        if rcfg.enabled and rcfg.execution_overlay_enabled and rcfg.size_scaling_enabled:
            adjustment = float(annotation.get("research_signal_score_adjustment") or 0.0)
            overlay_factor = self._research_live_overlay_factor()
            effective_size_per_point = float(rcfg.size_adjustment_per_score_point) * overlay_factor
            effective_floor = max(0.25, 1.0 - (1.0 - float(rcfg.size_floor_multiplier)) * overlay_factor)
            effective_ceiling = min(2.50, 1.0 + (float(rcfg.size_ceiling_multiplier) - 1.0) * overlay_factor)
            multiplier = 1.0 + adjustment * effective_size_per_point
            multiplier = max(effective_floor, min(effective_ceiling, multiplier))
        effective_size = base_size * multiplier if base_size > 0 else 0.0
        return {
            "order_size_usd": round(effective_size, 4),
            "order_size_multiplier": round(multiplier, 4),
        }

    def _research_gate_reason(self, action: str, annotation: dict[str, object]) -> str | None:
        if action not in ("DRY_RUN_SIGNAL", "TRADE"):
            return None
        if (
            action == "DRY_RUN_SIGNAL"
            and self.config.execution.dry_run
            and self.config.research.paper_gate_enabled
            and str(annotation.get("research_policy_action") or "") == "paper_blocked"
        ):
            cluster_id = str(annotation.get("research_cluster_id") or "unknown")
            sample_size = int(annotation.get("research_cluster_n") or 0)
            win_pct = float(annotation.get("research_cluster_win_pct") or 0.0)
            avg_pnl = float(annotation.get("research_cluster_avg_pnl") or 0.0)
            return f"research_policy:paper_blocked:{cluster_id}:n={sample_size}:win_pct={win_pct:.2f}:avg_pnl={avg_pnl:.4f}"
        rcfg = self.config.research
        live_level = self._research_live_aggressiveness_level()
        if not rcfg.enabled or not rcfg.execution_overlay_enabled:
            return None
        effective_block_enabled = bool(rcfg.thin_crowded_block_enabled)
        effective_live_enabled = bool(rcfg.thin_crowded_block_live_enabled)
        if live_level >= 6:
            effective_live_enabled = True
        if not effective_block_enabled:
            if not (action == "TRADE" and effective_live_enabled and live_level >= 6):
                return None
        if action == "TRADE" and not effective_live_enabled:
            return None
        regime = str(annotation.get("research_market_regime_1d") or "")
        adjustment = float(annotation.get("research_signal_score_adjustment") or 0.0)
        max_adjustment = float(rcfg.thin_crowded_block_max_adjustment)
        if action == "TRADE" and live_level >= 6:
            max_adjustment = float(RESEARCH_LIVE_BLOCK_THRESHOLDS[live_level])
        if regime != "thin_crowded" or adjustment > max_adjustment:
            return None
        liquidity = float(annotation.get("research_liquidity_score_1d") or 0.0)
        crowding = float(annotation.get("research_crowding_score_1d") or 0.0)
        return (
            "research_regime:thin_crowded_block"
            f":adj={adjustment:.2f}:liq={liquidity:.2f}:crowd={crowding:.2f}"
        )

    # -----------------------------------------------------------------------
    # Dual-leg filter chain
    # -----------------------------------------------------------------------

    def _evaluate_dual_leg(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """Run dual-leg arb filter chain. Returns signal if all pass."""
        return dual_leg_strategy.evaluate(self, coin, market, up_book, down_book, agg)

    # -----------------------------------------------------------------------
    # Single-leg filter chain
    # -----------------------------------------------------------------------

    def _evaluate_single_leg(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """Run single-leg momentum filter chain. Returns signal if all pass."""
        return single_leg_strategy.evaluate(self, coin, market, up_book, down_book, agg)

    # -----------------------------------------------------------------------
    # Lead-lag filter chain
    # -----------------------------------------------------------------------

    def _evaluate_lead_lag(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """Momentum-following repricing strategy for fast over-50c attacks."""
        return lead_lag_strategy.evaluate(self, coin, market, up_book, down_book, agg)

    # -----------------------------------------------------------------------
    # Swing dual-leg filter chain
    # -----------------------------------------------------------------------

    def _evaluate_swing_leg(self, coin, market, up_book, down_book, agg) -> TradeSignal | None:
        """
        Swing mean-reversion: buy one cheap side in a calm market, sell when it bounces.
        Entry requires low velocity, directional neutrality, and symmetric books (no one-sided momentum).
        Exit is handled by the position monitor: profit target, progressive loss cut, or near-close.
        """
        return swing_leg_strategy.evaluate(self, coin, market, up_book, down_book, agg)

    # -----------------------------------------------------------------------
    # Shared helpers
    # -----------------------------------------------------------------------

    def _log_decision(self, coin, market, up_book, down_book, agg, remaining,
                      filters, action, reason, strategy_type, **extra_fields) -> int:
        """Log decision to the tracker."""
        extra_fields.pop("strategy_type", None)
        explicit_order_size_usd = extra_fields.pop("order_size_usd", None)
        paper_total, _ = self.tracker.get_paper_capital()
        if explicit_order_size_usd is not None:
            order_size_usd = float(explicit_order_size_usd)
        elif strategy_type == "lead_lag":
            order_size_usd = self.config.lead_lag.order_size_usd
        elif strategy_type == "swing_leg":
            order_size_usd = self.config.swing_leg.order_size_usd
        elif strategy_type == "single_leg":
            order_size_usd = self.config.single_leg.order_size_usd
        else:
            order_size_usd = self.config.execution.order_size_usd
        ctx = self.tracker.get_runtime_context()
        decision = Decision(
            timestamp=datetime.now(timezone.utc),
            run_id=str(ctx.get("run_id") or ""),
            app_version=str(ctx.get("app_version") or ""),
            strategy_version=str(ctx.get("strategy_version") or ""),
            config_path=str(ctx.get("config_path") or ""),
            config_hash=str(ctx.get("config_hash") or ""),
            mode=self.mode,
            dry_run=self.config.execution.dry_run,
            order_size_usd=order_size_usd,
            paper_capital_total=paper_total,
            market_slug=market.slug,
            window_id=market.slug,
            coin=coin,
            market_end_time=market.end_time,
            market_start_time=market.start_time,
            strategy_type=strategy_type,
            up_best_ask=up_book.best_ask if up_book else 0,
            down_best_ask=down_book.best_ask if down_book else 0,
            combined_cost=(up_book.best_ask + down_book.best_ask) if (up_book and down_book) else 0,
            btc_price=agg.price if agg else 0,
            coin_velocity_30s=agg.velocity_30s if agg else 0,
            coin_velocity_60s=agg.velocity_60s if agg else 0,
            up_depth_usd=up_book.ask_depth_usd if up_book else 0,
            down_depth_usd=down_book.ask_depth_usd if down_book else 0,
            time_remaining_s=remaining,
            feed_count=agg.source_count if agg else 0,
            filter_results=filters,
            action=action,
            reason=reason,
            source_prices_json=json.dumps(agg.sources, sort_keys=True) if agg else "",
            source_ages_json=json.dumps(agg.source_ages_s, sort_keys=True) if agg else "",
            source_dispersion_pct=agg.source_dispersion_pct if agg else 0.0,
            source_staleness_max_s=agg.source_staleness_max_s if agg else 0.0,
            source_staleness_avg_s=agg.source_staleness_avg_s if agg else 0.0,
            **extra_fields,
        )
        return self.tracker.log_decision(decision)





