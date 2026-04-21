"""Lead-lag evaluator."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bot.models import FilterResult, TradeSignal

logger = logging.getLogger(__name__)


def evaluate(engine: Any, coin, market, up_book, down_book, agg) -> TradeSignal | None:
    """Run the lead-lag repricing filter chain."""
    cfg = engine.config.lead_lag
    params = engine._lead_lag_params(coin)
    filters: list[FilterResult] = []
    failed_reason = ""

    remaining = (market.end_time - datetime.now(timezone.utc)).total_seconds()
    if (coin or "").lower() in cfg.disabled_coins:
        engine._log_decision(
            coin,
            market,
            up_book,
            down_book,
            agg,
            remaining,
            filters,
            "SKIP",
            f"Lead-lag disabled for {coin.upper()}",
            "lead_lag",
        )
        return None

    f = FilterResult("market_active", market.accepting_orders, str(market.accepting_orders), "True")
    filters.append(f)
    if not f.passed:
        failed_reason = "Market not accepting orders"

    now = datetime.now(timezone.utc)
    remaining = (market.end_time - now).total_seconds()
    f = FilterResult("time_remaining", remaining > params["min_time_remaining_s"], f"{remaining:.0f}s", f">{params['min_time_remaining_s']}s")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Only {remaining:.0f}s remaining"

    books_ok = up_book is not None and down_book is not None
    f = FilterResult(
        "books_available",
        books_ok,
        f"up={'yes' if up_book else 'no'}, down={'yes' if down_book else 'no'}",
        "both available",
    )
    filters.append(f)
    if not books_ok:
        if not failed_reason:
            failed_reason = "Order books not available"
        engine._log_decision(coin, market, up_book, down_book, agg, remaining, filters, "SKIP", failed_reason, "lead_lag")
        return None

    if agg is None:
        f = FilterResult("coin_velocity", False, "no price data", "price data required")
        filters.append(f)
        engine._log_decision(coin, market, up_book, down_book, agg, remaining, filters, "SKIP", "No price data", "lead_lag")
        return None

    vel = agg.velocity_30s
    vel_ok = abs(vel) >= params["min_velocity_30s"]
    f = FilterResult("coin_velocity", vel_ok, f"30s={vel:.3f}%", f">={params['min_velocity_30s']}%")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"{coin.upper()} not moving enough: {vel:.3f}% (need >={params['min_velocity_30s']}%)"

    if vel > 0:
        side = "up"
        entry_price = up_book.best_ask
        entry_bid = up_book.best_bid
        entry_depth = up_book.ask_depth_usd
        opposite_depth = down_book.ask_depth_usd
    else:
        side = "down"
        entry_price = down_book.best_ask
        entry_bid = down_book.best_bid
        entry_depth = down_book.ask_depth_usd
        opposite_depth = up_book.ask_depth_usd

    spread = max(0.0, entry_price - entry_bid)
    depth_ratio = engine._safe_ratio(entry_depth, opposite_depth)
    in_range = params["min_entry"] <= entry_price <= params["max_entry"]
    f = FilterResult(
        "lag_window",
        in_range,
        f"{side.upper()}@{entry_price:.3f}",
        f"[{params['min_entry']:.2f}, {params['max_entry']:.2f}]",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        if entry_price < params["min_entry"]:
            failed_reason = f"{side.upper()}@{entry_price:.3f} already in single-leg range (too repriced)"
        else:
            failed_reason = f"{side.upper()}@{entry_price:.3f} too high - market not moving in expected direction"

    f = FilterResult("liquidity_depth", entry_depth >= params["min_book_depth_usd"], f"${entry_depth:.1f}", f">=${params['min_book_depth_usd']}")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Thin entry liquidity: ${entry_depth:.1f}"

    source_count = agg.source_count if agg else 0
    f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Only {source_count} live feed(s)"

    es_val = max(0.0, entry_price - entry_bid)
    f = FilterResult("entry_spread", es_val <= cfg.max_entry_spread, f"{es_val:.3f}", f"<={cfg.max_entry_spread:.3f}")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Entry spread too wide: {es_val:.3f} > {cfg.max_entry_spread:.3f}"

    sdp = agg.source_dispersion_pct if agg else 0.0
    f = FilterResult("source_dispersion", sdp <= cfg.max_source_dispersion_pct, f"{sdp:.3f}%", f"<={cfg.max_source_dispersion_pct:.3f}%")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Source dispersion too high: {sdp:.3f}% > {cfg.max_source_dispersion_pct:.3f}%"

    ssx = agg.source_staleness_max_s if agg else 0.0
    f = FilterResult("source_staleness", ssx <= cfg.max_source_staleness_s, f"{ssx:.2f}s", f"<={cfg.max_source_staleness_s:.2f}s")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Source staleness too high: {ssx:.2f}s > {cfg.max_source_staleness_s:.2f}s"

    risk_ok = engine.risk_manager.can_trade() if engine.risk_manager else True
    f = FilterResult("risk_limits", risk_ok, "ok" if risk_ok else "blocked", "ok")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = "Risk limits breached"

    all_passed = all(result.passed for result in filters)
    target_sell = engine._lead_lag_target_price(entry_price, coin)
    fee_rate = market.fee_rate
    fee_buy = engine._per_share_fee(entry_price, fee_rate)
    fee_sell = engine._per_share_fee(target_sell, fee_rate)
    expected_profit = (target_sell - entry_price) - fee_buy - fee_sell
    score_payload = engine._repricing_score(
        velocity_30s=vel,
        entry_price=entry_price,
        min_entry=params["min_entry"],
        max_entry=params["max_entry"],
        entry_depth=entry_depth,
        min_depth=params["min_book_depth_usd"],
        spread=spread,
        remaining=remaining,
        min_remaining=params["min_time_remaining_s"],
        max_remaining=250.0,
        depth_ratio=depth_ratio,
    )
    research_payload = engine._research_annotation(
        strategy_type="lead_lag",
        coin=coin,
        entry_price=entry_price,
        agg=agg,
        remaining=remaining,
    )
    score_payload = engine._apply_research_score(
        score_payload,
        research_payload,
        strategy_type="lead_lag",
    )
    order_size_payload = engine._research_order_size("lead_lag", research_payload)

    action = ("DRY_RUN_SIGNAL" if engine.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
    reason = f"Lead-lag {side.upper()} @{entry_price:.3f} vel={vel:.3f}%" if all_passed else failed_reason
    suppressed_reason = ""
    research_gate_reason = engine._research_gate_reason(action, research_payload)
    if research_gate_reason:
        action = "SUPPRESSED"
        reason = research_gate_reason
        suppressed_reason = research_gate_reason

    decision_id = engine._log_decision(
        coin,
        market,
        up_book,
        down_book,
        agg,
        remaining,
        filters,
        action,
        reason,
        "lead_lag",
        entry_price=entry_price,
        target_price=target_sell,
        expected_profit_per_share=expected_profit,
        entry_bid=entry_bid,
        entry_ask=entry_price,
        entry_spread=spread,
        entry_depth_side_usd=entry_depth,
        opposite_depth_usd=opposite_depth,
        depth_ratio=depth_ratio,
        suppressed_reason=suppressed_reason,
        order_size_usd=order_size_payload["order_size_usd"],
        resignal_cooldown_s=params["resignal_cooldown_s"],
        min_price_improvement=params["min_price_improvement"],
        **research_payload,
        **score_payload,
    )

    if not all_passed or action == "SUPPRESSED":
        return None

    signal = TradeSignal(
        market=market,
        strategy_type="lead_lag",
        decision_id=decision_id,
        side=side,
        entry_price=entry_price,
        target_sell_price=target_sell,
        entry_bid=entry_bid,
        entry_ask=entry_price,
        entry_spread=spread,
        entry_depth_side_usd=entry_depth,
        opposite_depth_usd=opposite_depth,
        depth_ratio=depth_ratio,
        fee_total=fee_buy + fee_sell,
        expected_profit=expected_profit,
        time_remaining_s=remaining,
        up_book=up_book,
        down_book=down_book,
        filter_results=filters,
        target_delta=max(0.0, target_sell - entry_price),
        hard_stop_delta=max(0.0, entry_price * params["hard_stop_loss_pct"]),
        order_size_usd=order_size_payload["order_size_usd"],
        order_size_multiplier=order_size_payload["order_size_multiplier"],
        resignal_cooldown_s=params["resignal_cooldown_s"],
        min_price_improvement=params["min_price_improvement"],
        **score_payload,
    )
    logger.info(
        f"{'[DRY RUN] ' if engine.config.execution.dry_run else ''}"
        f"LEAD-LAG SIGNAL [{coin.upper()}]: BUY {side.upper()}@{entry_price:.3f} "
        f"(vel={vel:+.3f}%) -> SELL@{target_sell:.3f} | score={score_payload['signal_score']:.1f} "
        f"| Est profit: ${expected_profit:.4f}"
    )
    return signal
