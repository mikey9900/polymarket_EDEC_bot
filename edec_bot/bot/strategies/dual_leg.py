"""Dual-leg evaluator."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bot.models import FilterResult, TradeSignal

logger = logging.getLogger(__name__)


def evaluate(engine: Any, coin, market, up_book, down_book, agg) -> TradeSignal | None:
    """Run the dual-leg arb filter chain."""
    cfg = engine.config.dual_leg
    filters: list[FilterResult] = []
    failed_reason = ""

    f = FilterResult("market_active", market.accepting_orders, str(market.accepting_orders), "True")
    filters.append(f)
    if not f.passed:
        failed_reason = "Market not accepting orders"

    now = datetime.now(timezone.utc)
    remaining = (market.end_time - now).total_seconds()
    f = FilterResult("time_remaining", remaining > cfg.min_time_remaining_s, f"{remaining:.0f}s", f">{cfg.min_time_remaining_s}s")
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
        engine._log_decision(coin, market, up_book, down_book, agg, remaining, filters, "SKIP", failed_reason, "dual_leg")
        return None

    threshold = cfg.price_threshold
    f = FilterResult(
        "price_threshold",
        up_book.best_ask <= threshold and down_book.best_ask <= threshold,
        f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}",
        f"<={threshold}",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Price above threshold: up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}"

    combined = up_book.best_ask + down_book.best_ask
    f = FilterResult("combined_cost", combined <= cfg.max_combined_cost, f"{combined:.3f}", f"<={cfg.max_combined_cost}")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Combined cost too high: {combined:.3f}"

    fee_up = engine._per_share_fee(up_book.best_ask, market.fee_rate)
    fee_down = engine._per_share_fee(down_book.best_ask, market.fee_rate)
    fee_total = fee_up + fee_down
    total_cost = combined + fee_total
    expected_profit = 1.0 - total_cost
    f = FilterResult(
        "edge_after_fees",
        expected_profit >= cfg.min_edge_after_fees,
        f"${expected_profit:.4f}",
        f">=${cfg.min_edge_after_fees}",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Edge too thin: ${expected_profit:.4f} after fees"

    if agg is not None:
        vel_ok = abs(agg.velocity_30s) <= cfg.max_velocity_30s and abs(agg.velocity_60s) <= cfg.max_velocity_60s
        f = FilterResult(
            "coin_velocity",
            vel_ok,
            f"30s={agg.velocity_30s:.3f}%, 60s={agg.velocity_60s:.3f}%",
            f"30s<={cfg.max_velocity_30s}%, 60s<={cfg.max_velocity_60s}%",
        )
    else:
        f = FilterResult("coin_velocity", False, "no price data", "price data required")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"{coin.upper()} trending: {f.value}"

    min_depth = cfg.min_book_depth_usd
    f = FilterResult(
        "liquidity_depth",
        up_book.ask_depth_usd >= min_depth and down_book.ask_depth_usd >= min_depth,
        f"up=${up_book.ask_depth_usd:.1f}, down=${down_book.ask_depth_usd:.1f}",
        f">=${min_depth}",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Thin liquidity: {f.value}"

    source_count = agg.source_count if agg else 0
    f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Only {source_count} live feed(s)"

    risk_ok = engine.risk_manager.can_trade() if engine.risk_manager else True
    f = FilterResult("risk_limits", risk_ok, "ok" if risk_ok else "blocked", "ok")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = "Risk limits breached"

    all_passed = all(result.passed for result in filters)
    action = ("DRY_RUN_SIGNAL" if engine.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
    reason = "All filters passed" if all_passed else failed_reason

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
        "dual_leg",
    )

    if not all_passed:
        return None

    signal = TradeSignal(
        market=market,
        strategy_type="dual_leg",
        decision_id=decision_id,
        up_price=up_book.best_ask,
        down_price=down_book.best_ask,
        combined_cost=combined,
        fee_total=fee_total,
        expected_profit=expected_profit,
        time_remaining_s=remaining,
        up_book=up_book,
        down_book=down_book,
        filter_results=filters,
    )
    logger.info(
        f"{'[DRY RUN] ' if engine.config.execution.dry_run else ''}"
        f"DUAL-LEG SIGNAL [{coin.upper()}]: UP@{up_book.best_ask:.3f} + DOWN@{down_book.best_ask:.3f}"
        f" = {combined:.3f} | Profit: ${expected_profit:.4f}"
    )
    return signal
