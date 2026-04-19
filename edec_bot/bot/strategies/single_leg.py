"""Single-leg evaluator."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bot.models import FilterResult, TradeSignal

logger = logging.getLogger(__name__)


def evaluate(engine: Any, coin, market, up_book, down_book, agg) -> TradeSignal | None:
    """Run the single-leg momentum filter chain."""
    cfg = engine.config.single_leg
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

    f = FilterResult("entry_window", remaining <= cfg.max_time_remaining_s, f"{remaining:.0f}s", f"<={cfg.max_time_remaining_s:.0f}s")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Too early: {remaining:.0f}s remaining (wait for direction)"

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
            f"Single-leg disabled for {coin.upper()} (feed coverage not approved)",
            "single_leg",
        )
        return None

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
        engine._log_decision(coin, market, up_book, down_book, agg, remaining, filters, "SKIP", failed_reason, "single_leg")
        return None

    min_vel = cfg.min_velocity_30s
    if agg is not None:
        vel_ok = abs(agg.velocity_30s) >= min_vel
        f = FilterResult("coin_velocity", vel_ok, f"30s={agg.velocity_30s:.3f}%", f">={min_vel}%")
    else:
        f = FilterResult("coin_velocity", False, "no price data", "price data required")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"{coin.upper()} not moving enough: {f.value} (need >={min_vel}%)"

    up_cheap = up_book.best_ask <= cfg.entry_max and down_book.best_ask >= cfg.opposite_min
    down_cheap = down_book.best_ask <= cfg.entry_max and up_book.best_ask >= cfg.opposite_min
    entry_ok = up_cheap or down_cheap

    if up_cheap:
        side = "up"
        entry_price = up_book.best_ask
        entry_bid = up_book.best_bid
        opposite_depth = down_book.ask_depth_usd
        entry_depth = up_book.ask_depth_usd
    elif down_cheap:
        side = "down"
        entry_price = down_book.best_ask
        entry_bid = down_book.best_bid
        opposite_depth = up_book.ask_depth_usd
        entry_depth = down_book.ask_depth_usd
    else:
        side = ""
        entry_price = min(up_book.best_ask, down_book.best_ask)
        entry_bid = 0.0
        opposite_depth = 0.0
        entry_depth = 0.0

    f = FilterResult(
        "entry_threshold",
        entry_ok,
        f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}",
        f"one side<={cfg.entry_max}, other>={cfg.opposite_min}",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = (
            f"No cheap side: up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f} "
            f"(need one <={cfg.entry_max}, other >={cfg.opposite_min})"
        )

    if side in ("up", "down"):
        floor_ok = entry_price >= cfg.entry_min
        f = FilterResult("entry_floor", floor_ok, f"{entry_price:.3f}", f">={cfg.entry_min:.2f}")
    else:
        f = FilterResult("entry_floor", True, "n/a", "n/a")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Ask too low: {entry_price:.3f} < floor {cfg.entry_min:.2f} (market near-resolved)"

    if agg is not None and side in ("up", "down"):
        vel60 = agg.velocity_60s
        div_ok = vel60 >= -cfg.max_vel_divergence if side == "up" else vel60 <= cfg.max_vel_divergence
        f = FilterResult(
            "vel_divergence",
            div_ok,
            f"30s={agg.velocity_30s:+.3f}% 60s={vel60:+.3f}%",
            f"60s aligned with {side} (max_div={cfg.max_vel_divergence}%)",
        )
    else:
        f = FilterResult("vel_divergence", True, "n/a", "n/a")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Vel divergence: 60s={agg.velocity_60s:+.3f}% opposes {side} direction"

    f = FilterResult("liquidity_depth", entry_depth >= cfg.min_book_depth_usd, f"${entry_depth:.1f}", f">=${cfg.min_book_depth_usd}")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Thin entry liquidity: ${entry_depth:.1f}"

    source_count = agg.source_count if agg else 0
    f = FilterResult("feed_count", source_count >= 2, str(source_count), ">=2")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Only {source_count} live feed(s)"

    if side in ("up", "down"):
        es = max(0.0, entry_price - entry_bid)
        f = FilterResult("entry_spread", es <= cfg.max_entry_spread, f"{es:.3f}", f"<={cfg.max_entry_spread:.3f}")
    else:
        f = FilterResult("entry_spread", True, "n/a", "n/a")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Entry spread too wide: {es:.3f} > {cfg.max_entry_spread:.3f}"

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
    notional_target = cfg.scalp_take_profit_bid
    fee_buy = engine._per_share_fee(entry_price, market.fee_rate)
    fee_sell = engine._per_share_fee(notional_target, market.fee_rate)
    expected_profit = (notional_target - entry_price) - fee_buy - fee_sell
    depth_ratio = engine._safe_ratio(entry_depth, opposite_depth)
    score_payload = engine._repricing_score(
        velocity_30s=agg.velocity_30s if agg else 0.0,
        entry_price=entry_price,
        min_entry=cfg.entry_min,
        max_entry=cfg.entry_max,
        entry_depth=entry_depth,
        min_depth=cfg.min_book_depth_usd,
        spread=max(0.0, entry_price - entry_bid),
        remaining=remaining,
        min_remaining=cfg.min_time_remaining_s,
        max_remaining=cfg.max_time_remaining_s,
        depth_ratio=depth_ratio,
    )

    action = ("DRY_RUN_SIGNAL" if engine.config.execution.dry_run else "TRADE") if all_passed else "SKIP"
    reason = f"Single-leg {side.upper() if side else '?'} @{entry_price:.3f}" if all_passed else failed_reason

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
        "single_leg",
        entry_price=entry_price,
        target_price=notional_target,
        expected_profit_per_share=expected_profit,
        entry_bid=entry_bid,
        entry_ask=entry_price,
        entry_spread=max(0.0, entry_price - entry_bid),
        entry_depth_side_usd=entry_depth,
        opposite_depth_usd=opposite_depth,
        depth_ratio=depth_ratio,
        resignal_cooldown_s=cfg.resignal_cooldown_s,
        min_price_improvement=cfg.min_price_improvement,
        **score_payload,
    )

    if not all_passed:
        return None

    signal = TradeSignal(
        market=market,
        strategy_type="single_leg",
        decision_id=decision_id,
        side=side,
        entry_price=entry_price,
        target_sell_price=notional_target,
        entry_bid=entry_bid,
        entry_ask=entry_price,
        entry_spread=max(0.0, entry_price - entry_bid),
        entry_depth_side_usd=entry_depth,
        opposite_depth_usd=opposite_depth,
        depth_ratio=depth_ratio,
        fee_total=fee_buy + fee_sell,
        expected_profit=expected_profit,
        time_remaining_s=remaining,
        up_book=up_book,
        down_book=down_book,
        filter_results=filters,
        target_delta=max(0.0, notional_target - entry_price),
        hard_stop_delta=max(0.0, entry_price * cfg.loss_cut_pct),
        resignal_cooldown_s=cfg.resignal_cooldown_s,
        min_price_improvement=cfg.min_price_improvement,
        **score_payload,
    )
    logger.info(
        f"{'[DRY RUN] ' if engine.config.execution.dry_run else ''}"
        f"SINGLE-LEG SIGNAL [{coin.upper()}]: BUY {side.upper()}@{entry_price:.3f} -> "
        f"SCALP@{notional_target:.2f} | Est profit: ${expected_profit:.4f}"
    )
    return signal
