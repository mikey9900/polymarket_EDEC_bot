"""Swing-leg evaluator."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bot.models import FilterResult, TradeSignal

logger = logging.getLogger(__name__)


def evaluate(engine: Any, coin, market, up_book, down_book, agg) -> TradeSignal | None:
    """Run the swing mean-reversion filter chain."""
    cfg = engine.config.swing_leg
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
        failed_reason = f"Only {remaining:.0f}s left - not enough time to leg in"

    f = FilterResult("entry_window", remaining <= cfg.max_time_remaining_s, f"{remaining:.0f}s", f"<={cfg.max_time_remaining_s:.0f}s")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Too early: {remaining:.0f}s remaining (wait for direction)"

    if coin in cfg.disabled_coins:
        if not failed_reason:
            failed_reason = f"Swing disabled on {coin.upper()} (momentum-driven, not mean-reversion)"
        engine._log_decision(coin, market, up_book, down_book, agg, remaining, filters, "SKIP", failed_reason, "swing_leg")
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
        engine._log_decision(coin, market, up_book, down_book, agg, remaining, filters, "SKIP", failed_reason, "swing_leg")
        return None

    up_cheap = up_book.best_ask <= cfg.first_leg_max
    dn_cheap = down_book.best_ask <= cfg.first_leg_max
    entry_ok = up_cheap or dn_cheap

    if up_cheap and not dn_cheap:
        side = "up"
        entry_price = up_book.best_ask
        entry_bid = up_book.best_bid
        entry_depth = up_book.ask_depth_usd
        other_depth = down_book.ask_depth_usd
    elif dn_cheap and not up_cheap:
        side = "down"
        entry_price = down_book.best_ask
        entry_bid = down_book.best_bid
        entry_depth = down_book.ask_depth_usd
        other_depth = up_book.ask_depth_usd
    elif up_cheap and dn_cheap:
        if up_book.best_ask <= down_book.best_ask:
            side = "up"
            entry_price = up_book.best_ask
            entry_bid = up_book.best_bid
            entry_depth = up_book.ask_depth_usd
            other_depth = down_book.ask_depth_usd
        else:
            side = "down"
            entry_price = down_book.best_ask
            entry_bid = down_book.best_bid
            entry_depth = down_book.ask_depth_usd
            other_depth = up_book.ask_depth_usd
    else:
        side = ""
        entry_price = 0.0
        entry_bid = 0.0
        entry_depth = 0.0
        other_depth = 0.0

    f = FilterResult(
        "first_leg_price",
        entry_ok,
        f"up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f}",
        f"one side<={cfg.first_leg_max}",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = (
            f"Neither side cheap enough: up={up_book.best_ask:.3f}, down={down_book.best_ask:.3f} "
            f"(need one <={cfg.first_leg_max})"
        )

    if side in ("up", "down"):
        floor_ok = entry_price >= cfg.first_leg_min
        f = FilterResult("first_leg_floor", floor_ok, f"{entry_price:.3f}", f">={cfg.first_leg_min:.2f}")
    else:
        f = FilterResult("first_leg_floor", True, "n/a", "n/a")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = (
            f"First leg ask too low: {entry_price:.3f} < floor {cfg.first_leg_min:.2f} "
            f"(market near-resolved, no recovery possible)"
        )

    if agg is not None and side in ("up", "down"):
        vel30 = agg.velocity_30s
        neutral_ok = vel30 <= 0 if side == "up" else vel30 >= 0
        expected = "vel_30s<=0 for UP" if side == "up" else "vel_30s>=0 for DOWN"
        f = FilterResult("directional_neutrality", neutral_ok, f"vel_30s={vel30:+.3f}% side={side}", expected)
    else:
        f = FilterResult("directional_neutrality", True, "n/a", "n/a")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Fading momentum: vel_30s={agg.velocity_30s:+.3f}% aligns with {side} - not a mean-reversion setup"

    combined = up_book.best_ask + down_book.best_ask
    not_already_arb = combined > engine.config.dual_leg.max_combined_cost
    f = FilterResult(
        "not_already_arb",
        not_already_arb,
        f"combined={combined:.3f}",
        f">{engine.config.dual_leg.max_combined_cost} (dual-leg handles cheaper)",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Already in arb range ({combined:.3f}) - dual-leg preferred"

    if agg is not None:
        vel_ok = abs(agg.velocity_30s) <= cfg.max_velocity_30s
        f = FilterResult("coin_velocity", vel_ok, f"30s={agg.velocity_30s:.3f}%", f"<={cfg.max_velocity_30s}%")
    else:
        f = FilterResult("coin_velocity", False, "no price data", "price data required")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"{coin.upper()} trending too hard: {f.value}"

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

    up_depth = up_book.ask_depth_usd
    dn_depth = down_book.ask_depth_usd
    min_depth = min(up_depth, dn_depth)
    if min_depth > 0:
        depth_ratio = max(up_depth, dn_depth) / min_depth
        sym_ok = depth_ratio <= cfg.max_depth_ratio
    else:
        depth_ratio = float("inf")
        sym_ok = False
    f = FilterResult(
        "liquidity_symmetry",
        sym_ok,
        f"ratio={depth_ratio:.1f}x (up=${up_depth:.1f}, dn=${dn_depth:.1f})",
        f"<={cfg.max_depth_ratio}x",
    )
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Asymmetric books: {depth_ratio:.1f}x ratio - second leg unlikely to fill"

    f = FilterResult("liquidity_depth", entry_depth >= cfg.min_book_depth_usd, f"${entry_depth:.1f}", f">=${cfg.min_book_depth_usd}")
    filters.append(f)
    if not f.passed and not failed_reason:
        failed_reason = f"Thin liquidity: ${entry_depth:.1f}"

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
    reason = f"Swing {side.upper()}@{entry_price:.3f} - waiting for other leg" if all_passed else failed_reason

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
        "swing_leg",
    )

    if not all_passed:
        return None

    depth_ratio = (entry_depth / other_depth) if other_depth else 0.0
    signal = TradeSignal(
        market=market,
        strategy_type="swing_leg",
        decision_id=decision_id,
        side=side,
        entry_price=entry_price,
        target_sell_price=cfg.first_leg_exit,
        entry_bid=entry_bid,
        entry_ask=entry_price,
        entry_spread=max(0.0, entry_price - entry_bid),
        entry_depth_side_usd=entry_depth,
        opposite_depth_usd=other_depth,
        depth_ratio=depth_ratio if depth_ratio is not None else 0.0,
        fee_total=engine._per_share_fee(entry_price, market.fee_rate),
        expected_profit=0.0,
        time_remaining_s=remaining,
        up_book=up_book,
        down_book=down_book,
        filter_results=filters,
    )
    logger.info(
        f"{'[DRY RUN] ' if engine.config.execution.dry_run else ''}"
        f"SWING SIGNAL [{coin.upper()}]: BUY {side.upper()}@{entry_price:.3f} "
        f"| other leg={combined - entry_price:.3f} "
        f"| exit if no 2nd leg @{cfg.first_leg_exit:.2f}"
    )
    return signal
