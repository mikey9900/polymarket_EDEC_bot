"""Startup recovery helpers for runtime state, live trades, and paper monitors."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.models import MarketInfo, SingleLegPosition, SwingPosition, TradeResult, TradeSignal

logger = logging.getLogger(__name__)

_RECOVERABLE_SINGLE = {"single_leg", "lead_lag"}


def _schedule_background_task(coro):
    return asyncio.create_task(coro)


def snapshot_runtime_state(risk_manager, executor, strategy_engine=None) -> dict[str, object]:
    state = {}
    if risk_manager and hasattr(risk_manager, "snapshot_runtime_state"):
        state.update(risk_manager.snapshot_runtime_state())
    if executor and hasattr(executor, "snapshot_runtime_state"):
        state.update(executor.snapshot_runtime_state())
    if strategy_engine is not None:
        state["mode"] = getattr(strategy_engine, "mode", "off")
        state["strategy_active"] = bool(getattr(strategy_engine, "is_active", False))
    return state


def apply_runtime_state(state: dict[str, object] | None, risk_manager, executor) -> None:
    if risk_manager and hasattr(risk_manager, "restore_runtime_state"):
        risk_manager.restore_runtime_state(state)
    if executor and state is not None and hasattr(executor, "restore_order_size_override"):
        executor.restore_order_size_override(
            state.get("order_size_usd"),
            active=bool(state.get("order_size_override_active", False)),
        )


def apply_strategy_runtime_state(
    strategy_engine,
    state: dict[str, object] | None,
    *,
    default_mode: str,
) -> str:
    desired_mode = str((state or {}).get("mode") or default_mode or "both")
    if not strategy_engine.set_mode(desired_mode):
        desired_mode = default_mode or "both"
        strategy_engine.set_mode(desired_mode)
    if (state or {}).get("paused") or (state or {}).get("kill_switch"):
        strategy_engine.stop_scanning()
    return desired_mode


def _as_float(value: object | None, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: object | None, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: object | None) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_dt(value: object | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fallback_market(row: dict[str, object], config) -> MarketInfo:
    start = _parse_dt(row.get("decision_market_start_time")) or datetime.now(timezone.utc)
    end = _parse_dt(row.get("decision_market_end_time")) or (start + timedelta(minutes=5))
    slug = str(row.get("market_slug") or "")
    coin = str(row.get("coin") or slug.split("-", 1)[0] or "btc").lower()
    return MarketInfo(
        event_id="",
        condition_id="",
        slug=slug,
        coin=coin,
        up_token_id="",
        down_token_id="",
        start_time=start,
        end_time=end,
        fee_rate=0.072,
        tick_size=getattr(config.polymarket, "tick_size", "0.01"),
        neg_risk=False,
    )


async def _market_for_row(scanner, row: dict[str, object], config) -> MarketInfo:
    slug = str(row.get("market_slug") or "")
    coin = str(row.get("coin") or "")
    market = await scanner.get_market_by_slug(slug, coin=coin) if scanner else None
    return market or _fallback_market(row, config)


def _build_signal(row: dict[str, object], market: MarketInfo) -> TradeSignal:
    strategy_type = str(row.get("strategy_type") or "dual_leg")
    return TradeSignal(
        market=market,
        strategy_type=strategy_type,
        decision_id=_as_int(row.get("decision_id")),
        side=str(row.get("side") or ""),
        up_price=_as_float(row.get("up_price")),
        down_price=_as_float(row.get("down_price")),
        combined_cost=_as_float(row.get("combined_cost")),
        entry_price=_as_float(row.get("entry_price")),
        target_sell_price=_as_float(row.get("target_price")),
        fee_total=_as_float(row.get("fee_total")),
    )


def _build_result(row: dict[str, object], signal: TradeSignal) -> TradeResult:
    result = TradeResult(
        signal=signal,
        strategy_type=str(row.get("strategy_type") or signal.strategy_type),
        trade_id=_as_int(row.get("id")),
        up_order_id=str(row.get("up_order_id") or "") or None,
        down_order_id=str(row.get("down_order_id") or "") or None,
        buy_order_id=str(row.get("buy_order_id") or "") or None,
        sell_order_id=str(row.get("sell_order_id") or "") or None,
        side=str(row.get("side") or signal.side),
        total_cost=_as_float(row.get("combined_cost")),
        fee_total=_as_float(row.get("fee_total")),
        shares=_as_float(row.get("shares")),
        shares_requested=_as_float(row.get("shares_requested")),
        shares_filled=_as_float(row.get("shares_filled")),
        blocked_min_5_shares=_as_bool(row.get("blocked_min_5_shares")),
        status=str(row.get("status") or ""),
        abort_cost=_as_float(row.get("abort_cost")),
        error=str(row.get("error") or ""),
        entry_order_submitted_at=str(row.get("entry_order_submitted_at") or ""),
        entry_filled_at=str(row.get("entry_filled_at") or ""),
        entry_time_to_fill_s=_as_float(row.get("entry_time_to_fill_s")),
        entry_limit_price=_as_float(row.get("entry_limit_price")),
        entry_fill_price=_as_float(row.get("entry_fill_price")),
        entry_slippage=_as_float(row.get("entry_slippage")),
        entry_fill_ratio=_as_float(row.get("entry_fill_ratio")),
        exit_order_submitted_at=str(row.get("exit_order_submitted_at") or ""),
        exit_filled_at=str(row.get("exit_filled_at") or ""),
        exit_limit_price=_as_float(row.get("exit_limit_price")),
        exit_fill_price=_as_float(row.get("exit_fill_price")),
        exit_slippage=_as_float(row.get("exit_slippage")),
        exit_reason=str(row.get("exit_reason") or ""),
        exit_price=_as_float(row.get("exit_price")),
        realized_pnl=_as_float(row.get("pnl")),
        time_remaining_s=_as_float(row.get("time_remaining_s")),
        bid_at_exit=_as_float(row.get("bid_at_exit")),
        ask_at_exit=_as_float(row.get("ask_at_exit")),
        exit_spread=_as_float(row.get("exit_spread")),
        max_bid_seen=_as_float(row.get("max_bid_seen")),
        min_bid_seen=_as_float(row.get("min_bid_seen")),
        time_to_max_bid_s=_as_float(row.get("time_to_max_bid_s")),
        time_to_min_bid_s=_as_float(row.get("time_to_min_bid_s")),
        first_profit_time_s=_as_float(row.get("first_profit_time_s")),
        scalp_hit=_as_bool(row.get("scalp_hit")),
        high_confidence_hit=_as_bool(row.get("high_confidence_hit")),
        hold_to_resolution=_as_bool(row.get("hold_to_resolution")),
        mfe=_as_float(row.get("mfe")),
        mae=_as_float(row.get("mae")),
        peak_net_pnl=_as_float(row.get("peak_net_pnl")),
        trough_net_pnl=_as_float(row.get("trough_net_pnl")),
        stall_exit_triggered=_as_bool(row.get("stall_exit_triggered")),
        dynamic_loss_cut_pct=_as_float(row.get("dynamic_loss_cut_pct")),
        loss_pct_at_exit=_as_float(row.get("loss_pct_at_exit")),
        favorable_excursion=_as_float(row.get("favorable_excursion")),
        ever_profitable=_as_bool(row.get("ever_profitable")),
        cancel_repost_count=_as_int(row.get("cancel_repost_count")),
    )
    if result.shares <= 0:
        result.shares = result.shares_filled or result.shares_requested
    return result


def _single_position(row: dict[str, object], market: MarketInfo, signal: TradeSignal) -> SingleLegPosition:
    token_id = market.up_token_id if signal.side == "up" else market.down_token_id
    return SingleLegPosition(
        market=market,
        side=signal.side,
        token_id=token_id,
        entry_price=signal.entry_price,
        target_price=signal.target_sell_price,
        shares=_as_float(row.get("shares_filled")) or _as_float(row.get("shares")) or _as_float(row.get("shares_requested")),
        buy_order_id=str(row.get("buy_order_id") or ""),
        sell_order_id=str(row.get("sell_order_id") or "") or None,
        strategy_type=signal.strategy_type,
        decision_id=_as_int(row.get("decision_id")),
        trade_id=_as_int(row.get("id")),
        requested_shares=_as_float(row.get("shares_requested")) or _as_float(row.get("shares")),
        hold_to_resolution=_as_bool(row.get("hold_to_resolution")),
        pending_exit_reason=str(row.get("exit_reason") or ""),
        pending_exit_price=_as_float(row.get("exit_limit_price")),
        entry_order_submitted_at=str(row.get("entry_order_submitted_at") or ""),
        entry_filled_at=str(row.get("entry_filled_at") or ""),
        cancel_repost_count=_as_int(row.get("cancel_repost_count")),
    )


def _swing_position(row: dict[str, object], market: MarketInfo, signal: TradeSignal) -> SwingPosition:
    token_id = market.up_token_id if signal.side == "up" else market.down_token_id
    return SwingPosition(
        market=market,
        first_side=signal.side,
        first_token_id=token_id,
        first_entry_price=signal.entry_price,
        first_shares=_as_float(row.get("shares_filled")) or _as_float(row.get("shares")) or _as_float(row.get("shares_requested")),
        decision_id=_as_int(row.get("decision_id")),
        trade_id=_as_int(row.get("id")),
        requested_shares=_as_float(row.get("shares_requested")) or _as_float(row.get("shares")),
        first_buy_order_id=str(row.get("buy_order_id") or ""),
        sell_order_id=str(row.get("sell_order_id") or "") or None,
        hold_to_resolution=_as_bool(row.get("hold_to_resolution")),
        pending_exit_reason=str(row.get("exit_reason") or ""),
        pending_exit_price=_as_float(row.get("exit_limit_price")),
        entry_order_submitted_at=str(row.get("entry_order_submitted_at") or ""),
        entry_filled_at=str(row.get("entry_filled_at") or ""),
        cancel_repost_count=_as_int(row.get("cancel_repost_count")),
    )


async def _recover_submitted_single(engine, row: dict[str, object], market: MarketInfo, signal: TradeSignal, result: TradeResult) -> str:
    if not engine.client or not result.buy_order_id:
        logger.warning("Skipping submitted single-leg recovery for %s; no live client/order id", market.slug)
        return "skipped"
    position = _single_position(row, market, signal)
    engine._pending_single_entries[position.buy_order_id] = position
    engine.reserve_buy_order(position.buy_order_id, position.entry_price * (position.requested_shares or position.shares))
    try:
        order_status = await asyncio.to_thread(engine.client.get_order, position.buy_order_id)
    except Exception as exc:
        logger.warning("Submitted single-leg recovery status check failed for %s: %s", market.slug, exc)
        _schedule_background_task(engine._monitor_single_leg_entry(position, result))
        return "pending"
    status = engine._response_status(order_status)
    if engine._filled_shares(order_status, position.requested_shares or position.shares) > 0:
        await engine._monitor_single_leg_entry(position, result)
        return "open"
    if engine._is_terminal_order_state(status):
        engine._pending_single_entries.pop(position.buy_order_id, None)
        engine.release_buy_order(position.buy_order_id)
        if result.trade_id:
            engine.tracker.update_live_trade(
                result.trade_id,
                status="failed",
                error=f"Recovered entry ended without fill (status={status or 'unknown'})",
            )
        return "terminal"
    _schedule_background_task(engine._monitor_single_leg_entry(position, result))
    return "pending"


async def _recover_open_single(engine, row: dict[str, object], market: MarketInfo, signal: TradeSignal, result: TradeResult) -> str:
    position = _single_position(row, market, signal)
    engine._open_positions[position.buy_order_id] = position
    engine.risk_manager.open_position(result)
    if position.sell_order_id:
        engine.reserve_sell_order(position.sell_order_id, position.token_id, position.shares)
    if position.token_id:
        _schedule_background_task(engine._monitor_single_leg(position, result))
        return "monitoring"
    logger.warning("Recovered single-leg position for %s without token ids; resolution-only tracking", market.slug)
    return "resolution_only"


async def _recover_submitted_swing(engine, row: dict[str, object], market: MarketInfo, signal: TradeSignal, result: TradeResult) -> str:
    if not engine.client or not result.buy_order_id:
        logger.warning("Skipping submitted swing recovery for %s; no live client/order id", market.slug)
        return "skipped"
    position = _swing_position(row, market, signal)
    engine._pending_swing_entries[market.coin] = position
    engine.reserve_buy_order(position.first_buy_order_id, position.first_entry_price * (position.requested_shares or position.first_shares))
    try:
        order_status = await asyncio.to_thread(engine.client.get_order, position.first_buy_order_id)
    except Exception as exc:
        logger.warning("Submitted swing recovery status check failed for %s: %s", market.slug, exc)
        _schedule_background_task(engine._monitor_swing_entry(position, result))
        return "pending"
    status = engine._response_status(order_status)
    if engine._filled_shares(order_status, position.requested_shares or position.first_shares) > 0:
        await engine._monitor_swing_entry(position, result)
        return "open"
    if engine._is_terminal_order_state(status):
        engine._pending_swing_entries.pop(market.coin, None)
        engine.release_buy_order(position.first_buy_order_id)
        if result.trade_id:
            engine.tracker.update_live_trade(
                result.trade_id,
                status="failed",
                error=f"Recovered swing entry ended without fill (status={status or 'unknown'})",
            )
        return "terminal"
    _schedule_background_task(engine._monitor_swing_entry(position, result))
    return "pending"


async def _recover_open_swing(engine, row: dict[str, object], market: MarketInfo, signal: TradeSignal, result: TradeResult) -> str:
    position = _swing_position(row, market, signal)
    engine._open_swing_positions[market.coin] = position
    engine.risk_manager.open_position(result)
    if position.sell_order_id:
        engine.reserve_sell_order(position.sell_order_id, position.first_token_id, position.first_shares)
    if position.first_token_id:
        _schedule_background_task(engine._monitor_swing_leg(position, result))
        return "monitoring"
    logger.warning("Recovered swing position for %s without token ids; resolution-only tracking", market.slug)
    return "resolution_only"


async def _recover_live_trade(engine, scanner, row: dict[str, object]) -> str:
    market = await _market_for_row(scanner, row, engine.config)
    signal = _build_signal(row, market)
    result = _build_result(row, signal)
    strategy_type = signal.strategy_type
    status = str(row.get("status") or "").lower()
    if strategy_type in _RECOVERABLE_SINGLE:
        if status == "submitted":
            return await _recover_submitted_single(engine, row, market, signal, result)
        if status == "open":
            return await _recover_open_single(engine, row, market, signal, result)
    if strategy_type == "swing_leg":
        if status == "submitted":
            return await _recover_submitted_swing(engine, row, market, signal, result)
        if status == "open":
            return await _recover_open_swing(engine, row, market, signal, result)
    if strategy_type == "dual_leg" and status == "success":
        engine.risk_manager.open_position(result)
        return "resolution_only"
    logger.debug("Skipping unsupported recoverable trade row %s (%s/%s)", row.get("id"), strategy_type, status)
    return "skipped"


async def _recover_paper_trade(engine, scanner, row: dict[str, object]) -> str:
    market = await _market_for_row(scanner, row, engine.config)
    now = datetime.now(timezone.utc)
    if market.end_time <= now:
        return "waiting_resolution"
    strategy_type = str(row.get("strategy_type") or "")
    side = str(row.get("side") or "")
    token_id = market.up_token_id if side == "up" else market.down_token_id
    if strategy_type in _RECOVERABLE_SINGLE:
        if not token_id:
            return "skipped"
        _schedule_background_task(
            engine._monitor_paper_single_leg(
                trade_id=_as_int(row.get("id")),
                market=market,
                token_id=token_id,
                entry_price=_as_float(row.get("entry_price")),
                target_sell=_as_float(row.get("target_price")),
                shares=_as_float(row.get("shares")),
                strategy_type=strategy_type,
            )
        )
        return "monitoring"
    if strategy_type == "swing_leg":
        if not token_id:
            return "skipped"
        position = SwingPosition(
            market=market,
            first_side=side,
            first_token_id=token_id,
            first_entry_price=_as_float(row.get("entry_price")),
            first_shares=_as_float(row.get("shares")),
            decision_id=_as_int(row.get("decision_id")),
            first_paper_trade_id=_as_int(row.get("id")),
        )
        engine._open_swing_positions[market.coin] = position
        _schedule_background_task(engine._monitor_swing_leg(position))
        return "monitoring"
    return "waiting_resolution"


async def recover_runtime(engine, tracker, scanner) -> dict[str, int]:
    summary = {
        "live_rows": 0,
        "live_monitors": 0,
        "live_pending": 0,
        "live_resolution_only": 0,
        "paper_rows": 0,
        "paper_monitors": 0,
    }
    for row in tracker.get_recoverable_live_trades():
        summary["live_rows"] += 1
        outcome = await _recover_live_trade(engine, scanner, row)
        if outcome == "monitoring" or outcome == "open":
            summary["live_monitors"] += 1
        elif outcome == "pending":
            summary["live_pending"] += 1
        elif outcome == "resolution_only":
            summary["live_resolution_only"] += 1
    for row in tracker.get_open_paper_trades():
        summary["paper_rows"] += 1
        outcome = await _recover_paper_trade(engine, scanner, row)
        if outcome == "monitoring":
            summary["paper_monitors"] += 1
    return summary
