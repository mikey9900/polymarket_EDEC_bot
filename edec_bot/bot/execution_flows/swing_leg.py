"""Swing-leg execution flows."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import Any

from bot.models import SwingPosition, TradeResult, TradeSignal

logger = logging.getLogger(__name__)


async def execute(engine: Any, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
    """Buy first leg of a swing trade and start monitoring for the second leg."""
    result = TradeResult(signal=signal, strategy_type="swing_leg", side=signal.side, status="pending")
    resolved_decision_id = signal.decision_id or decision_id
    entry_submitted_at = datetime.now(timezone.utc)
    result.entry_order_submitted_at = entry_submitted_at.isoformat()
    result.entry_limit_price = signal.entry_price
    market = signal.market
    coin = market.coin.upper()

    if market.coin in engine._open_swing_positions or market.coin in engine._pending_swing_entries:
        result.status = "failed"
        result.error = f"Swing position already open for {coin}"
        logger.debug(result.error)
        return result

    token_id = market.up_token_id if signal.side == "up" else market.down_token_id
    order_size = engine._strategy_order_size_usd(signal.strategy_type, signal)
    shares = math.floor(order_size / signal.entry_price)
    result.shares_requested = shares
    result.shares_filled = 0.0

    if engine.config.execution.dry_run:
        result.status = "dry_run"
        result.shares = shares
        result.shares_filled = shares
        result.entry_filled_at = result.entry_order_submitted_at
        result.entry_time_to_fill_s = 0.0
        result.entry_fill_ratio = 1.0
        result.entry_fill_price = signal.entry_price
        result.total_cost = signal.entry_price * shares
        cost = signal.entry_price * shares

        if not engine.tracker.has_paper_capital(cost):
            logger.info(f"[DRY RUN] [{coin}] Swing skipped - insufficient paper capital")
            result.status = "failed"
            return result

        trade_id = engine.tracker.log_paper_trade(
            market.slug,
            market.coin,
            "swing_leg",
            signal.side,
            signal.entry_price,
            signal.target_sell_price,
            shares,
            signal.fee_total * shares,
            decision_id=resolved_decision_id,
            market_end_time=market.end_time.isoformat(),
            market_start_time=market.start_time.isoformat(),
            signal_context=signal.signal_context,
            signal_overlap_count=signal.signal_overlap_count,
            order_size_usd=order_size,
            shares_requested=shares,
            shares_filled=shares,
            entry_bid=signal.entry_bid,
            entry_ask=signal.entry_ask or signal.entry_price,
            entry_spread=signal.entry_spread,
            entry_depth_side_usd=signal.entry_depth_side_usd,
            opposite_depth_usd=signal.opposite_depth_usd,
            depth_ratio=signal.depth_ratio,
            window_id=market.slug,
            signal_score=signal.signal_score,
            score_velocity=signal.score_velocity,
            score_entry=signal.score_entry,
            score_depth=signal.score_depth,
            score_spread=signal.score_spread,
            score_time=signal.score_time,
            score_balance=signal.score_balance,
            target_delta=signal.target_delta,
            hard_stop_delta=signal.hard_stop_delta,
        )
        logger.info(
            f"[DRY RUN] [{coin}] SWING: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
            f"| {shares} shares, cost=${cost:.2f} | exit target@{signal.target_sell_price:.2f}"
        )
        position = SwingPosition(
            market=market,
            first_side=signal.side,
            first_token_id=token_id,
            first_entry_price=signal.entry_price,
            first_shares=shares,
            decision_id=resolved_decision_id,
            first_paper_trade_id=trade_id,
        )
        engine._open_swing_positions[market.coin] = position
        engine._schedule_background_task(engine._monitor_swing_leg(position))
        return result

    if shares < 5:
        result.status = "failed"
        result.blocked_min_5_shares = True
        result.error = f"Shares too small: {shares}"
        return result

    entry_cost = signal.entry_price * shares
    if not await engine.can_submit_buy_order(entry_cost):
        result.status = "failed"
        result.error = f"Insufficient collateral after reservations (need ${entry_cost:.2f})"
        logger.warning(f"[{coin}] {result.error}")
        return result

    position = None
    try:
        buy_order = await asyncio.to_thread(
            engine.client.create_order,
            {"token_id": token_id, "price": signal.entry_price, "size": shares, "side": "BUY"},
            {"tick_size": market.tick_size, "neg_risk": market.neg_risk},
        )
        buy_resp = await asyncio.to_thread(engine.client.post_order, buy_order, "GTC")
        buy_order_id = buy_resp.get("orderID", buy_resp.get("id", ""))

        if not buy_order_id:
            result.status = "failed"
            result.error = f"Swing buy rejected: {buy_resp}"
            logger.warning(f"[{coin}] {result.error}")
            return result

        result.buy_order_id = buy_order_id
        result.total_cost = signal.entry_price * shares
        result.shares = shares
        position = SwingPosition(
            market=market,
            first_side=signal.side,
            first_token_id=token_id,
            first_entry_price=signal.entry_price,
            first_shares=shares,
            decision_id=resolved_decision_id,
            first_buy_order_id=buy_order_id,
            entry_order_submitted_at=result.entry_order_submitted_at,
            requested_shares=shares,
        )
        filled_shares = engine._filled_shares(buy_resp, shares)
        if engine._has_any_fill(buy_resp):
            actual_shares = filled_shares or shares
            if actual_shares < shares and not engine._is_order_filled(buy_resp, shares):
                try:
                    await asyncio.to_thread(engine.client.cancel, buy_order_id)
                except Exception as cancel_err:
                    logger.warning(f"[{coin}] Cancel partially filled swing entry failed: {cancel_err}")
            position.first_shares = actual_shares
            result.status = "open"
            result.shares = actual_shares
            result.shares_filled = actual_shares
            result.total_cost = signal.entry_price * actual_shares
            result.fee_total = engine._per_share_fee(signal.entry_price, market.fee_rate) * actual_shares
            fill_time = datetime.now(timezone.utc)
            result.entry_filled_at = fill_time.isoformat()
            result.entry_time_to_fill_s = max(
                0.0,
                (fill_time - entry_submitted_at).total_seconds(),
            )
            result.entry_fill_price = engine._filled_price(buy_resp, signal.entry_price)
            result.entry_slippage = result.entry_fill_price - signal.entry_price
            result.entry_fill_ratio = actual_shares / max(shares, 1e-9)
            position.entry_filled_at = result.entry_filled_at
            engine._open_swing_positions[market.coin] = position
            engine._schedule_background_task(engine._monitor_swing_leg(position, result))
            logger.info(
                f"[{coin}] SWING LEG 1 filled: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"({actual_shares:.0f} shares, order {buy_order_id})"
            )
            return result

        result.status = "submitted"
        engine.reserve_buy_order(buy_order_id, entry_cost)
        engine._pending_swing_entries[market.coin] = position
        engine._schedule_background_task(engine._monitor_swing_entry(position, result))
        logger.info(
            f"[{coin}] SWING LEG 1 resting on book: BUY {signal.side.upper()}@{signal.entry_price:.3f} "
            f"({shares} shares, order {buy_order_id}, status={engine._response_status(buy_resp) or 'unknown'})"
        )

    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)
        logger.error(f"[{coin}] Swing execution error: {exc}")

    finally:
        if resolved_decision_id and result.status not in ("dry_run", "failed"):
            result.trade_id = engine.tracker.log_trade(resolved_decision_id, result)
            if position is not None:
                position.trade_id = result.trade_id
        if result.status != "dry_run":
            engine.risk_manager.record_attempt()
            if result.status == "open":
                engine.risk_manager.open_position(result)

    return result


async def monitor_entry(engine: Any, position: SwingPosition, result: TradeResult):
    """Wait for a resting swing entry to fill before opening the live position."""
    coin = position.market.coin.upper()
    while True:
        await asyncio.sleep(1)
        now = datetime.now(timezone.utc)
        if now >= position.market.end_time:
            engine._pending_swing_entries.pop(position.market.coin, None)
            canceled = await engine.cancel_live_order(position.first_buy_order_id)
            if canceled:
                engine.release_buy_order(position.first_buy_order_id)
            if result.trade_id and hasattr(engine.tracker, "update_live_trade"):
                engine.tracker.update_live_trade(
                    result.trade_id,
                    status="failed",
                    error="Swing entry order expired before fill",
                )
            logger.info(f"[{coin}] Swing entry order expired before fill: {position.first_buy_order_id}")
            return

        try:
            order_status = await asyncio.to_thread(engine.client.get_order, position.first_buy_order_id)
        except Exception as exc:
            logger.debug(f"[{coin}] Swing entry status check failed: {exc}")
            continue

        requested_shares = position.requested_shares or position.first_shares
        filled_shares = engine._filled_shares(order_status, requested_shares)
        if filled_shares > 0:
            if filled_shares < requested_shares and not engine._is_terminal_order_state(engine._response_status(order_status)):
                try:
                    await asyncio.to_thread(engine.client.cancel, position.first_buy_order_id)
                except Exception as cancel_err:
                    logger.warning(f"[{coin}] Cancel partially filled swing entry failed: {cancel_err}")
            actual_shares = filled_shares or requested_shares
            position.first_shares = actual_shares
            result.status = "open"
            result.shares = actual_shares
            result.shares_filled = actual_shares
            result.total_cost = position.first_entry_price * actual_shares
            result.fee_total = engine._per_share_fee(position.first_entry_price, position.market.fee_rate) * actual_shares
            result.entry_filled_at = datetime.now(timezone.utc).isoformat()
            submitted_at = position.entry_order_submitted_at or result.entry_order_submitted_at
            if submitted_at:
                try:
                    start = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
                    result.entry_time_to_fill_s = max(
                        0.0,
                        (datetime.now(timezone.utc) - start).total_seconds(),
                    )
                except Exception:
                    result.entry_time_to_fill_s = 0.0
            result.entry_fill_price = engine._filled_price(order_status, position.first_entry_price)
            result.entry_slippage = result.entry_fill_price - position.first_entry_price
            result.entry_fill_ratio = actual_shares / max(requested_shares, 1e-9)
            position.entry_filled_at = result.entry_filled_at
            engine._pending_swing_entries.pop(position.market.coin, None)
            engine.release_buy_order(position.first_buy_order_id)
            engine._open_swing_positions[position.market.coin] = position
            if result.trade_id and hasattr(engine.tracker, "update_live_trade"):
                engine.tracker.update_live_trade(
                    result.trade_id,
                    status="open",
                    total_cost=result.total_cost,
                    fee_total=result.fee_total,
                    shares=result.shares,
                    shares_requested=result.shares_requested,
                    shares_filled=result.shares_filled,
                    entry_filled_at=result.entry_filled_at,
                    entry_time_to_fill_s=result.entry_time_to_fill_s,
                    entry_fill_price=result.entry_fill_price,
                    entry_slippage=result.entry_slippage,
                    entry_fill_ratio=result.entry_fill_ratio,
                )
                position.trade_id = result.trade_id
            elif result.signal.decision_id:
                result.trade_id = engine.tracker.log_trade(result.signal.decision_id, result)
                position.trade_id = result.trade_id
            engine.risk_manager.open_position(result)
            logger.info(
                f"[{coin}] SWING entry filled: BUY {position.first_side.upper()}@{position.first_entry_price:.3f} "
                f"({actual_shares:.0f} shares, order {position.first_buy_order_id})"
            )
            await engine._monitor_swing_leg(position, result)
            return

        status = engine._response_status(order_status)
        if engine._is_terminal_order_state(status):
            engine._pending_swing_entries.pop(position.market.coin, None)
            engine.release_buy_order(position.first_buy_order_id)
            if result.trade_id and hasattr(engine.tracker, "update_live_trade"):
                engine.tracker.update_live_trade(
                    result.trade_id,
                    status="failed",
                    error=f"Swing entry ended without fill (status={status or 'unknown'})",
                )
            logger.info(
                f"[{coin}] Swing entry ended without fill: {position.first_buy_order_id} "
                f"(status={status or 'unknown'})"
            )
            return


async def monitor_position(engine: Any, position: SwingPosition, result: TradeResult | None = None):
    """Monitor a swing mean-reversion position with smart exit logic."""
    cfg = engine.config.swing_leg
    coin = position.market.coin.upper()
    is_dry = engine.config.execution.dry_run
    monitor_started_at = datetime.now(timezone.utc)
    max_bid_seen = None
    min_bid_seen = None
    time_to_max_bid_s = None
    time_to_min_bid_s = None
    first_profit_time_s = None
    high_confidence_hit = False
    peak_net_pnl = None
    trough_net_pnl = None
    mfe = None
    mae = None
    ever_profitable = False
    if result is not None:
        max_bid_seen = result.max_bid_seen or None
        min_bid_seen = result.min_bid_seen or None
        time_to_max_bid_s = result.time_to_max_bid_s or None
        time_to_min_bid_s = result.time_to_min_bid_s or None
        first_profit_time_s = result.first_profit_time_s or None
        high_confidence_hit = bool(result.high_confidence_hit)
        peak_net_pnl = result.peak_net_pnl if result.peak_net_pnl else None
        trough_net_pnl = result.trough_net_pnl if result.trough_net_pnl else None
        mfe = result.mfe if result.mfe else None
        mae = result.mae if result.mae else None
        ever_profitable = bool(result.ever_profitable)

    while True:
        await engine._wait_book_update()

        now = datetime.now(timezone.utc)
        remaining = (position.market.end_time - now).total_seconds()

        if remaining <= 0:
            if is_dry:
                engine._open_swing_positions.pop(position.market.coin, None)
                logger.info(f"[{coin}] Swing monitor ended - market closed")
            else:
                logger.info(f"[{coin}] Swing position awaiting market resolution")
            return

        if not is_dry and position.sell_order_id and result is not None:
            try:
                order_status = await asyncio.to_thread(engine.client.get_order, position.sell_order_id)
                status = engine._response_status(order_status)
                if engine._is_order_filled(order_status, position.first_shares):
                    engine.release_sell_order(position.sell_order_id)
                    exit_price = engine._filled_price(order_status, position.pending_exit_price or position.first_entry_price)
                    actual_profit = engine._net_pnl(
                        position.first_entry_price,
                        exit_price,
                        position.market.fee_rate,
                        position.first_shares,
                    )
                    result.status = "closed_win" if actual_profit > 0 else "closed_loss"
                    result.sell_order_id = position.sell_order_id
                    result.exit_reason = position.pending_exit_reason or "manual"
                    result.exit_filled_at = datetime.now(timezone.utc).isoformat()
                    result.exit_price = exit_price
                    result.exit_fill_price = exit_price
                    result.exit_slippage = exit_price - (result.exit_limit_price or position.pending_exit_price or exit_price)
                    result.realized_pnl = actual_profit
                    result.time_remaining_s = remaining
                    result.loss_pct_at_exit = max(
                        0.0,
                        (position.first_entry_price - exit_price) / max(position.first_entry_price, 1e-9),
                    )
                    result.favorable_excursion = max(0.0, (max_bid_seen or position.first_entry_price) - position.first_entry_price)
                    engine.tracker.close_live_trade(
                        result.trade_id,
                        status=result.status,
                        exit_reason=result.exit_reason,
                        exit_price=exit_price,
                        pnl=actual_profit,
                        time_remaining_s=remaining,
                        exit_limit_price=result.exit_limit_price or position.pending_exit_price,
                        exit_fill_price=exit_price,
                        max_bid_seen=max_bid_seen,
                        min_bid_seen=min_bid_seen,
                        time_to_max_bid_s=time_to_max_bid_s,
                        time_to_min_bid_s=time_to_min_bid_s,
                        first_profit_time_s=first_profit_time_s,
                        high_confidence_hit=high_confidence_hit,
                        hold_to_resolution=position.hold_to_resolution,
                        mfe=mfe,
                        mae=mae,
                        peak_net_pnl=peak_net_pnl,
                        trough_net_pnl=trough_net_pnl,
                        dynamic_loss_cut_pct=result.dynamic_loss_cut_pct or None,
                        loss_pct_at_exit=result.loss_pct_at_exit,
                        favorable_excursion=result.favorable_excursion,
                        ever_profitable=ever_profitable,
                        cancel_repost_count=position.cancel_repost_count,
                    )
                    engine.risk_manager.close_position(result, actual_profit)
                    engine._open_swing_positions.pop(position.market.coin, None)
                    logger.info(f"[{coin}] SWING SELL FILLED @{exit_price:.3f} pnl=${actual_profit:+.4f}")
                    return
                if engine._is_terminal_order_state(status):
                    engine.release_sell_order(position.sell_order_id)
                    position.sell_order_id = None
                    result.sell_order_id = None
                    position.pending_exit_reason = ""
                    position.pending_exit_price = 0.0
                    if result.trade_id:
                        engine.tracker.update_live_trade(
                            result.trade_id,
                            sell_order_id="",
                            status="open",
                            cancel_repost_count=position.cancel_repost_count,
                        )
                else:
                    continue
            except Exception as exc:
                logger.debug(f"[{coin}] Swing sell status check failed: {exc}")
                continue

        try:
            first_book = await engine._fetch_book_price(position.first_token_id)
        except Exception as exc:
            logger.debug(f"[{coin}] Swing book fetch error: {exc}")
            continue

        bid = first_book.best_bid
        ask = first_book.best_ask
        loss_pct = (position.first_entry_price - bid) / position.first_entry_price if bid > 0 else 1.0
        fee_buy = engine._per_share_fee(position.first_entry_price, position.market.fee_rate)
        fee_sell = engine._per_share_fee(bid, position.market.fee_rate)
        net_pnl = (bid - position.first_entry_price - fee_buy - fee_sell) * position.first_shares
        elapsed_s = max(0.0, (datetime.now(timezone.utc) - monitor_started_at).total_seconds())
        changed = False
        if bid > 0 and (max_bid_seen is None or bid > max_bid_seen):
            max_bid_seen = bid
            time_to_max_bid_s = elapsed_s
            changed = True
        if bid > 0 and (min_bid_seen is None or bid < min_bid_seen):
            min_bid_seen = bid
            time_to_min_bid_s = elapsed_s
            changed = True
        if peak_net_pnl is None or net_pnl > peak_net_pnl:
            peak_net_pnl = net_pnl
            changed = True
        if trough_net_pnl is None or net_pnl < trough_net_pnl:
            trough_net_pnl = net_pnl
            changed = True
        favorable_excursion = max(0.0, (max_bid_seen or bid) - position.first_entry_price)
        mfe = favorable_excursion
        mae = max(0.0, position.first_entry_price - (min_bid_seen or bid))
        if not ever_profitable and net_pnl > 0:
            ever_profitable = True
            changed = True
        if result is not None:
            result.max_bid_seen = max_bid_seen or 0.0
            result.min_bid_seen = min_bid_seen or 0.0
            result.time_to_max_bid_s = time_to_max_bid_s or 0.0
            result.time_to_min_bid_s = time_to_min_bid_s or 0.0
            result.first_profit_time_s = first_profit_time_s or 0.0
            result.high_confidence_hit = high_confidence_hit
            result.hold_to_resolution = position.hold_to_resolution
            result.mfe = mfe or 0.0
            result.mae = mae or 0.0
            result.peak_net_pnl = peak_net_pnl or 0.0
            result.trough_net_pnl = trough_net_pnl or 0.0
            result.favorable_excursion = favorable_excursion
            result.ever_profitable = ever_profitable
            result.cancel_repost_count = position.cancel_repost_count

        if bid >= cfg.high_confidence_bid:
            high_confidence_hit = True
            if is_dry and position.first_paper_trade_id:
                engine.tracker.record_paper_trade_path(
                    position.first_paper_trade_id,
                    max_bid_seen=max_bid_seen,
                    min_bid_seen=min_bid_seen,
                    time_to_max_bid_s=time_to_max_bid_s,
                    time_to_min_bid_s=time_to_min_bid_s,
                    first_profit_time_s=first_profit_time_s,
                    high_confidence_hit=True,
                    hold_to_resolution=True,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
            elif result is not None and result.trade_id:
                result.high_confidence_hit = True
                result.hold_to_resolution = True
                engine.tracker.record_live_trade_path(
                    result.trade_id,
                    max_bid_seen=max_bid_seen,
                    min_bid_seen=min_bid_seen,
                    time_to_max_bid_s=time_to_max_bid_s,
                    time_to_min_bid_s=time_to_min_bid_s,
                    first_profit_time_s=first_profit_time_s,
                    high_confidence_hit=True,
                    hold_to_resolution=True,
                    mfe=mfe,
                    mae=mae,
                    peak_net_pnl=peak_net_pnl,
                    trough_net_pnl=trough_net_pnl,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                    cancel_repost_count=position.cancel_repost_count,
                )
                engine.tracker.update_live_trade(
                    result.trade_id,
                    hold_to_resolution=True,
                    cancel_repost_count=position.cancel_repost_count,
                )
            logger.info(
                f"[{coin}] SWING HIGH-CONFIDENCE @{bid:.3f} - "
                f"holding {position.first_side.upper()} to resolution ({remaining:.0f}s)"
            )
            if is_dry:
                engine._open_swing_positions.pop(position.market.coin, None)
            else:
                position.hold_to_resolution = True
            return

        time_factor = min(cfg.loss_cut_max_factor, remaining / cfg.time_pressure_s)
        dynamic_loss_cut = cfg.loss_cut_pct * time_factor

        if first_profit_time_s is None:
            if net_pnl > 0:
                first_profit_time_s = elapsed_s
                changed = True

        if is_dry and position.first_paper_trade_id and changed:
            engine.tracker.record_paper_trade_path(
                position.first_paper_trade_id,
                max_bid_seen=max_bid_seen,
                min_bid_seen=min_bid_seen,
                time_to_max_bid_s=time_to_max_bid_s,
                time_to_min_bid_s=time_to_min_bid_s,
                first_profit_time_s=first_profit_time_s,
                high_confidence_hit=high_confidence_hit,
                hold_to_resolution=False,
                favorable_excursion=favorable_excursion,
                ever_profitable=ever_profitable,
            )
        elif result is not None and result.trade_id and changed:
            engine.tracker.record_live_trade_path(
                result.trade_id,
                max_bid_seen=max_bid_seen,
                min_bid_seen=min_bid_seen,
                time_to_max_bid_s=time_to_max_bid_s,
                time_to_min_bid_s=time_to_min_bid_s,
                first_profit_time_s=first_profit_time_s,
                high_confidence_hit=high_confidence_hit,
                hold_to_resolution=position.hold_to_resolution,
                mfe=mfe,
                mae=mae,
                peak_net_pnl=peak_net_pnl,
                trough_net_pnl=trough_net_pnl,
                favorable_excursion=favorable_excursion,
                ever_profitable=ever_profitable,
                cancel_repost_count=position.cancel_repost_count,
            )

        if loss_pct > 0 and loss_pct >= dynamic_loss_cut:
            exit_bid = max(bid, 0.01)
            fee_val = engine._per_share_fee(exit_bid, position.market.fee_rate) * position.first_shares
            pnl = (exit_bid - position.first_entry_price) * position.first_shares - fee_val
            status = "closed_win" if pnl > 0 else "closed_loss"
            logger.info(
                f"[{coin}] SWING LOSS CUT @{exit_bid:.3f} "
                f"(loss={loss_pct:.0%} >= {dynamic_loss_cut:.0%}, {remaining:.0f}s) "
                f"pnl=${pnl:+.4f}"
            )
            if is_dry and position.first_paper_trade_id:
                engine.tracker.close_paper_trade_early(
                    position.first_paper_trade_id,
                    exit_bid,
                    pnl,
                    status,
                    exit_reason="loss_cut",
                    time_remaining_s=remaining,
                    bid_at_exit=exit_bid,
                    ask_at_exit=ask,
                    loss_cut_threshold_pct=dynamic_loss_cut,
                    loss_pct_at_exit=loss_pct,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
            else:
                if not engine.can_reserve_sell_shares(position.first_token_id, position.first_shares):
                    logger.warning(f"[{coin}] Swing loss-cut exit blocked - shares already reserved for sale")
                    continue
                try:
                    sell_order = await asyncio.to_thread(
                        engine.client.create_order,
                        {"token_id": position.first_token_id, "price": max(0.01, exit_bid - 0.02), "size": position.first_shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                    )
                    sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                    submitted_at = datetime.now(timezone.utc).isoformat()
                    position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                    engine.reserve_sell_order(position.sell_order_id, position.first_token_id, position.first_shares)
                    position.pending_exit_reason = "loss_cut"
                    position.pending_exit_price = max(0.01, exit_bid - 0.02)
                    if result is not None:
                        result.sell_order_id = position.sell_order_id
                        result.exit_order_submitted_at = submitted_at
                        result.exit_limit_price = position.pending_exit_price
                        result.exit_reason = "loss_cut"
                        result.dynamic_loss_cut_pct = dynamic_loss_cut
                        result.loss_pct_at_exit = loss_pct
                        if result.trade_id:
                            engine.tracker.update_live_trade(
                                result.trade_id,
                                sell_order_id=position.sell_order_id,
                                status="open",
                                exit_order_submitted_at=submitted_at,
                                exit_limit_price=position.pending_exit_price,
                                exit_reason="loss_cut",
                                dynamic_loss_cut_pct=dynamic_loss_cut,
                                cancel_repost_count=position.cancel_repost_count,
                            )
                except Exception as exc:
                    logger.error(f"[{coin}] Swing loss cut sell failed: {exc}")
            if is_dry:
                engine._open_swing_positions.pop(position.market.coin, None)
                return
            continue

        if net_pnl > 0:
            pnl = net_pnl
            logger.info(f"[{coin}] SWING NET-POSITIVE EXIT @{bid:.3f} pnl=${pnl:+.4f}")
            if is_dry and position.first_paper_trade_id:
                engine.tracker.close_paper_trade_early(
                    position.first_paper_trade_id,
                    bid,
                    pnl,
                    "closed_win" if pnl > 0 else "closed_loss",
                    exit_reason="profit_target",
                    time_remaining_s=remaining,
                    bid_at_exit=bid,
                    ask_at_exit=ask,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
            else:
                if not engine.can_reserve_sell_shares(position.first_token_id, position.first_shares):
                    logger.warning(f"[{coin}] Swing profit exit blocked - shares already reserved for sale")
                    continue
                try:
                    sell_order = await asyncio.to_thread(
                        engine.client.create_order,
                        {"token_id": position.first_token_id, "price": bid, "size": position.first_shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                    )
                    sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                    submitted_at = datetime.now(timezone.utc).isoformat()
                    position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                    engine.reserve_sell_order(position.sell_order_id, position.first_token_id, position.first_shares)
                    position.pending_exit_reason = "profit_target"
                    position.pending_exit_price = bid
                    if result is not None:
                        result.sell_order_id = position.sell_order_id
                        result.exit_order_submitted_at = submitted_at
                        result.exit_limit_price = bid
                        result.exit_reason = "profit_target"
                        if result.trade_id:
                            engine.tracker.update_live_trade(
                                result.trade_id,
                                sell_order_id=position.sell_order_id,
                                status="open",
                                exit_order_submitted_at=submitted_at,
                                exit_limit_price=bid,
                                exit_reason="profit_target",
                                cancel_repost_count=position.cancel_repost_count,
                            )
                except Exception as exc:
                    logger.error(f"[{coin}] Swing target sell failed: {exc}")
            if is_dry:
                engine._open_swing_positions.pop(position.market.coin, None)
                return
            continue

        if remaining <= 30:
            fee_val = engine._per_share_fee(bid, position.market.fee_rate) * position.first_shares
            pnl = (bid - position.first_entry_price) * position.first_shares - fee_val
            status = "closed_win" if pnl > 0 else "closed_loss"
            logger.info(f"[{coin}] SWING NEAR-CLOSE exit @{bid:.3f} pnl=${pnl:+.4f} ({remaining:.0f}s)")
            if is_dry and position.first_paper_trade_id:
                engine.tracker.close_paper_trade_early(
                    position.first_paper_trade_id,
                    bid,
                    pnl,
                    status,
                    exit_reason="near_close",
                    time_remaining_s=remaining,
                    bid_at_exit=bid,
                    ask_at_exit=ask,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
            else:
                if not engine.can_reserve_sell_shares(position.first_token_id, position.first_shares):
                    logger.warning(f"[{coin}] Swing near-close exit blocked - shares already reserved for sale")
                    continue
                try:
                    sell_order = await asyncio.to_thread(
                        engine.client.create_order,
                        {"token_id": position.first_token_id, "price": bid, "size": position.first_shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                    )
                    sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                    submitted_at = datetime.now(timezone.utc).isoformat()
                    position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                    engine.reserve_sell_order(position.sell_order_id, position.first_token_id, position.first_shares)
                    position.pending_exit_reason = "near_close"
                    position.pending_exit_price = bid
                    if result is not None:
                        result.sell_order_id = position.sell_order_id
                        result.exit_order_submitted_at = submitted_at
                        result.exit_limit_price = bid
                        result.exit_reason = "near_close"
                        if result.trade_id:
                            engine.tracker.update_live_trade(
                                result.trade_id,
                                sell_order_id=position.sell_order_id,
                                status="open",
                                exit_order_submitted_at=submitted_at,
                                exit_limit_price=bid,
                                exit_reason="near_close",
                                cancel_repost_count=position.cancel_repost_count,
                            )
                except Exception as exc:
                    logger.error(f"[{coin}] Swing near-close sell failed: {exc}")
            if is_dry:
                engine._open_swing_positions.pop(position.market.coin, None)
                return
            continue
