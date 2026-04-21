"""Single-leg and lead-lag execution flows."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import Any

from bot.models import SingleLegPosition, TradeResult, TradeSignal

logger = logging.getLogger(__name__)


async def execute(engine: Any, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
    """Buy one side with a GTC limit and monitor it with strategy-specific exits."""
    result = TradeResult(signal=signal, strategy_type=signal.strategy_type, side=signal.side, status="pending")
    resolved_decision_id = signal.decision_id or decision_id
    entry_submitted_at = datetime.now(timezone.utc)
    result.entry_order_submitted_at = entry_submitted_at.isoformat()
    result.entry_limit_price = signal.entry_price

    market = signal.market
    coin = market.coin.upper()
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
        result.fee_total = signal.fee_total
        cost = signal.entry_price * shares
        if engine.tracker.has_paper_capital(cost):
            trade_id = engine.tracker.log_paper_trade(
                market.slug,
                market.coin,
                signal.strategy_type,
                signal.side,
                signal.entry_price,
                signal.target_sell_price,
                shares,
                signal.fee_total,
                decision_id=resolved_decision_id,
                market_end_time=market.end_time.isoformat(),
                market_start_time=market.start_time.isoformat(),
                signal_context=signal.signal_context,
                signal_overlap_count=signal.signal_overlap_count,
                order_size_usd=order_size,
                shares_requested=shares,
                shares_filled=shares,
                blocked_min_5_shares=False,
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
                f"[DRY RUN] [{coin}] Paper trade ({signal.strategy_type}): "
                f"BUY {signal.side.upper()}@{signal.entry_price:.3f} "
                f"-> EXIT@{signal.target_sell_price:.3f} | score={signal.signal_score:.1f} "
                f"| {shares} shares, cost=${cost:.2f}"
            )
            asyncio.create_task(
                engine._monitor_paper_single_leg(
                    trade_id=trade_id,
                    market=market,
                    token_id=token_id,
                    entry_price=signal.entry_price,
                    target_sell=signal.target_sell_price,
                    shares=shares,
                    strategy_type=signal.strategy_type,
                )
            )
        else:
            logger.info(f"[DRY RUN] [{coin}] Skipped - insufficient paper capital (need ${cost:.2f})")
        return result

    if shares < 5:
        result.status = "failed"
        result.blocked_min_5_shares = True
        result.error = f"Shares too small: {shares} (min 5)"
        logger.warning(f"[{coin}] {result.error}")
        return result

    result.shares = shares
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
            result.error = f"Buy order rejected: {buy_resp}"
            logger.warning(f"[{coin}] {result.error}")
            return result

        result.buy_order_id = buy_order_id
        result.total_cost = signal.entry_price * shares
        position = SingleLegPosition(
            market=market,
            side=signal.side,
            token_id=token_id,
            entry_price=signal.entry_price,
            target_price=signal.target_sell_price,
            shares=shares,
            buy_order_id=buy_order_id,
            sell_order_id=None,
            strategy_type=signal.strategy_type,
            decision_id=resolved_decision_id,
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
                    logger.warning(f"[{coin}] Cancel partially filled buy failed: {cancel_err}")
            position.shares = actual_shares
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
            engine._open_positions[buy_order_id] = position
            asyncio.create_task(engine._monitor_single_leg(position, result))
            if signal.strategy_type == "lead_lag":
                logger.info(
                    f"[{coin}] LEAD-LAG BUY filled: {actual_shares:.0f} {signal.side.upper()} "
                    f"@ {signal.entry_price:.3f} (order {buy_order_id}) - target@{signal.target_sell_price:.3f}"
                )
            else:
                logger.info(
                    f"[{coin}] SINGLE-LEG BUY filled: {actual_shares:.0f} {signal.side.upper()} "
                    f"@ {signal.entry_price:.3f} (order {buy_order_id}) - scalp@"
                    f"{signal.target_sell_price:.2f}, runner@{engine.config.single_leg.high_confidence_bid:.2f}"
                )
            return result

        hold_if_unfilled = bool(engine.config.single_leg.hold_if_unfilled)
        status_label = engine._response_status(buy_resp) or "unknown"
        if hold_if_unfilled:
            result.status = "submitted"
            engine._pending_single_entries[buy_order_id] = position
            asyncio.create_task(engine._monitor_single_leg_entry(position, result))
            logger.info(
                f"[{coin}] {signal.strategy_type.upper()} BUY resting on book: "
                f"{shares} {signal.side.upper()} @ {signal.entry_price:.3f} "
                f"(order {buy_order_id}, status={status_label})"
            )
            return result

        try:
            await asyncio.to_thread(engine.client.cancel, buy_order_id)
        except Exception as cancel_err:
            logger.warning(f"[{coin}] Cancel unfilled buy failed: {cancel_err}")
        result.status = "failed"
        result.error = f"Buy order was not filled immediately (status={status_label})"
        logger.warning(f"[{coin}] {result.error}")
        return result

    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)
        logger.error(f"[{coin}] Single-leg execution error: {exc}")
        return result

    finally:
        if resolved_decision_id and result.status not in ("dry_run", "submitted"):
            result.trade_id = engine.tracker.log_trade(resolved_decision_id, result)
            if position is not None:
                position.trade_id = result.trade_id
        if result.status != "dry_run":
            engine.risk_manager.record_attempt()
            if result.status == "open":
                engine.risk_manager.open_position(result)


async def monitor_entry(engine: Any, position: SingleLegPosition, result: TradeResult):
    """Wait for a resting live entry order to actually fill before opening the position."""
    coin = position.market.coin.upper()
    while True:
        await asyncio.sleep(1)
        now = datetime.now(timezone.utc)
        if now >= position.market.end_time:
            engine._pending_single_entries.pop(position.buy_order_id, None)
            try:
                await asyncio.to_thread(engine.client.cancel, position.buy_order_id)
            except Exception:
                pass
            logger.info(f"[{coin}] Entry order expired before fill: {position.buy_order_id}")
            return

        try:
            order_status = await asyncio.to_thread(engine.client.get_order, position.buy_order_id)
        except Exception as exc:
            logger.debug(f"[{coin}] Entry order status check failed: {exc}")
            continue

        requested_shares = position.requested_shares or position.shares
        filled_shares = engine._filled_shares(order_status, requested_shares)
        if filled_shares > 0:
            if filled_shares < requested_shares and not engine._is_terminal_order_state(engine._response_status(order_status)):
                try:
                    await asyncio.to_thread(engine.client.cancel, position.buy_order_id)
                except Exception as cancel_err:
                    logger.warning(f"[{coin}] Cancel partially filled entry failed: {cancel_err}")
            actual_shares = filled_shares or requested_shares
            position.shares = actual_shares
            result.status = "open"
            result.shares = actual_shares
            result.shares_filled = actual_shares
            result.total_cost = position.entry_price * actual_shares
            result.fee_total = engine._per_share_fee(position.entry_price, position.market.fee_rate) * actual_shares
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
            result.entry_fill_price = engine._filled_price(order_status, position.entry_price)
            result.entry_slippage = result.entry_fill_price - position.entry_price
            result.entry_fill_ratio = actual_shares / max(requested_shares, 1e-9)
            position.entry_filled_at = result.entry_filled_at
            engine._pending_single_entries.pop(position.buy_order_id, None)
            engine._open_positions[position.buy_order_id] = position
            if position.decision_id:
                result.trade_id = engine.tracker.log_trade(position.decision_id, result)
                position.trade_id = result.trade_id
            engine.risk_manager.open_position(result)
            logger.info(
                f"[{coin}] {position.strategy_type.upper()} entry filled: "
                f"{actual_shares:.0f} {position.side.upper()} @ {position.entry_price:.3f} "
                f"(order {position.buy_order_id})"
            )
            await engine._monitor_single_leg(position, result)
            return

        status = engine._response_status(order_status)
        if engine._is_terminal_order_state(status):
            engine._pending_single_entries.pop(position.buy_order_id, None)
            logger.info(
                f"[{coin}] Entry order ended without fill: {position.buy_order_id} "
                f"(status={status or 'unknown'})"
            )
            return


async def monitor_position(engine: Any, position: SingleLegPosition, result: TradeResult):
    """Monitor an open single-leg or lead-lag live position with strategy-specific exits."""
    coin = position.market.coin.upper()
    cfg = engine.config.single_leg
    high_confidence_held = position.hold_to_resolution
    monitor_started_at = datetime.now(timezone.utc)
    max_bid_seen = result.max_bid_seen or None
    min_bid_seen = result.min_bid_seen or None
    time_to_max_bid_s = result.time_to_max_bid_s or None
    time_to_min_bid_s = result.time_to_min_bid_s or None
    first_profit_time_s = result.first_profit_time_s or None
    scalp_hit = bool(result.scalp_hit)
    high_confidence_hit = bool(result.high_confidence_hit)
    peak_net_pnl = result.peak_net_pnl if result.peak_net_pnl else None
    trough_net_pnl = result.trough_net_pnl if result.trough_net_pnl else None
    mfe = result.mfe if result.mfe else None
    mae = result.mae if result.mae else None
    ever_profitable = bool(result.ever_profitable)

    def sync_result(favorable_excursion: float) -> None:
        result.max_bid_seen = max_bid_seen or 0.0
        result.min_bid_seen = min_bid_seen or 0.0
        result.time_to_max_bid_s = time_to_max_bid_s or 0.0
        result.time_to_min_bid_s = time_to_min_bid_s or 0.0
        result.first_profit_time_s = first_profit_time_s or 0.0
        result.scalp_hit = scalp_hit
        result.high_confidence_hit = high_confidence_hit
        result.hold_to_resolution = high_confidence_held
        result.mfe = mfe or 0.0
        result.mae = mae or 0.0
        result.peak_net_pnl = peak_net_pnl or 0.0
        result.trough_net_pnl = trough_net_pnl or 0.0
        result.favorable_excursion = favorable_excursion
        result.ever_profitable = ever_profitable
        result.cancel_repost_count = position.cancel_repost_count

    while True:
        await engine._wait_book_update()

        now = datetime.now(timezone.utc)
        remaining = (position.market.end_time - now).total_seconds()
        if remaining <= 0:
            logger.info(f"[{coin}] Live position awaiting market resolution: {position.buy_order_id}")
            return

        try:
            book = await engine._fetch_book_price(position.token_id)
            bid = book.best_bid
            ask = book.best_ask
        except Exception:
            continue

        if bid <= 0:
            continue

        elapsed_s = max(0.0, (datetime.now(timezone.utc) - monitor_started_at).total_seconds())
        changed = False
        if max_bid_seen is None or bid > max_bid_seen:
            max_bid_seen = bid
            time_to_max_bid_s = elapsed_s
            changed = True
        if min_bid_seen is None or bid < min_bid_seen:
            min_bid_seen = bid
            time_to_min_bid_s = elapsed_s
            changed = True

        net_pnl = engine._net_pnl(position.entry_price, bid, position.market.fee_rate, position.shares)
        favorable_excursion = max(0.0, (max_bid_seen if max_bid_seen is not None else bid) - position.entry_price)
        loss_pct = max(0.0, (position.entry_price - bid) / max(position.entry_price, 1e-9))
        if peak_net_pnl is None or net_pnl > peak_net_pnl:
            peak_net_pnl = net_pnl
            changed = True
        if trough_net_pnl is None or net_pnl < trough_net_pnl:
            trough_net_pnl = net_pnl
            changed = True
        mfe = favorable_excursion
        mae = max(0.0, position.entry_price - (min_bid_seen if min_bid_seen is not None else bid))
        if first_profit_time_s is None and net_pnl > 0:
            first_profit_time_s = elapsed_s
            changed = True
        if not ever_profitable and net_pnl > 0:
            ever_profitable = True
            changed = True
        if position.strategy_type != "lead_lag":
            if bid >= cfg.scalp_take_profit_bid and not scalp_hit:
                scalp_hit = True
                changed = True
            if bid >= cfg.high_confidence_bid and not high_confidence_hit:
                high_confidence_hit = True
                changed = True
        sync_result(favorable_excursion)
        if changed and result.trade_id:
            engine.tracker.record_live_trade_path(
                result.trade_id,
                max_bid_seen=max_bid_seen,
                min_bid_seen=min_bid_seen,
                time_to_max_bid_s=time_to_max_bid_s,
                time_to_min_bid_s=time_to_min_bid_s,
                first_profit_time_s=first_profit_time_s,
                scalp_hit=scalp_hit,
                high_confidence_hit=high_confidence_hit,
                hold_to_resolution=high_confidence_held,
                mfe=mfe,
                mae=mae,
                peak_net_pnl=peak_net_pnl,
                trough_net_pnl=trough_net_pnl,
                favorable_excursion=favorable_excursion,
                ever_profitable=ever_profitable,
                cancel_repost_count=position.cancel_repost_count,
            )

        if position.sell_order_id and not high_confidence_held:
            try:
                order_status = await asyncio.to_thread(engine.client.get_order, position.sell_order_id)
                status = engine._response_status(order_status)
                if engine._is_order_filled(order_status, position.shares):
                    exit_price = engine._filled_price(order_status, position.pending_exit_price or position.target_price)
                    actual_profit = engine._net_pnl(
                        position.entry_price,
                        exit_price,
                        position.market.fee_rate,
                        position.shares,
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
                    result.bid_at_exit = bid
                    result.ask_at_exit = ask
                    result.exit_spread = ask - bid
                    result.loss_pct_at_exit = loss_pct
                    result.stall_exit_triggered = result.exit_reason == "stall_exit"
                    engine.tracker.close_live_trade(
                        result.trade_id,
                        status=result.status,
                        exit_reason=result.exit_reason,
                        exit_price=exit_price,
                        pnl=actual_profit,
                        time_remaining_s=remaining,
                        bid_at_exit=bid,
                        ask_at_exit=ask,
                        exit_limit_price=result.exit_limit_price or position.pending_exit_price,
                        exit_fill_price=exit_price,
                        max_bid_seen=max_bid_seen,
                        min_bid_seen=min_bid_seen,
                        time_to_max_bid_s=time_to_max_bid_s,
                        time_to_min_bid_s=time_to_min_bid_s,
                        first_profit_time_s=first_profit_time_s,
                        scalp_hit=scalp_hit,
                        high_confidence_hit=high_confidence_hit,
                        hold_to_resolution=high_confidence_held,
                        mfe=mfe,
                        mae=mae,
                        peak_net_pnl=peak_net_pnl,
                        trough_net_pnl=trough_net_pnl,
                        stall_exit_triggered=result.stall_exit_triggered,
                        dynamic_loss_cut_pct=result.dynamic_loss_cut_pct or None,
                        loss_pct_at_exit=loss_pct,
                        favorable_excursion=favorable_excursion,
                        ever_profitable=ever_profitable,
                        cancel_repost_count=position.cancel_repost_count,
                    )
                    engine.risk_manager.close_position(result, actual_profit)
                    logger.info(
                        f"[{coin}] {position.strategy_type.upper()} SELL FILLED @ {exit_price:.3f} "
                        f"(pnl=${actual_profit:+.4f})"
                    )
                    engine._open_positions.pop(position.buy_order_id, None)
                    return
                if engine._is_terminal_order_state(status):
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
                logger.warning(f"[{coin}] Order status check failed: {exc}")
                continue

        if position.strategy_type == "lead_lag":
            exit_reason, net_pnl, loss_pct = engine._lead_lag_exit_reason(
                coin=position.market.coin,
                entry_price=position.entry_price,
                target_price=position.target_price,
                bid=bid,
                remaining=remaining,
                elapsed_s=elapsed_s,
                max_bid_seen=max_bid_seen,
                ever_profitable=ever_profitable,
                shares=position.shares,
                fee_rate=position.market.fee_rate,
            )
            if exit_reason and not position.sell_order_id:
                try:
                    exit_price = engine._live_exit_price(bid, exit_reason)
                    sell_order = await asyncio.to_thread(
                        engine.client.create_order,
                        {"token_id": position.token_id, "price": exit_price, "size": position.shares, "side": "SELL"},
                        {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                    )
                    sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                    submitted_at = datetime.now(timezone.utc).isoformat()
                    position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                    position.pending_exit_reason = exit_reason
                    position.pending_exit_price = exit_price
                    result.sell_order_id = position.sell_order_id
                    result.exit_order_submitted_at = submitted_at
                    result.exit_limit_price = exit_price
                    result.exit_reason = exit_reason
                    result.time_remaining_s = remaining
                    result.loss_pct_at_exit = loss_pct
                    result.stall_exit_triggered = exit_reason == "stall_exit"
                    if result.trade_id:
                        engine.tracker.update_live_trade(
                            result.trade_id,
                            sell_order_id=position.sell_order_id,
                            status="open",
                            exit_order_submitted_at=submitted_at,
                            exit_limit_price=exit_price,
                            exit_reason=exit_reason,
                            cancel_repost_count=position.cancel_repost_count,
                            hold_to_resolution=high_confidence_held,
                            stall_exit_triggered=result.stall_exit_triggered,
                        )
                    logger.info(
                        f"[{coin}] LEAD-LAG {exit_reason.upper()} @{exit_price:.3f} "
                        f"(bid={bid:.3f}, pnl=${net_pnl:+.4f}, loss={loss_pct:.1%}, {remaining:.0f}s left)"
                    )
                except Exception as exc:
                    logger.error(f"[{coin}] Lead-lag sell failed: {exc}")
            continue

        dynamic_loss_cut = engine._dynamic_single_leg_loss_cut(remaining)

        if bid >= cfg.scalp_take_profit_bid and bid < cfg.high_confidence_bid and net_pnl >= cfg.scalp_min_profit_usd:
            try:
                scalp_price = engine._live_exit_price(bid, "profit_target")
                sell_order = await asyncio.to_thread(
                    engine.client.create_order,
                    {"token_id": position.token_id, "price": scalp_price, "size": position.shares, "side": "SELL"},
                    {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                )
                sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                submitted_at = datetime.now(timezone.utc).isoformat()
                position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                position.pending_exit_reason = "profit_target"
                position.pending_exit_price = scalp_price
                result.sell_order_id = position.sell_order_id
                result.exit_order_submitted_at = submitted_at
                result.exit_limit_price = scalp_price
                result.exit_reason = "profit_target"
                result.time_remaining_s = remaining
                if result.trade_id:
                    engine.tracker.update_live_trade(
                        result.trade_id,
                        sell_order_id=position.sell_order_id,
                        status="open",
                        exit_order_submitted_at=submitted_at,
                        exit_limit_price=scalp_price,
                        exit_reason="profit_target",
                        cancel_repost_count=position.cancel_repost_count,
                    )
                logger.info(
                    f"[{coin}] SCALP EXIT @{scalp_price:.3f} "
                    f"(net pnl=${net_pnl:+.4f}, target>={cfg.scalp_take_profit_bid:.2f})"
                )
            except Exception as exc:
                logger.error(f"[{coin}] Scalp sell failed: {exc}")
            continue

        if bid >= cfg.high_confidence_bid and not high_confidence_held:
            high_confidence_held = True
            position.hold_to_resolution = True
            result.high_confidence_hit = True
            result.hold_to_resolution = True
            if result.trade_id:
                engine.tracker.record_live_trade_path(
                    result.trade_id,
                    max_bid_seen=max_bid_seen,
                    min_bid_seen=min_bid_seen,
                    time_to_max_bid_s=time_to_max_bid_s,
                    time_to_min_bid_s=time_to_min_bid_s,
                    first_profit_time_s=first_profit_time_s,
                    scalp_hit=scalp_hit,
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
                f"[{coin}] HIGH-CONFIDENCE @{bid:.3f} - "
                f"holding {position.side.upper()} to resolution ({remaining:.0f}s left)"
            )
            continue

        if not high_confidence_held and loss_pct > 0 and loss_pct >= dynamic_loss_cut:
            if position.sell_order_id:
                try:
                    await asyncio.to_thread(engine.client.cancel, position.sell_order_id)
                except Exception:
                    pass
                position.cancel_repost_count += 1
                position.sell_order_id = None
                result.sell_order_id = None
                result.cancel_repost_count = position.cancel_repost_count
            if position.sell_order_id:
                continue
            try:
                emergency_price = engine._live_exit_price(bid, "loss_cut")
                sell_order = await asyncio.to_thread(
                    engine.client.create_order,
                    {"token_id": position.token_id, "price": emergency_price, "size": position.shares, "side": "SELL"},
                    {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                )
                sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                submitted_at = datetime.now(timezone.utc).isoformat()
                position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                position.pending_exit_reason = "loss_cut"
                position.pending_exit_price = emergency_price
                result.sell_order_id = position.sell_order_id
                result.exit_order_submitted_at = submitted_at
                result.exit_limit_price = emergency_price
                result.exit_reason = "loss_cut"
                result.dynamic_loss_cut_pct = dynamic_loss_cut
                result.time_remaining_s = remaining
                result.loss_pct_at_exit = loss_pct
                if result.trade_id:
                    engine.tracker.update_live_trade(
                        result.trade_id,
                        sell_order_id=position.sell_order_id,
                        status="open",
                        exit_order_submitted_at=submitted_at,
                        exit_limit_price=emergency_price,
                        exit_reason="loss_cut",
                        dynamic_loss_cut_pct=dynamic_loss_cut,
                        cancel_repost_count=position.cancel_repost_count,
                    )
                logger.info(
                    f"[{coin}] LOSS CUT @{emergency_price:.3f} "
                    f"(loss={loss_pct:.0%} >= {dynamic_loss_cut:.0%}, {remaining:.0f}s left)"
                )
            except Exception as exc:
                logger.error(f"[{coin}] Loss cut sell failed: {exc}")
            continue

        if not high_confidence_held and remaining <= 30:
            if position.sell_order_id:
                try:
                    await asyncio.to_thread(engine.client.cancel, position.sell_order_id)
                except Exception:
                    pass
                position.cancel_repost_count += 1
                position.sell_order_id = None
                result.sell_order_id = None
                result.cancel_repost_count = position.cancel_repost_count
            if position.sell_order_id:
                continue
            try:
                emergency_price = engine._live_exit_price(bid, "near_close")
                sell_order = await asyncio.to_thread(
                    engine.client.create_order,
                    {"token_id": position.token_id, "price": emergency_price, "size": position.shares, "side": "SELL"},
                    {"tick_size": position.market.tick_size, "neg_risk": position.market.neg_risk},
                )
                sell_resp = await asyncio.to_thread(engine.client.post_order, sell_order, "GTC")
                submitted_at = datetime.now(timezone.utc).isoformat()
                position.sell_order_id = sell_resp.get("orderID", sell_resp.get("id", ""))
                position.pending_exit_reason = "near_close"
                position.pending_exit_price = emergency_price
                result.sell_order_id = position.sell_order_id
                result.exit_order_submitted_at = submitted_at
                result.exit_limit_price = emergency_price
                result.exit_reason = "near_close"
                result.time_remaining_s = remaining
                if result.trade_id:
                    engine.tracker.update_live_trade(
                        result.trade_id,
                        sell_order_id=position.sell_order_id,
                        status="open",
                        exit_order_submitted_at=submitted_at,
                        exit_limit_price=emergency_price,
                        exit_reason="near_close",
                        cancel_repost_count=position.cancel_repost_count,
                    )
                logger.info(f"[{coin}] NEAR-CLOSE emergency sell @ {emergency_price:.3f}")
            except Exception as exc:
                logger.error(f"[{coin}] Near-close sell failed: {exc}")
            continue


async def monitor_paper_position(
    engine: Any,
    trade_id: int,
    market,
    token_id: str,
    entry_price: float,
    target_sell: float,
    shares: float,
    strategy_type: str = "single_leg",
):
    """Watch a paper trade's live bid and apply the same exit structure as runtime logic."""
    cfg = engine.config.single_leg
    coin = market.coin.upper()
    monitor_started_at = datetime.now(timezone.utc)
    max_bid_seen = None
    min_bid_seen = None
    time_to_max_bid_s = None
    time_to_min_bid_s = None
    first_profit_time_s = None
    scalp_hit = False
    high_confidence_hit = False
    peak_net_pnl = None
    trough_net_pnl = None
    mfe = None
    mae = None
    ever_profitable = False

    while True:
        await engine._wait_book_update()

        now = datetime.now(timezone.utc)
        remaining = (market.end_time - now).total_seconds()
        if remaining <= 0:
            return

        try:
            book = await engine._fetch_book_price(token_id)
            bid = book.best_bid
            ask = book.best_ask
            if bid <= 0:
                continue

            elapsed_s = max(0.0, (datetime.now(timezone.utc) - monitor_started_at).total_seconds())
            changed = False
            if max_bid_seen is None or bid > max_bid_seen:
                max_bid_seen = bid
                time_to_max_bid_s = elapsed_s
                changed = True
            if min_bid_seen is None or bid < min_bid_seen:
                min_bid_seen = bid
                time_to_min_bid_s = elapsed_s
                changed = True

            net_pnl = engine._net_pnl(entry_price, bid, market.fee_rate, shares)
            favorable_excursion = max(0.0, (max_bid_seen or bid) - entry_price)
            if not ever_profitable and net_pnl > 0:
                ever_profitable = True
                changed = True
            if peak_net_pnl is None or net_pnl > peak_net_pnl:
                peak_net_pnl = net_pnl
                changed = True
            if trough_net_pnl is None or net_pnl < trough_net_pnl:
                trough_net_pnl = net_pnl
                changed = True
            mfe = favorable_excursion
            mae = max(0.0, entry_price - (min_bid_seen or bid))
            if first_profit_time_s is None and net_pnl > 0:
                first_profit_time_s = elapsed_s
                changed = True
            if bid >= cfg.scalp_take_profit_bid and not scalp_hit:
                scalp_hit = True
                changed = True
            if bid >= cfg.high_confidence_bid and not high_confidence_hit:
                high_confidence_hit = True
                changed = True
            if changed:
                engine.tracker.record_paper_trade_path(
                    trade_id,
                    max_bid_seen=max_bid_seen,
                    min_bid_seen=min_bid_seen,
                    time_to_max_bid_s=time_to_max_bid_s,
                    time_to_min_bid_s=time_to_min_bid_s,
                    first_profit_time_s=first_profit_time_s,
                    scalp_hit=scalp_hit,
                    high_confidence_hit=high_confidence_hit,
                    hold_to_resolution=False,
                    mfe=mfe,
                    mae=mae,
                    peak_net_pnl=peak_net_pnl,
                    trough_net_pnl=trough_net_pnl,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )

            if strategy_type == "lead_lag":
                exit_reason, net_pnl, loss_pct = engine._lead_lag_exit_reason(
                    coin=market.coin,
                    entry_price=entry_price,
                    target_price=target_sell,
                    bid=bid,
                    remaining=remaining,
                    elapsed_s=elapsed_s,
                    max_bid_seen=max_bid_seen,
                    ever_profitable=ever_profitable,
                    shares=shares,
                    fee_rate=market.fee_rate,
                )
                if exit_reason:
                    status = "closed_win" if net_pnl > 0 else "closed_loss"
                    engine.tracker.close_paper_trade_early(
                        trade_id,
                        bid,
                        net_pnl,
                        status,
                        exit_reason=exit_reason,
                        time_remaining_s=remaining,
                        bid_at_exit=bid,
                        ask_at_exit=ask,
                        stall_exit_triggered=(exit_reason == "stall_exit"),
                        loss_cut_threshold_pct=engine.config.lead_lag.hard_stop_loss_pct,
                        loss_pct_at_exit=loss_pct,
                        favorable_excursion=favorable_excursion,
                        ever_profitable=ever_profitable,
                    )
                    logger.info(
                        f"[{coin}] Paper LEAD-LAG {exit_reason.upper()} @{bid:.3f} "
                        f"(pnl=${net_pnl:+.4f}, loss={loss_pct:.1%}, {remaining:.0f}s left)"
                    )
                    return
                continue

            if bid >= cfg.high_confidence_bid:
                engine.tracker.record_paper_trade_path(
                    trade_id,
                    max_bid_seen=max_bid_seen,
                    min_bid_seen=min_bid_seen,
                    time_to_max_bid_s=time_to_max_bid_s,
                    time_to_min_bid_s=time_to_min_bid_s,
                    first_profit_time_s=first_profit_time_s,
                    scalp_hit=scalp_hit,
                    high_confidence_hit=True,
                    hold_to_resolution=True,
                    mfe=mfe,
                    mae=mae,
                    peak_net_pnl=peak_net_pnl,
                    trough_net_pnl=trough_net_pnl,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
                logger.info(f"[{coin}] Paper HIGH-CONFIDENCE @{bid:.3f} - holding to resolution ({remaining:.0f}s)")
                return

            if bid >= cfg.scalp_take_profit_bid and bid < cfg.high_confidence_bid and net_pnl >= cfg.scalp_min_profit_usd:
                engine.tracker.close_paper_trade_early(
                    trade_id,
                    bid,
                    net_pnl,
                    "closed_win",
                    exit_reason="profit_target",
                    time_remaining_s=remaining,
                    bid_at_exit=bid,
                    ask_at_exit=ask,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
                logger.info(
                    f"[{coin}] Paper SCALP EXIT @{bid:.3f} "
                    f"(net pnl=${net_pnl:+.4f}, target>={cfg.scalp_take_profit_bid:.2f})"
                )
                return

            loss_pct = (entry_price - bid) / entry_price
            dynamic_loss_cut = engine._dynamic_single_leg_loss_cut(remaining)
            if loss_pct > 0 and loss_pct >= dynamic_loss_cut:
                status = "closed_win" if net_pnl > 0 else "closed_loss"
                engine.tracker.close_paper_trade_early(
                    trade_id,
                    bid,
                    net_pnl,
                    status,
                    exit_reason="loss_cut",
                    time_remaining_s=remaining,
                    bid_at_exit=bid,
                    ask_at_exit=ask,
                    loss_cut_threshold_pct=dynamic_loss_cut,
                    loss_pct_at_exit=loss_pct,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
                logger.info(
                    f"[{coin}] Paper LOSS CUT @{bid:.3f} "
                    f"(loss={loss_pct:.0%} >= {dynamic_loss_cut:.0%}, {remaining:.0f}s) "
                    f"pnl=${net_pnl:+.4f}"
                )
                return

            if remaining <= 30:
                status = "closed_win" if net_pnl > 0 else "closed_loss"
                engine.tracker.close_paper_trade_early(
                    trade_id,
                    bid,
                    net_pnl,
                    status,
                    exit_reason="near_close",
                    time_remaining_s=remaining,
                    bid_at_exit=bid,
                    ask_at_exit=ask,
                    favorable_excursion=favorable_excursion,
                    ever_profitable=ever_profitable,
                )
                logger.info(f"[{coin}] Paper NEAR-CLOSE exit @{bid:.3f} pnl=${net_pnl:+.4f}")
                return

        except Exception as exc:
            logger.debug(f"[{coin}] Paper monitor error: {exc}")
