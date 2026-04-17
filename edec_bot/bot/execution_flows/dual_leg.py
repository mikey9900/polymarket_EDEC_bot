"""Dual-leg execution flow."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from bot.models import DualOrderState, TradeResult, TradeSignal

logger = logging.getLogger(__name__)


async def execute(engine: Any, signal: TradeSignal, decision_id: int = 0) -> TradeResult:
    """Execute a dual-leg arb trade using FOK orders."""
    result = TradeResult(signal=signal, strategy_type="dual_leg")
    resolved_decision_id = signal.decision_id or decision_id

    if engine.config.execution.dry_run:
        result.status = "dry_run"
        result.up_fill_price = signal.up_price
        result.down_fill_price = signal.down_price
        result.total_cost = signal.combined_cost
        result.fee_total = signal.fee_total
        result.shares = engine._calc_shares(signal.up_price)
        result.shares_requested = result.shares
        result.shares_filled = result.shares
        cost = signal.combined_cost * result.shares
        if engine.tracker.has_paper_capital(cost):
            engine.tracker.log_paper_trade(
                signal.market.slug,
                signal.market.coin,
                "dual_leg",
                "both",
                signal.combined_cost,
                1.0,
                result.shares,
                signal.fee_total,
                decision_id=resolved_decision_id,
                market_end_time=signal.market.end_time.isoformat(),
            )
            logger.info(
                f"[DRY RUN] Paper trade: UP@{signal.up_price:.3f} + DOWN@{signal.down_price:.3f} "
                f"({result.shares:.0f} shares, cost=${cost:.2f}) [{signal.market.coin.upper()}]"
            )
        else:
            logger.info(f"[DRY RUN] Skipped - insufficient paper capital (need ${cost:.2f})")
        return result

    try:
        engine.state = DualOrderState.PLACING_FIRST
        shares = engine._calc_shares(signal.up_price)
        result.shares_requested = shares
        if shares < 5:
            result.status = "failed"
            result.blocked_min_5_shares = True
            result.error = f"Shares too small: {shares} (min 5)"
            logger.warning(result.error)
            return result

        result.shares = shares
        tick_size = signal.market.tick_size
        neg_risk = signal.market.neg_risk

        up_order = await asyncio.to_thread(
            engine.client.create_order,
            {"token_id": signal.market.up_token_id, "price": signal.up_price, "size": shares, "side": "BUY"},
            {"tick_size": tick_size, "neg_risk": neg_risk},
        )
        down_order = await asyncio.to_thread(
            engine.client.create_order,
            {"token_id": signal.market.down_token_id, "price": signal.down_price, "size": shares, "side": "BUY"},
            {"tick_size": tick_size, "neg_risk": neg_risk},
        )

        logger.info(f"[{signal.market.coin.upper()}] Placing UP: {shares} @ {signal.up_price:.3f}")
        up_resp = await asyncio.to_thread(engine.client.post_order, up_order, "FOK")

        if not engine._is_filled(up_resp):
            result.status = "failed"
            result.error = f"UP order rejected: {up_resp}"
            engine.state = DualOrderState.DONE
            logger.warning(result.error)
            return result

        result.up_order_id = up_resp.get("orderID", up_resp.get("id", ""))
        result.up_filled = True
        result.up_fill_price = signal.up_price
        engine.state = DualOrderState.FIRST_PLACED

        engine.state = DualOrderState.PLACING_SECOND
        logger.info(f"[{signal.market.coin.upper()}] Placing DOWN: {shares} @ {signal.down_price:.3f}")
        down_resp = await asyncio.to_thread(engine.client.post_order, down_order, "FOK")

        if not engine._is_filled(down_resp):
            engine.state = DualOrderState.ABORTING
            logger.warning("DOWN rejected - aborting, selling UP position...")
            abort_cost = await engine._abort_sell(
                signal.market.up_token_id,
                shares,
                signal.up_price,
                signal.market.fee_rate,
                tick_size,
                neg_risk,
            )
            result.status = "partial_abort"
            result.abort_cost = abort_cost
            result.error = f"DOWN rejected: {down_resp}"
            engine.state = DualOrderState.DONE
            return result

        result.down_order_id = down_resp.get("orderID", down_resp.get("id", ""))
        result.down_filled = True
        result.down_fill_price = signal.down_price
        result.total_cost = signal.combined_cost
        result.fee_total = signal.fee_total
        result.status = "success"
        engine.state = DualOrderState.DONE

        logger.info(
            f"[{signal.market.coin.upper()}] DUAL-LEG SUCCESS: "
            f"UP@{signal.up_price:.3f} + DOWN@{signal.down_price:.3f} "
            f"= {signal.combined_cost:.3f} | {shares:.0f} shares | "
            f"Est. profit: ${signal.expected_profit:.4f}"
        )
        return result

    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)
        logger.error(f"Dual-leg execution error: {exc}")

        if result.up_filled and not result.down_filled:
            engine.state = DualOrderState.ABORTING
            try:
                abort_cost = await engine._abort_sell(
                    signal.market.up_token_id,
                    result.shares,
                    signal.up_price,
                    signal.market.fee_rate,
                    signal.market.tick_size,
                    signal.market.neg_risk,
                )
                result.abort_cost = abort_cost
                result.status = "partial_abort"
            except Exception as abort_err:
                logger.critical(
                    f"ABORT SELL FAILED: {abort_err}. "
                    f"Naked position: {result.shares} UP shares in {signal.market.slug}"
                )

        engine.state = DualOrderState.DONE
        return result

    finally:
        if resolved_decision_id and result.status != "dry_run":
            engine.tracker.log_trade(resolved_decision_id, result)
        if result.status != "dry_run":
            engine.risk_manager.record_trade(result)
