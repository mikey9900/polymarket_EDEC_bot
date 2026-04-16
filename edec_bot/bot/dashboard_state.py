"""Shared live snapshot and action layer for the HA dashboard/API."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from version import __version__

from bot.config import Config
from bot.risk_manager import RiskManager
from bot.tracker import DecisionTracker

logger = logging.getLogger(__name__)

MODE_LABELS = {
    "both": "ALL",
    "dual": "DUAL",
    "single": "SINGLE",
    "lead": "LEAD-LAG",
    "swing": "SWING",
    "off": "OFF",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _unix_to_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except Exception:
        return None


class DashboardStateService:
    """Caches live runtime state and publishes it to HTTP/WebSocket consumers."""

    def __init__(
        self,
        config: Config,
        tracker: DecisionTracker,
        risk_manager: RiskManager,
        *,
        scanner=None,
        strategy_engine=None,
        executor=None,
        aggregator=None,
        update_interval_s: float = 0.25,
        history_sample_interval_s: float = 1.0,
        history_points: int = 600,
        recent_trades_limit: int = 10,
        recent_outcomes_limit: int = 6,
    ):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.aggregator = aggregator
        self.update_interval_s = max(0.1, float(update_interval_s))
        self.history_sample_interval_s = max(self.update_interval_s, float(history_sample_interval_s))
        self.history_points = max(60, int(history_points))
        self.recent_trades_limit = max(1, int(recent_trades_limit))
        self.recent_outcomes_limit = max(1, int(recent_outcomes_limit))

        self._task: asyncio.Task | None = None
        self._snapshot_lock = asyncio.Lock()
        self._current_snapshot: dict[str, Any] | None = None
        self._current_snapshot_json: str = ""
        self._listeners: set[asyncio.Queue] = set()

        self._history: dict[str, deque] = {
            coin: deque(maxlen=self.history_points) for coin in self.config.coins
        }
        self._reference_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._last_history_at: dict[str, float] = {coin: 0.0 for coin in self.config.coins}
        self._last_runtime_sync: tuple[str, float, float] | None = None

        self._cached_paper_stats: dict[str, Any] = {}
        self._cached_daily_stats: dict[str, Any] = {}
        self._cached_recent_trades: list[dict[str, Any]] = []
        self._cached_outcomes: dict[str, list[str]] = {coin: [] for coin in self.config.coins}
        self._cached_outcome_details: dict[str, list[dict[str, Any]]] = {
            coin: [] for coin in self.config.coins
        }
        self._last_summary_refresh = 0.0
        self._summary_refresh_interval_s = 1.0

    async def start(self) -> None:
        if self._task:
            return
        await self.refresh(force_broadcast=False)
        self._task = asyncio.create_task(self._run(), name="edec-dashboard-state")
        logger.info(
            "Dashboard state service started (update=%sms, history=%ss x %s points)",
            int(self.update_interval_s * 1000),
            int(self.history_sample_interval_s),
            self.history_points,
        )

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        for listener in list(self._listeners):
            try:
                listener.put_nowait(None)
            except Exception:
                pass
        self._listeners.clear()

    def register_listener(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._listeners.add(queue)
        return queue

    def unregister_listener(self, queue: asyncio.Queue) -> None:
        self._listeners.discard(queue)

    async def get_snapshot(self, include_history: bool = True) -> dict[str, Any]:
        async with self._snapshot_lock:
            if self._current_snapshot is None:
                await self._refresh_locked(force_broadcast=False)
            snapshot = copy.deepcopy(self._current_snapshot or {})
        if include_history:
            snapshot["series"] = {
                coin: list(self._history.get(coin, ())) for coin in self.config.coins
            }
        return snapshot

    async def refresh(self, force_broadcast: bool = True) -> dict[str, Any]:
        async with self._snapshot_lock:
            return await self._refresh_locked(force_broadcast=force_broadcast)

    async def start_scanning(self) -> dict[str, Any]:
        if self.strategy_engine:
            self.strategy_engine.start_scanning()
        self.risk_manager.resume()
        self.risk_manager.deactivate_kill_switch()
        return await self.refresh(force_broadcast=True)

    async def stop_scanning(self) -> dict[str, Any]:
        if self.strategy_engine:
            self.strategy_engine.stop_scanning()
        self.risk_manager.pause()
        return await self.refresh(force_broadcast=True)

    async def activate_kill_switch(self) -> dict[str, Any]:
        if self.strategy_engine:
            self.strategy_engine.stop_scanning()
        self.risk_manager.activate_kill_switch("Manual kill via HA dashboard")
        return await self.refresh(force_broadcast=True)

    async def reset_stats(self) -> dict[str, Any]:
        self.tracker.reset_paper_stats()
        self.risk_manager.reset_daily_stats()
        self._invalidate_summary_cache()
        return await self.refresh(force_broadcast=True)

    async def set_budget(self, usd: float) -> dict[str, Any]:
        if not self.executor:
            raise RuntimeError("Executor is not available")
        if not math.isfinite(usd) or usd <= 0:
            raise ValueError("Budget must be a positive number")
        self.executor.set_order_size(float(usd))
        return await self.refresh(force_broadcast=True)

    async def set_capital(self, usd: float) -> dict[str, Any]:
        if not math.isfinite(usd) or usd <= 0:
            raise ValueError("Capital must be a positive number")
        self.tracker.set_paper_capital(float(usd))
        self._invalidate_summary_cache()
        return await self.refresh(force_broadcast=True)

    def action_capabilities(self) -> dict[str, bool]:
        return {
            "start": self.strategy_engine is not None,
            "stop": self.strategy_engine is not None,
            "kill": True,
            "reset_stats": True,
            "set_budget": self.executor is not None,
            "set_capital": True,
        }

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.update_interval_s)
            await self.refresh(force_broadcast=False)

    async def _refresh_locked(self, force_broadcast: bool) -> dict[str, Any]:
        snapshot = self._build_snapshot(include_history=False)
        snapshot_json = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
        changed = snapshot_json != self._current_snapshot_json
        self._current_snapshot = snapshot
        self._current_snapshot_json = snapshot_json
        if changed or force_broadcast:
            await self._broadcast({
                "type": "patch" if self._listeners else "patch",
                "data": snapshot,
            })
        return snapshot

    def _invalidate_summary_cache(self) -> None:
        self._last_summary_refresh = 0.0

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        if not self._listeners:
            return
        dead: list[asyncio.Queue] = []
        for listener in list(self._listeners):
            try:
                if listener.full():
                    listener.get_nowait()
                listener.put_nowait(payload)
            except Exception:
                dead.append(listener)
        for listener in dead:
            self._listeners.discard(listener)

    def _build_snapshot(self, include_history: bool = False) -> dict[str, Any]:
        now_dt = _utc_now()
        now_ts = time.time()
        self._refresh_summary_cache(now_ts)

        paper = copy.deepcopy(self._cached_paper_stats)
        daily = copy.deepcopy(self._cached_daily_stats)
        recent_trades = copy.deepcopy(self._cached_recent_trades)
        outcomes = copy.deepcopy(self._cached_outcomes)
        outcome_details = copy.deepcopy(self._cached_outcome_details)
        risk_status = self.risk_manager.get_status()
        mode = self.strategy_engine.mode if self.strategy_engine else "unknown"
        order_size = self.executor.order_size_usd if self.executor else self.config.execution.order_size_usd

        self._sync_runtime_context(mode=mode, order_size_usd=order_size, paper_total=paper.get("total_capital", 0.0))
        runtime = self.tracker.get_runtime_context() or self.tracker.latest_run_metadata() or {}

        coins_payload = []
        for coin in self.config.coins:
            coin_payload = self._build_coin_snapshot(
                coin=coin,
                now_dt=now_dt,
                now_ts=now_ts,
                outcomes=outcomes.get(coin, []),
                outcome_details=outcome_details.get(coin, []),
            )
            if include_history:
                coin_payload["series"] = list(self._history.get(coin, ()))
            coins_payload.append(coin_payload)

        snapshot = {
            "generated_at": now_dt.isoformat(),
            "version": __version__,
            "transport": {
                "update_interval_ms": int(self.update_interval_s * 1000),
                "history_sample_interval_ms": int(self.history_sample_interval_s * 1000),
                "history_points": self.history_points,
            },
            "bot": {
                "run_id": runtime.get("run_id"),
                "started_at": runtime.get("started_at"),
                "app_version": runtime.get("app_version") or __version__,
                "strategy_version": runtime.get("strategy_version"),
                "config_path": runtime.get("config_path"),
                "config_hash": runtime.get("config_hash"),
                "dry_run": bool(self.config.execution.dry_run),
                "mode": mode,
                "mode_label": MODE_LABELS.get(mode, str(mode).upper()),
                "is_active": bool(self.strategy_engine.is_active) if self.strategy_engine else False,
                "is_paused": bool(risk_status.get("paused")),
                "kill_switch": bool(risk_status.get("kill_switch")),
                "order_size_usd": order_size,
                "coins": list(self.config.coins),
            },
            "stats": {
                "paper": paper,
                "daily": daily,
                "risk": risk_status,
            },
            "recent_trades": recent_trades,
            "actions": self.action_capabilities(),
            "coins": coins_payload,
        }

        if include_history:
            snapshot["series"] = {
                coin: list(self._history.get(coin, ())) for coin in self.config.coins
            }
        return snapshot

    def _refresh_summary_cache(self, now_ts: float) -> None:
        if (now_ts - self._last_summary_refresh) < self._summary_refresh_interval_s:
            return
        self._cached_paper_stats = self.tracker.get_paper_stats()
        self._cached_daily_stats = self.tracker.get_daily_stats()
        self._cached_recent_trades = self.tracker.get_recent_trades(limit=self.recent_trades_limit)
        self._cached_outcomes = {
            coin: self.tracker.get_coin_recent_outcomes(coin, limit=self.recent_outcomes_limit)
            for coin in self.config.coins
        }
        self._cached_outcome_details = {
            coin: self.tracker.get_coin_recent_outcome_details(coin, limit=self.recent_outcomes_limit)
            for coin in self.config.coins
        }
        self._last_summary_refresh = now_ts

    def _build_coin_snapshot(
        self,
        *,
        coin: str,
        now_dt: datetime,
        now_ts: float,
        outcomes: list[str],
        outcome_details: list[dict[str, Any]],
    ) -> dict[str, Any]:
        market = self.scanner.get_market(coin) if self.scanner else None
        up_book, down_book = self.scanner.get_books(coin) if self.scanner else (None, None)
        agg = self.aggregator.get_aggregated_price(coin) if self.aggregator else None
        signal = self._classify_signal(up_book.best_ask if up_book else None, down_book.best_ask if down_book else None)
        reference = self._reference_for_market(coin=coin, market=market, aggregated_price=agg.price if agg else None, captured_at=now_dt)

        payload = {
            "coin": coin,
            "market": {
                "active": market is not None,
                "slug": market.slug if market else None,
                "question": market.question if market else None,
                "label": (
                    market.reference_label
                    if market and market.reference_label
                    else (market.question if market and market.question else (market.slug if market else None))
                ),
                "accepting_orders": bool(market.accepting_orders) if market else False,
                "start_time": _iso(market.start_time) if market else None,
                "end_time": _iso(market.end_time) if market else None,
                "seconds_remaining": max(0.0, (market.end_time - now_dt).total_seconds()) if market else None,
                "reference_price": reference.get("price"),
                "reference_source": reference.get("source"),
                "reference_label": reference.get("label"),
            },
            "price": {
                "spot": agg.price if agg else None,
                "updated_at": _unix_to_iso(agg.timestamp if agg else None),
                "velocity_30s": agg.velocity_30s if agg else None,
                "velocity_60s": agg.velocity_60s if agg else None,
                "is_trending": bool(agg.is_trending) if agg else False,
                "source_count": agg.source_count if agg else 0,
                "sources": agg.sources if agg else {},
            },
            "book": {
                "up": self._book_payload(up_book),
                "down": self._book_payload(down_book),
            },
            "signal": signal,
            "recent_outcomes": outcomes,
            "recent_resolution_details": outcome_details,
        }

        self._append_history_sample(
            coin=coin,
            now_ts=now_ts,
            market_slug=market.slug if market else None,
            reference_price=reference.get("price"),
            spot_price=agg.price if agg else None,
            up_ask=up_book.best_ask if up_book else None,
            down_ask=down_book.best_ask if down_book else None,
        )
        return payload

    @staticmethod
    def _book_payload(book) -> dict[str, Any] | None:
        if not book:
            return None
        return {
            "token_id": book.token_id,
            "best_bid": book.best_bid,
            "best_ask": book.best_ask,
            "bid_depth_usd": book.bid_depth_usd,
            "ask_depth_usd": book.ask_depth_usd,
            "updated_at": _unix_to_iso(book.timestamp),
        }

    def _reference_for_market(
        self,
        *,
        coin: str,
        market,
        aggregated_price: float | None,
        captured_at: datetime,
    ) -> dict[str, Any]:
        if not market:
            return {"price": None, "source": None, "label": None}

        key = (coin, market.slug)
        cached = self._reference_cache.get(key)
        direct_reference = getattr(market, "reference_price", None)
        if direct_reference is not None:
            cached = {
                "price": float(direct_reference),
                "source": "market",
                "label": market.reference_label or market.question or market.slug,
                "captured_at": captured_at.isoformat(),
            }
            self._reference_cache[key] = cached
            return cached

        if cached is None and aggregated_price is not None:
            cached = {
                "price": float(aggregated_price),
                "source": "runtime_fallback",
                "label": market.reference_label or market.question or market.slug,
                "captured_at": captured_at.isoformat(),
            }
            self._reference_cache[key] = cached
        elif cached is not None and not cached.get("label"):
            cached["label"] = market.reference_label or market.question or market.slug
        return cached or {"price": None, "source": None, "label": None}

    def _append_history_sample(
        self,
        *,
        coin: str,
        now_ts: float,
        market_slug: str | None,
        reference_price: float | None,
        spot_price: float | None,
        up_ask: float | None,
        down_ask: float | None,
    ) -> None:
        if (now_ts - self._last_history_at.get(coin, 0.0)) < self.history_sample_interval_s:
            return
        self._history[coin].append({
            "ts": _unix_to_iso(now_ts),
            "market_slug": market_slug,
            "reference_price": reference_price,
            "spot_price": spot_price,
            "up_ask": up_ask,
            "down_ask": down_ask,
        })
        self._last_history_at[coin] = now_ts

    def _sync_runtime_context(self, *, mode: str, order_size_usd: float, paper_total: float) -> None:
        sync_key = (mode, round(float(order_size_usd), 8), round(float(paper_total), 8))
        if sync_key == self._last_runtime_sync:
            return
        current = self.tracker.get_runtime_context()
        if not current:
            return
        updated = dict(current)
        updated["mode"] = mode
        updated["order_size_usd"] = order_size_usd
        updated["paper_capital_total"] = paper_total
        updated["dry_run"] = self.config.execution.dry_run
        self.tracker.set_runtime_context(updated)
        self._last_runtime_sync = sync_key

    def _classify_signal(self, up_ask: float | None, down_ask: float | None) -> dict[str, Any]:
        if up_ask is None or down_ask is None:
            return {"kind": None, "side": None, "label": "No signal"}

        combined = up_ask + down_ask
        dual = self.config.dual_leg
        single = self.config.single_leg
        lead = self.config.lead_lag
        swing = self.config.swing_leg

        if combined <= dual.max_combined_cost:
            return {"kind": "dual", "side": "both", "label": "Dual-leg arb"}
        if up_ask <= single.entry_max and down_ask >= single.opposite_min:
            return {"kind": "single", "side": "up", "label": "Single-leg UP"}
        if down_ask <= single.entry_max and up_ask >= single.opposite_min:
            return {"kind": "single", "side": "down", "label": "Single-leg DOWN"}
        if lead.min_entry <= up_ask <= lead.max_entry:
            return {"kind": "lead", "side": "up", "label": "Lead-lag UP"}
        if lead.min_entry <= down_ask <= lead.max_entry:
            return {"kind": "lead", "side": "down", "label": "Lead-lag DOWN"}
        if up_ask <= swing.first_leg_max:
            return {"kind": "swing", "side": "up", "label": "Swing UP"}
        if down_ask <= swing.first_leg_max:
            return {"kind": "swing", "side": "down", "label": "Swing DOWN"}
        return {"kind": None, "side": None, "label": "No signal"}
