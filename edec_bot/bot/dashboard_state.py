"""Lightweight dashboard state provider for HA ingress/live API."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("edec.dashboard_state")


class DashboardStateService:
    """Collects compact runtime snapshots for the live dashboard API."""

    def __init__(
        self,
        *,
        config,
        tracker,
        risk_manager,
        scanner,
        strategy_engine,
        executor,
        aggregator,
        update_interval_s: float = 0.25,
        history_sample_interval_s: float = 1.0,
        history_points: int = 600,
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
        self.history_points = max(10, int(history_points))

        self._lock = asyncio.Lock()
        self._loop_task: asyncio.Task | None = None
        self._running = False
        self._state: dict[str, Any] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=self.history_points)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
            self._loop_task = None

    async def _run(self) -> None:
        next_history_at = 0.0
        loop = asyncio.get_running_loop()
        while self._running:
            try:
                snapshot = self._build_snapshot()
                now = loop.time()
                async with self._lock:
                    self._state = snapshot
                    if now >= next_history_at:
                        self._history.append(snapshot)
                        next_history_at = now + self.history_sample_interval_s
                await asyncio.sleep(self.update_interval_s)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Dashboard snapshot update failed: %s", exc)
                await asyncio.sleep(self.update_interval_s)

    def _build_snapshot(self) -> dict[str, Any]:
        paper_total, paper_balance = self.tracker.get_paper_capital()
        price_summary: dict[str, float | None] = {}
        for coin in self.config.coins:
            agg = self.aggregator.get_aggregated_price(coin)
            price_summary[coin] = None if agg is None else float(agg.price)

        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "dry_run": bool(self.config.execution.dry_run),
            "mode": getattr(self.strategy_engine, "mode", "unknown"),
            "coins": list(self.config.coins),
            "paper": {
                "total": float(paper_total),
                "balance": float(paper_balance),
            },
            "prices": price_summary,
        }

    async def get_state(self) -> dict[str, Any]:
        async with self._lock:
            return dict(self._state)

    async def get_history(self) -> list[dict[str, Any]]:
        async with self._lock:
            return list(self._history)
