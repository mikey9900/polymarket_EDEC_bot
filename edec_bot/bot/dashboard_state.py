"""Lightweight dashboard state provider for HA ingress/live API."""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any

from bot.control_plane import CONTROL_REQUEST_TIMEOUT_S, ControlPlane, ControlRequest
from bot.dashboard_snapshot import DashboardSnapshotBuilder


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
        session_export_fn=None,
        control_plane: ControlPlane | None = None,
        update_interval_s: float = 0.1,
        history_sample_interval_s: float = 0.5,
        history_points: int = 600,
        price_series_points: int = 240,
        slow_refresh_interval_s: float = 5.0,
    ):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.aggregator = aggregator
        self.session_export_fn = session_export_fn
        self.control_plane = control_plane or ControlPlane(
            config=config,
            tracker=tracker,
            risk_manager=risk_manager,
            strategy_engine=strategy_engine,
            executor=executor,
            session_export_fn=session_export_fn,
        )
        self.update_interval_s = max(0.05, float(update_interval_s))
        self.history_sample_interval_s = max(self.update_interval_s, float(history_sample_interval_s))
        self.history_points = max(10, int(history_points))
        self.slow_refresh_interval_s = max(self.update_interval_s, float(slow_refresh_interval_s))

        self._builder = DashboardSnapshotBuilder(
            config=config,
            tracker=tracker,
            scanner=scanner,
            strategy_engine=strategy_engine,
            executor=executor,
            aggregator=aggregator,
            control_plane=self.control_plane,
            price_series_points=price_series_points,
        )
        self._reader = self._builder.reader
        self._slow_cache = self._builder._slow_cache
        self._coin_price_series = self._builder._coin_price_series
        self._market_strikes = self._builder._market_strikes

        self._lock = asyncio.Lock()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._thread_lock = threading.Lock()
        self._thread_state: dict[str, Any] = {}
        self._thread_history: list[dict[str, Any]] = []
        self._loop_task: asyncio.Task | None = None
        self._slow_loop_task: asyncio.Task | None = None
        self._running = False
        self._state: dict[str, Any] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=self.history_points)

    def __getattr__(self, name: str) -> Any:
        for target in (self._builder, self.control_plane):
            if hasattr(target, name):
                return getattr(target, name)
        raise AttributeError(name)

    async def start(self) -> None:
        if self._running:
            return
        self._owner_loop = asyncio.get_running_loop()
        self._running = True
        self._loop_task = asyncio.create_task(self._run())
        self._slow_loop_task = asyncio.create_task(self._slow_loop())

    async def stop(self) -> None:
        self._running = False
        for task in (self._loop_task, self._slow_loop_task):
            if task:
                task.cancel()
        await asyncio.gather(
            *(t for t in (self._loop_task, self._slow_loop_task) if t),
            return_exceptions=True,
        )
        self._loop_task = None
        self._slow_loop_task = None
        self._owner_loop = None

    async def _store_snapshot(self, snapshot: dict[str, Any], *, sample_history: bool) -> None:
        async with self._lock:
            self._state = snapshot
            if sample_history:
                self._history.append(self._builder.compact_for_history(snapshot))
        with self._thread_lock:
            self._thread_state = snapshot
            if sample_history:
                self._thread_history = list(self._history)

    async def _slow_loop(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._refresh_slow_cache)
        except Exception:
            pass
        while self._running:
            try:
                await asyncio.sleep(self.slow_refresh_interval_s)
                if not self._running:
                    break
                await loop.run_in_executor(None, self._refresh_slow_cache)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self.slow_refresh_interval_s)

    async def _run(self) -> None:
        next_history_at = 0.0
        loop = asyncio.get_running_loop()
        while self._running:
            try:
                now_loop = loop.time()
                sample_history = now_loop >= next_history_at
                if sample_history:
                    self._sample_price_series()
                snapshot = self._build_snapshot()
                await self._store_snapshot(snapshot, sample_history=sample_history)
                if sample_history:
                    next_history_at = now_loop + self.history_sample_interval_s
                await asyncio.sleep(self.update_interval_s)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self.update_interval_s)

    def _refresh_slow_cache(self) -> None:
        self._builder.refresh_slow_cache()

    def _sample_price_series(self) -> None:
        self._builder.sample_price_series()

    def _build_snapshot(self) -> dict[str, Any]:
        return self._builder.build_snapshot()

    async def get_state(self) -> dict[str, Any]:
        async with self._lock:
            return dict(self._state)

    async def get_history(self) -> list[dict[str, Any]]:
        async with self._lock:
            return list(self._history)

    def get_state_threadsafe(self) -> dict[str, Any]:
        with self._thread_lock:
            return self._thread_state

    def get_history_threadsafe(self) -> list[dict[str, Any]]:
        with self._thread_lock:
            return self._thread_history

    def _apply_control(self, action: str, value: Any = None) -> dict[str, Any]:
        return self.control_plane.apply_sync(ControlRequest(action, value)).to_dict()

    async def _run_control_job(self, job) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, job)

    async def _apply_control_async(self, action: str, value: Any = None) -> dict[str, Any]:
        result = await self.control_plane.apply_async(
            ControlRequest(action, value),
            self._run_control_job,
        )
        snapshot = self._build_snapshot()
        await self._store_snapshot(snapshot, sample_history=False)
        result.state = snapshot
        return result.to_dict()

    def apply_control_threadsafe(self, payload: dict[str, Any]) -> dict[str, Any]:
        loop = self._owner_loop
        if loop is None or not loop.is_running():
            return {
                "ok": False,
                "status": 503,
                "message": "Dashboard control loop is unavailable.",
            }
        request = ControlRequest.from_payload(payload)
        future = asyncio.run_coroutine_threadsafe(
            self._apply_control_async(request.action, request.value),
            loop,
        )
        try:
            return future.result(timeout=CONTROL_REQUEST_TIMEOUT_S)
        except FutureTimeout:
            return {
                "ok": False,
                "status": 504,
                "message": "Dashboard control request timed out.",
            }
        except Exception:
            return {
                "ok": False,
                "status": 500,
                "message": "Dashboard control request failed.",
            }
