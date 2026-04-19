"""Lightweight dashboard state provider for HA ingress/live API."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from bot.tracker import ReadOnlyTrackerProxy

logger = logging.getLogger("edec.dashboard_state")


# Feed names we display LEDs for. Order matters → drives the LED row left→right.
EXPECTED_FEEDS = ("binance", "coinbase", "coingecko", "polymarket_rtds")
CONTROL_MODE_ORDER = ("both", "dual", "single", "lead", "swing", "off")
CONTROL_BUDGET_OPTIONS = (1, 2, 5, 10, 15, 20)


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
        update_interval_s: float = 0.1,
        history_sample_interval_s: float = 0.5,
        history_points: int = 600,
        price_series_points: int = 240,
        slow_refresh_interval_s: float = 5.0,
    ):
        self.config = config
        self.tracker = tracker
        # Dedicated read-only SQLite connection for off-loop slow-tier queries.
        # Lets the dashboard worker read while the loop's tracker.conn keeps
        # accepting writes from strategy evaluations. Falls back to the live
        # tracker for tests that pass mocks without a real db_path.
        db_path = getattr(tracker, "db_path", None)
        self._reader = ReadOnlyTrackerProxy(db_path) if db_path else tracker
        self.risk_manager = risk_manager
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.aggregator = aggregator
        # Floor at 50ms so we don't pin a CPU.
        self.update_interval_s = max(0.05, float(update_interval_s))
        self.history_sample_interval_s = max(self.update_interval_s, float(history_sample_interval_s))
        self.history_points = max(10, int(history_points))
        self.price_series_points = max(20, int(price_series_points))
        # DB-derived data (recent_resolutions, session_stats, recent_signals) is the
        # heavy part of each snapshot — cache it and refresh at this slower cadence.
        self.slow_refresh_interval_s = max(self.update_interval_s, float(slow_refresh_interval_s))

        self._lock = asyncio.Lock()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        # Cross-thread accessor for the dashboard server (which runs in its own
        # thread + event loop so it stays responsive when the bot's loop hitches).
        self._thread_lock = threading.Lock()
        self._thread_state: dict[str, Any] = {}
        self._thread_history: list[dict[str, Any]] = []
        self._loop_task: asyncio.Task | None = None
        self._running = False
        self._state: dict[str, Any] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=self.history_points)
        # Per-coin rolling underlying-price series: {coin: deque[(t_unix, price)]}
        self._coin_price_series: dict[str, deque[tuple[float, float]]] = {}
        # Slow-tier cache (refreshed every slow_refresh_interval_s)
        self._slow_cache: dict[str, Any] = {
            "recent_signals": {},
            "session_by_coin": {},
            "recent_resolutions_by_coin": {},
            "paper_capital": (0.0, 0.0),
        }
        self._next_slow_refresh_at: float = 0.0
        # Per-coin captured strike: {coin: (slug, strike_price)}. Sampled from
        # the live aggregator the first time we see each new market window so
        # the dashboard can draw the open-price line.
        self._market_strikes: dict[str, tuple[str, float]] = {}
        self._slow_loop_task: asyncio.Task | None = None
        self._slow_refresh_in_flight: bool = False

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
                self._history.append(self._compact_for_history(snapshot))
        with self._thread_lock:
            self._thread_state = snapshot
            if sample_history:
                self._thread_history = list(self._history)

    async def _slow_loop(self) -> None:
        """Refresh the DB-backed slow tier on its own cadence.

        Runs as an independent task so the 100ms snapshot loop never has to
        wait on SQLite — keeps live price/timer fresh while DB reads happen.
        """
        loop = asyncio.get_running_loop()
        # Prime immediately so the first snapshot has data.
        try:
            await loop.run_in_executor(None, self._refresh_slow_cache)
        except Exception as exc:
            logger.debug("Initial slow refresh failed: %s", exc)
        while self._running:
            try:
                await asyncio.sleep(self.slow_refresh_interval_s)
                if not self._running:
                    break
                await loop.run_in_executor(None, self._refresh_slow_cache)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Slow refresh failed: %s", exc)

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
            except Exception as exc:
                logger.debug("Dashboard snapshot update failed: %s", exc)
                await asyncio.sleep(self.update_interval_s)

    def _refresh_slow_cache(self) -> None:
        """Pull DB-backed data once, store in cache. Runs in a worker thread.

        Uses self._reader (a dedicated SQLite connection) so reads here never
        block the main loop's writer connection.
        """
        try:
            recent_signals = self._reader.get_recent_signals_by_coin(max_age_s=30.0)
        except Exception as exc:
            logger.debug("recent_signals query failed: %s", exc)
            recent_signals = self._slow_cache.get("recent_signals", {})
        try:
            session_by_coin = self._reader.get_session_stats_by_coin()
        except Exception as exc:
            logger.debug("session_by_coin query failed: %s", exc)
            session_by_coin = self._slow_cache.get("session_by_coin", {})
        resolutions_by_coin: dict[str, list[dict]] = {}
        for coin in self.config.coins:
            try:
                resolutions_by_coin[coin] = self._reader.get_coin_recent_resolutions(coin, limit=4)
            except Exception as exc:
                logger.debug("recent_resolutions query failed for %s: %s", coin, exc)
                resolutions_by_coin[coin] = self._slow_cache.get(
                    "recent_resolutions_by_coin", {}
                ).get(coin, [])
        try:
            paper_capital = self._reader.get_paper_capital()
        except Exception as exc:
            logger.debug("paper_capital query failed: %s", exc)
            paper_capital = self._slow_cache.get("paper_capital", (0.0, 0.0))
        self._slow_cache = {
            "recent_signals": recent_signals,
            "session_by_coin": session_by_coin,
            "recent_resolutions_by_coin": resolutions_by_coin,
            "paper_capital": paper_capital,
        }

    # ------------------------------------------------------------------
    # Snapshot assembly
    # ------------------------------------------------------------------

    def _sample_price_series(self) -> None:
        """Append latest aggregated price for each coin into the rolling series."""
        now = time.time()
        for coin in self.config.coins:
            agg = self.aggregator.get_aggregated_price(coin)
            if agg is None:
                continue
            series = self._coin_price_series.setdefault(
                coin, deque(maxlen=self.price_series_points)
            )
            series.append((now, float(agg.price)))

    def _build_snapshot(self) -> dict[str, Any]:
        slow = self._slow_cache
        recent_signals = slow.get("recent_signals", {})
        session_by_coin = slow.get("session_by_coin", {})
        resolutions_by_coin = slow.get("recent_resolutions_by_coin", {})
        paper_total, paper_balance = slow.get("paper_capital", (0.0, 0.0))

        open_singles = list(self.executor.get_open_positions().values())
        open_swings = list(getattr(self.executor, "_open_swing_positions", {}).values())

        coins_payload: dict[str, Any] = {}
        for coin in self.config.coins:
            coins_payload[coin] = self._build_coin_payload(
                coin,
                recent_signals=recent_signals.get(coin, []),
                session=session_by_coin.get(coin, {"wins": 0, "losses": 0, "open": 0, "pnl": 0.0}),
                resolutions=resolutions_by_coin.get(coin, []),
                open_singles=open_singles,
                open_swings=open_swings,
            )

        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "dry_run": bool(self.config.execution.dry_run),
            "mode": getattr(self.strategy_engine, "mode", "unknown"),
            "controls": self._build_controls_payload(),
            "coins_order": list(self.config.coins),
            "coins": coins_payload,
            "summary": {
                "paper": {
                    "total": float(paper_total),
                    "balance": float(paper_balance),
                    "pnl": float(paper_balance) - float(paper_total),
                },
            },
        }

    def _build_controls_payload(self) -> dict[str, Any]:
        risk_status = self.risk_manager.get_status() if self.risk_manager else {}
        kill_switch = bool(risk_status.get("kill_switch", False))
        paused = bool(risk_status.get("paused", False))
        scanning = bool(getattr(self.strategy_engine, "is_active", False))
        if kill_switch:
            state = "killed"
        elif paused or not scanning:
            state = "paused"
        else:
            state = "running"
        order_size = (
            float(self.executor.order_size_usd)
            if self.executor is not None
            else float(self.config.execution.order_size_usd)
        )
        return {
            "state": state,
            "kill_switch": kill_switch,
            "paused": paused,
            "scanning": scanning,
            "mode": getattr(self.strategy_engine, "mode", "unknown"),
            "order_size_usd": order_size,
            "available_modes": list(CONTROL_MODE_ORDER),
            "budget_options": list(CONTROL_BUDGET_OPTIONS),
        }

    def _build_coin_payload(
        self,
        coin: str,
        *,
        recent_signals: list[dict],
        session: dict,
        resolutions: list[dict],
        open_singles: list,
        open_swings: list,
    ) -> dict[str, Any]:
        agg = self.aggregator.get_aggregated_price(coin)
        live_price = float(agg.price) if agg is not None else None

        sources_payload = self._build_sources_payload(coin, agg)
        market_payload = self._build_market_payload(coin, live_price)
        open_trades_payload = self._build_open_trades_payload(coin, open_singles, open_swings)

        # Chart color: green if above strike, red if below, neutral otherwise
        strike = market_payload.get("strike") if market_payload else None
        if live_price is not None and strike is not None:
            chart_color = "green" if live_price >= strike else "red"
        else:
            chart_color = "neutral"

        series = list(self._coin_price_series.get(coin, ()))
        price_series = [{"t": t, "p": p} for t, p in series]

        return {
            "coin": coin,
            "live_price": live_price,
            "sources": sources_payload,
            "market": market_payload,
            "bot_signals": recent_signals,
            "open_trades": open_trades_payload,
            "session": session,
            "recent_resolutions": resolutions,
            "price_series": price_series,
            "chart_color": chart_color,
        }

    def _build_sources_payload(self, coin: str, agg) -> dict[str, Any]:
        active_sources = agg.sources if agg is not None else {}
        active_ages = agg.source_ages_s if agg is not None else {}
        feeds = []
        for name in EXPECTED_FEEDS:
            if name in active_sources:
                feeds.append({
                    "name": name,
                    "active": True,
                    "age_s": float(active_ages.get(name, 0.0)),
                    "price": float(active_sources[name]),
                })
            else:
                feeds.append({
                    "name": name,
                    "active": False,
                    "age_s": None,
                    "price": None,
                })
        return {
            "count": int(agg.source_count) if agg is not None else 0,
            "expected": len(EXPECTED_FEEDS),
            "feeds": feeds,
        }

    def _build_market_payload(self, coin: str, live_price: float | None) -> dict[str, Any] | None:
        market = self.scanner.get_market(coin)
        if market is None:
            return None
        up_book, down_book = self.scanner.get_books(coin)

        now = datetime.now(timezone.utc)
        try:
            time_remaining_s = max(0.0, (market.end_time - now).total_seconds())
        except Exception:
            time_remaining_s = None

        up_bid = float(up_book.best_bid) if up_book else None
        up_ask = float(up_book.best_ask) if up_book else None
        down_bid = float(down_book.best_bid) if down_book else None
        down_ask = float(down_book.best_ask) if down_book else None

        # Market-implied probabilities = mid of YES/NO books. Sum ≈ 1.0.
        market_prediction = None
        if up_bid is not None and up_ask is not None and down_bid is not None and down_ask is not None:
            up_prob = (up_bid + up_ask) / 2.0
            down_prob = (down_bid + down_ask) / 2.0
            market_prediction = {
                "up_prob": round(up_prob, 4),
                "down_prob": round(down_prob, 4),
            }

        # Capture strike on first sight of each market window. The 5-min
        # up/down market resolves UP if BTC's close > open, so the strike line
        # on the chart is the BTC price sampled when the window opened.
        # MarketInfo.reference_price isn't populated by the scanner, so we
        # snapshot from the live aggregator the first time we see this slug.
        strike: float | None = None
        if market.reference_price is not None:
            strike = float(market.reference_price)
        else:
            cached = self._market_strikes.get(coin)
            if cached and cached[0] == market.slug:
                strike = cached[1]
            elif live_price is not None:
                self._market_strikes[coin] = (market.slug, float(live_price))
                strike = float(live_price)
        strike_label = market.reference_label or ("open" if strike is not None else "")

        return {
            "slug": market.slug,
            "question": market.question,
            "strike": strike,
            "strike_label": strike_label,
            "start_time": market.start_time.isoformat() if market.start_time else None,
            "end_time": market.end_time.isoformat() if market.end_time else None,
            "time_remaining_s": time_remaining_s,
            "up_bid": up_bid,
            "up_ask": up_ask,
            "down_bid": down_bid,
            "down_ask": down_ask,
            "market_prediction": market_prediction,
        }

    def _build_open_trades_payload(self, coin: str, open_singles: list, open_swings: list) -> list[dict]:
        out: list[dict] = []
        # Single-leg / lead-lag / etc. positions
        for pos in open_singles:
            if getattr(pos.market, "coin", None) != coin:
                continue
            current_bid = self._current_bid_for_position(pos.market.coin, pos.side)
            entry = float(pos.entry_price)
            shares = float(pos.shares)
            unrealized = (current_bid - entry) * shares if current_bid is not None else None
            out.append({
                "strategy": getattr(pos, "strategy_type", "single_leg"),
                "side": pos.side,
                "entry_price": entry,
                "target_price": float(pos.target_price) if pos.target_price is not None else None,
                "shares": shares,
                "current_bid": current_bid,
                "unrealized_pnl": unrealized,
                "hold_to_resolution": bool(getattr(pos, "hold_to_resolution", False)),
            })
        # Swing positions (first leg)
        for pos in open_swings:
            if getattr(pos.market, "coin", None) != coin:
                continue
            current_bid = self._current_bid_for_position(pos.market.coin, pos.first_side)
            entry = float(pos.first_entry_price)
            shares = float(pos.first_shares)
            unrealized = (current_bid - entry) * shares if current_bid is not None else None
            out.append({
                "strategy": "swing_leg",
                "side": pos.first_side,
                "entry_price": entry,
                "target_price": None,
                "shares": shares,
                "current_bid": current_bid,
                "unrealized_pnl": unrealized,
                "hold_to_resolution": bool(getattr(pos, "hold_to_resolution", False)),
            })
        return out

    def _current_bid_for_position(self, coin: str, side: str) -> float | None:
        up_book, down_book = self.scanner.get_books(coin)
        book = up_book if (side or "").upper() == "UP" else down_book
        if book is None:
            return None
        try:
            return float(book.best_bid)
        except Exception:
            return None

    @staticmethod
    def _compact_for_history(snapshot: dict[str, Any]) -> dict[str, Any]:
        """Strip heavy per-coin series before pushing into the history deque."""
        light_coins = {}
        for coin, payload in snapshot.get("coins", {}).items():
            light_coins[coin] = {
                "live_price": payload.get("live_price"),
                "session": payload.get("session"),
                "chart_color": payload.get("chart_color"),
            }
        return {
            "timestamp_utc": snapshot.get("timestamp_utc"),
            "summary": snapshot.get("summary"),
            "coins": light_coins,
        }

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    async def get_state(self) -> dict[str, Any]:
        async with self._lock:
            return dict(self._state)

    async def get_history(self) -> list[dict[str, Any]]:
        async with self._lock:
            return list(self._history)

    # ------------------------------------------------------------------
    # Thread-safe accessors for the dashboard HTTP server thread
    # ------------------------------------------------------------------

    def get_state_threadsafe(self) -> dict[str, Any]:
        with self._thread_lock:
            return self._thread_state

    def get_history_threadsafe(self) -> list[dict[str, Any]]:
        with self._thread_lock:
            return self._thread_history

    def _apply_control(self, action: str, value: Any = None) -> dict[str, Any]:
        action = str(action or "").strip().lower()
        if action == "start":
            if self.strategy_engine:
                self.strategy_engine.start_scanning()
            if self.risk_manager:
                self.risk_manager.resume()
                self.risk_manager.deactivate_kill_switch()
            return {"ok": True, "status": 200, "message": "Scanning started."}
        if action == "stop":
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            if self.risk_manager:
                self.risk_manager.pause()
            return {"ok": True, "status": 200, "message": "Trading paused."}
        if action == "kill":
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            if self.risk_manager:
                self.risk_manager.activate_kill_switch("Manual kill via HA dashboard")
            return {"ok": True, "status": 200, "message": "Kill switch activated."}
        if action == "mode":
            mode = str(value or "").strip().lower()
            if mode not in CONTROL_MODE_ORDER:
                return {"ok": False, "status": 400, "message": f"Unknown mode: {mode or 'empty'}"}
            if not self.strategy_engine or not self.strategy_engine.set_mode(mode):
                return {"ok": False, "status": 400, "message": "Strategy engine unavailable."}
            return {"ok": True, "status": 200, "message": f"Mode set to {mode.upper()}."}
        if action == "budget":
            try:
                amount = float(value)
            except (TypeError, ValueError):
                return {"ok": False, "status": 400, "message": "Budget must be a number."}
            if amount <= 0:
                return {"ok": False, "status": 400, "message": "Budget must be positive."}
            if not self.executor:
                return {"ok": False, "status": 400, "message": "Execution engine unavailable."}
            self.executor.set_order_size(amount)
            return {"ok": True, "status": 200, "message": f"Budget set to ${amount:.0f} per trade."}
        return {"ok": False, "status": 400, "message": f"Unknown action: {action or 'empty'}"}

    async def _apply_control_async(self, action: str, value: Any = None) -> dict[str, Any]:
        result = self._apply_control(action, value)
        snapshot = self._build_snapshot()
        await self._store_snapshot(snapshot, sample_history=False)
        result["state"] = snapshot
        return result

    def apply_control_threadsafe(self, payload: dict[str, Any]) -> dict[str, Any]:
        loop = self._owner_loop
        if loop is None or not loop.is_running():
            return {
                "ok": False,
                "status": 503,
                "message": "Dashboard control loop is unavailable.",
            }
        future = asyncio.run_coroutine_threadsafe(
            self._apply_control_async(payload.get("action", ""), payload.get("value")),
            loop,
        )
        try:
            return future.result(timeout=5.0)
        except Exception as exc:
            logger.debug("Dashboard control request failed: %s", exc)
            return {
                "ok": False,
                "status": 500,
                "message": "Dashboard control request failed.",
            }
