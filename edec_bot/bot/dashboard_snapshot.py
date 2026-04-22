"""Snapshot builder for the HA dashboard."""

from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from bot.tracker import ReadOnlyTrackerProxy

logger = logging.getLogger("edec.dashboard_state")

EXPECTED_FEEDS = ("binance", "coinbase", "coingecko", "polymarket_rtds")


class DashboardSnapshotBuilder:
    def __init__(
        self,
        *,
        config,
        tracker,
        scanner,
        strategy_engine,
        executor,
        aggregator,
        control_plane,
        price_series_points: int = 240,
    ):
        self.config = config
        self.tracker = tracker
        db_path = getattr(tracker, "db_path", None)
        self.reader = ReadOnlyTrackerProxy(db_path) if db_path else tracker
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.aggregator = aggregator
        self.control_plane = control_plane
        self.price_series_points = max(20, int(price_series_points))
        self._coin_price_series: dict[str, deque[tuple[float, float]]] = {}
        self._market_strikes: dict[str, tuple[str, float]] = {}
        self._slow_cache: dict[str, Any] = {
            "recent_signals": {},
            "session_by_coin": {},
            "recent_resolutions_by_coin": {},
            "paper_capital": (0.0, 0.0),
            "open_paper_trades": [],
        }

    def refresh_slow_cache(self) -> None:
        try:
            recent_signals = self.reader.get_recent_signals_by_coin(max_age_s=30.0)
        except Exception as exc:
            logger.debug("recent_signals query failed: %s", exc)
            recent_signals = self._slow_cache.get("recent_signals", {})
        try:
            session_by_coin = self.reader.get_session_stats_by_coin()
        except Exception as exc:
            logger.debug("session_by_coin query failed: %s", exc)
            session_by_coin = self._slow_cache.get("session_by_coin", {})
        resolutions_by_coin: dict[str, list[dict]] = {}
        for coin in self.config.coins:
            scanner_resolutions: list[dict] = []
            if self.scanner is not None and hasattr(self.scanner, "get_recent_resolutions"):
                try:
                    scanner_resolutions = list(self.scanner.get_recent_resolutions(coin, limit=4))
                except Exception as exc:
                    logger.debug("scanner recent_resolutions failed for %s: %s", coin, exc)
            try:
                if scanner_resolutions:
                    resolutions_by_coin[coin] = scanner_resolutions
                else:
                    resolutions_by_coin[coin] = self.reader.get_coin_recent_resolutions(coin, limit=4)
            except Exception as exc:
                logger.debug("recent_resolutions query failed for %s: %s", coin, exc)
                resolutions_by_coin[coin] = scanner_resolutions or self._slow_cache.get(
                    "recent_resolutions_by_coin", {}
                ).get(coin, [])
        try:
            paper_capital = self.reader.get_paper_capital()
        except Exception as exc:
            logger.debug("paper_capital query failed: %s", exc)
            paper_capital = self._slow_cache.get("paper_capital", (0.0, 0.0))
        try:
            open_paper_trades = self.reader.get_open_paper_trades()
        except Exception as exc:
            logger.debug("open_paper_trades query failed: %s", exc)
            open_paper_trades = self._slow_cache.get("open_paper_trades", [])
        self._slow_cache.clear()
        self._slow_cache.update(
            {
                "recent_signals": recent_signals,
                "session_by_coin": session_by_coin,
                "recent_resolutions_by_coin": resolutions_by_coin,
                "paper_capital": paper_capital,
                "open_paper_trades": open_paper_trades,
            }
        )

    def sample_price_series(self) -> None:
        now = time.time()
        for coin in self.config.coins:
            agg = self.aggregator.get_aggregated_price(coin)
            if agg is None:
                continue
            series = self._coin_price_series.setdefault(
                coin, deque(maxlen=self.price_series_points)
            )
            series.append((now, float(agg.price)))

    def build_snapshot(self) -> dict[str, Any]:
        slow = self._slow_cache
        recent_signals = slow.get("recent_signals", {})
        session_by_coin = slow.get("session_by_coin", {})
        resolutions_by_coin = slow.get("recent_resolutions_by_coin", {})
        paper_total, paper_balance = slow.get("paper_capital", (0.0, 0.0))
        open_paper_trades = slow.get("open_paper_trades", [])
        paper_adjustments = self._build_open_paper_adjustments(open_paper_trades)
        paper_equity = float(paper_balance) + sum(
            float(item.get("liquidation_value", 0.0))
            for item in paper_adjustments.values()
        )

        open_singles = list(self.executor.get_open_positions().values())
        open_swings = list(getattr(self.executor, "_open_swing_positions", {}).values())

        coins_payload: dict[str, Any] = {}
        for coin in self.config.coins:
            base_session = session_by_coin.get(coin, {"wins": 0, "losses": 0, "open": 0, "pnl": 0.0})
            coins_payload[coin] = self._build_coin_payload(
                coin,
                recent_signals=recent_signals.get(coin, []),
                session=self._augment_session_payload(
                    base_session,
                    paper_adjustments.get(coin, {"open_count": 0, "unrealized_pnl": 0.0}),
                ),
                resolutions=resolutions_by_coin.get(coin, []),
                open_singles=open_singles,
                open_swings=open_swings,
            )

        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "dry_run": bool(self.config.execution.dry_run),
            "mode": getattr(self.strategy_engine, "mode", "unknown"),
            "controls": self.control_plane.build_controls_payload(),
            **self._build_codex_payload(),
            "coins_order": list(self.config.coins),
            "coins": coins_payload,
            "summary": {
                "paper": {
                    "total": float(paper_total),
                    "balance": float(paper_equity),
                    "pnl": float(paper_equity) - float(paper_total),
                },
            },
        }

    def _build_codex_payload(self) -> dict[str, Any]:
        manager = getattr(self.control_plane, "codex_manager", None)
        if manager is None:
            return {"codex": {}, "tuner": {}}
        try:
            return manager.snapshot()
        except Exception as exc:
            logger.debug("codex snapshot failed: %s", exc)
            return {"codex": {}, "tuner": {}}

    def _build_open_paper_adjustments(self, open_trades: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        adjustments: dict[str, dict[str, float]] = {}
        for trade in open_trades or []:
            coin = str(trade.get("coin") or "").lower()
            if not coin:
                continue
            liquidation_value, unrealized_pnl = self._estimate_open_paper_mark_to_market(trade)
            bucket = adjustments.setdefault(
                coin,
                {"open_count": 0.0, "liquidation_value": 0.0, "unrealized_pnl": 0.0},
            )
            bucket["open_count"] += 1.0
            bucket["liquidation_value"] += float(liquidation_value)
            bucket["unrealized_pnl"] += float(unrealized_pnl)
        return adjustments

    def _augment_session_payload(self, session: dict[str, Any], adjustment: dict[str, float]) -> dict[str, Any]:
        payload = {
            "wins": int((session or {}).get("wins", 0) or 0),
            "losses": int((session or {}).get("losses", 0) or 0),
            "open": int((session or {}).get("open", 0) or 0),
            "pnl": float((session or {}).get("pnl", 0.0) or 0.0),
        }
        open_count = int(adjustment.get("open_count", 0) or 0)
        payload["open"] = max(payload["open"], open_count)
        payload["realized_pnl"] = payload["pnl"]
        payload["unrealized_pnl"] = float(adjustment.get("unrealized_pnl", 0.0) or 0.0)
        payload["pnl"] = payload["realized_pnl"] + payload["unrealized_pnl"]
        return payload

    def _estimate_open_paper_mark_to_market(self, trade: dict[str, Any]) -> tuple[float, float]:
        coin = str(trade.get("coin") or "").lower()
        strategy_type = str(trade.get("strategy_type") or "").lower()
        cost = float(trade.get("cost") or 0.0)
        shares = float(trade.get("shares") or 0.0)
        if not coin or shares <= 0:
            return cost, 0.0

        market = self.scanner.get_market(coin) if self.scanner else None
        market_matches = bool(
            market
            and str(trade.get("market_slug") or "") == str(getattr(market, "slug", ""))
        )
        if not market_matches:
            return cost, 0.0

        if strategy_type == "dual_leg":
            up_book, down_book = self.scanner.get_books(coin)
            if not up_book or not down_book:
                return cost, 0.0
            combined_bid = float(up_book.best_bid) + float(down_book.best_bid)
            liquidation_value = max(0.0, combined_bid * shares)
            unrealized_pnl = liquidation_value - cost - float(trade.get("fee_total") or 0.0)
            return max(0.0, cost + unrealized_pnl), unrealized_pnl

        bid = self._current_bid_for_position(coin, str(trade.get("side") or ""))
        if bid is None:
            return cost, 0.0
        entry_price = float(trade.get("entry_price") or 0.0)
        fee_rate = float(getattr(market, "fee_rate", 0.0) or 0.0)
        fee_buy = fee_rate * entry_price * (1.0 - entry_price)
        fee_sell = fee_rate * bid * (1.0 - bid)
        unrealized_pnl = (bid - entry_price - fee_buy - fee_sell) * shares
        return max(0.0, cost + unrealized_pnl), unrealized_pnl

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

        sources_payload = self._build_sources_payload(agg)
        market_payload = self._build_market_payload(coin, live_price)
        open_trades_payload = self._build_open_trades_payload(coin, open_singles, open_swings)
        if live_price is not None and market_payload and market_payload.get("strike") is not None:
            chart_color = "green" if live_price >= market_payload["strike"] else "red"
        else:
            chart_color = "neutral"

        series = list(self._coin_price_series.get(coin, ()))
        price_series = [{"t": t, "p": p} for t, p in series]
        resolution_payload = [{"winner": str(r.get("winner") or "")} for r in (resolutions or [])[:4]]

        return {
            "coin": coin,
            "live_price": live_price,
            "sources": sources_payload,
            "market": market_payload,
            "bot_signals": recent_signals,
            "open_trades": open_trades_payload,
            "session": session,
            "recent_resolutions": resolution_payload,
            "price_series": price_series,
            "chart_color": chart_color,
        }

    def _build_sources_payload(self, agg) -> dict[str, Any]:
        active_sources = agg.sources if agg is not None else {}
        active_ages = agg.source_ages_s if agg is not None else {}
        feeds = []
        for name in EXPECTED_FEEDS:
            if name in active_sources:
                feeds.append(
                    {
                        "name": name,
                        "active": True,
                        "age_s": float(active_ages.get(name, 0.0)),
                        "price": float(active_sources[name]),
                    }
                )
            else:
                feeds.append({"name": name, "active": False, "age_s": None, "price": None})
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
        market_prediction = None
        if up_bid is not None and up_ask is not None and down_bid is not None and down_ask is not None:
            up_prob = (up_bid + up_ask) / 2.0
            down_prob = (down_bid + down_ask) / 2.0
            market_prediction = {"up_prob": round(up_prob, 4), "down_prob": round(down_prob, 4)}
        strike: float | None = None
        if market.reference_price is not None:
            candidate = float(market.reference_price)
            if candidate > 0 and (
                live_price is None
                or live_price <= 0
                or 0.5 <= (candidate / float(live_price)) <= 1.5
            ):
                strike = candidate
        if strike is None:
            cached = self._market_strikes.get(coin)
            if cached and cached[0] == market.slug:
                strike = cached[1]
            elif live_price is not None:
                self._market_strikes[coin] = (market.slug, float(live_price))
                strike = float(live_price)
        return {
            "slug": market.slug,
            "question": market.question,
            "strike": strike,
            "volume": float(market.volume) if market.volume is not None else None,
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
        for pos in open_singles:
            if getattr(pos.market, "coin", None) != coin:
                continue
            current_bid = self._current_bid_for_position(pos.market.coin, pos.side)
            entry = float(pos.entry_price)
            shares = float(pos.shares)
            unrealized = (current_bid - entry) * shares if current_bid is not None else None
            out.append(
                {
                    "strategy": getattr(pos, "strategy_type", "single_leg"),
                    "side": pos.side,
                    "entry_price": entry,
                    "target_price": float(pos.target_price) if pos.target_price is not None else None,
                    "shares": shares,
                    "current_bid": current_bid,
                    "unrealized_pnl": unrealized,
                    "hold_to_resolution": bool(getattr(pos, "hold_to_resolution", False)),
                }
            )
        for pos in open_swings:
            if getattr(pos.market, "coin", None) != coin:
                continue
            current_bid = self._current_bid_for_position(pos.market.coin, pos.first_side)
            entry = float(pos.first_entry_price)
            shares = float(pos.first_shares)
            unrealized = (current_bid - entry) * shares if current_bid is not None else None
            out.append(
                {
                    "strategy": "swing_leg",
                    "side": pos.first_side,
                    "entry_price": entry,
                    "target_price": None,
                    "shares": shares,
                    "current_bid": current_bid,
                    "unrealized_pnl": unrealized,
                    "hold_to_resolution": bool(getattr(pos, "hold_to_resolution", False)),
                }
            )
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
    def compact_for_history(snapshot: dict[str, Any]) -> dict[str, Any]:
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
