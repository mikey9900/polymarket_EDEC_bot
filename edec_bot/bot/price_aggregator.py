"""Combines multiple price feeds into aggregated views with per-coin velocity tracking."""

import asyncio
import logging
import time
from collections import deque

from bot.models import AggregatedPrice, PriceTick

logger = logging.getLogger(__name__)

# Weights for weighted median calculation
SOURCE_WEIGHTS = {
    "polymarket_rtds": 3,  # Chainlink oracle — what Polymarket resolves against
    "binance": 2,
    "coinbase": 2,
    "coingecko": 1,
}


class PriceAggregator:
    def __init__(self, staleness_max_s: float = 5.0, max_velocity_30s: float = 0.15,
                 max_velocity_60s: float = 0.25):
        self.staleness_max_s = staleness_max_s
        self.max_velocity_30s = max_velocity_30s
        self.max_velocity_60s = max_velocity_60s

        # Per-coin price history for velocity calculation (2 minutes of ticks each)
        self._histories: dict[str, deque[PriceTick]] = {}
        # Per-coin latest tick from each source: {coin: {source: PriceTick}}
        self._latest: dict[str, dict[str, PriceTick]] = {}
        self._running = False

    async def run(self, queue: asyncio.Queue):
        """Continuously consume price ticks from the shared queue."""
        self._running = True
        while self._running:
            try:
                tick = await asyncio.wait_for(queue.get(), timeout=1.0)
                coin = tick.coin

                if coin not in self._histories:
                    self._histories[coin] = deque(maxlen=500)
                if coin not in self._latest:
                    self._latest[coin] = {}

                self._latest[coin][tick.source] = tick
                self._histories[coin].append(tick)
            except asyncio.TimeoutError:
                continue

    def stop(self):
        self._running = False

    def get_aggregated_price(self, coin: str = "btc") -> AggregatedPrice | None:
        """Compute weighted median price, velocity, and trend detection for a coin."""
        now = time.time()
        latest = self._latest.get(coin, {})

        # Filter out stale sources
        active = {
            src: tick for src, tick in latest.items()
            if (now - tick.timestamp) < self.staleness_max_s
        }

        if not active:
            return None

        # Weighted median
        price = self._weighted_median(active)

        # Velocity calculations
        vel_30 = self._calc_velocity(coin, now, 30)
        vel_60 = self._calc_velocity(coin, now, 60)
        is_trending = (
            abs(vel_30) > self.max_velocity_30s or
            abs(vel_60) > self.max_velocity_60s
        )

        return AggregatedPrice(
            price=price,
            timestamp=now,
            velocity_30s=vel_30,
            velocity_60s=vel_60,
            is_trending=is_trending,
            source_count=len(active),
            sources={src: tick.price for src, tick in active.items()},
            coin=coin,
        )

    def get_source_count(self, coin: str = "btc") -> int:
        """How many non-stale feeds are live for a coin."""
        now = time.time()
        latest = self._latest.get(coin, {})
        return sum(
            1 for tick in latest.values()
            if (now - tick.timestamp) < self.staleness_max_s
        )

    def get_all_coins_snapshot(self) -> dict[str, AggregatedPrice | None]:
        """Return aggregated price for every coin that has received ticks."""
        return {coin: self.get_aggregated_price(coin) for coin in self._latest}

    def _weighted_median(self, active: dict[str, PriceTick]) -> float:
        """Compute weighted median of active price sources."""
        entries = []
        for src, tick in active.items():
            weight = SOURCE_WEIGHTS.get(src, 1)
            entries.extend([tick.price] * weight)
        entries.sort()
        mid = len(entries) // 2
        if len(entries) % 2 == 0:
            return (entries[mid - 1] + entries[mid]) / 2
        return entries[mid]

    def _calc_velocity(self, coin: str, now: float, seconds: int) -> float:
        """Calculate % price change over the last N seconds for a coin."""
        history = self._histories.get(coin)
        if not history:
            return 0.0

        current_price = history[-1].price
        target_time = now - seconds

        # Find the price closest to N seconds ago
        old_price = None
        for tick in reversed(history):
            if tick.timestamp <= target_time:
                old_price = tick.price
                break

        if old_price is None or old_price == 0:
            return 0.0

        return ((current_price - old_price) / old_price) * 100
