"""Live orderbook monitor for one coin using the bot's existing feeds and scanner."""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.config import load_config  # noqa: E402
from bot.market_scanner import MarketScanner  # noqa: E402
from bot.price_aggregator import PriceAggregator  # noqa: E402
from bot.price_feeds import start_all_feeds  # noqa: E402


def _format_price(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.4f}"


def render_monitor_snapshot(snapshot: dict[str, object]) -> str:
    market = snapshot.get("market")
    agg = snapshot.get("agg")
    up = snapshot.get("up_book")
    down = snapshot.get("down_book")
    sources = snapshot.get("sources", {})
    source_ages = snapshot.get("source_ages", {})
    reference = snapshot.get("reference_price")
    gap = snapshot.get("gap")
    lines = [
        f"coin: {snapshot.get('coin', '').upper()}",
        f"active slug: {market.slug if market else 'n/a'}",
        f"time remaining: {snapshot.get('time_remaining_s', 0.0):.1f}s",
        f"aggregated price: {_format_price(getattr(agg, 'price', None))}",
        f"price to beat: {_format_price(reference)}",
        f"current gap: {_format_price(gap)}",
        f"UP bid/ask: {_format_price(getattr(up, 'best_bid', None))} / {_format_price(getattr(up, 'best_ask', None))}",
        f"UP depth bid/ask: {_format_price(getattr(up, 'bid_depth_usd', None))} / {_format_price(getattr(up, 'ask_depth_usd', None))}",
        f"DOWN bid/ask: {_format_price(getattr(down, 'best_bid', None))} / {_format_price(getattr(down, 'best_ask', None))}",
        f"DOWN depth bid/ask: {_format_price(getattr(down, 'bid_depth_usd', None))} / {_format_price(getattr(down, 'ask_depth_usd', None))}",
        f"sources: {', '.join(f'{name}={price:.2f}' for name, price in sorted(sources.items())) or 'n/a'}",
        f"source ages: {', '.join(f'{name}={age:.2f}s' for name, age in sorted(source_ages.items())) or 'n/a'}",
    ]
    return "\n".join(lines)


def _build_snapshot(*, coin: str, scanner: MarketScanner, aggregator: PriceAggregator) -> dict[str, object]:
    market = scanner.get_market(coin)
    up_book, down_book = scanner.get_books(coin)
    agg = aggregator.get_aggregated_price(coin)
    reference_price = getattr(market, "reference_price", None) if market else None
    gap = None
    if agg is not None and reference_price is not None:
        gap = float(agg.price) - float(reference_price)
    now = datetime.now(timezone.utc)
    time_remaining_s = max(0.0, (market.end_time - now).total_seconds()) if market else 0.0
    return {
        "coin": coin,
        "market": market,
        "agg": agg,
        "up_book": up_book,
        "down_book": down_book,
        "reference_price": reference_price,
        "gap": gap,
        "time_remaining_s": time_remaining_s,
        "sources": getattr(agg, "sources", {}) if agg else {},
        "source_ages": getattr(agg, "source_ages_s", {}) if agg else {},
    }


async def _run_monitor(config_path: str, coin: str, *, interval_s: float, once: bool) -> None:
    config = replace(load_config(config_path), coins=(coin.lower(),))
    price_queue: asyncio.Queue = asyncio.Queue()
    aggregator = PriceAggregator(
        staleness_max_s=config.feeds.price_staleness_max_s,
        max_velocity_30s=config.dual_leg.max_velocity_30s,
        max_velocity_60s=config.dual_leg.max_velocity_60s,
    )
    scanner = MarketScanner(config)
    feed_pairs = []
    tasks = []
    try:
        feed_pairs = start_all_feeds(config, price_queue)
        tasks.extend(task for task, _feed in feed_pairs)
        tasks.append(asyncio.create_task(aggregator.run(price_queue)))
        tasks.append(asyncio.create_task(scanner.run()))
        await asyncio.sleep(max(1.0, interval_s))
        while True:
            snapshot = _build_snapshot(coin=coin.lower(), scanner=scanner, aggregator=aggregator)
            print("\x1bc", end="")
            print(render_monitor_snapshot(snapshot))
            if once:
                return
            await asyncio.sleep(interval_s)
    finally:
        aggregator.stop()
        scanner.stop()
        for _task, feed in feed_pairs:
            feed.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(scanner.aclose(), return_exceptions=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor one coin's current Polymarket orderbook.")
    parser.add_argument("--config", default=str(ROOT / "edec_bot" / "config_phase_a_single.yaml"))
    parser.add_argument("--coin", default="btc")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    asyncio.run(_run_monitor(args.config, args.coin, interval_s=max(0.25, args.interval), once=args.once))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
