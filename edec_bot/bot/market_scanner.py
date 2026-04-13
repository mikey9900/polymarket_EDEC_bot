"""Finds active 5-min up/down markets for all configured coins and monitors order books."""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import httpx

from bot.config import Config
from bot.models import MarketInfo, OrderBookSnapshot
from bot.clob_ws_feed import ClobWebSocketFeed

logger = logging.getLogger(__name__)


class MarketScanner:
    def __init__(self, config: Config):
        self.config = config
        self.coins = list(config.coins)

        # Per-coin state
        self._markets: dict[str, MarketInfo | None] = {c: None for c in self.coins}
        self._up_books: dict[str, OrderBookSnapshot | None] = {c: None for c in self.coins}
        self._down_books: dict[str, OrderBookSnapshot | None] = {c: None for c in self.coins}

        # Markets that have ended but not yet had their outcome resolved
        self._expired_markets: list[MarketInfo] = []

        # Real-time WebSocket book feed
        self._ws_feed = ClobWebSocketFeed()

        self._running = False
        self._http = httpx.AsyncClient(timeout=10.0)

    async def run(self):
        """Launch WebSocket feed + parallel monitoring tasks for all coins."""
        self._running = True
        tasks = [asyncio.create_task(self._ws_feed.run())]
        tasks += [asyncio.create_task(self._monitor_coin(coin)) for coin in self.coins]
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self):
        self._running = False
        self._ws_feed.stop()

    # --- Public accessors ---

    def get_market(self, coin: str) -> MarketInfo | None:
        return self._markets.get(coin)

    def get_books(self, coin: str) -> tuple[OrderBookSnapshot | None, OrderBookSnapshot | None]:
        return self._up_books.get(coin), self._down_books.get(coin)

    def get_book_for_token(self, token_id: str) -> OrderBookSnapshot | None:
        """Return the live WebSocket-cached book for any token ID."""
        return self._ws_feed.get_book(token_id)

    def pop_expired_markets(self) -> list[MarketInfo]:
        """Return and clear all markets that have ended but not yet been resolved."""
        expired = self._expired_markets.copy()
        self._expired_markets.clear()
        return expired

    def get_all_active(self) -> dict[str, MarketInfo]:
        """Return all coins that currently have an active market."""
        return {c: m for c, m in self._markets.items() if m is not None}

    def get_status_snapshot(self) -> dict:
        """Return a dict of coin → (up_ask, down_ask) for Telegram /status."""
        result = {}
        for coin in self.coins:
            up = self._up_books.get(coin)
            down = self._down_books.get(coin)
            if up and down:
                result[coin] = {"up_ask": up.best_ask, "down_ask": down.best_ask}
            else:
                result[coin] = None
        return result

    # --- Internal monitoring loop per coin ---

    async def _monitor_coin(self, coin: str):
        """Continuously discover and monitor one coin's 5-min market."""
        while self._running:
            try:
                market = await self._discover_market(coin)
                if market and market.accepting_orders:
                    self._markets[coin] = market
                    logger.info(f"[{coin.upper()}] Market: {market.slug} → ends {market.end_time.strftime('%H:%M:%S')} UTC")
                    await self._poll_books_until_end(coin, market)
                else:
                    self._markets[coin] = None
                    self._up_books[coin] = None
                    self._down_books[coin] = None
                    await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{coin.upper()}] Monitor error: {e}")
                await asyncio.sleep(5)

    async def _discover_market(self, coin: str) -> MarketInfo | None:
        """Find the current or next active 5-min market for a coin via Gamma API."""
        try:
            url = f"{self.config.polymarket.gamma_base_url}/events"
            now = time.time()

            for offset in [0, 300, -300]:
                window_ts = int(now - (now % 300)) + offset
                slug = f"{coin}-updown-5m-{window_ts}"
                resp = await self._http.get(url, params={"slug": slug, "limit": 1})

                if resp.status_code == 200:
                    events = resp.json()
                    if events:
                        return self._parse_event(events[0], coin)

            logger.debug(f"[{coin.upper()}] No active market found")
            return None
        except Exception as e:
            logger.error(f"[{coin.upper()}] Discovery error: {e}")
            return None

    def _parse_event(self, event: dict, coin: str) -> MarketInfo | None:
        """Parse a Gamma API event into a MarketInfo."""
        try:
            markets = event.get("markets", [])
            if not markets:
                return None

            market = markets[0]

            raw_tokens = market.get("clobTokenIds", [])
            raw_outcomes = market.get("outcomes", [])
            if isinstance(raw_tokens, str):
                raw_tokens = json.loads(raw_tokens)
            if isinstance(raw_outcomes, str):
                raw_outcomes = json.loads(raw_outcomes)

            if len(raw_tokens) < 2 or len(raw_outcomes) < 2:
                return None

            # Map outcomes to Up/Down indices
            up_idx, down_idx = 0, 1
            for i, outcome in enumerate(raw_outcomes):
                if str(outcome).lower() in ("up", "yes"):
                    up_idx = i
                elif str(outcome).lower() in ("down", "no"):
                    down_idx = i

            fee_schedule = market.get("feeSchedule", {})
            if isinstance(fee_schedule, str):
                fee_schedule = json.loads(fee_schedule)
            fee_rate = fee_schedule.get("rate", 0.072)

            end_str = market.get("endDate", event.get("endDate", ""))
            start_str = market.get("eventStartTime", market.get("startDate", ""))

            return MarketInfo(
                event_id=event.get("id", ""),
                condition_id=market.get("conditionId", ""),
                slug=event.get("slug", market.get("slug", "")),
                coin=coin,
                up_token_id=raw_tokens[up_idx],
                down_token_id=raw_tokens[down_idx],
                end_time=self._parse_time(end_str),
                start_time=self._parse_time(start_str),
                fee_rate=fee_rate,
                tick_size=self.config.polymarket.tick_size,
                neg_risk=market.get("negRisk", False),
                accepting_orders=market.get("acceptingOrders", True),
            )
        except Exception as e:
            logger.error(f"[{coin.upper()}] Parse error: {e}")
            return None

    async def _poll_books_until_end(self, coin: str, market: MarketInfo):
        """Subscribe to WebSocket book updates until the market window closes.
        Falls back to HTTP polling until the WebSocket delivers the first snapshot."""
        token_ids = [market.up_token_id, market.down_token_id]
        await self._ws_feed.subscribe(token_ids)
        logger.info(f"[{coin.upper()}] WS subscribed for {market.slug}")

        while self._running:
            now = datetime.now(timezone.utc)
            if now >= market.end_time:
                logger.info(f"[{coin.upper()}] Market {market.slug} ended — queued for outcome check")
                self._expired_markets.append(market)
                self._markets[coin] = None
                self._up_books[coin] = None
                self._down_books[coin] = None
                await self._ws_feed.unsubscribe(token_ids)
                break

            # Sync from WebSocket cache (real-time)
            up_book = self._ws_feed.get_book(market.up_token_id)
            down_book = self._ws_feed.get_book(market.down_token_id)

            if up_book:
                self._up_books[coin] = up_book
            if down_book:
                self._down_books[coin] = down_book

            # HTTP fallback — only fires until WebSocket delivers first snapshot
            if not up_book or not down_book:
                try:
                    if not up_book:
                        fetched = await self._fetch_book(market.up_token_id)
                        self._up_books[coin] = fetched
                    if not down_book:
                        fetched = await self._fetch_book(market.down_token_id)
                        self._down_books[coin] = fetched
                except Exception as e:
                    logger.warning(f"[{coin.upper()}] HTTP book fallback error: {e}")

            await asyncio.sleep(0.1)

    async def _fetch_book(self, token_id: str) -> OrderBookSnapshot:
        """Fetch order book for one token. Polymarket sorts outside-in (worst first)."""
        url = f"{self.config.polymarket.clob_base_url}/book"
        resp = await self._http.get(url, params={"token_id": token_id})
        resp.raise_for_status()
        data = resp.json()

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        # Polymarket CLOB: bids ascending (best bid = last), asks descending (best ask = last)
        best_bid = float(bids[-1]["price"]) if bids else 0.0
        best_ask = float(asks[-1]["price"]) if asks else 1.0

        # Depth: last 3 entries are closest to mid price
        bid_depth = sum(float(b["size"]) * float(b["price"]) for b in bids[-3:])
        ask_depth = sum(float(a["size"]) * float(a["price"]) for a in asks[-3:])

        return OrderBookSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_depth_usd=bid_depth,
            ask_depth_usd=ask_depth,
            timestamp=time.time(),
        )

    async def get_market_outcome(self, market: MarketInfo) -> str | None:
        """Query resolved outcome for a market. Returns 'UP' or 'DOWN', or None if not yet resolved."""

        def _parse_winner(data) -> str | None:
            """Extract normalized UP/DOWN winner from a market dict."""
            if not isinstance(data, dict):
                return None
            if not data.get("resolved"):
                return None
            outcomes = data.get("outcomes", [])
            prices = data.get("outcomePrices", [])
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    return None
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    return None
            for i, price in enumerate(prices):
                try:
                    if float(price) >= 0.99 and i < len(outcomes):
                        raw = str(outcomes[i]).lower()
                        if raw in ("up", "yes"):
                            return "UP"
                        elif raw in ("down", "no"):
                            return "DOWN"
                        else:
                            return str(outcomes[i]).upper()
                except (ValueError, TypeError):
                    continue
            return None

        try:
            # Primary: query individual market by condition_id
            if market.condition_id:
                url = f"{self.config.polymarket.gamma_base_url}/markets/{market.condition_id}"
                resp = await self._http.get(url, timeout=8.0)
                if resp.status_code == 200:
                    winner = _parse_winner(resp.json())
                    if winner:
                        logger.debug(f"[{market.slug}] Outcome resolved via markets endpoint: {winner}")
                        return winner

            # Fallback: re-query the events endpoint (same one used for discovery)
            url = f"{self.config.polymarket.gamma_base_url}/events"
            resp = await self._http.get(url, params={"slug": market.slug, "limit": 1}, timeout=8.0)
            if resp.status_code == 200:
                events = resp.json()
                if isinstance(events, list) and events:
                    for mkt in events[0].get("markets", []):
                        winner = _parse_winner(mkt)
                        if winner:
                            logger.debug(f"[{market.slug}] Outcome resolved via events fallback: {winner}")
                            return winner

            logger.debug(f"[{market.slug}] Outcome not yet available (condition_id={market.condition_id!r})")
            return None

        except Exception as e:
            logger.error(f"Outcome query error for {market.slug}: {e}")
            return None

    @staticmethod
    def _parse_time(time_str: str) -> datetime:
        if not time_str:
            return datetime.now(timezone.utc)
        try:
            return datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)
