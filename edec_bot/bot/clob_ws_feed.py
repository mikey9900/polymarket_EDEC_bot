"""
Real-time order book feed via Polymarket CLOB WebSocket.

Subscribes to token IDs and maintains a live cache of OrderBookSnapshots.
Handles ping/pong keepalive, reconnects with exponential backoff, and
dynamic subscribe/unsubscribe without tearing down the connection.
"""

import asyncio
import json
import logging
import time

import websockets

from bot.models import OrderBookSnapshot

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_S = 10
RECONNECT_BASE_S = 1.0
RECONNECT_MAX_S = 30.0


class ClobWebSocketFeed:
    def __init__(self):
        self._books: dict[str, OrderBookSnapshot] = {}   # token_id → snapshot
        self._subscribed: set[str] = set()               # currently subscribed token IDs
        self._ws = None
        self._running = False
        self._any_update_event = asyncio.Event()         # fired on every book update

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    async def run(self):
        """Main WebSocket loop — runs forever, reconnects on failure."""
        self._running = True
        backoff = RECONNECT_BASE_S

        while self._running:
            try:
                async with websockets.connect(WS_URL, ping_interval=None, open_timeout=20) as ws:
                    self._ws = ws
                    backoff = RECONNECT_BASE_S
                    logger.info("CLOB WebSocket connected")

                    # Step 1: initial handshake
                    await ws.send(json.dumps({"assets_ids": [], "type": "market"}))

                    # Step 2: re-subscribe any tokens we were tracking before reconnect
                    if self._subscribed:
                        await ws.send(json.dumps({
                            "assets_ids": list(self._subscribed),
                            "type": "market",
                            "custom_feature_enabled": True,
                        }))
                        logger.info(f"CLOB WS re-subscribed {len(self._subscribed)} token(s)")

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw in ws:
                            if not self._running:
                                return
                            if raw == "PONG":
                                continue
                            try:
                                self._handle_message(json.loads(raw))
                            except json.JSONDecodeError:
                                pass
                    finally:
                        ping_task.cancel()
                        self._ws = None

            except asyncio.CancelledError:
                return
            except Exception as e:
                self._ws = None
                logger.warning(f"CLOB WS disconnected ({e}), reconnecting in {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_S)

    def stop(self):
        self._running = False

    async def aclose(self):
        """Gracefully stop the feed and close any active websocket."""
        self.stop()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception as e:
                logger.debug(f"CLOB WS close failed: {e}")
        self._ws = None
        self._any_update_event.set()

    async def subscribe(self, token_ids: list[str]):
        """Subscribe to real-time book updates for the given token IDs."""
        new_ids = [t for t in token_ids if t not in self._subscribed]
        if not new_ids:
            return
        self._subscribed.update(new_ids)
        if self._ws is not None:
            try:
                await self._ws.send(json.dumps({
                    "assets_ids": new_ids,
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                logger.debug(f"CLOB WS subscribed: {[t[:8] for t in new_ids]}")
            except Exception as e:
                logger.warning(f"CLOB WS subscribe failed: {e}")

    async def unsubscribe(self, token_ids: list[str]):
        """Unsubscribe from book updates and clear cached data."""
        for t in token_ids:
            self._subscribed.discard(t)
            self._books.pop(t, None)
        if self._ws is not None:
            try:
                await self._ws.send(json.dumps({
                    "operation": "unsubscribe",
                    "assets_ids": token_ids,
                }))
                logger.debug(f"CLOB WS unsubscribed: {[t[:8] for t in token_ids]}")
            except Exception as e:
                logger.warning(f"CLOB WS unsubscribe failed: {e}")

    def get_book(self, token_id: str) -> OrderBookSnapshot | None:
        """Return the latest cached snapshot for a token, or None if not yet received."""
        return self._books.get(token_id)

    async def wait_any_update(self):
        """Suspend until the next book update arrives for any subscribed token."""
        self._any_update_event.clear()
        await self._any_update_event.wait()

    def is_connected(self) -> bool:
        return self._ws is not None

    # -----------------------------------------------------------------------
    # Internal
    # -----------------------------------------------------------------------

    async def _ping_loop(self, ws):
        """Send PING every 10s to keep the connection alive."""
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            try:
                await ws.send("PING")
            except Exception:
                return

    def _handle_message(self, event: dict):
        etype = event.get("event_type")

        if etype == "book":
            self._handle_book(event)

        elif etype == "best_bid_ask":
            self._handle_best_bid_ask(event)

        elif etype == "price_change":
            self._handle_price_change(event)

    def _handle_book(self, event: dict):
        """Full book snapshot — seed/rebuild local state."""
        token_id = event.get("asset_id", "")
        if not token_id:
            return

        bids = event.get("bids", [])
        asks = event.get("asks", [])

        best_bid = max((float(b["price"]) for b in bids), default=0.0)
        best_ask = min((float(a["price"]) for a in asks), default=1.0)

        # Depth: top 3 ask levels closest to mid
        sorted_asks = sorted(asks, key=lambda a: float(a["price"]))[:3]
        ask_depth = sum(float(a["price"]) * float(a["size"]) for a in sorted_asks)
        sorted_bids = sorted(bids, key=lambda b: float(b["price"]), reverse=True)[:3]
        bid_depth = sum(float(b["price"]) * float(b["size"]) for b in sorted_bids)

        self._books[token_id] = OrderBookSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_depth_usd=bid_depth,
            ask_depth_usd=ask_depth,
            timestamp=time.time(),
        )
        self._any_update_event.set()

    def _handle_best_bid_ask(self, event: dict):
        """Lightweight NBBO update — fastest path for monitoring best prices."""
        token_id = event.get("asset_id", "")
        if not token_id:
            return

        existing = self._books.get(token_id)
        best_bid = float(event.get("best_bid") or (existing.best_bid if existing else 0.0))
        best_ask = float(event.get("best_ask") or (existing.best_ask if existing else 1.0))

        self._books[token_id] = OrderBookSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_depth_usd=existing.bid_depth_usd if existing else 0.0,
            ask_depth_usd=existing.ask_depth_usd if existing else 0.0,
            timestamp=time.time(),
        )
        self._any_update_event.set()

    def _handle_price_change(self, event: dict):
        """Incremental delta — update best bid/ask from embedded NBBO fields."""
        for change in event.get("price_changes", []):
            token_id = change.get("asset_id", "")
            if not token_id or token_id not in self._books:
                continue
            existing = self._books[token_id]
            raw_bid = change.get("best_bid")
            raw_ask = change.get("best_ask")
            best_bid = float(raw_bid) if raw_bid else existing.best_bid
            best_ask = float(raw_ask) if raw_ask else existing.best_ask
            self._books[token_id] = OrderBookSnapshot(
                token_id=token_id,
                best_bid=best_bid,
                best_ask=best_ask,
                bid_depth_usd=existing.bid_depth_usd,
                ask_depth_usd=existing.ask_depth_usd,
                timestamp=time.time(),
            )
            self._any_update_event.set()
