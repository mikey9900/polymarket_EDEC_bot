"""Real-time price feeds from multiple exchanges, with per-coin support."""

import asyncio
import json
import logging
import time

import websockets

from bot.models import PriceTick

logger = logging.getLogger(__name__)


class BinanceFeed:
    """Binance WebSocket ticker for one symbol (e.g. btcusdt)."""

    def __init__(self, symbol: str, coin: str):
        self.symbol = symbol.lower()
        self.coin = coin
        self.url = f"wss://stream.binance.com:9443/ws/{self.symbol}@ticker"
        self._running = False

    async def run(self, queue: asyncio.Queue):
        self._running = True
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(self.url, open_timeout=20) as ws:
                    logger.info(f"Binance feed connected: {self.symbol}")
                    backoff = 1
                    async for msg in ws:
                        if not self._running:
                            break
                        data = json.loads(msg)
                        await queue.put(PriceTick(
                            source="binance",
                            price=float(data["c"]),
                            timestamp=data["E"] / 1000.0,
                            coin=self.coin,
                        ))
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning(f"Binance {self.symbol} disconnected: {e}. Retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                logger.error(f"Binance {self.symbol} error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._running = False


class CoinbaseFeed:
    """Coinbase WebSocket for BTC-USD (BTC only, used as primary reference)."""

    def __init__(self, product: str = "BTC-USD"):
        self.url = "wss://ws-feed.exchange.coinbase.com"
        self.product = product
        self._running = False

    async def run(self, queue: asyncio.Queue):
        self._running = True
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(self.url, open_timeout=20) as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "product_ids": [self.product],
                        "channels": ["ticker"],
                    }))
                    logger.info(f"Coinbase feed connected: {self.product}")
                    backoff = 1
                    async for msg in ws:
                        if not self._running:
                            break
                        data = json.loads(msg)
                        if data.get("type") == "ticker" and "price" in data:
                            await queue.put(PriceTick(
                                source="coinbase",
                                price=float(data["price"]),
                                timestamp=time.time(),
                                coin="btc",
                            ))
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning(f"Coinbase disconnected: {e}. Retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                logger.error(f"Coinbase error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._running = False


class PolymarketRTDSFeed:
    """Polymarket Real-Time Data Service — Chainlink oracle BTC price."""

    def __init__(self):
        self.url = "wss://ws-live-data.polymarket.com"
        self._running = False

    async def run(self, queue: asyncio.Queue):
        self._running = True
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(self.url, open_timeout=20) as ws:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [
                            {"topic": "crypto_prices", "type": "update", "filters": "btcusdt"}
                        ],
                    }))
                    logger.info("Polymarket RTDS feed connected")
                    backoff = 1

                    async def ping_loop():
                        while self._running:
                            try:
                                await ws.ping()
                                await asyncio.sleep(5)
                            except Exception:
                                break

                    ping_task = asyncio.create_task(ping_loop())
                    try:
                        async for msg in ws:
                            if not self._running:
                                break
                            data = json.loads(msg)
                            price = data.get("price") or (data.get("data") or {}).get("price")
                            if price is not None:
                                await queue.put(PriceTick(
                                    source="polymarket_rtds",
                                    price=float(price),
                                    timestamp=time.time(),
                                    coin="btc",
                                ))
                    finally:
                        ping_task.cancel()
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning(f"Polymarket RTDS disconnected: {e}. Retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                logger.error(f"Polymarket RTDS error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._running = False


class CoinGeckoPoller:
    """CoinGecko REST polling for multiple coin prices (fallback)."""

    COINGECKO_IDS = {
        "btc": "bitcoin",
        "eth": "ethereum",
        "sol": "solana",
        "xrp": "ripple",
        "bnb": "binancecoin",
        "doge": "dogecoin",
        "hype": "hyperliquid",
    }

    def __init__(self, coins: list[str], interval_s: int = 30):
        self.coins = coins
        self.interval_s = interval_s
        self._running = False

    async def run(self, queue: asyncio.Queue):
        from pycoingecko import CoinGeckoAPI
        self._running = True
        cg = CoinGeckoAPI()
        ids = [self.COINGECKO_IDS[c] for c in self.coins if c in self.COINGECKO_IDS]

        while self._running:
            try:
                result = await asyncio.to_thread(
                    cg.get_price, ids=",".join(ids), vs_currencies="usd"
                )
                ts = time.time()
                for coin in self.coins:
                    cg_id = self.COINGECKO_IDS.get(coin)
                    if cg_id and cg_id in result:
                        price = result[cg_id]["usd"]
                        await queue.put(PriceTick(
                            source="coingecko",
                            price=float(price),
                            timestamp=ts,
                            coin=coin,
                        ))
            except Exception as e:
                logger.warning(f"CoinGecko poll error: {e}")
            await asyncio.sleep(self.interval_s)

    def stop(self):
        self._running = False


def start_all_feeds(config, queue: asyncio.Queue) -> list:
    """Launch all price feed tasks. Returns list of (task, feed) tuples."""
    feeds = []
    binance_symbols = config.feeds.binance_symbols

    # Binance WebSocket per coin
    for coin in config.coins:
        symbol = binance_symbols.get(coin)
        if symbol:
            feeds.append(BinanceFeed(symbol=symbol, coin=coin))

    # Coinbase for BTC reference
    feeds.append(CoinbaseFeed(config.feeds.coinbase_product))

    # Polymarket RTDS for BTC (Chainlink oracle)
    feeds.append(PolymarketRTDSFeed())

    # CoinGecko for all coins
    feeds.append(CoinGeckoPoller(list(config.coins), config.feeds.coingecko_poll_interval_s))

    tasks = [(asyncio.create_task(f.run(queue)), f) for f in feeds]
    return tasks


# Standalone test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def test():
        queue = asyncio.Queue()
        feeds = [
            BinanceFeed("btcusdt", "btc"),
            BinanceFeed("ethusdt", "eth"),
            BinanceFeed("solusdt", "sol"),
            CoinbaseFeed(),
        ]
        tasks = [asyncio.create_task(f.run(queue)) for f in feeds]
        count = 0
        while count < 30:
            tick = await queue.get()
            print(f"[{tick.coin:>4}][{tick.source:>18}] ${tick.price:,.2f}")
            count += 1
        for f in feeds:
            f.stop()
        for t in tasks:
            t.cancel()

    asyncio.run(test())
