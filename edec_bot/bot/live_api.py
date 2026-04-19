"""Minimal built-in HTTP server for Home Assistant ingress."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger("edec.live_api")


class LiveApiServer:
    """Small asyncio HTTP server (no extra dependency) for ingress compatibility."""

    def __init__(self, dashboard_state, host: str = "0.0.0.0", port: int = 8099):
        self.dashboard_state = dashboard_state
        self.host = host
        self.port = int(port)
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        logger.info("Dashboard API listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            req = await reader.readuntil(b"\r\n\r\n")
            first_line = req.decode("utf-8", errors="ignore").splitlines()[0] if req else ""
            method, path, _ = (first_line.split(" ", 2) + ["", "", ""])[:3]
            if method != "GET":
                await self._send(writer, 405, {"error": "method_not_allowed"})
                return

            if path in ("/", "/index.html"):
                html = self._index_html()
                await self._send(writer, 200, html, content_type="text/html; charset=utf-8")
                return
            if path == "/health":
                await self._send(writer, 200, {"status": "ok", "timestamp_utc": datetime.now(timezone.utc).isoformat()})
                return
            if path == "/api/state":
                await self._send(writer, 200, await self.dashboard_state.get_state())
                return
            if path == "/api/history":
                await self._send(writer, 200, await self.dashboard_state.get_history())
                return

            await self._send(writer, 404, {"error": "not_found", "path": path})
        except asyncio.IncompleteReadError:
            pass
        except Exception as exc:
            logger.debug("HTTP request error: %s", exc)
            try:
                await self._send(writer, 500, {"error": "internal_error"})
            except Exception:
                pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def _send(self, writer: asyncio.StreamWriter, status: int, body, *, content_type: str = "application/json") -> None:
        if isinstance(body, (dict, list)):
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            content_type = "application/json; charset=utf-8"
        elif isinstance(body, str):
            payload = body.encode("utf-8")
        else:
            payload = bytes(body)

        reason = {
            200: "OK",
            404: "Not Found",
            405: "Method Not Allowed",
            500: "Internal Server Error",
        }.get(status, "OK")
        headers = [
            f"HTTP/1.1 {status} {reason}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(payload)}",
            "Connection: close",
            "",
            "",
        ]
        writer.write("\r\n".join(headers).encode("utf-8") + payload)
        await writer.drain()

    @staticmethod
    def _index_html() -> str:
        return _DASHBOARD_HTML


_DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>EDEC TERMINAL</title>
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=VT323&family=Press+Start+2P&display=swap" rel="stylesheet" />
<style>
  /* ============================================================
     EDEC TERMINAL — 90s electronics theme
     ============================================================ */
  :root {
    --bg-0: #050912;
    --bg-1: #0a1024;
    --bg-2: #121a35;
    --chrome-hi: #6a7186;
    --chrome-lo: #1a2138;
    --neon-cyan: #00f0ff;
    --neon-magenta: #ff2bd6;
    --neon-lime: #39ff14;
    --neon-amber: #ffb000;
    --neon-red: #ff3b3b;
    --neon-purple: #b070ff;
    --text-dim: #7a86b0;
    --text: #cfe6ff;
  }

  * { box-sizing: border-box; }

  html, body {
    margin: 0;
    padding: 0;
    min-height: 100%;
    background: var(--bg-0);
    color: var(--text);
    font-family: "VT323", "Courier New", monospace;
    font-size: 20px;
    line-height: 1.2;
    overflow-x: hidden;
  }

  /* CRT scanlines + faint vignette */
  body::before {
    content: "";
    position: fixed; inset: 0;
    background:
      repeating-linear-gradient(
        to bottom,
        rgba(0, 240, 255, 0.04) 0px,
        rgba(0, 240, 255, 0.04) 1px,
        transparent 1px,
        transparent 3px
      );
    pointer-events: none;
    z-index: 999;
    mix-blend-mode: screen;
  }
  body::after {
    content: "";
    position: fixed; inset: 0;
    background: radial-gradient(ellipse at center, transparent 50%, rgba(0,0,0,0.6) 100%);
    pointer-events: none;
    z-index: 998;
  }

  /* ============================================================
     Top bar
     ============================================================ */
  header.topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 22px;
    background: linear-gradient(180deg, #1a2342 0%, #0a1024 100%);
    border-bottom: 2px solid var(--chrome-hi);
    box-shadow:
      inset 0 1px 0 #2c3865,
      inset 0 -2px 0 #000,
      0 0 24px rgba(0, 240, 255, 0.15);
  }
  .brand {
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 16px;
    color: var(--neon-cyan);
    text-shadow: 0 0 6px var(--neon-cyan), 0 0 14px rgba(0,240,255,0.5);
    letter-spacing: 2px;
  }
  .brand .pulse {
    display: inline-block;
    width: 10px; height: 10px;
    border-radius: 50%;
    background: var(--neon-lime);
    box-shadow: 0 0 8px var(--neon-lime);
    margin-right: 10px;
    animation: blink 1.4s infinite;
    vertical-align: middle;
  }
  .topstats { display: flex; gap: 22px; align-items: center; }
  .topstats .pill {
    padding: 4px 10px;
    border: 1px solid var(--chrome-hi);
    border-radius: 4px;
    background: #11193a;
    box-shadow: inset 0 1px 0 #2c3865, 0 0 6px rgba(0,240,255,0.2);
    font-size: 18px;
    color: var(--text);
  }
  .topstats .pill .lbl { color: var(--text-dim); font-size: 14px; margin-right: 6px; letter-spacing: 1px; }
  .topstats .pill .val.green  { color: var(--neon-lime);  text-shadow: 0 0 5px var(--neon-lime); }
  .topstats .pill .val.red    { color: var(--neon-red);   text-shadow: 0 0 5px var(--neon-red); }
  .topstats .pill .val.cyan   { color: var(--neon-cyan);  text-shadow: 0 0 5px var(--neon-cyan); }
  .topstats .pill .val.amber  { color: var(--neon-amber); text-shadow: 0 0 5px var(--neon-amber); }

  @keyframes blink {
    0%, 60% { opacity: 1; }
    70%, 100% { opacity: 0.25; }
  }

  /* ============================================================
     Container
     ============================================================ */
  main.stack {
    display: flex;
    flex-direction: column;
    gap: 18px;
    padding: 18px;
    max-width: 1400px;
    margin: 0 auto;
  }

  /* ============================================================
     Coin card — full width, beveled chrome
     ============================================================ */
  .card {
    position: relative;
    background: linear-gradient(180deg, #0c1430 0%, #060a1e 100%);
    border: 2px solid var(--chrome-hi);
    border-radius: 8px;
    box-shadow:
      inset 0 1px 0 #2c3865,
      inset 0 -2px 0 #000,
      0 0 18px rgba(0, 240, 255, 0.10),
      0 4px 0 #000;
    overflow: hidden;
    transition: transform 0.18s ease, box-shadow 0.18s ease;
  }
  .card.dragging {
    opacity: 0.55;
    transform: scale(0.99);
  }
  .card.drop-target {
    box-shadow:
      inset 0 1px 0 #2c3865,
      inset 0 -2px 0 #000,
      0 0 0 2px var(--neon-cyan),
      0 0 24px rgba(0, 240, 255, 0.7);
  }
  .card.color-green::before {
    content: "";
    position: absolute; inset: 0;
    background: linear-gradient(180deg, rgba(57,255,20,0.07), transparent 40%);
    pointer-events: none;
  }
  .card.color-red::before {
    content: "";
    position: absolute; inset: 0;
    background: linear-gradient(180deg, rgba(255,59,59,0.07), transparent 40%);
    pointer-events: none;
  }

  .card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 16px;
    background: linear-gradient(180deg, #1a2247 0%, #0d1532 100%);
    border-bottom: 1px solid var(--chrome-lo);
    cursor: grab;
    user-select: none;
  }
  .card-header:active { cursor: grabbing; }
  .card-header .left {
    display: flex; align-items: center; gap: 12px;
  }
  .grip {
    color: var(--text-dim);
    font-size: 22px;
    letter-spacing: -2px;
    margin-right: 4px;
    text-shadow: 0 1px 0 #000;
  }
  .coin-name {
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 14px;
    color: var(--neon-cyan);
    text-shadow: 0 0 5px var(--neon-cyan), 0 0 12px rgba(0,240,255,0.4);
    letter-spacing: 2px;
  }
  .live-price {
    font-size: 26px;
    color: var(--text);
    text-shadow: 0 0 4px rgba(207,230,255,0.5);
  }
  .live-price.green { color: var(--neon-lime); text-shadow: 0 0 6px var(--neon-lime); }
  .live-price.red   { color: var(--neon-red);  text-shadow: 0 0 6px var(--neon-red); }

  .card-header .right {
    display: flex; align-items: center; gap: 14px;
    color: var(--text-dim);
    font-size: 18px;
  }
  .timer {
    color: var(--neon-amber);
    text-shadow: 0 0 4px var(--neon-amber);
    font-size: 22px;
  }

  /* ============================================================
     Body grid
     ============================================================ */
  .card-body {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-gap: 14px;
    padding: 14px 16px 18px 16px;
  }
  .card-body .span2 { grid-column: 1 / -1; }

  .panel {
    background: rgba(10, 15, 38, 0.6);
    border: 1px solid #1f2a55;
    border-radius: 5px;
    padding: 10px 12px;
    box-shadow: inset 0 0 14px rgba(0, 0, 0, 0.4);
  }
  .panel h4 {
    margin: 0 0 8px 0;
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 10px;
    color: var(--neon-magenta);
    text-shadow: 0 0 4px var(--neon-magenta);
    letter-spacing: 1.5px;
  }

  /* LED row */
  .leds { display: flex; gap: 14px; align-items: center; flex-wrap: wrap; }
  .led {
    display: flex; flex-direction: column; align-items: center; gap: 3px;
    min-width: 44px;
  }
  .led .dot {
    width: 14px; height: 14px;
    border-radius: 50%;
    background: #20263d;
    border: 1px solid #000;
    box-shadow: inset 0 -2px 3px rgba(0,0,0,0.6), inset 0 1px 1px rgba(255,255,255,0.06);
  }
  .led.on .dot {
    background: radial-gradient(circle at 35% 30%, #b3ffd1, var(--neon-lime) 70%);
    box-shadow:
      inset 0 -2px 3px rgba(0,0,0,0.4),
      0 0 8px var(--neon-lime),
      0 0 14px rgba(57,255,20,0.6);
    animation: blink 2s infinite;
  }
  .led.stale .dot {
    background: radial-gradient(circle at 35% 30%, #ffe8a8, var(--neon-amber) 70%);
    box-shadow: 0 0 6px var(--neon-amber);
  }
  .led .lbl {
    font-size: 12px; color: var(--text-dim); letter-spacing: 1px;
    font-family: "Press Start 2P", "VT323", monospace;
  }
  .led.on .lbl { color: var(--text); }

  /* Big readouts */
  .strike-row {
    display: flex; align-items: baseline; justify-content: space-between;
    font-size: 22px;
  }
  .strike-row .big {
    color: var(--neon-amber);
    font-size: 28px;
    text-shadow: 0 0 6px var(--neon-amber);
  }
  .strike-row .lbl { color: var(--text-dim); font-size: 14px; letter-spacing: 1px; }

  /* Prediction bar */
  .predbar {
    height: 22px;
    background: #07091a;
    border: 1px solid var(--chrome-lo);
    border-radius: 3px;
    position: relative;
    overflow: hidden;
    margin-top: 6px;
  }
  .predbar .up {
    position: absolute; left: 0; top: 0; bottom: 0;
    background: linear-gradient(180deg, #66ffaa 0%, var(--neon-lime) 100%);
    box-shadow: 0 0 8px var(--neon-lime);
  }
  .predbar .down {
    position: absolute; right: 0; top: 0; bottom: 0;
    background: linear-gradient(180deg, #ff8a8a 0%, var(--neon-red) 100%);
    box-shadow: 0 0 8px var(--neon-red);
  }
  .predbar .label-up, .predbar .label-down {
    position: absolute; top: 1px;
    font-size: 16px;
    color: #000;
    text-shadow: 0 0 2px #fff;
    padding: 0 6px;
    z-index: 2;
  }
  .predbar .label-up { left: 6px; }
  .predbar .label-down { right: 6px; }

  /* Signal/trade list rows */
  .row { display: flex; justify-content: space-between; gap: 10px; padding: 4px 0; }
  .row + .row { border-top: 1px dashed #1c2548; }
  .row .a { color: var(--text); }
  .row .b { color: var(--text-dim); }
  .strategy-tag {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 3px;
    background: #1a234a;
    color: var(--neon-cyan);
    font-size: 14px;
    letter-spacing: 1px;
    text-shadow: 0 0 3px var(--neon-cyan);
    border: 1px solid #2a3a78;
  }
  .side-up   { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .side-down { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); }
  .score {
    display: inline-block;
    min-width: 46px;
    text-align: right;
    color: var(--neon-amber);
    text-shadow: 0 0 4px var(--neon-amber);
  }
  .pnl-pos { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .pnl-neg { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); }
  .muted { color: var(--text-dim); font-style: italic; }

  /* Recent resolutions tape */
  .tape {
    display: flex; gap: 8px; align-items: center;
    flex-wrap: wrap;
  }
  .tape .seg {
    border: 1px solid var(--chrome-lo);
    border-radius: 3px;
    padding: 4px 8px;
    background: #07091a;
    box-shadow: inset 0 0 8px rgba(0,0,0,0.5);
    font-size: 16px;
    display: flex; align-items: center; gap: 6px;
  }
  .tape .seg.win-up   { border-color: var(--neon-lime); }
  .tape .seg.win-down { border-color: var(--neon-red); }
  .tape .arrow.up   { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); font-size: 18px; }
  .tape .arrow.down { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); font-size: 18px; }
  .tape .seg .traded { color: var(--neon-cyan); font-size: 12px; }
  .tape .seg .nope   { color: var(--text-dim); font-size: 12px; }

  /* Session readout */
  .session {
    display: flex; gap: 18px; flex-wrap: wrap; align-items: center;
    font-size: 20px;
  }
  .session .item .lbl {
    color: var(--text-dim); font-size: 12px; letter-spacing: 1px;
    font-family: "Press Start 2P", "VT323", monospace;
    display: block;
  }
  .session .w { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .session .l { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); }
  .session .o { color: var(--neon-amber); text-shadow: 0 0 4px var(--neon-amber); }

  /* Chart placeholder (Step 3 will replace) */
  .chart-slot {
    height: 130px;
    border: 1px dashed #2a3a78;
    border-radius: 4px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text-dim);
    background:
      repeating-linear-gradient(
        to right,
        rgba(0,240,255,0.05) 0 1px,
        transparent 1px 40px
      ),
      repeating-linear-gradient(
        to bottom,
        rgba(0,240,255,0.05) 0 1px,
        transparent 1px 30px
      ),
      #050912;
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 10px;
    letter-spacing: 2px;
  }

  /* No-data state */
  .nodata {
    text-align: center;
    color: var(--text-dim);
    padding: 40px;
    font-style: italic;
    font-size: 22px;
  }

  /* Mobile */
  @media (max-width: 720px) {
    .card-body { grid-template-columns: 1fr; }
    .card-header .right { font-size: 14px; }
    .live-price { font-size: 22px; }
  }
</style>
</head>
<body>
  <header class="topbar">
    <div class="brand"><span class="pulse"></span>EDEC TERMINAL <span style="color:var(--text-dim);font-size:10px">v5.0.24</span></div>
    <div class="topstats">
      <div class="pill"><span class="lbl">MODE</span><span id="t-mode" class="val cyan">—</span></div>
      <div class="pill"><span class="lbl">DRY</span><span id="t-dry"  class="val amber">—</span></div>
      <div class="pill"><span class="lbl">P&amp;L</span><span id="t-pnl" class="val green">—</span></div>
      <div class="pill"><span class="lbl">BAL</span><span id="t-bal" class="val cyan">—</span></div>
      <div class="pill"><span class="lbl">UTC</span><span id="t-ts"  class="val cyan">—</span></div>
    </div>
  </header>

  <main id="stack" class="stack">
    <div id="loading" class="nodata">⏳ ESTABLISHING UPLINK…</div>
  </main>

<script>
(() => {
  // ----- Constants -----
  const POLL_MS = 1000;
  const STORAGE_KEY = "edec_card_order_v1";
  const FEED_LABELS = { binance: "BNC", coinbase: "CB ", coingecko: "CG ", polymarket_rtds: "RTDS" };

  // ----- Helpers -----
  const $ = (id) => document.getElementById(id);
  const fmtPrice = (p) => p == null ? "—" : (p >= 1000 ? p.toLocaleString(undefined, {maximumFractionDigits: 2}) : p.toFixed(p < 1 ? 4 : 2));
  const fmtPct = (x) => (x == null ? "—" : (x*100).toFixed(0) + "%");
  const fmtUsd = (x) => (x == null ? "—" : (x >= 0 ? "+" : "") + "$" + x.toFixed(2));
  const fmtSecs = (s) => {
    if (s == null) return "—";
    s = Math.max(0, Math.round(s));
    const m = Math.floor(s / 60), r = s % 60;
    return m + ":" + String(r).padStart(2, "0");
  };
  const escapeHtml = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

  // ----- Card order persistence (per-device via localStorage) -----
  function loadOrder() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY)) || []; } catch { return []; }
  }
  function saveOrder(arr) {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(arr)); } catch {}
  }
  function effectiveOrder(serverOrder) {
    const stored = loadOrder();
    const known = new Set(serverOrder);
    const out = stored.filter(c => known.has(c));
    serverOrder.forEach(c => { if (!out.includes(c)) out.push(c); });
    return out;
  }

  // ----- Drag and drop -----
  let dragCoin = null;
  function bindDrag(card) {
    const header = card.querySelector(".card-header");
    header.draggable = true;
    header.addEventListener("dragstart", (e) => {
      dragCoin = card.dataset.coin;
      card.classList.add("dragging");
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", dragCoin);
    });
    header.addEventListener("dragend", () => {
      card.classList.remove("dragging");
      document.querySelectorAll(".card.drop-target").forEach(el => el.classList.remove("drop-target"));
      dragCoin = null;
    });
    card.addEventListener("dragover", (e) => {
      if (!dragCoin || dragCoin === card.dataset.coin) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = "move";
      card.classList.add("drop-target");
    });
    card.addEventListener("dragleave", () => card.classList.remove("drop-target"));
    card.addEventListener("drop", (e) => {
      e.preventDefault();
      card.classList.remove("drop-target");
      const src = document.querySelector(`.card[data-coin="${dragCoin}"]`);
      if (!src || src === card) return;
      const stack = $("stack");
      const rect = card.getBoundingClientRect();
      const before = e.clientY < rect.top + rect.height / 2;
      stack.insertBefore(src, before ? card : card.nextSibling);
      const newOrder = Array.from(stack.querySelectorAll(".card")).map(c => c.dataset.coin);
      saveOrder(newOrder);
    });
  }

  // ----- Card rendering -----
  function ensureCard(coin) {
    let card = document.querySelector(`.card[data-coin="${coin}"]`);
    if (card) return card;
    card = document.createElement("section");
    card.className = "card";
    card.dataset.coin = coin;
    card.innerHTML = `
      <div class="card-header">
        <div class="left">
          <span class="grip">⋮⋮</span>
          <span class="coin-name">🪙 ${coin.toUpperCase()}</span>
          <span class="live-price" data-field="price">—</span>
        </div>
        <div class="right">
          <span data-field="strike-mini" class="muted">strike —</span>
          <span class="timer">⏱ <span data-field="timer">—</span></span>
        </div>
      </div>
      <div class="card-body">
        <div class="panel">
          <h4>📡 DATA FEEDS</h4>
          <div class="leds" data-field="leds"></div>
        </div>
        <div class="panel">
          <h4>🎯 STRIKE (MARKET OPEN)</h4>
          <div class="strike-row">
            <span class="big" data-field="strike">—</span>
            <span class="lbl" data-field="strike-label">—</span>
          </div>
        </div>

        <div class="panel span2">
          <h4>🔮 MARKET PREDICTION</h4>
          <div class="predbar">
            <div class="up"   data-field="predbar-up"   style="width:0%"></div>
            <div class="down" data-field="predbar-down" style="width:0%"></div>
            <span class="label-up"   data-field="predbar-up-lbl">UP —</span>
            <span class="label-down" data-field="predbar-down-lbl">— DOWN</span>
          </div>
        </div>

        <div class="panel">
          <h4>🤖 BOT STRATEGIES (LIVE)</h4>
          <div data-field="signals"><div class="muted">no live signals</div></div>
        </div>
        <div class="panel">
          <h4>💼 OPEN TRADES</h4>
          <div data-field="trades"><div class="muted">no open trades</div></div>
        </div>

        <div class="panel">
          <h4>📼 LAST 4 RESOLUTIONS</h4>
          <div class="tape" data-field="tape"></div>
        </div>
        <div class="panel">
          <h4>🏆 SESSION (THIS COIN)</h4>
          <div class="session" data-field="session"></div>
        </div>

        <div class="panel span2">
          <h4>📈 LIVE CHART (Step 3)</h4>
          <div class="chart-slot">
            CHART MODULE OFFLINE — installing in next phase
          </div>
        </div>
      </div>
    `;
    bindDrag(card);
    return card;
  }

  function renderLeds(host, sources) {
    const feeds = (sources && sources.feeds) || [];
    host.innerHTML = feeds.map(f => {
      const cls = f.active ? (f.age_s != null && f.age_s > 3 ? "stale" : "on") : "";
      const lbl = FEED_LABELS[f.name] || f.name.toUpperCase().slice(0,4);
      return `<div class="led ${cls}"><span class="dot"></span><span class="lbl">${lbl}</span></div>`;
    }).join("");
  }

  function renderSignals(host, signals) {
    if (!signals || !signals.length) {
      host.innerHTML = '<div class="muted">no live signals</div>';
      return;
    }
    host.innerHTML = signals.map(s => {
      const sideCls = s.side === "UP" ? "side-up" : (s.side === "DOWN" ? "side-down" : "");
      const arrow = s.side === "UP" ? "▲" : (s.side === "DOWN" ? "▼" : "•");
      const buy = s.entry_price != null ? s.entry_price.toFixed(2) : "—";
      const tgt = s.target_price != null ? s.target_price.toFixed(2) : "—";
      return `
        <div class="row">
          <span class="a">
            <span class="strategy-tag">${escapeHtml(s.strategy)}</span>
            <span class="${sideCls}">${arrow} ${s.side || "?"}</span>
          </span>
          <span class="b">
            buy ${buy} → tgt ${tgt}
            <span class="score">${s.score.toFixed(1)}</span>
          </span>
        </div>`;
    }).join("");
  }

  function renderTrades(host, trades) {
    if (!trades || !trades.length) {
      host.innerHTML = '<div class="muted">no open trades</div>';
      return;
    }
    host.innerHTML = trades.map(t => {
      const sideCls = t.side === "UP" ? "side-up" : "side-down";
      const arrow = t.side === "UP" ? "▲" : "▼";
      const pnlCls = t.unrealized_pnl == null ? "" : (t.unrealized_pnl >= 0 ? "pnl-pos" : "pnl-neg");
      const pnlStr = t.unrealized_pnl == null ? "—" : fmtUsd(t.unrealized_pnl);
      const tgt = t.target_price != null ? `→ ${t.target_price.toFixed(2)}` : (t.hold_to_resolution ? "→ HOLD" : "");
      const bid = t.current_bid != null ? t.current_bid.toFixed(2) : "—";
      return `
        <div class="row">
          <span class="a">
            <span class="strategy-tag">${escapeHtml(t.strategy)}</span>
            <span class="${sideCls}">${arrow} ${t.side}</span>
            ${t.entry_price.toFixed(2)} ${tgt}
          </span>
          <span class="b">bid ${bid} <span class="${pnlCls}">${pnlStr}</span></span>
        </div>`;
    }).join("");
  }

  function renderTape(host, resolutions) {
    if (!resolutions || !resolutions.length) {
      host.innerHTML = '<span class="muted">no history yet</span>';
      return;
    }
    // Display oldest-first L→R so newest is on right (like a tape)
    const reversed = resolutions.slice().reverse();
    host.innerHTML = reversed.map(r => {
      const upper = (r.winner || "").toUpperCase();
      const isUp = upper === "UP";
      const cls = isUp ? "win-up" : "win-down";
      const arrow = isUp ? "▲" : "▼";
      const arrowCls = isUp ? "up" : "down";
      const traded = r.did_we_trade
        ? `<span class="traded">${r.trade_pnl >= 0 ? "+" : ""}$${r.trade_pnl.toFixed(2)}</span>`
        : `<span class="nope">—</span>`;
      return `<div class="seg ${cls}"><span class="arrow ${arrowCls}">${arrow}</span>${traded}</div>`;
    }).join("");
  }

  function renderSession(host, session) {
    const s = session || {wins:0, losses:0, open:0, pnl:0};
    const pnlCls = s.pnl >= 0 ? "w" : "l";
    host.innerHTML = `
      <div class="item"><span class="lbl">WINS</span><span class="w">${s.wins}</span></div>
      <div class="item"><span class="lbl">LOSSES</span><span class="l">${s.losses}</span></div>
      <div class="item"><span class="lbl">OPEN</span><span class="o">${s.open}</span></div>
      <div class="item"><span class="lbl">P&amp;L</span><span class="${pnlCls}">${fmtUsd(s.pnl)}</span></div>
    `;
  }

  function renderCoin(coin, payload) {
    const card = ensureCard(coin);
    card.classList.toggle("color-green", payload.chart_color === "green");
    card.classList.toggle("color-red",   payload.chart_color === "red");

    const get = (k) => card.querySelector(`[data-field="${k}"]`);
    const priceEl = get("price");
    priceEl.textContent = "$" + fmtPrice(payload.live_price);
    priceEl.classList.toggle("green", payload.chart_color === "green");
    priceEl.classList.toggle("red",   payload.chart_color === "red");

    const m = payload.market;
    if (m) {
      get("timer").textContent = fmtSecs(m.time_remaining_s);
      get("strike").textContent = m.strike != null ? "$" + fmtPrice(m.strike) : "—";
      get("strike-label").textContent = m.strike_label || "open";
      get("strike-mini").textContent = m.strike != null ? `strike $${fmtPrice(m.strike)}` : "no market";

      const mp = m.market_prediction;
      if (mp) {
        const upPct = Math.max(0, Math.min(100, mp.up_prob * 100));
        const dnPct = Math.max(0, Math.min(100, mp.down_prob * 100));
        get("predbar-up").style.width = upPct + "%";
        get("predbar-down").style.width = dnPct + "%";
        get("predbar-up-lbl").textContent  = `▲ UP ${fmtPct(mp.up_prob)}`;
        get("predbar-down-lbl").textContent = `${fmtPct(mp.down_prob)} DOWN ▼`;
      } else {
        get("predbar-up").style.width = "0%";
        get("predbar-down").style.width = "0%";
        get("predbar-up-lbl").textContent = "▲ UP —";
        get("predbar-down-lbl").textContent = "— DOWN ▼";
      }
    } else {
      get("timer").textContent = "—";
      get("strike").textContent = "—";
      get("strike-label").textContent = "no market";
      get("strike-mini").textContent = "no market";
      get("predbar-up").style.width = "0%";
      get("predbar-down").style.width = "0%";
      get("predbar-up-lbl").textContent = "▲ UP —";
      get("predbar-down-lbl").textContent = "— DOWN ▼";
    }

    renderLeds(get("leds"), payload.sources);
    renderSignals(get("signals"), payload.bot_signals);
    renderTrades(get("trades"), payload.open_trades);
    renderTape(get("tape"), payload.recent_resolutions);
    renderSession(get("session"), payload.session);
    return card;
  }

  function renderTopbar(state) {
    $("t-mode").textContent = (state.mode || "—").toUpperCase();
    $("t-dry").textContent  = state.dry_run ? "ON" : "OFF";
    $("t-dry").className = "val " + (state.dry_run ? "amber" : "red");
    const pnl = state.summary && state.summary.paper ? state.summary.paper.pnl : null;
    const bal = state.summary && state.summary.paper ? state.summary.paper.balance : null;
    $("t-pnl").textContent = pnl == null ? "—" : fmtUsd(pnl);
    $("t-pnl").className = "val " + (pnl == null ? "cyan" : (pnl >= 0 ? "green" : "red"));
    $("t-bal").textContent = bal == null ? "—" : "$" + bal.toFixed(2);
    const ts = state.timestamp_utc || "";
    $("t-ts").textContent = ts ? ts.slice(11,19) : "—";
  }

  function applyState(state) {
    if (!state || !state.coins) return;
    const loading = $("loading");
    if (loading) loading.remove();
    renderTopbar(state);

    const stack = $("stack");
    const order = effectiveOrder(state.coins_order || Object.keys(state.coins));

    // Render & order cards
    order.forEach((coin) => {
      const payload = state.coins[coin];
      if (!payload) return;
      const card = renderCoin(coin, payload);
      if (card.parentNode !== stack) stack.appendChild(card);
    });
    // Re-order DOM to match `order`
    let prev = null;
    order.forEach((coin) => {
      const card = stack.querySelector(`.card[data-coin="${coin}"]`);
      if (!card) return;
      if (prev) {
        if (prev.nextSibling !== card) stack.insertBefore(card, prev.nextSibling);
      } else {
        if (stack.firstChild !== card) stack.insertBefore(card, stack.firstChild);
      }
      prev = card;
    });
  }

  let inFlight = false;
  async function poll() {
    if (inFlight) return;
    inFlight = true;
    try {
      const res = await fetch("api/state", { cache: "no-store" });
      if (res.ok) applyState(await res.json());
    } catch (e) {
      // Silently ignore — keep last frame visible
    } finally {
      inFlight = false;
    }
  }
  poll();
  setInterval(poll, POLL_MS);
})();
</script>
</body>
</html>
"""
