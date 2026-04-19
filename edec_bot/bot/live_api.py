"""Minimal built-in HTTP server for Home Assistant ingress.

Runs on its own thread + event loop so the dashboard stays responsive even
if the main bot loop hitches on long-running work.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone

from version import __version__

logger = logging.getLogger("edec.live_api")


class LiveApiServer:
    """Small asyncio HTTP server (no extra dependency) for ingress compatibility."""

    def __init__(
        self,
        dashboard_state,
        host: str = "0.0.0.0",
        port: int = 8099,
        app_version: str = __version__,
    ):
        self.dashboard_state = dashboard_state
        self.host = host
        self.port = int(port)
        self.app_version = str(app_version)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: asyncio.AbstractServer | None = None
        self._ready = threading.Event()
        self._stop = threading.Event()

    def start_threaded(self) -> None:
        """Spin up the server on a dedicated thread + event loop."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._ready.clear()
        self._thread = threading.Thread(
            target=self._run_thread, name="edec-dashboard-api", daemon=True
        )
        self._thread.start()
        # Wait briefly for the server to bind so we can log a coherent error.
        self._ready.wait(timeout=5.0)

    def stop_threaded(self) -> None:
        if self._loop is None or not self._loop.is_running():
            return
        self._stop.set()
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._thread = None
        self._loop = None
        self._server = None

    def _run_thread(self) -> None:
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._serve())
        except Exception as exc:
            logger.error("Dashboard API thread crashed: %s", exc)
        finally:
            try:
                if self._loop is not None and not self._loop.is_closed():
                    self._loop.close()
            except Exception:
                pass

    async def _serve(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        logger.info(
            "Dashboard API listening on http://%s:%s (own thread)",
            self.host, self.port,
        )
        self._ready.set()
        async with self._server:
            await self._server.serve_forever()

    @staticmethod
    async def _read_chunked_body(reader: asyncio.StreamReader) -> bytes:
        chunks: list[bytes] = []
        while True:
            size_line = await reader.readuntil(b"\r\n")
            size_token = size_line.decode("utf-8", errors="ignore").split(";", 1)[0].strip()
            if not size_token:
                continue
            chunk_size = int(size_token, 16)
            if chunk_size == 0:
                while True:
                    trailer_line = await reader.readuntil(b"\r\n")
                    if trailer_line == b"\r\n":
                        break
                break
            chunks.append(await reader.readexactly(chunk_size))
            await reader.readexactly(2)
        return b"".join(chunks)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            req = await reader.readuntil(b"\r\n\r\n")
            request_text = req.decode("utf-8", errors="ignore")
            lines = request_text.splitlines()
            first_line = lines[0] if lines else ""
            method, raw_path, _ = (first_line.split(" ", 2) + ["", "", ""])[:3]
            path = raw_path.split("?", 1)[0]
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if not line or ":" not in line:
                    continue
                name, value = line.split(":", 1)
                headers[name.strip().lower()] = value.strip()
            content_length = int(headers.get("content-length", "0") or 0)
            transfer_encoding = headers.get("transfer-encoding", "").lower()
            body = b""
            if content_length > 0:
                body = await reader.readexactly(content_length)
            elif "chunked" in transfer_encoding:
                body = await self._read_chunked_body(reader)

            if method not in ("GET", "POST"):
                await self._send(writer, 405, {"error": "method_not_allowed"})
                return

            if method == "GET" and path in ("/", "/index.html"):
                html = self._index_html()
                await self._send(writer, 200, html, content_type="text/html; charset=utf-8")
                return
            if method == "GET" and path == "/health":
                await self._send(writer, 200, {"status": "ok", "timestamp_utc": datetime.now(timezone.utc).isoformat()})
                return
            if method == "GET" and path == "/api/state":
                # Thread-safe snapshot — no await on bot loop.
                await self._send(writer, 200, self.dashboard_state.get_state_threadsafe())
                return
            if method == "GET" and path == "/api/history":
                await self._send(writer, 200, self.dashboard_state.get_history_threadsafe())
                return
            if method == "POST" and path == "/api/control":
                try:
                    payload = json.loads(body.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    await self._send(writer, 400, {"ok": False, "message": "Invalid JSON body."})
                    return
                if not isinstance(payload, dict):
                    await self._send(writer, 400, {"ok": False, "message": "Control payload must be an object."})
                    return
                result = self.dashboard_state.apply_control_threadsafe(payload)
                await self._send(writer, int(result.get("status", 200)), result)
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
            400: "Bad Request",
            200: "OK",
            404: "Not Found",
            405: "Method Not Allowed",
            500: "Internal Server Error",
            503: "Service Unavailable",
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

    def _index_html(self) -> str:
        return _DASHBOARD_HTML.replace("__APP_VERSION__", self.app_version)


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
    font-size: 18px;
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
    gap: 12px;
    padding: 10px 16px;
    background: linear-gradient(180deg, #1a2342 0%, #0a1024 100%);
    border-bottom: 2px solid var(--chrome-hi);
    box-shadow:
      inset 0 1px 0 #2c3865,
      inset 0 -2px 0 #000,
      0 0 24px rgba(0, 240, 255, 0.15);
  }
  .brand {
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 14px;
    color: var(--neon-cyan);
    text-shadow: 0 0 6px var(--neon-cyan), 0 0 14px rgba(0,240,255,0.5);
    letter-spacing: 1.5px;
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
  .topstats { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
  .topstats .pill {
    padding: 3px 8px;
    border: 1px solid var(--chrome-hi);
    border-radius: 4px;
    background: #11193a;
    box-shadow: inset 0 1px 0 #2c3865, 0 0 6px rgba(0,240,255,0.2);
    font-size: 16px;
    color: var(--text);
  }
  .topstats .pill .lbl { color: var(--text-dim); font-size: 12px; margin-right: 5px; letter-spacing: 1px; }
  .topstats .pill .val.green  { color: var(--neon-lime);  text-shadow: 0 0 5px var(--neon-lime); }
  .topstats .pill .val.red    { color: var(--neon-red);   text-shadow: 0 0 5px var(--neon-red); }
  .topstats .pill .val.cyan   { color: var(--neon-cyan);  text-shadow: 0 0 5px var(--neon-cyan); }
  .topstats .pill .val.amber  { color: var(--neon-amber); text-shadow: 0 0 5px var(--neon-amber); }

  @keyframes blink {
    0%, 60% { opacity: 1; }
    70%, 100% { opacity: 0.25; }
  }

  .uplink {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border: 1px solid var(--chrome-hi);
    border-radius: 4px;
    background: #0e1530;
    font-family: "VT323", monospace;
    font-size: 14px;
    letter-spacing: 1px;
    transition: background 200ms, border-color 200ms;
  }
  .uplink .udot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--neon-lime);
    box-shadow: 0 0 6px var(--neon-lime);
  }
  .uplink .uage { color: var(--text-dim); font-size: 12px; }
  .uplink-ok    { color: var(--neon-lime);  border-color: rgba(170,255,0,0.5); }
  .uplink-stale { color: var(--neon-amber); border-color: rgba(255,170,0,0.6); background: #2a1f08; animation: blink 0.9s infinite; }
  .uplink-stale .udot { background: var(--neon-amber); box-shadow: 0 0 8px var(--neon-amber); }
  .uplink-dead  { color: var(--neon-red);   border-color: rgba(255,30,80,0.7);  background: #320812; animation: blink 0.5s infinite; }
  .uplink-dead .udot  { background: var(--neon-red);   box-shadow: 0 0 10px var(--neon-red); }

  /* ============================================================
     Control deck
     ============================================================ */
  .control-deck {
    max-width: 1400px;
    margin: 12px auto 0 auto;
    padding: 0 12px;
  }
  .control-shell {
    background: linear-gradient(180deg, #0f1736 0%, #070c1f 100%);
    border: 2px solid var(--chrome-hi);
    border-radius: 8px;
    box-shadow:
      inset 0 1px 0 #2c3865,
      inset 0 -2px 0 #000,
      0 0 18px rgba(0, 240, 255, 0.10),
      0 4px 0 #000;
    padding: 10px 12px;
  }
  .control-shell h3 {
    margin: 0 0 10px 0;
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 10px;
    color: var(--neon-magenta);
    text-shadow: 0 0 4px var(--neon-magenta);
    letter-spacing: 1.5px;
  }
  .control-grid {
    display: grid;
    grid-template-columns: 1.1fr 1.6fr 1.2fr;
    gap: 10px;
  }
  .control-block {
    background: rgba(10, 15, 38, 0.68);
    border: 1px solid #1f2a55;
    border-radius: 5px;
    padding: 8px 10px;
    box-shadow: inset 0 0 14px rgba(0, 0, 0, 0.4);
  }
  .control-block .head {
    display: block;
    margin-bottom: 8px;
    color: var(--text-dim);
    font-size: 11px;
    letter-spacing: 1px;
    font-family: "Press Start 2P", "VT323", monospace;
  }
  .control-row {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
    align-items: center;
  }
  .control-readout {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
    margin-top: 8px;
  }
  .readout-pill {
    padding: 4px 7px;
    border: 1px solid var(--chrome-hi);
    border-radius: 4px;
    background: #11193a;
    box-shadow: inset 0 1px 0 #2c3865, 0 0 6px rgba(0,240,255,0.2);
    color: var(--text);
    font-size: 14px;
  }
  .readout-pill .lbl {
    color: var(--text-dim);
    font-size: 10px;
    margin-right: 4px;
    letter-spacing: 1px;
  }
  .ctl-btn {
    appearance: none;
    border: 1px solid var(--chrome-hi);
    border-radius: 4px;
    padding: 5px 8px;
    background: linear-gradient(180deg, #1a2347 0%, #0d1530 100%);
    color: var(--text);
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 10px;
    letter-spacing: 1px;
    cursor: pointer;
    box-shadow: inset 0 1px 0 #2c3865, 0 2px 0 #000, 0 0 8px rgba(0,240,255,0.15);
    transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
  }
  .ctl-btn:hover:not(:disabled) {
    transform: translateY(-1px);
    box-shadow: inset 0 1px 0 #2c3865, 0 3px 0 #000, 0 0 12px rgba(0,240,255,0.3);
  }
  .ctl-btn:disabled {
    opacity: 0.45;
    cursor: wait;
  }
  .ctl-btn.active {
    color: var(--neon-cyan);
    border-color: rgba(0, 240, 255, 0.7);
    box-shadow: inset 0 1px 0 #2c3865, 0 2px 0 #000, 0 0 14px rgba(0,240,255,0.45);
  }
  .ctl-btn.warn.active,
  .ctl-btn.warn:hover:not(:disabled) {
    color: var(--neon-red);
    border-color: rgba(255, 59, 59, 0.8);
    box-shadow: inset 0 1px 0 #5f2430, 0 2px 0 #000, 0 0 14px rgba(255,59,59,0.45);
  }
  .ctl-status {
    margin-top: 12px;
    min-height: 20px;
    padding: 6px 8px;
    border: 1px dashed #2a3a78;
    border-radius: 4px;
    color: var(--text-dim);
    font-size: 16px;
    letter-spacing: 1px;
    background: rgba(6, 10, 30, 0.7);
  }
  .ctl-status.ok {
    color: var(--neon-lime);
    text-shadow: 0 0 4px var(--neon-lime);
  }
  .ctl-status.err {
    color: var(--neon-red);
    text-shadow: 0 0 4px var(--neon-red);
  }
  .ctl-status.busy {
    color: var(--neon-amber);
    text-shadow: 0 0 4px var(--neon-amber);
  }

  /* ============================================================
     Container
     ============================================================ */
  main.stack {
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 12px;
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
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto auto;
    align-items: center;
    gap: 8px;
    padding: 7px 10px;
    background: linear-gradient(180deg, #1a2247 0%, #0d1532 100%);
    border-bottom: 1px solid var(--chrome-lo);
    cursor: grab;
    user-select: none;
  }
  .card-header:active { cursor: grabbing; }
  .card-header .left {
    display: flex; align-items: center; gap: 8px; min-width: 0;
    flex-wrap: nowrap;
    overflow: hidden;
  }
  .ticker-lock {
    display: inline-grid;
    grid-template-columns: auto 96px;
    align-items: center;
    gap: 4px;
    min-width: 0;
    flex: 0 0 auto;
    padding: 2px 8px 2px 9px;
    border: 1px solid #2a3a78;
    border-radius: 6px;
    background: linear-gradient(180deg, rgba(0,240,255,0.10), rgba(8, 14, 24, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(0,240,255,0.08);
  }
  .session-lock {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 0;
    padding: 2px 8px;
    border: 1px solid #6a2c74;
    border-radius: 6px;
    background: linear-gradient(180deg, rgba(255,0,255,0.08), rgba(10, 12, 26, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(255,0,255,0.07);
  }
  .timer-lock {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 0;
    padding: 1px 6px;
    border: 1px solid #7a5e18;
    border-radius: 6px;
    background: linear-gradient(180deg, rgba(255,184,0,0.12), rgba(22, 14, 4, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(255,184,0,0.08);
  }
  .card-header .mid { min-width: 0; }
  .grip {
    color: var(--text-dim);
    font-size: 22px;
    letter-spacing: -2px;
    margin-right: 4px;
    text-shadow: 0 1px 0 #000;
  }
  .coin-name {
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 12px;
    color: var(--neon-cyan);
    text-shadow: 0 0 5px var(--neon-cyan), 0 0 12px rgba(0,240,255,0.4);
    letter-spacing: 1px;
    white-space: nowrap;
    flex: 0 0 auto;
  }
  .live-price {
    font-size: 22px;
    color: var(--text);
    text-shadow: 0 0 4px rgba(207,230,255,0.5);
    width: 96px;
    min-width: 96px;
    text-align: left;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
    flex: 0 0 auto;
  }

  .card-header .right {
    display: flex; align-items: center; gap: 8px;
    color: var(--text-dim);
    font-size: 16px;
    justify-self: end;
  }
  .timer {
    color: var(--neon-amber);
    text-shadow: 0 0 4px var(--neon-amber);
    font-size: 16px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 38px;
    line-height: 1;
  }
  .session-inline {
    display: flex; align-items: center; justify-content: center;
    gap: 6px; flex-wrap: wrap;
  }
  .session-inline .item {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 2px 7px;
    border: 1px solid #24315f;
    border-radius: 999px;
    background: rgba(7, 11, 28, 0.82);
    box-shadow: inset 0 0 8px rgba(0, 0, 0, 0.35);
  }
  .session-inline .item.wins {
    border-color: #1f6a36;
    background: linear-gradient(180deg, rgba(57,255,20,0.20), rgba(7, 18, 15, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(57,255,20,0.12);
  }
  .session-inline .item.losses {
    border-color: #7a2222;
    background: linear-gradient(180deg, rgba(255,59,59,0.18), rgba(24, 8, 10, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(255,59,59,0.10);
  }
  .session-inline .item.open {
    border-color: #7a5e18;
    background: linear-gradient(180deg, rgba(255,184,0,0.18), rgba(24, 17, 6, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(255,184,0,0.10);
  }
  .session-inline .item.pnl {
    border-color: #225a86;
    background: linear-gradient(180deg, rgba(0,240,255,0.14), rgba(8, 14, 24, 0.92));
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.35), 0 0 10px rgba(0,240,255,0.10);
  }
  .session-inline .lbl {
    color: var(--text-dim);
    font-size: 8px;
    letter-spacing: 1px;
    font-family: "Press Start 2P", "VT323", monospace;
  }
  .session-inline .val {
    font-size: 12px;
  }

  /* ============================================================
     Body grid
     ============================================================ */
  .card-body {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-gap: 8px;
    padding: 8px 10px 10px 10px;
  }
  .card-body .span2 { grid-column: 1 / -1; }

  .panel {
    background: rgba(10, 15, 38, 0.6);
    border: 1px solid #1f2a55;
    border-radius: 5px;
    padding: 7px 8px;
    box-shadow: inset 0 0 14px rgba(0, 0, 0, 0.4);
  }
  .panel h4 {
    margin: 0 0 6px 0;
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 9px;
    color: var(--neon-magenta);
    text-shadow: 0 0 4px var(--neon-magenta);
    letter-spacing: 1.5px;
  }

  /* LED row */
  .leds { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .led {
    display: flex; flex-direction: column; align-items: center; gap: 3px;
    min-width: 36px;
  }
  .led .dot {
    width: 12px; height: 12px;
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
    font-size: 10px; color: var(--text-dim); letter-spacing: 1px;
    font-family: "Press Start 2P", "VT323", monospace;
  }
  .led.on .lbl { color: var(--text); }

  /* Big readouts */
  .strike-row {
    display: flex; align-items: baseline; justify-content: space-between;
    font-size: 18px;
  }
  .strike-row.compact {
    justify-content: flex-start;
    gap: 7px;
    flex-wrap: nowrap;
    min-width: 0;
    flex: 0 0 auto;
  }
  .strike-row .big {
    color: var(--neon-amber);
    font-size: 22px;
    text-shadow: 0 0 6px var(--neon-amber);
  }
  .strike-row .delta {
    font-size: 14px;
    letter-spacing: 0.5px;
  }
  .strike-row .delta.up { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .strike-row .delta.down { color: var(--neon-red); text-shadow: 0 0 4px var(--neon-red); }
  .strike-row .delta.flat { color: var(--text-dim); }
  .strike-row .vol {
    font-size: 14px;
    color: var(--neon-cyan);
    text-shadow: 0 0 4px rgba(0,240,255,0.35);
  }
  .market-strip {
    display: flex; align-items: center; justify-content: space-between;
    gap: 10px; flex-wrap: nowrap;
  }

  /* Prediction bar */
  .predbar {
    height: 16px;
    background: #07091a;
    border: 1px solid var(--chrome-lo);
    border-radius: 3px;
    position: relative;
    overflow: hidden;
    margin-top: 0;
    flex: 1 1 190px;
    min-width: 168px;
    max-width: 250px;
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
    font-size: 12px;
    color: #000;
    text-shadow: 0 0 2px #fff;
    padding: 0 6px;
    z-index: 2;
  }
  .predbar .label-up { left: 6px; }
  .predbar .label-down { right: 6px; }

  /* Signal/trade list rows */
  .row { display: flex; justify-content: space-between; gap: 8px; padding: 2px 0; font-size: 15px; }
  .row + .row { border-top: 1px dashed #1c2548; }
  .row .a { color: var(--text); }
  .row .b { color: var(--text-dim); }
  .strategy-tag {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 3px;
    background: #1a234a;
    color: var(--neon-cyan);
    font-size: 11px;
    letter-spacing: 1px;
    text-shadow: 0 0 3px var(--neon-cyan);
    border: 1px solid #2a3a78;
  }
  .side-up   { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .side-down { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); }
  .score {
    display: inline-block;
    min-width: 40px;
    text-align: right;
    color: var(--neon-amber);
    text-shadow: 0 0 4px var(--neon-amber);
  }
  .pnl-pos { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .pnl-neg { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); }
  .muted { color: var(--text-dim); font-style: italic; }

  /* Recent resolutions tape */
  .tape {
    display: flex; gap: 6px; align-items: center;
    flex-wrap: wrap;
  }
  .tape .seg {
    border: 1px solid var(--chrome-lo);
    border-radius: 3px;
    padding: 3px 6px;
    background: #07091a;
    box-shadow: inset 0 0 8px rgba(0,0,0,0.5);
    font-size: 14px;
    display: flex; align-items: center; gap: 6px;
  }
  .tape .seg.win-up   { border-color: var(--neon-lime); }
  .tape .seg.win-down { border-color: var(--neon-red); }
  .tape .arrow.up   { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); font-size: 16px; }
  .tape .arrow.down { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); font-size: 16px; }
  .tape .seg .traded { color: var(--neon-cyan); font-size: 12px; }
  .tape .seg .nope   { color: var(--text-dim); font-size: 12px; }

  /* Session readout */
  .session {
    display: flex; gap: 12px; flex-wrap: wrap; align-items: center;
    font-size: 16px;
  }
  .session .item .lbl {
    color: var(--text-dim); font-size: 10px; letter-spacing: 1px;
    font-family: "Press Start 2P", "VT323", monospace;
    display: block;
  }
  .session .w { color: var(--neon-lime); text-shadow: 0 0 4px var(--neon-lime); }
  .session .l { color: var(--neon-red);  text-shadow: 0 0 4px var(--neon-red); }
  .session .o { color: var(--neon-amber); text-shadow: 0 0 4px var(--neon-amber); }

  /* Live chart slot (Step 3) */
  .chart-slot {
    position: relative;
    height: 118px;
    border: 1px solid #2a3a78;
    border-radius: 4px;
    overflow: hidden;
    background:
      repeating-linear-gradient(
        to right,
        rgba(0,240,255,0.05) 0 1px,
        transparent 1px 40px
      ),
      repeating-linear-gradient(
        to bottom,
        rgba(0,240,255,0.05) 0 1px,
        transparent 1px 28px
      ),
      #050912;
  }
  .chart-svg {
    display: block;
    width: 100%;
    height: 100%;
  }
  .chart-empty {
    position: absolute; inset: 0;
    display: flex; align-items: center; justify-content: center;
    color: var(--text-dim);
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 10px;
    letter-spacing: 2px;
  }
  .chart-meta {
    position: absolute;
    top: 6px; left: 8px;
    display: flex; gap: 14px;
    flex-wrap: wrap;
    max-width: 48%;
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 9px;
    letter-spacing: 1px;
    pointer-events: none;
  }
  .chart-meta .lo  { color: var(--text-dim); }
  .chart-meta .hi  { color: var(--text-dim); }
  .chart-meta .now { color: var(--neon-cyan); text-shadow: 0 0 4px var(--neon-cyan); }
  .chart-meta .stk { color: var(--neon-amber); text-shadow: 0 0 4px var(--neon-amber); }
  .chart-feeds {
    position: absolute;
    top: 6px; right: 8px;
    display: flex; gap: 5px;
    flex-wrap: wrap;
    justify-content: flex-end;
    max-width: 44%;
    pointer-events: none;
  }
  .chart-feed {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 2px 5px;
    border: 1px solid #24315f;
    border-radius: 999px;
    background: rgba(7, 11, 28, 0.82);
    color: var(--text-dim);
    font-family: "Press Start 2P", "VT323", monospace;
    font-size: 7px;
    letter-spacing: 1px;
    box-shadow: inset 0 0 8px rgba(0, 0, 0, 0.35);
  }
  .chart-feed .dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: #24304f;
    border: 1px solid #000;
  }
  .chart-feed.on .dot {
    background: radial-gradient(circle at 35% 30%, #b3ffd1, var(--neon-lime) 70%);
    box-shadow: 0 0 6px var(--neon-lime);
  }
  .chart-feed.stale .dot {
    background: radial-gradient(circle at 35% 30%, #ffe8a8, var(--neon-amber) 70%);
    box-shadow: 0 0 5px var(--neon-amber);
  }
  .chart-feed.off .dot {
    background: #24304f;
    box-shadow: none;
  }
  .chart-resolutions {
    position: absolute;
    right: 8px; bottom: 6px;
    display: flex; align-items: center; gap: 6px;
    pointer-events: none;
  }
  .chart-res-dot {
    width: 10px; height: 10px;
    border-radius: 50%;
    border: 1px solid #000;
    background: #24304f;
    box-shadow: inset 0 -2px 3px rgba(0, 0, 0, 0.55);
  }
  .chart-res-dot.yes {
    background: radial-gradient(circle at 35% 30%, #b3ffd1, var(--neon-lime) 70%);
    box-shadow: 0 0 8px var(--neon-lime);
  }
  .chart-res-dot.no {
    background: radial-gradient(circle at 35% 30%, #ffb3b3, var(--neon-red) 70%);
    box-shadow: 0 0 8px var(--neon-red);
  }

  /* No-data state */
  .nodata {
    text-align: center;
    color: var(--text-dim);
    padding: 28px;
    font-style: italic;
    font-size: 18px;
  }

  @media (max-width: 860px) {
    .control-grid { grid-template-columns: 1fr; }
    .chart-slot { height: 104px; }
    .ticker-lock { grid-template-columns: auto 92px; gap: 4px; }
    .live-price { font-size: 20px; width: 92px; min-width: 92px; }
    .coin-name { font-size: 11px; }
    .card-header .right { font-size: 13px; }
    .timer { font-size: 15px; min-width: 36px; }
    .session-inline { gap: 4px; }
    .session-inline .item { padding: 2px 5px; }
    .session-inline .val { font-size: 11px; }
  }

  /* Mobile */
  @media (max-width: 720px) {
    header.topbar { align-items: flex-start; flex-direction: column; }
    .brand { font-size: 12px; }
    .topstats { width: 100%; gap: 8px; justify-content: flex-start; }
    .topstats .pill { font-size: 14px; }
    .topstats .pill .lbl { font-size: 10px; }
    .uplink { font-size: 12px; }
    .control-deck { padding: 0 10px; }
    .control-shell { padding: 8px 10px; }
    .control-block { padding: 7px 8px; }
    .control-readout { gap: 4px; }
    .readout-pill { font-size: 12px; }
    .readout-pill .lbl { font-size: 9px; }
    .ctl-btn { font-size: 9px; padding: 4px 6px; }
    .ctl-status { font-size: 14px; }
    main.stack { padding: 10px; gap: 10px; }
    .card-header { padding: 7px 10px; }
    .card-body { grid-template-columns: minmax(0,1fr) minmax(0,1fr); grid-gap: 8px; padding: 8px 10px 10px 10px; }
    .panel { padding: 7px 8px; }
    .panel h4 { font-size: 8px; margin-bottom: 6px; }
    .strike-row { font-size: 14px; }
    .strike-row .big { font-size: 18px; }
    .strike-row .delta, .strike-row .vol { font-size: 11px; }
    .market-strip { gap: 8px; }
    .predbar { height: 16px; }
    .predbar { min-width: 142px; max-width: none; }
    .predbar .label-up, .predbar .label-down { font-size: 10px; top: 0; }
    .row { flex-direction: column; gap: 2px; font-size: 14px; }
    .strategy-tag { font-size: 10px; }
    .session-inline { gap: 4px; }
    .session-inline .lbl { font-size: 7px; }
    .session-inline .val { font-size: 10px; }
    .session { gap: 8px; font-size: 14px; }
    .session .item .lbl { font-size: 9px; }
    .chart-slot { height: 84px; }
    .chart-empty { font-size: 8px; letter-spacing: 1px; }
    .chart-meta { font-size: 7px; gap: 8px; }
    .chart-feed { font-size: 6px; padding: 2px 4px; }
    .chart-res-dot { width: 8px; height: 8px; }
  }

  @media (max-width: 430px) {
    .card-body { grid-template-columns: 1fr; }
    .ticker-lock { grid-template-columns: auto 84px; gap: 3px; padding: 2px 6px 2px 7px; }
    .live-price { font-size: 18px; width: 84px; min-width: 84px; }
    .coin-name { font-size: 10px; }
    .card-header .right { font-size: 11px; }
    .timer { font-size: 14px; min-width: 34px; }
    .market-strip { gap: 6px; }
    .predbar { min-width: 126px; }
    .chart-slot { height: 78px; }
  }
</style>
</head>
<body>
  <header class="topbar">
    <div class="brand"><span class="pulse"></span>EDEC TERMINAL <span style="color:var(--text-dim);font-size:10px">v__APP_VERSION__</span></div>
    <div class="topstats">
      <div class="pill"><span class="lbl">MODE</span><span id="t-mode" class="val cyan">—</span></div>
      <div class="pill"><span class="lbl">DRY</span><span id="t-dry"  class="val amber">—</span></div>
      <div class="pill"><span class="lbl">P&amp;L</span><span id="t-pnl" class="val green">—</span></div>
      <div class="pill"><span class="lbl">BAL</span><span id="t-bal" class="val cyan">—</span></div>
      <div class="pill"><span class="lbl">UTC</span><span id="t-ts"  class="val cyan">—</span></div>
      <div id="uplink" class="uplink uplink-ok"><span class="udot"></span><span id="uplink-lbl">LINK</span><span id="uplink-age" class="uage">0ms</span></div>
    </div>
  </header>

  <section class="control-deck">
    <div class="control-shell">
      <h3>CONTROL DECK</h3>
      <div class="control-grid">
        <div class="control-block">
          <span class="head">BOT CONTROL</span>
          <div class="control-row">
            <button id="btn-start" type="button" class="ctl-btn" data-action="start">START</button>
            <button id="btn-stop" type="button" class="ctl-btn" data-action="stop">STOP</button>
            <button id="btn-kill" type="button" class="ctl-btn warn" data-action="kill">KILL</button>
          </div>
          <div class="control-readout">
            <div class="readout-pill"><span class="lbl">STATE</span><span id="ctrl-state">-</span></div>
            <div class="readout-pill"><span class="lbl">MODE</span><span id="ctrl-mode">-</span></div>
            <div class="readout-pill"><span class="lbl">BUDGET</span><span id="ctrl-budget">-</span></div>
          </div>
        </div>
        <div class="control-block">
          <span class="head">MODE SELECT</span>
          <div class="control-row">
            <button type="button" class="ctl-btn" data-action="mode" data-value="both">ALL</button>
            <button type="button" class="ctl-btn" data-action="mode" data-value="dual">DUAL</button>
            <button type="button" class="ctl-btn" data-action="mode" data-value="single">SINGLE</button>
            <button type="button" class="ctl-btn" data-action="mode" data-value="lead">LEAD</button>
            <button type="button" class="ctl-btn" data-action="mode" data-value="swing">SWING</button>
            <button type="button" class="ctl-btn" data-action="mode" data-value="off">OFF</button>
          </div>
        </div>
        <div class="control-block">
          <span class="head">BUDGET PER TRADE</span>
          <div class="control-row">
            <button type="button" class="ctl-btn" data-action="budget" data-value="1">$1</button>
            <button type="button" class="ctl-btn" data-action="budget" data-value="2">$2</button>
            <button type="button" class="ctl-btn" data-action="budget" data-value="5">$5</button>
            <button type="button" class="ctl-btn" data-action="budget" data-value="10">$10</button>
            <button type="button" class="ctl-btn" data-action="budget" data-value="15">$15</button>
            <button type="button" class="ctl-btn" data-action="budget" data-value="20">$20</button>
          </div>
        </div>
      </div>
      <div id="ctrl-status" class="ctl-status">CONTROL LINK STANDBY</div>
    </div>
  </section>

  <main id="stack" class="stack">
    <div id="loading" class="nodata">⏳ ESTABLISHING UPLINK…</div>
  </main>

<script>
(() => {
  // ----- Constants -----
  const POLL_MS = 100;             // 10 Hz
  const STALE_AFTER_MS = 1500;     // banner + dim if no frame for this long
  const DEAD_AFTER_MS = 5000;      // hard "uplink lost" threshold
  const STORAGE_KEY = "edec_card_order_v1";
  const FEED_LABELS = { binance: "BNC", coinbase: "CB ", coingecko: "CG ", polymarket_rtds: "RTDS" };
  const MODE_NAMES = { both: "ALL", dual: "DUAL", single: "SINGLE", lead: "LEAD", swing: "SWING", off: "OFF" };

  // ----- Helpers -----
  const $ = (id) => document.getElementById(id);
  const fmtPrice = (p) => p == null ? "—" : (p >= 1000 ? p.toLocaleString(undefined, {maximumFractionDigits: 2}) : p.toFixed(p < 1 ? 4 : 2));
  const fmtPct = (x) => (x == null ? "—" : (x*100).toFixed(0) + "%");
  const fmtUsd = (x) => (x == null ? "—" : (x >= 0 ? "+" : "") + "$" + x.toFixed(2));
  const fmtSignedPrice = (x) => (x == null ? "—" : (x >= 0 ? "+" : "") + "$" + fmtPrice(Math.abs(x)));
  const fmtVolumeCompact = (x) => {
    if (x == null || !Number.isFinite(x)) return "—";
    const abs = Math.abs(x);
    if (abs >= 1000000) return (x / 1000000).toFixed(1) + "m";
    return (x / 1000).toFixed(1) + "k";
  };
  const fmtSecs = (s) => {
    if (s == null) return "—";
    s = Math.max(0, Math.round(s));
    const m = Math.floor(s / 60), r = s % 60;
    return m + ":" + String(r).padStart(2, "0");
  };
  const escapeHtml = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

  // Skip innerHTML if the rendered key matches the previous frame.
  // Prevents LED-blink restarts and DOM thrash when nothing changed.
  function cachedSet(el, key, html) {
    if (el.__lastKey === key) return;
    el.__lastKey = key;
    el.innerHTML = html;
  }
  function setText(el, value) {
    if (el.__lastText === value) return;
    el.__lastText = value;
    el.textContent = value;
  }
  function setAttr(el, name, value) {
    const k = "__lastAttr_" + name;
    if (el[k] === value) return;
    el[k] = value;
    el.setAttribute(name, value);
  }
  function setStyle(el, prop, value) {
    const k = "__lastStyle_" + prop;
    if (el[k] === value) return;
    el[k] = value;
    el.style[prop] = value;
  }
  function setClassList(el, cls, on) {
    const k = "__lastCls_" + cls;
    const v = !!on;
    if (el[k] === v) return;
    el[k] = v;
    el.classList.toggle(cls, v);
  }
  function setDisabled(el, value) {
    const v = !!value;
    if (el.__lastDisabled === v) return;
    el.__lastDisabled = v;
    el.disabled = v;
  }
  function setControlStatus(text, cls) {
    const host = $("ctrl-status");
    if (!host) return;
    setText(host, text);
    host.className = "ctl-status" + (cls ? " " + cls : "");
  }

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
          <div class="ticker-lock">
          <span class="coin-name">${coin.toUpperCase()}</span>
          <span class="live-price" data-field="price">—</span>
          </div>
        </div>
        <div class="mid">
          <div class="session-lock">
            <div class="session-inline" data-field="session-inline"></div>
          </div>
        </div>
        <div class="right">
          <div class="timer-lock">
            <span class="timer"><span data-field="timer">—</span></span>
          </div>
        </div>
      </div>
      <div class="card-body">
        <div class="panel span2">
          <div class="market-strip">
            <div class="strike-row compact">
              <span class="big" data-field="strike">—</span>
              <span class="delta flat" data-field="strike-delta">—</span>
              <span class="vol" data-field="market-volume">—</span>
            </div>
            <div class="predbar">
              <div class="up"   data-field="predbar-up"   style="width:0%"></div>
              <div class="down" data-field="predbar-down" style="width:0%"></div>
              <span class="label-up"   data-field="predbar-up-lbl">YES —</span>
              <span class="label-down" data-field="predbar-down-lbl">— NO</span>
            </div>
          </div>
        </div>

        <div class="panel">
          <h4>🤖 BOT STRATEGIES</h4>
          <div data-field="signals"><div class="muted">no live signals</div></div>
        </div>
        <div class="panel">
          <h4>💼 OPEN TRADES</h4>
          <div data-field="trades"><div class="muted">no open trades</div></div>
        </div>

        <div class="panel span2">
          <div class="chart-slot" data-field="chart"></div>
        </div>
      </div>
    `;
    bindDrag(card);
    return card;
  }

  let controlBusy = false;
  async function sendControl(action, value) {
    action = (action || "").trim();
    if (!action) {
      setControlStatus("CONTROL MAPPING ERROR.", "err");
      return;
    }
    if (controlBusy) return;
    if (action === "kill" && !window.confirm("Activate kill switch and stop scanning?")) {
      return;
    }
    controlBusy = true;
    document.querySelectorAll(".ctl-btn").forEach((btn) => setDisabled(btn, true));
    setControlStatus("SENDING CONTROL...", "busy");
    try {
      const payload = { action };
      if (value != null && value !== "") payload.value = value;
      const res = await fetch("api/control", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({ ok: false, message: "Control response was not valid JSON." }));
      if (data && data.state) {
        applyState(data.state);
        lastFrameAt = performance.now();
      }
      setControlStatus(
        data && data.message ? data.message : (res.ok ? "CONTROL UPDATED." : "CONTROL FAILED."),
        res.ok && data && data.ok ? "ok" : "err"
      );
    } catch (err) {
      setControlStatus("CONTROL REQUEST FAILED.", "err");
    } finally {
      controlBusy = false;
      document.querySelectorAll(".ctl-btn").forEach((btn) => setDisabled(btn, false));
    }
  }
  function bindControls() {
    document.querySelectorAll(".ctl-btn[data-action]").forEach((btn) => {
      btn.addEventListener("click", (event) => {
        event.preventDefault();
        const target = event.currentTarget;
        sendControl(
          target.getAttribute("data-action"),
          target.getAttribute("data-value")
        );
      });
    });
  }

  function renderChartFeeds(host, sources) {
    const feeds = (sources && sources.feeds) || [];
    // Round age to coarse buckets so the cache key doesn't churn every 100ms.
    const key = JSON.stringify(feeds.map(f => [f.name, !!f.active, f.age_s != null && f.age_s > 3 ? 1 : 0]));
    cachedSet(host, key, feeds.map(f => {
      const cls = f.active ? (f.age_s != null && f.age_s > 3 ? "stale" : "on") : "off";
      const lbl = FEED_LABELS[f.name] || f.name.toUpperCase().slice(0,4);
      return `<div class="chart-feed ${cls}"><span class="dot"></span><span class="lbl">${lbl}</span></div>`;
    }).join(""));
  }

  function renderSignals(host, signals) {
    if (!signals || !signals.length) {
      cachedSet(host, "empty", '<div class="muted">no live signals</div>');
      return;
    }
    const html = signals.map(s => {
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
    cachedSet(host, html, html);  // key === html, simple but correct
  }

  function renderTrades(host, trades) {
    if (!trades || !trades.length) {
      cachedSet(host, "empty", '<div class="muted">no open trades</div>');
      return;
    }
    const html = trades.map(t => {
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
    cachedSet(host, html, html);
  }

  function renderResolutionDots(host, resolutions) {
    if (!resolutions || !resolutions.length) {
      cachedSet(host, "empty", "");
      return;
    }
    // Oldest-left so the newest result stays on the right edge.
    const reversed = resolutions.slice().reverse();
    const html = reversed.map(r => {
      const upper = (r.winner || "").toUpperCase();
      const cls = upper === "UP" || upper === "YES" ? "yes" : "no";
      return `<span class="chart-res-dot ${cls}"></span>`;
    }).join("");
    cachedSet(host, html, html);
  }

  // ----- Live chart (SVG, in-place updates) -----
  const SVG_NS = "http://www.w3.org/2000/svg";
  const CHART_W = 400, CHART_H = 170, CHART_PADX = 6, CHART_PADY = 14;

  // Mount a chart skeleton into the host the first time we see it.
  // Returns the cached refs so subsequent ticks can update attributes only.
  function ensureChart(host, coin) {
    if (host.__chart) return host.__chart;
    host.innerHTML = "";
    const gid = `g-${coin}`, cidA = `ca-${coin}`, cidB = `cb-${coin}`;
    const svg = document.createElementNS(SVG_NS, "svg");
    svg.setAttribute("class", "chart-svg");
    svg.setAttribute("viewBox", `0 0 ${CHART_W} ${CHART_H}`);
    svg.setAttribute("preserveAspectRatio", "none");
    svg.innerHTML = `
      <defs>
        <filter id="${gid}" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="1.4" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
        <clipPath id="${cidA}"><rect data-r="above" x="0" y="0" width="${CHART_W}" height="${CHART_H}"/></clipPath>
        <clipPath id="${cidB}"><rect data-r="below" x="0" y="0" width="${CHART_W}" height="0"/></clipPath>
      </defs>
      <line data-r="strike" x1="0" y1="-10" x2="${CHART_W}" y2="-10"
            stroke="var(--neon-amber)" stroke-width="1.2"
            stroke-dasharray="5 4" opacity="0" filter="url(#${gid})"/>
      <polyline data-r="green" fill="none" stroke="var(--neon-lime)"
                stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"
                filter="url(#${gid})" clip-path="url(#${cidA})"/>
      <polyline data-r="red"   fill="none" stroke="var(--neon-red)"
                stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"
                filter="url(#${gid})" clip-path="url(#${cidB})"/>
      <polyline data-r="solo"  fill="none" stroke="var(--neon-cyan)"
                stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"
                filter="url(#${gid})" style="display:none"/>
      <circle data-r="dot" cx="-10" cy="-10" r="3.2" fill="var(--neon-cyan)" filter="url(#${gid})"/>
    `;
    const meta = document.createElement("div");
    meta.className = "chart-meta";
    meta.innerHTML = `
      <span class="hi"  data-r="hi">▲ —</span>
      <span class="lo"  data-r="lo">▼ —</span>
      <span class="stk" data-r="stk" style="display:none">━ —</span>
      <span class="now" data-r="now">● —</span>
    `;
    const empty = document.createElement("div");
    empty.className = "chart-empty";
    empty.textContent = "⏳ AWAITING TELEMETRY";
    const feeds = document.createElement("div");
    feeds.className = "chart-feeds";
    feeds.setAttribute("data-r", "feeds");
    const resolutions = document.createElement("div");
    resolutions.className = "chart-resolutions";
    resolutions.setAttribute("data-r", "resolutions");
    host.appendChild(svg);
    host.appendChild(meta);
    host.appendChild(feeds);
    host.appendChild(resolutions);
    host.appendChild(empty);

    host.__chart = {
      svg, meta, empty, feeds, resolutions,
      strikeLine: svg.querySelector('[data-r="strike"]'),
      green: svg.querySelector('[data-r="green"]'),
      red:   svg.querySelector('[data-r="red"]'),
      solo:  svg.querySelector('[data-r="solo"]'),
      dot:   svg.querySelector('[data-r="dot"]'),
      clipAbove: svg.querySelector('[data-r="above"]'),
      clipBelow: svg.querySelector('[data-r="below"]'),
      mHi:  meta.querySelector('[data-r="hi"]'),
      mLo:  meta.querySelector('[data-r="lo"]'),
      mStk: meta.querySelector('[data-r="stk"]'),
      mNow: meta.querySelector('[data-r="now"]'),
    };
    return host.__chart;
  }

  function renderChart(host, coin, payload) {
    const c = ensureChart(host, coin);
    const series = payload.price_series || [];
    const market = payload.market || null;
    const strike = market ? market.strike : null;
    const color  = payload.chart_color || "neutral";

    renderChartFeeds(c.feeds, payload.sources);
    renderResolutionDots(c.resolutions, payload.recent_resolutions);

    if (series.length < 2) {
      setStyle(c.empty, "display", "flex");
      setStyle(c.svg,   "opacity", "0.25");
      return;
    }
    setStyle(c.empty, "display", "none");
    setStyle(c.svg,   "opacity", "1");

    const xs = series.map(p => p.t);
    const ys = series.map(p => p.p);
    let xMin = xs[0], xMax = xs[xs.length - 1];
    if (market && market.start_time && market.end_time) {
      const s = Date.parse(market.start_time) / 1000;
      const e = Date.parse(market.end_time) / 1000;
      if (Number.isFinite(s) && Number.isFinite(e) && e > s) { xMin = s; xMax = e; }
    }
    const xRange = Math.max(1, xMax - xMin);

    let yMin = Math.min.apply(null, ys);
    let yMax = Math.max.apply(null, ys);
    if (strike != null) { yMin = Math.min(yMin, strike); yMax = Math.max(yMax, strike); }
    if (yMax - yMin < 1e-9) { yMax = yMin + 1; }
    const yPad = (yMax - yMin) * 0.12;
    yMin -= yPad; yMax += yPad;
    const yRange = yMax - yMin;

    const sx = t => CHART_PADX + ((t - xMin) / xRange) * (CHART_W - 2 * CHART_PADX);
    const sy = p => CHART_PADY + (1 - (p - yMin) / yRange) * (CHART_H - 2 * CHART_PADY);

    let points = "";
    for (let i = 0; i < series.length; i++) {
      const p = series[i];
      points += sx(p.t).toFixed(1) + "," + sy(p.p).toFixed(1) + " ";
    }
    const last = series[series.length - 1];
    const lastX = sx(last.t), lastY = sy(last.p);
    const lineColor = color === "green" ? "var(--neon-lime)"
                    : color === "red"   ? "var(--neon-red)"
                    : "var(--neon-cyan)";

    if (strike != null) {
      const strikeY = sy(strike);
      // Show split (green-above / red-below) line.
      setAttr(c.strikeLine, "y1", strikeY.toFixed(1));
      setAttr(c.strikeLine, "y2", strikeY.toFixed(1));
      setAttr(c.strikeLine, "opacity", "0.9");
      setAttr(c.clipAbove, "y", "0");
      setAttr(c.clipAbove, "height", strikeY.toFixed(1));
      setAttr(c.clipBelow, "y", strikeY.toFixed(1));
      setAttr(c.clipBelow, "height", (CHART_H - strikeY).toFixed(1));
      setAttr(c.green, "points", points);
      setAttr(c.red,   "points", points);
      setStyle(c.green, "display", "");
      setStyle(c.red,   "display", "");
      setStyle(c.solo,  "display", "none");
      setStyle(c.mStk,  "display", "");
      setText(c.mStk, "━ " + fmtPrice(strike));
    } else {
      // No strike → single cyan line, hide split + strike line.
      setAttr(c.strikeLine, "opacity", "0");
      setAttr(c.solo, "points", points);
      setAttr(c.solo, "stroke", lineColor);
      setStyle(c.solo,  "display", "");
      setStyle(c.green, "display", "none");
      setStyle(c.red,   "display", "none");
      setStyle(c.mStk,  "display", "none");
    }
    setAttr(c.dot, "cx", lastX.toFixed(1));
    setAttr(c.dot, "cy", lastY.toFixed(1));
    setAttr(c.dot, "fill", lineColor);

    setText(c.mHi,  "▲ " + fmtPrice(yMax));
    setText(c.mLo,  "▼ " + fmtPrice(yMin));
    setText(c.mNow, "● " + fmtPrice(last.p));
  }

  function renderSessionInline(host, session) {
    const s = session || {wins:0, losses:0, open:0, pnl:0};
    const pnlCls = s.pnl >= 0 ? "w" : "l";
    const key = JSON.stringify([s.wins, s.losses, s.open, Number(s.pnl || 0).toFixed(2)]);
    cachedSet(host, key, `
      <span class="item wins"><span class="lbl">W</span><span class="val w">${s.wins}</span></span>
      <span class="item losses"><span class="lbl">L</span><span class="val l">${s.losses}</span></span>
      <span class="item open"><span class="lbl">O</span><span class="val o">${s.open}</span></span>
      <span class="item pnl"><span class="lbl">P</span><span class="val ${pnlCls}">${fmtUsd(s.pnl)}</span></span>
    `);
  }

  function renderCoin(coin, payload) {
    const card = ensureCard(coin);
    card.classList.toggle("color-green", payload.chart_color === "green");
    card.classList.toggle("color-red",   payload.chart_color === "red");

    const get = (k) => card.querySelector(`[data-field="${k}"]`);
    const priceEl = get("price");
    priceEl.textContent = "$" + fmtPrice(payload.live_price);
    const m = payload.market;
    if (m) {
      get("timer").textContent = fmtSecs(m.time_remaining_s);
      get("strike").textContent = m.strike != null ? "$" + fmtPrice(m.strike) : "—";
      const deltaEl = get("strike-delta");
      const volEl = get("market-volume");
      if (m.strike != null && payload.live_price != null) {
        const delta = payload.live_price - m.strike;
        deltaEl.textContent = fmtSignedPrice(delta);
        deltaEl.className = "delta " + (delta > 0 ? "up" : (delta < 0 ? "down" : "flat"));
      } else {
        deltaEl.textContent = "—";
        deltaEl.className = "delta flat";
      }
      volEl.textContent = fmtVolumeCompact(m.volume);

      const mp = m.market_prediction;
      if (mp) {
        const upPct = Math.max(0, Math.min(100, mp.up_prob * 100));
        const dnPct = Math.max(0, Math.min(100, mp.down_prob * 100));
        get("predbar-up").style.width = upPct + "%";
        get("predbar-down").style.width = dnPct + "%";
        get("predbar-up-lbl").textContent  = `YES ${fmtPct(mp.up_prob)}`;
        get("predbar-down-lbl").textContent = `${fmtPct(mp.down_prob)} NO`;
      } else {
        get("predbar-up").style.width = "0%";
        get("predbar-down").style.width = "0%";
        get("predbar-up-lbl").textContent = "YES —";
        get("predbar-down-lbl").textContent = "— NO";
      }
    } else {
      get("timer").textContent = "—";
      get("strike").textContent = "—";
      get("strike-delta").textContent = "—";
      get("strike-delta").className = "delta flat";
      get("market-volume").textContent = "—";
      get("predbar-up").style.width = "0%";
      get("predbar-down").style.width = "0%";
      get("predbar-up-lbl").textContent = "YES —";
      get("predbar-down-lbl").textContent = "— NO";
    }

    renderSignals(get("signals"), payload.bot_signals);
    renderTrades(get("trades"), payload.open_trades);
    renderSessionInline(get("session-inline"), payload.session);
    renderChart(get("chart"), coin, payload);
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

  function renderControls(state) {
    const controls = state.controls || {};
    const stateName = String(controls.state || "unknown").toUpperCase();
    setText($("ctrl-state"), stateName);
    setText($("ctrl-mode"), MODE_NAMES[controls.mode] || String(controls.mode || "—").toUpperCase());
    if (controls.order_size_usd == null) {
      setText($("ctrl-budget"), "—");
    } else {
      setText($("ctrl-budget"), "$" + Number(controls.order_size_usd).toFixed(0));
    }
    document.querySelectorAll('.ctl-btn[data-action="mode"]').forEach((btn) => {
      setClassList(btn, "active", btn.dataset.value === controls.mode);
    });
    document.querySelectorAll('.ctl-btn[data-action="budget"]').forEach((btn) => {
      const active = controls.order_size_usd != null && Math.abs(Number(btn.dataset.value) - Number(controls.order_size_usd)) < 0.001;
      setClassList(btn, "active", active);
    });
    setClassList($("btn-start"), "active", controls.state === "running");
    setClassList($("btn-stop"), "active", controls.state === "paused");
    setClassList($("btn-kill"), "active", controls.state === "killed");
  }

  function applyState(state) {
    if (!state) return;
    const loading = $("loading");
    if (loading) loading.remove();
    renderTopbar(state);
    renderControls(state);
    if (!state.coins) return;

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
  let lastFrameAt = 0;
  let lastUplinkClass = "";
  async function poll() {
    if (inFlight) return;
    inFlight = true;
    try {
      const res = await fetch("api/state", { cache: "no-store" });
      if (res.ok) {
        applyState(await res.json());
        lastFrameAt = performance.now();
      }
    } catch (e) {
      // Silently ignore — keep last frame visible; staleness banner will show
    } finally {
      inFlight = false;
    }
  }
  function tickUplink() {
    const uplink = document.getElementById("uplink");
    const lbl    = document.getElementById("uplink-lbl");
    const age    = document.getElementById("uplink-age");
    if (!uplink) return;
    const dt = lastFrameAt ? (performance.now() - lastFrameAt) : Infinity;
    let cls, label;
    if (dt >= DEAD_AFTER_MS)        { cls = "uplink uplink-dead";  label = "OFFLINE"; }
    else if (dt >= STALE_AFTER_MS)  { cls = "uplink uplink-stale"; label = "STALE";   }
    else                            { cls = "uplink uplink-ok";    label = "LINK";    }
    if (cls !== lastUplinkClass) { uplink.className = cls; lastUplinkClass = cls; }
    setText(lbl, label);
    setText(age, isFinite(dt) ? Math.round(dt) + "ms" : "—");
  }
  poll();
  bindControls();
  setInterval(poll, POLL_MS);
  setInterval(tickUplink, 200);
})();
</script>
</body>
</html>
"""
