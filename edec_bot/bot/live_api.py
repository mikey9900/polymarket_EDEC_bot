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
        return """<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <title>EDEC Dashboard API</title>
    <style>
      body { font-family: sans-serif; margin: 2rem; color: #222; }
      code { background: #f5f5f5; padding: 0.2rem 0.4rem; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>EDEC Dashboard API</h1>
    <p>Ingress is healthy.</p>
    <ul>
      <li><code>/health</code></li>
      <li><code>/api/state</code></li>
      <li><code>/api/history</code></li>
    </ul>
  </body>
</html>
"""
