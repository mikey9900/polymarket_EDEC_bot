"""Embedded HTTP/WebSocket API for the Home Assistant dashboard."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiohttp import WSMsgType, web

from bot.dashboard_state import DashboardStateService

logger = logging.getLogger(__name__)


class LiveApiServer:
    def __init__(
        self,
        state_service: DashboardStateService,
        *,
        host: str = "0.0.0.0",
        port: int = 8099,
        allowed_ips: set[str] | None = None,
        static_dir: str | Path | None = None,
    ):
        self.state_service = state_service
        self.host = host
        self.port = int(port)
        self.allowed_ips = set(allowed_ips or {"127.0.0.1", "::1", "172.30.32.2"})
        self.static_dir = Path(static_dir) if static_dir else Path(__file__).resolve().parent.parent / "web"
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def start(self) -> None:
        if self._runner:
            return

        app = web.Application(middlewares=[self._allowlist_middleware])
        app.router.add_get("/", self._handle_root)
        app.router.add_static("/assets", str(self.static_dir / "assets"), show_index=False)
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/api/state", self._handle_state)
        app.router.add_get("/api/ws", self._handle_ws)
        app.router.add_post("/api/actions/start", self._handle_start)
        app.router.add_post("/api/actions/stop", self._handle_stop)
        app.router.add_post("/api/actions/kill", self._handle_kill)
        app.router.add_post("/api/actions/reset-stats", self._handle_reset_stats)
        app.router.add_post("/api/actions/set-budget", self._handle_set_budget)
        app.router.add_post("/api/actions/set-capital", self._handle_set_capital)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()

        self._app = app
        self._runner = runner
        self._site = site
        logger.info("Live API listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        site = self._site
        runner = self._runner
        self._site = None
        self._runner = None
        self._app = None
        if site:
            await site.stop()
        if runner:
            await runner.cleanup()

    async def _handle_root(self, request: web.Request) -> web.Response:
        index_path = self.static_dir / "index.html"
        if not index_path.exists():
            raise web.HTTPNotFound(text="Dashboard UI is not available")
        base_path = (request.headers.get("X-Ingress-Path") or "").rstrip("/")
        html = index_path.read_text(encoding="utf-8")
        html = html.replace("__EDEC_BASE_PATH__", base_path)
        return web.Response(text=html, content_type="text/html")

    async def _handle_health(self, request: web.Request) -> web.Response:
        snapshot = await self.state_service.get_snapshot(include_history=False)
        return web.json_response({
            "ok": True,
            "generated_at": snapshot.get("generated_at"),
            "mode": snapshot.get("bot", {}).get("mode"),
        })

    async def _handle_state(self, request: web.Request) -> web.Response:
        include_history = request.query.get("history", "1").strip().lower() not in ("0", "false", "no")
        snapshot = await self.state_service.get_snapshot(include_history=include_history)
        return web.json_response(snapshot)

    async def _handle_ws(self, request: web.Request) -> web.StreamResponse:
        ws = web.WebSocketResponse(heartbeat=15.0, autoping=True)
        await ws.prepare(request)

        queue = self.state_service.register_listener()
        sender = asyncio.create_task(self._ws_sender(ws, queue), name="edec-live-api-ws-sender")
        try:
            snapshot = await self.state_service.get_snapshot(include_history=True)
            await ws.send_json({"type": "snapshot", "data": snapshot})

            async for msg in ws:
                if msg.type == WSMsgType.TEXT and msg.data == "ping":
                    await ws.send_json({"type": "pong"})
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                    break
        finally:
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
            self.state_service.unregister_listener(queue)

        return ws

    async def _ws_sender(self, ws: web.WebSocketResponse, queue: asyncio.Queue) -> None:
        while not ws.closed:
            payload = await queue.get()
            if payload is None:
                break
            await ws.send_json(payload)

    async def _handle_start(self, request: web.Request) -> web.Response:
        return await self._run_action("start", self.state_service.start_scanning)

    async def _handle_stop(self, request: web.Request) -> web.Response:
        return await self._run_action("stop", self.state_service.stop_scanning)

    async def _handle_kill(self, request: web.Request) -> web.Response:
        return await self._run_action("kill", self.state_service.activate_kill_switch)

    async def _handle_reset_stats(self, request: web.Request) -> web.Response:
        return await self._run_action("reset-stats", self.state_service.reset_stats)

    async def _handle_set_budget(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        value = self._read_number(payload, "budget")
        return await self._run_action("set-budget", self.state_service.set_budget, value)

    async def _handle_set_capital(self, request: web.Request) -> web.Response:
        payload = await self._read_json(request)
        value = self._read_number(payload, "capital")
        return await self._run_action("set-capital", self.state_service.set_capital, value)

    async def _run_action(self, action: str, fn, *args) -> web.Response:
        try:
            snapshot = await fn(*args)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        except RuntimeError as exc:
            raise web.HTTPConflict(text=str(exc)) from exc
        return web.json_response({"ok": True, "action": action, "snapshot": snapshot})

    @staticmethod
    async def _read_json(request: web.Request) -> dict:
        if request.content_length in (None, 0):
            return {}
        try:
            payload = await request.json()
        except Exception as exc:
            raise web.HTTPBadRequest(text=f"Invalid JSON body: {exc}") from exc
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON body must be an object")
        return payload

    @staticmethod
    def _read_number(payload: dict, field: str) -> float:
        raw = payload.get(field)
        if raw is None:
            raise web.HTTPBadRequest(text=f"Missing required field '{field}'")
        try:
            return float(raw)
        except Exception as exc:
            raise web.HTTPBadRequest(text=f"Field '{field}' must be numeric") from exc

    @web.middleware
    async def _allowlist_middleware(self, request: web.Request, handler):
        remote = request.remote
        if remote and remote not in self.allowed_ips:
            raise web.HTTPForbidden(text="Dashboard is only available via Home Assistant ingress")
        return await handler(request)
