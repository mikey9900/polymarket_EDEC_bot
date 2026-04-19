import asyncio
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.live_api import LiveApiServer


class _FakeDashboardState:
    def __init__(self):
        self.control_payloads = []

    def get_state_threadsafe(self):
        return {"controls": {"state": "running"}, "coins": {}, "coins_order": []}

    def get_history_threadsafe(self):
        return []

    def apply_control_threadsafe(self, payload):
        self.control_payloads.append(payload)
        return {
            "ok": True,
            "status": 200,
            "message": "Mode set to LEAD.",
            "state": {"controls": {"state": "running", "mode": "lead"}, "coins": {}, "coins_order": []},
        }


class LiveApiServerTests(unittest.TestCase):
    def test_index_html_renders_runtime_version(self):
        server = LiveApiServer(dashboard_state=object(), app_version="9.9.9")

        html = server._index_html()

        self.assertIn("v9.9.9", html)
        self.assertNotIn("__APP_VERSION__", html)
        self.assertIn('type="button" class="ctl-btn" data-action="start"', html)
        self.assertIn('data-field="session-inline"', html)
        self.assertNotIn('data-field="pred-copy"', html)
        self.assertIn('data-field="strike-delta"', html)
        self.assertIn('data-field="market-volume"', html)
        self.assertNotIn('data-field="strike-pct"', html)
        self.assertIn('class="ticker-lock"', html)
        self.assertIn('class="session-lock"', html)
        self.assertIn('class="timer-lock"', html)
        self.assertIn('chart-feeds', html)
        self.assertIn('chart-resolutions', html)
        self.assertIn('const fmtVolumeCompact = (x) =>', html)
        self.assertNotIn('class="grip"', html)
        self.assertNotIn('header.draggable = true', html)
        self.assertNotIn('const STORAGE_KEY = "edec_card_order_v1";', html)
        self.assertIn('return `<span class="chart-res-dot ${cls}"></span>`;', html)
        self.assertNotIn('data-field="strike-mini"', html)
        self.assertNotIn('data-field="strike-label"', html)
        self.assertIn('🤖 BOT STRATEGIES</h4>', html)
        self.assertNotIn('🔮 MARKET LINE', html)
        self.assertNotIn('📈 LIVE CHART', html)
        self.assertNotIn('🤖 BOT STRATEGIES (LIVE)', html)
        self.assertNotIn('title="${upper || "UNKNOWN"} | ${pnl}"', html)
        self.assertIn('const fmtSignedPrice = (x) =>', html)
        self.assertNotIn('priceEl.classList.toggle("green"', html)
        self.assertNotIn('priceEl.classList.toggle("red"', html)
        self.assertNotIn('⏱', html)
        self.assertIn('class="item wins"', html)
        self.assertIn('class="item losses"', html)
        self.assertIn('class="item open"', html)
        self.assertIn('class="item pnl"', html)
        self.assertIn('font-variant-numeric: tabular-nums;', html)
        self.assertIn('width: 96px;', html)
        self.assertIn('border-radius: 6px;', html)
        self.assertNotIn('🪙', html)
        self.assertIn('rgba(255,0,255,0.08)', html)
        self.assertIn('rgba(255,184,0,0.12)', html)
        self.assertIn('text-align: left;', html)
        self.assertIn('white-space: nowrap;', html)


class LiveApiServerHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dashboard_state = _FakeDashboardState()
        self.server = LiveApiServer(dashboard_state=self.dashboard_state, app_version="1.2.3")
        self.http_server = await asyncio.start_server(self.server._handle_client, "127.0.0.1", 0)
        sock = self.http_server.sockets[0]
        self.host, self.port = sock.getsockname()[:2]

    async def asyncTearDown(self):
        self.http_server.close()
        await self.http_server.wait_closed()

    async def _round_trip(self, request_bytes: bytes) -> tuple[str, str]:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        writer.write(request_bytes)
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        head, body = response.split(b"\r\n\r\n", 1)
        return head.decode("utf-8", errors="ignore"), body.decode("utf-8", errors="ignore")

    async def test_get_state_endpoint_returns_snapshot(self):
        head, body = await self._round_trip(
            b"GET /api/state HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )

        self.assertIn("200 OK", head)
        payload = json.loads(body)
        self.assertEqual(payload["controls"]["state"], "running")

    async def test_post_control_endpoint_routes_payload(self):
        body = json.dumps({"action": "mode", "value": "lead"}).encode("utf-8")
        request = (
            b"POST /api/control HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
            + body
        )

        head, response_body = await self._round_trip(request)

        self.assertIn("200 OK", head)
        self.assertEqual(self.dashboard_state.control_payloads, [{"action": "mode", "value": "lead"}])
        payload = json.loads(response_body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["state"]["controls"]["mode"], "lead")

    async def test_post_control_rejects_invalid_json(self):
        request = (
            b"POST /api/control HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 5\r\n\r\n"
            b"{bad}"
        )

        head, response_body = await self._round_trip(request)

        self.assertIn("400 Bad Request", head)
        payload = json.loads(response_body)
        self.assertFalse(payload["ok"])

    async def test_post_control_accepts_chunked_body(self):
        body = b'{"action":"budget","value":"15"}'
        chunked_body = (
            f"{len(body):X}\r\n".encode("utf-8")
            + body
            + b"\r\n0\r\n\r\n"
        )
        request = (
            b"POST /api/control HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
            + chunked_body
        )

        head, response_body = await self._round_trip(request)

        self.assertIn("200 OK", head)
        self.assertEqual(self.dashboard_state.control_payloads, [{"action": "budget", "value": "15"}])
        payload = json.loads(response_body)
        self.assertTrue(payload["ok"])


if __name__ == "__main__":
    unittest.main()
