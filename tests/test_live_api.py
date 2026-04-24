import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.live_api import LiveApiServer


class _FakeDashboardState:
    def __init__(self):
        self.control_payloads = []

    def get_state_threadsafe(self):
        return {
            "controls": {
                "state": "running",
                "available_actions": {
                    "start": True,
                    "research_run_now": True,
                    "research_reset_runner": True,
                    "research_set_proposal_aggressiveness": True,
                    "research_set_live_aggressiveness": True,
                    "tuner_run_now": True,
                },
                "last_message": "CONTROL LINK STANDBY",
                "last_ok": None,
            },
            "codex": {
                "queue_depth": 0,
                "active_run": None,
                "healthy": True,
                "last_heartbeat_at": "2026-04-22T12:00:00+00:00",
                "last_run": {
                    "ok": True,
                    "summary": "Daily refresh built 10 clusters; warehouse fetched 42 fills; local candidate ready.",
                    "finished_at": "2026-04-22T11:59:00+00:00",
                },
                "next_queued_job": None,
                "daily_research_metrics": {
                    "run_id": "run-1",
                    "finished_at": "2026-04-22T11:59:00+00:00",
                    "ok": True,
                    "cluster_count": 10,
                    "outcome_count": 48,
                    "fill_flow_rows": 3,
                    "fetched_fill_count": 42,
                    "inserted_fill_count": 42,
                    "recent_window_count": 2,
                    "recent_asset_count": 4,
                    "recent_fetched_fill_count": 8,
                    "recent_inserted_fill_count": 8,
                    "history_window_count": 6,
                    "history_asset_count": 12,
                    "history_fetched_fill_count": 34,
                    "history_inserted_fill_count": 34,
                    "fills_enriched_rows": 15,
                    "market_5m_registry_rows": 2070,
                    "candidate_status": "ready",
                },
                "daily_local_candidate": {
                    "candidate_id": "local-1",
                    "status": "ready",
                    "summary": "Daily local candidate ready.",
                    "paths": {"report_json": "data/research/tuner_report.json"},
                },
                "daily_local_candidate_details": {
                    "candidate_id": "local-1",
                    "status": "ready",
                    "summary": "2 config changes proposed from 48 closed trades.",
                    "generated_at": "2026-04-22T12:15:00+00:00",
                    "change_count": 2,
                    "top_changes": ["single_leg.min_velocity_30s", "single_leg.entry_min"],
                    "data": {"closed": 48, "win_pct": 54.2, "total_pnl": 3.15},
                    "changes": [
                        {
                            "path": "single_leg.min_velocity_30s",
                            "current": 0.12,
                            "recommended": 0.15,
                            "evidence": "Velocity buckets improved above 0.15.",
                        }
                    ],
                    "advisories": ["depth_check is rejecting too many signals."],
                    "no_change": ["single_leg.entry_max already fits the viable band."],
                },
                "research_controls": {
                    "proposal_aggressiveness_level": 5,
                    "live_aggressiveness_level": 5,
                    "updated_at": "2026-04-22T12:05:00+00:00",
                    "updated_by": "dashboard",
                },
            },
            "tuner": {
                "cadence": "weekly",
                "candidate_status": "ready",
                "schedule_enabled": True,
                "primary_candidate_source": "weekly_ai",
                "daily_research_metrics": {
                    "run_id": "run-1",
                    "finished_at": "2026-04-22T11:59:00+00:00",
                    "ok": True,
                    "cluster_count": 10,
                    "outcome_count": 48,
                    "fill_flow_rows": 3,
                    "fetched_fill_count": 42,
                    "inserted_fill_count": 42,
                    "recent_window_count": 2,
                    "recent_asset_count": 4,
                    "recent_fetched_fill_count": 8,
                    "recent_inserted_fill_count": 8,
                    "history_window_count": 6,
                    "history_asset_count": 12,
                    "history_fetched_fill_count": 34,
                    "history_inserted_fill_count": 34,
                    "fills_enriched_rows": 15,
                    "market_5m_registry_rows": 2070,
                    "candidate_status": "ready",
                },
                "daily_local_candidate": {"status": "ready"},
                "weekly_ai_candidate": {"status": "ready"},
                "weekly_review_bundle": {
                    "status": "ready",
                    "paths": {
                        "desktop_prompt": "data/research/weekly_desktop_prompt.txt",
                        "bundle_md": "data/research/weekly_review_bundle.md",
                    },
                },
                "daily_local_last_result": "success",
                "weekly_ai_last_result": "success",
                "candidate_summary": "Weekly AI candidate ready.",
            },
            "research_runtime": {
                "artifact_path": "data/research/runtime_policy.json",
                "artifact_exists": True,
                "last_loaded_at": "2026-04-22T12:01:00+00:00",
                "last_source_modified_at": "2026-04-22T12:00:55+00:00",
                "reload_count": 2,
                "cluster_count": 12,
                "coin_feature_count": 5,
                "last_error": None,
            },
            "coins": {},
            "coins_order": [],
        }

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
        self.assertIn('data-action="reset_stats"', html)
        self.assertIn('data-action="session_export"', html)
        self.assertIn('data-action="research_run_now"', html)
        self.assertIn('data-action="research_reset_runner"', html)
        self.assertIn('data-action="research_set_proposal_aggressiveness" data-value="10"', html)
        self.assertIn('data-action="research_set_live_aggressiveness" data-value="10"', html)
        self.assertIn('data-action="tuner_run_now"', html)
        self.assertIn('data-action="tuner_schedule_pause"', html)
        self.assertIn('data-action="tuner_schedule_resume"', html)
        self.assertIn('data-action="tuner_set_cadence" data-value="weekly"', html)
        self.assertIn('data-action="tuner_set_cadence" data-value="manual"', html)
        self.assertIn('CLEAR STATS', html)
        self.assertIn('EXPORT SESSION', html)
        self.assertIn('RUN RESEARCH', html)
        self.assertIn('STOP RESEARCH', html)
        self.assertIn('VIEW CHANGES', html)
        self.assertIn('PROMOTE', html)
        self.assertIn('REJECT', html)
        self.assertIn('class="topbar-meta"', html)
        self.assertIn('class="session-summary-strip"', html)
        self.assertIn('id="t-mode-top"', html)
        self.assertIn('data-action="budget" data-value="50"', html)
        self.assertIn('data-action="budget" data-value="100"', html)
        self.assertIn("SESSION EXPORT", html)
        self.assertIn("function apiUrl(path)", html)
        self.assertIn('fetch(apiUrl("api/state"), { cache: "no-store" });', html)
        self.assertIn('fetch(apiUrl("api/control"), {', html)
        self.assertNotIn('data-action="export_today"', html)
        self.assertNotIn('data-action="export_all"', html)
        self.assertNotIn('data-action="export_recent"', html)
        self.assertNotIn('data-action="archive_now"', html)
        self.assertNotIn('data-action="archive_latest"', html)
        self.assertNotIn('data-action="archive_health"', html)
        self.assertNotIn('data-action="sync_repo_latest"', html)
        self.assertNotIn('data-action="fetch_github"', html)
        self.assertNotIn("ARCHIVE + SYNC", html)
        self.assertIn("function syncControlButtons(controls)", html)
        self.assertIn('controls.last_message || "CONTROL LINK STANDBY"', html)
        self.assertIn('setClassList(btn, "unavailable", !enabled);', html)
        self.assertIn('id="codex-queue"', html)
        self.assertIn('id="codex-active"', html)
        self.assertIn('id="codex-led-cluster"', html)
        self.assertIn('id="codex-note"', html)
        self.assertIn('id="codex-meta"', html)
        self.assertIn('id="codex-warehouse"', html)
        self.assertIn('data-modal-close="research"', html)
        self.assertNotIn('data-action="tuner_skip_next"', html)
        self.assertNotIn('data-action="tuner_promote_latest"', html)
        self.assertNotIn('data-action="tuner_reject_latest"', html)
        self.assertIn('id="codex-live"', html)
        self.assertIn('id="proposal-aggr-slider"', html)
        self.assertIn('id="live-aggr-slider"', html)
        self.assertIn('id="research-modal-overlay"', html)
        self.assertIn('id="research-modal-changes"', html)
        self.assertIn('id="tuner-cadence"', html)
        self.assertIn('id="tuner-next"', html)
        self.assertIn('id="tuner-primary"', html)
        self.assertIn('id="tuner-daily"', html)
        self.assertIn('id="tuner-weekly"', html)
        self.assertIn('id="tuner-candidate"', html)
        self.assertIn('id="tuner-desktop-note"', html)
        self.assertIn('id="tuner-desktop-path"', html)
        self.assertIn("function apiUrl(path)", html)
        self.assertIn("const baseName = (value) =>", html)
        self.assertIn("function describeCodexRunner(codex)", html)
        self.assertIn("function describeResearchRuntime(runtime)", html)
        self.assertIn("function describeResearchWarehouse(codex, runtime)", html)
        self.assertIn("function renderResearchModal(codex)", html)
        self.assertIn("function renderResearchSlider(groupId, action, level, warn)", html)
        self.assertIn("RUNNER HEARTBEAT STALE DURING", html)
        self.assertIn("pulseCodexLed(codexLedCluster, codexStatus.pulseToken);", html)
        self.assertIn("WAREHOUSE USED", html)
        self.assertIn("0 RECENT 5M WINDOWS", html)
        self.assertIn("REGISTRY", html)
        self.assertIn("GOLDSKY RETURNED 0 FILLS", html)
        self.assertIn("ENRICHED TOTAL", html)
        self.assertIn("DESKTOP REVIEW READY: OPEN", html)
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
        self.assertNotIn('.tape .seg .traded', html)
        self.assertIn('return `<span class="chart-res-dot ${cls}"></span>`;', html)
        self.assertNotIn('class="chart-meta"', html)
        self.assertNotIn('data-r="hi"', html)
        self.assertNotIn('data-r="lo"', html)
        self.assertNotIn('data-r="stk"', html)
        self.assertNotIn('data-r="now"', html)
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
        self.assertIn('.topbar-meta .pill { display: none; }', html)
        self.assertIn('.session-pill .val.green', html)
        self.assertIn('color: #ffe3a6;', html)
        self.assertIn('text-align: left;', html)
        self.assertIn('white-space: nowrap;', html)
        self.assertNotIn('.control-block.span-2 { grid-column: span 2; }', html)

    def test_threaded_server_stops_cleanly_without_error_log(self):
        server = LiveApiServer(dashboard_state=_FakeDashboardState(), host="127.0.0.1", port=0)

        with mock.patch("bot.live_api.logger.error") as mock_error:
            server.start_threaded()
            self.assertTrue(server._ready.wait(timeout=2.0))
            self.assertIsNotNone(server._thread)
            self.assertTrue(server._thread.is_alive())

            server.stop_threaded()

        self.assertIsNone(server._thread)
        self.assertIsNone(server._loop)
        self.assertIsNone(server._server)
        mock_error.assert_not_called()


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

    async def test_post_control_timeout_uses_gateway_timeout_reason(self):
        class _TimeoutDashboardState(_FakeDashboardState):
            def apply_control_threadsafe(self, payload):
                self.control_payloads.append(payload)
                return {"ok": False, "status": 504, "message": "Dashboard control request timed out."}

        dashboard_state = _TimeoutDashboardState()
        server = LiveApiServer(dashboard_state=dashboard_state, app_version="1.2.3")
        http_server = await asyncio.start_server(server._handle_client, "127.0.0.1", 0)
        sock = http_server.sockets[0]
        host, port = sock.getsockname()[:2]
        try:
            reader, writer = await asyncio.open_connection(host, port)
            body = json.dumps({"action": "session_export"}).encode("utf-8")
            request = (
                b"POST /api/control HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
                + body
            )
            writer.write(request)
            await writer.drain()
            response = await reader.read()
            writer.close()
            await writer.wait_closed()
        finally:
            http_server.close()
            await http_server.wait_closed()

        head, response_body = response.split(b"\r\n\r\n", 1)
        self.assertIn("504 Gateway Timeout", head.decode("utf-8", errors="ignore"))
        payload = json.loads(response_body.decode("utf-8", errors="ignore"))
        self.assertFalse(payload["ok"])


if __name__ == "__main__":
    unittest.main()
