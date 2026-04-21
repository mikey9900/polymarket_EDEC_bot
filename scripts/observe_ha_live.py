#!/usr/bin/env python
"""Capture live HA EDEC bot state for later analysis.

This script talks to Home Assistant over the standard websocket API to:

1. Discover the ingress URL for the requested add-on slug.
2. Mint an ingress session token.
3. Poll the add-on's read-only state/history endpoints on a fixed cadence.
4. Persist raw snapshots plus derived events for later review.

No secrets are written to disk. Set the Home Assistant token in an env var.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import websockets


DEFAULT_ADDON_SLUG = "7be6395f_edec_bot"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Event:
    ts: str
    event_type: str
    coin: str | None
    details: dict[str, Any]


class HAIngressClient:
    def __init__(self, ha_url: str, token: str, addon_slug: str):
        self.ha_url = ha_url.rstrip("/")
        self.token = token
        self.addon_slug = addon_slug
        self.ws_url = self._to_ws_url(self.ha_url) + "/api/websocket"
        self.ingress_url_path: str | None = None
        self.ingress_session: str | None = None
        self._ws_msg_id = 0
        self._http = httpx.Client(base_url=self.ha_url, timeout=10.0)

    @staticmethod
    def _to_ws_url(base_url: str) -> str:
        if base_url.startswith("https://"):
            return "wss://" + base_url[len("https://") :]
        if base_url.startswith("http://"):
            return "ws://" + base_url[len("http://") :]
        raise ValueError(f"Unsupported HA URL: {base_url}")

    async def _ws_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        async with websockets.connect(self.ws_url, open_timeout=10) as ws:
            hello = json.loads(await ws.recv())
            if hello.get("type") != "auth_required":
                raise RuntimeError(f"Unexpected websocket hello: {hello}")
            await ws.send(json.dumps({"type": "auth", "access_token": self.token}))
            auth_resp = json.loads(await ws.recv())
            if auth_resp.get("type") != "auth_ok":
                raise RuntimeError(f"HA websocket auth failed: {auth_resp}")
            self._ws_msg_id += 1
            payload = dict(payload)
            payload["id"] = self._ws_msg_id
            await ws.send(json.dumps(payload))
            return json.loads(await ws.recv())

    async def resolve_ingress(self) -> str:
        if self.ingress_url_path:
            return self.ingress_url_path
        resp = await self._ws_command(
            {
                "type": "supervisor/api",
                "method": "get",
                "endpoint": f"/addons/{self.addon_slug}/info",
            }
        )
        if not resp.get("success"):
            raise RuntimeError(f"Unable to resolve add-on info: {resp}")
        info = resp["result"]
        ingress_url = str(info.get("ingress_url") or "").strip()
        if not ingress_url:
            raise RuntimeError(f"Add-on {self.addon_slug} has no ingress URL")
        self.ingress_url_path = ingress_url.rstrip("/")
        return self.ingress_url_path

    async def renew_ingress_session(self) -> str:
        resp = await self._ws_command(
            {
                "type": "supervisor/api",
                "method": "post",
                "endpoint": "/ingress/session",
            }
        )
        if not resp.get("success"):
            raise RuntimeError(f"Unable to mint ingress session: {resp}")
        session = str(resp["result"]["session"]).strip()
        if not session:
            raise RuntimeError("Supervisor returned an empty ingress session")
        self.ingress_session = session
        return session

    def _cookies(self) -> dict[str, str]:
        if not self.ingress_session:
            raise RuntimeError("Ingress session is not initialized")
        return {"ingress_session": self.ingress_session}

    def _ingress_path(self, suffix: str) -> str:
        if not self.ingress_url_path:
            raise RuntimeError("Ingress URL path is not initialized")
        suffix = suffix if suffix.startswith("/") else f"/{suffix}"
        return f"{self.ingress_url_path}{suffix}"

    def get_json(self, suffix: str) -> Any:
        url = self._ingress_path(suffix)
        resp = self._http.get(url, cookies=self._cookies())
        if resp.status_code == 401:
            raise PermissionError("Ingress session expired")
        resp.raise_for_status()
        return resp.json()

    def post_json(self, suffix: str, payload: dict[str, Any]) -> Any:
        url = self._ingress_path(suffix)
        resp = self._http.post(url, json=payload, cookies=self._cookies())
        if resp.status_code == 401:
            raise PermissionError("Ingress session expired")
        resp.raise_for_status()
        return resp.json()

    async def request_json(self, suffix: str, *, method: str = "get", payload: dict[str, Any] | None = None) -> Any:
        for _ in range(2):
            try:
                if method == "get":
                    return self.get_json(suffix)
                return self.post_json(suffix, payload or {})
            except PermissionError:
                await self.renew_ingress_session()
        raise RuntimeError(f"Unable to call ingress endpoint {suffix!r} after session renewal")


def signal_key(signal: dict[str, Any]) -> tuple[Any, ...]:
    return (
        signal.get("strategy"),
        signal.get("action"),
        signal.get("side"),
        signal.get("entry_price"),
        signal.get("target_price"),
        round(float(signal.get("score") or 0.0), 2),
    )


def trade_key(trade: dict[str, Any]) -> tuple[Any, ...]:
    return (
        trade.get("strategy"),
        trade.get("side"),
        trade.get("entry_price"),
        trade.get("target_price"),
        trade.get("shares"),
        trade.get("hold_to_resolution"),
    )


def coin_snapshot_digest(coin_payload: dict[str, Any]) -> dict[str, Any]:
    market = coin_payload.get("market") or {}
    sources = coin_payload.get("sources") or {}
    return {
        "live_price": coin_payload.get("live_price"),
        "market_slug": market.get("slug"),
        "strike": market.get("strike"),
        "time_remaining_s": market.get("time_remaining_s"),
        "up_bid": market.get("up_bid"),
        "up_ask": market.get("up_ask"),
        "down_bid": market.get("down_bid"),
        "down_ask": market.get("down_ask"),
        "source_count": sources.get("count"),
        "expected_sources": sources.get("expected"),
        "signals": [signal_key(s) for s in (coin_payload.get("bot_signals") or [])],
        "open_trades": [trade_key(t) for t in (coin_payload.get("open_trades") or [])],
        "session": coin_payload.get("session"),
    }


def emit_events(prev_state: dict[str, Any] | None, state: dict[str, Any]) -> list[Event]:
    events: list[Event] = []
    ts = str(state.get("timestamp_utc") or utc_now())

    if prev_state is None:
        events.append(Event(ts=ts, event_type="observation_started", coin=None, details={}))
        return events

    prev_summary = ((prev_state.get("summary") or {}).get("paper") or {})
    curr_summary = ((state.get("summary") or {}).get("paper") or {})
    if prev_summary != curr_summary:
        events.append(
            Event(
                ts=ts,
                event_type="paper_summary_changed",
                coin=None,
                details={"previous": prev_summary, "current": curr_summary},
            )
        )

    prev_controls = prev_state.get("controls") or {}
    curr_controls = state.get("controls") or {}
    if prev_controls != curr_controls:
        events.append(
            Event(
                ts=ts,
                event_type="controls_changed",
                coin=None,
                details={"previous": prev_controls, "current": curr_controls},
            )
        )

    prev_coins = prev_state.get("coins") or {}
    curr_coins = state.get("coins") or {}
    for coin, curr_payload in curr_coins.items():
        prev_payload = prev_coins.get(coin) or {}
        prev_digest = coin_snapshot_digest(prev_payload) if prev_payload else {}
        curr_digest = coin_snapshot_digest(curr_payload)

        if prev_digest.get("market_slug") != curr_digest.get("market_slug"):
            events.append(
                Event(
                    ts=ts,
                    event_type="market_window_changed",
                    coin=coin,
                    details={
                        "previous": prev_digest.get("market_slug"),
                        "current": curr_digest.get("market_slug"),
                        "strike": curr_digest.get("strike"),
                    },
                )
            )

        if prev_digest.get("source_count") != curr_digest.get("source_count"):
            events.append(
                Event(
                    ts=ts,
                    event_type="source_count_changed",
                    coin=coin,
                    details={
                        "previous": prev_digest.get("source_count"),
                        "current": curr_digest.get("source_count"),
                        "expected": curr_digest.get("expected_sources"),
                    },
                )
            )

        if prev_digest.get("signals") != curr_digest.get("signals"):
            events.append(
                Event(
                    ts=ts,
                    event_type="signals_changed",
                    coin=coin,
                    details={
                        "previous": prev_payload.get("bot_signals") or [],
                        "current": curr_payload.get("bot_signals") or [],
                    },
                )
            )

        if prev_digest.get("open_trades") != curr_digest.get("open_trades"):
            events.append(
                Event(
                    ts=ts,
                    event_type="open_trades_changed",
                    coin=coin,
                    details={
                        "previous": prev_payload.get("open_trades") or [],
                        "current": curr_payload.get("open_trades") or [],
                    },
                )
            )
    return events


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: Any) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


async def observe(args: argparse.Namespace) -> int:
    token = os.getenv(args.token_env)
    if not token:
        raise RuntimeError(f"Missing Home Assistant token in env var {args.token_env}")

    started = datetime.now(timezone.utc)
    run_dir = Path(args.output_dir) / started.strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    snapshots_path = run_dir / "state_snapshots.jsonl"
    history_path = run_dir / "history_snapshots.jsonl"
    events_path = run_dir / "events.jsonl"

    client = HAIngressClient(args.ha_url, token, args.addon_slug)
    ingress_path = await client.resolve_ingress()
    await client.renew_ingress_session()

    metadata = {
        "started_at_utc": started.isoformat(),
        "ha_url": args.ha_url,
        "addon_slug": args.addon_slug,
        "ingress_path": ingress_path,
        "poll_interval_s": args.poll_interval,
        "history_interval_s": args.history_interval,
        "duration_s": args.duration,
        "trigger_session_export_at_end": bool(args.trigger_session_export_at_end),
    }
    write_json(run_dir / "metadata.json", metadata)

    prev_state: dict[str, Any] | None = None
    next_history_at = 0.0
    deadline = time.monotonic() + args.duration
    poll_count = 0

    while time.monotonic() < deadline:
        state = await client.request_json("/api/state")
        append_jsonl(snapshots_path, state)
        for event in emit_events(prev_state, state):
            append_jsonl(events_path, asdict(event))
        prev_state = state
        poll_count += 1

        now = time.monotonic()
        if now >= next_history_at:
            history = await client.request_json("/api/history")
            append_jsonl(
                history_path,
                {
                    "captured_at_utc": utc_now(),
                    "history_points": len(history) if isinstance(history, list) else None,
                    "history": history,
                },
            )
            next_history_at = now + args.history_interval

        sleep_s = max(0.0, args.poll_interval - (time.monotonic() - now))
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)

    export_result: dict[str, Any] | None = None
    if args.trigger_session_export_at_end:
        try:
            export_result = await client.request_json(
                "/api/control",
                method="post",
                payload={"action": "session_export"},
            )
        except Exception as exc:  # pragma: no cover - best-effort cleanup
            export_result = {"ok": False, "message": f"session_export_failed: {exc}"}
        write_json(run_dir / "session_export_result.json", export_result)

    completed = datetime.now(timezone.utc)
    summary = {
        "started_at_utc": started.isoformat(),
        "completed_at_utc": completed.isoformat(),
        "duration_s": (completed - started).total_seconds(),
        "poll_count": poll_count,
        "snapshot_file": str(snapshots_path),
        "history_file": str(history_path),
        "events_file": str(events_path),
        "session_export_result": export_result,
    }
    write_json(run_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ha-url", required=True, help="Home Assistant base URL, e.g. http://192.168.1.83:8123")
    parser.add_argument("--addon-slug", default=DEFAULT_ADDON_SLUG, help="Supervisor add-on slug to observe")
    parser.add_argument("--token-env", default="HA_TOKEN", help="Env var containing the Home Assistant long-lived token")
    parser.add_argument("--output-dir", default="data/live_observation", help="Directory for observation outputs")
    parser.add_argument("--duration", type=int, default=3600, help="Observation duration in seconds")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between /api/state polls")
    parser.add_argument("--history-interval", type=float, default=30.0, help="Seconds between /api/history captures")
    parser.add_argument(
        "--trigger-session-export-at-end",
        action="store_true",
        help="Call the add-on's session export control when observation finishes",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(observe(args))
    except KeyboardInterrupt:
        print("observation interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
