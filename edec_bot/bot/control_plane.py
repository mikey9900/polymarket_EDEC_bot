"""Shared dashboard/backup control surface."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable


CONTROL_MODE_ORDER = ("both", "dual", "single", "lead", "swing", "off")
CONTROL_BUDGET_OPTIONS = (1, 2, 5, 10, 15, 20, 50, 100)
CONTROL_REQUEST_TIMEOUT_S = 180.0

RunBlocking = Callable[[Callable[[], Any]], Awaitable[Any]]


@dataclass(frozen=True)
class ControlRequest:
    action: str
    value: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", str(self.action or "").strip().lower())

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "ControlRequest":
        payload = payload or {}
        return cls(payload.get("action", ""), payload.get("value"))


@dataclass
class ControlResult:
    ok: bool
    status: int
    message: str
    action: str
    state: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "ok": bool(self.ok),
            "status": int(self.status),
            "message": str(self.message),
        }
        if self.state is not None:
            payload["state"] = self.state
        return payload


class ControlPlane:
    """Applies operator controls across dashboard and backup surfaces."""

    def __init__(
        self,
        *,
        config,
        tracker,
        risk_manager,
        strategy_engine,
        executor,
        session_export_fn=None,
    ):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.session_export_fn = session_export_fn
        self._last_control: dict[str, Any] = {
            "action": None,
            "ok": None,
            "message": "CONTROL LINK STANDBY",
            "timestamp_utc": None,
        }

    def last_control(self) -> dict[str, Any]:
        return dict(self._last_control)

    def available_actions(self) -> dict[str, bool]:
        return {
            "start": bool(self.strategy_engine or self.risk_manager),
            "stop": bool(self.strategy_engine or self.risk_manager),
            "kill": bool(self.strategy_engine or self.risk_manager),
            "mode": bool(self.strategy_engine),
            "budget": bool(self.executor),
            "reset_stats": bool(self.tracker or self.risk_manager),
            "session_export": callable(self.session_export_fn),
        }

    def _current_state_name(self) -> str:
        risk_status = self.risk_manager.get_status() if self.risk_manager else {}
        kill_switch = bool(risk_status.get("kill_switch", False))
        paused = bool(risk_status.get("paused", False))
        scanning = bool(getattr(self.strategy_engine, "is_active", False))
        if kill_switch:
            return "killed"
        if paused or not scanning:
            return "paused"
        return "running"

    def build_controls_payload(self) -> dict[str, Any]:
        risk_status = self.risk_manager.get_status() if self.risk_manager else {}
        order_size = (
            float(self.executor.order_size_usd)
            if self.executor is not None
            else float(self.config.execution.order_size_usd)
        )
        last_control = self.last_control()
        return {
            "state": self._current_state_name(),
            "kill_switch": bool(risk_status.get("kill_switch", False)),
            "paused": bool(risk_status.get("paused", False)),
            "scanning": bool(getattr(self.strategy_engine, "is_active", False)),
            "mode": getattr(self.strategy_engine, "mode", "unknown"),
            "order_size_usd": order_size,
            "available_actions": self.available_actions(),
            "last_message": last_control.get("message"),
            "last_ok": last_control.get("ok"),
        }

    def _record_message(self, request: ControlRequest, result: ControlResult) -> ControlResult:
        from datetime import datetime, timezone

        self._last_control = {
            "action": request.action,
            "ok": result.ok,
            "message": result.message,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        return result

    def unavailable(self, action: str, message: str, *, status: int = 400) -> ControlResult:
        return self._record_message(
            ControlRequest(action),
            ControlResult(ok=False, status=status, message=message, action=action),
        )

    def apply_sync(self, request: ControlRequest) -> ControlResult:
        action = request.action
        value = request.value
        if action == "start":
            if self.strategy_engine:
                self.strategy_engine.start_scanning()
            if self.risk_manager:
                self.risk_manager.resume()
                self.risk_manager.deactivate_kill_switch()
            return self._record_message(
                request,
                ControlResult(ok=True, status=200, message="Scanning started.", action=action),
            )
        if action == "stop":
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            if self.risk_manager:
                self.risk_manager.pause()
            return self._record_message(
                request,
                ControlResult(ok=True, status=200, message="Trading paused.", action=action),
            )
        if action == "kill":
            if self.strategy_engine:
                self.strategy_engine.stop_scanning()
            if self.risk_manager:
                self.risk_manager.activate_kill_switch("Manual kill via HA dashboard")
            return self._record_message(
                request,
                ControlResult(ok=True, status=200, message="Kill switch activated.", action=action),
            )
        if action == "mode":
            mode = str(value or "").strip().lower()
            if mode not in CONTROL_MODE_ORDER:
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message=f"Unknown mode: {mode or 'empty'}", action=action),
                )
            if not self.strategy_engine or not self.strategy_engine.set_mode(mode):
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message="Strategy engine unavailable.", action=action),
                )
            return self._record_message(
                request,
                ControlResult(ok=True, status=200, message=f"Mode set to {mode.upper()}.", action=action),
            )
        if action == "budget":
            try:
                amount = float(value)
            except (TypeError, ValueError):
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message="Budget must be a number.", action=action),
                )
            if amount <= 0:
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message="Budget must be positive.", action=action),
                )
            if not self.executor:
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message="Execution engine unavailable.", action=action),
                )
            self.executor.set_order_size(amount)
            return self._record_message(
                request,
                ControlResult(ok=True, status=200, message=f"Budget set to ${amount:.0f} per trade.", action=action),
            )
        if action == "reset_stats":
            did_reset = False
            if self.tracker and hasattr(self.tracker, "reset_paper_stats"):
                self.tracker.reset_paper_stats()
                did_reset = True
            if self.risk_manager and hasattr(self.risk_manager, "reset_daily_stats"):
                self.risk_manager.reset_daily_stats()
                did_reset = True
            if not did_reset:
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message="Reset controls are unavailable.", action=action),
                )
            return self._record_message(
                request,
                ControlResult(ok=True, status=200, message="Paper stats and daily risk reset.", action=action),
            )
        return self._record_message(
            request,
            ControlResult(ok=False, status=400, message=f"Unknown action: {action or 'empty'}", action=action),
        )

    async def apply_async(self, request: ControlRequest, run_blocking: RunBlocking) -> ControlResult:
        if request.action != "session_export":
            return self.apply_sync(request)
        if not callable(self.session_export_fn):
            return self._record_message(
                request,
                ControlResult(ok=False, status=400, message="Session export is not configured.", action=request.action),
            )
        result = await run_blocking(self.session_export_fn)
        session_folder = Path(str(result.get("session_dir") or "")).name or str(
            result.get("session_folder") or ""
        ).strip()
        return self._record_message(
            request,
            ControlResult(
                ok=True,
                status=200,
                message=(
                    f"Session export ready: {int(result.get('trade_count', 0))} trades, "
                    f"{int(result.get('signal_count', 0))} signals, "
                    f"folder {session_folder or 'ready'}."
                ),
                action=request.action,
            ),
        )
