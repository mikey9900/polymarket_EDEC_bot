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
        codex_manager=None,
    ):
        self.config = config
        self.tracker = tracker
        self.risk_manager = risk_manager
        self.strategy_engine = strategy_engine
        self.executor = executor
        self.session_export_fn = session_export_fn
        self.codex_manager = codex_manager
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
            "research_run_now": self.codex_manager is not None,
            "tuner_run_now": self.codex_manager is not None,
            "tuner_schedule_pause": self.codex_manager is not None,
            "tuner_schedule_resume": self.codex_manager is not None,
            "tuner_set_cadence": self.codex_manager is not None,
            "tuner_skip_next": self.codex_manager is not None,
            "tuner_promote_latest": self.codex_manager is not None,
            "tuner_reject_latest": self.codex_manager is not None,
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
        if request.action in {
            "research_run_now",
            "tuner_run_now",
            "tuner_schedule_pause",
            "tuner_schedule_resume",
            "tuner_set_cadence",
            "tuner_skip_next",
            "tuner_promote_latest",
            "tuner_reject_latest",
        }:
            if self.codex_manager is None:
                return self._record_message(
                    request,
                    ControlResult(ok=False, status=400, message="Codex automation is not configured.", action=request.action),
                )
            result = await run_blocking(lambda: self._apply_codex_action(request))
            return self._record_message(
                request,
                ControlResult(
                    ok=bool(result.get("ok", False)),
                    status=int(result.get("status", 200 if result.get("ok", False) else 400)),
                    message=str(result.get("message") or "Codex action applied."),
                    action=request.action,
                ),
            )
        if request.action != "session_export":
            return self.apply_sync(request)
        if not callable(self.session_export_fn):
            return self._record_message(
                request,
                ControlResult(ok=False, status=400, message="Session export is not configured.", action=request.action),
            )
        try:
            result = await run_blocking(self.session_export_fn)
        except Exception as exc:  # noqa: BLE001
            return self._record_message(
                request,
                ControlResult(
                    ok=False,
                    status=400,
                    message=f"Session export failed: {exc}",
                    action=request.action,
                ),
            )
        session_folder = Path(str(result.get("session_dir") or "")).name or str(
            result.get("session_folder") or ""
        ).strip()
        warning_parts: list[str] = []
        dropbox_error = str(result.get("dropbox_error") or "").strip()
        if dropbox_error:
            warning_parts.append(dropbox_error)
        github_pushes = result.get("github_pushes") or {}
        github_failures = [
            key for key, payload in github_pushes.items() if isinstance(payload, dict) and not bool(payload.get("ok"))
        ]
        if github_failures:
            warning_parts.append(f"GitHub push issues for {len(github_failures)} file(s).")
        message = (
            f"Session export ready: {int(result.get('trade_count', 0))} trades, "
            f"{int(result.get('signal_count', 0))} signals, "
            f"folder {session_folder or 'ready'}."
        )
        if warning_parts:
            message = f"{message} Warning: {' '.join(warning_parts[:2])}"
        return self._record_message(
            request,
            ControlResult(
                ok=True,
                status=200,
                message=message,
                action=request.action,
            ),
        )

    def _apply_codex_action(self, request: ControlRequest) -> dict[str, Any]:
        action = request.action
        value = request.value
        try:
            if action == "research_run_now":
                result = self.codex_manager.enqueue_daily_refresh(requested_by="dashboard")
                return {
                    "ok": True,
                    "status": 200,
                    "message": "Daily research + local tuning queued." if result.get("queued") else "Daily research + local tuning is already queued.",
                }
            if action == "tuner_run_now":
                result = self.codex_manager.enqueue_tuning_proposal(requested_by="dashboard")
                return {
                    "ok": True,
                    "status": 200,
                    "message": "Weekly desktop review bundle queued." if result.get("queued") else "Weekly desktop review bundle is already queued.",
                }
            if action == "tuner_schedule_pause":
                result = self.codex_manager.pause_tuner_schedule()
                return {"ok": True, "status": 200, "message": str(result.get("message") or "Tuning schedule paused.")}
            if action == "tuner_schedule_resume":
                result = self.codex_manager.resume_tuner_schedule()
                return {"ok": True, "status": 200, "message": str(result.get("message") or "Tuning schedule resumed.")}
            if action == "tuner_set_cadence":
                result = self.codex_manager.set_tuner_cadence(str(value or "weekly"))
                return {"ok": True, "status": 200, "message": str(result.get("message") or "Tuning cadence updated.")}
            if action == "tuner_skip_next":
                result = self.codex_manager.skip_next_tuner_run()
                return {"ok": True, "status": 200, "message": str(result.get("message") or "Next tuning run skipped.")}
            if action == "tuner_promote_latest":
                result = self.codex_manager.enqueue_promote_candidate(requested_by="dashboard")
                return {
                    "ok": True,
                    "status": 200,
                    "message": "Candidate promotion queued." if result.get("queued") else "Candidate promotion is already queued.",
                }
            if action == "tuner_reject_latest":
                result = self.codex_manager.enqueue_reject_candidate(requested_by="dashboard")
                return {
                    "ok": True,
                    "status": 200,
                    "message": "Candidate rejection queued." if result.get("queued") else "Candidate rejection is already queued.",
                }
            return {"ok": False, "status": 400, "message": f"Unknown Codex action: {action}"}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "status": 400, "message": str(exc)}
