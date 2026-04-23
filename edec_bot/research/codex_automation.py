"""Shared filesystem orchestration for HA-local Codex jobs."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from .artifacts import build_artifacts
from .paths import (
    CODEX_LATEST_PATH,
    CODEX_LOCK_PATH,
    CODEX_QUEUE_ROOT,
    CODEX_RUNS_ROOT,
    CODEX_STATE_PATH,
    DEFAULT_CONFIG_PATH,
    LOCAL_TRACKER_DB,
    SHARED_DATA_ROOT,
    WAREHOUSE_PATH,
    ensure_codex_dirs,
    resolve_repo_path,
)
from .sources import GammaMarketSource, GoldskyFillSource
from .sync import sync_recent_5m_fills, sync_recent_markets
from .tuner import (
    TuningError,
    build_weekly_ai_context,
    build_weekly_review_bundle,
    load_tuner_state,
    maybe_run_tuner_heartbeat,
    promote_tuning_candidate,
    propose_tuning,
    reject_tuning_candidate,
)
from .warehouse import ResearchWarehouse


JOB_TYPES = {
    "daily_research_refresh",
    "tuning_proposal",
    "promote_candidate",
    "reject_candidate",
    "repo_task",
}
DEFAULT_TUNER_REASON = "Rejected by operator."
STALE_RUNNER_LOCK_SECONDS = 600
STALE_ACTIVE_RUN_SECONDS = 1800
SCHEDULE_DEFAULTS = {
    "daily_research_refresh": {
        "schedule_enabled": True,
        "cadence": "daily",
        "hour_local": 6,
        "minute_local": 15,
        "day_of_week": None,
    },
    "tuning_proposal": {
        "schedule_enabled": True,
        "cadence": "weekly",
        "hour_local": 6,
        "minute_local": 30,
        "day_of_week": "monday",
        "skip_next_auto_run": False,
    },
}


class CodexAutomationManager:
    """Coordinates queue-backed research and tuning jobs for HA."""

    def __init__(
        self,
        *,
        state_path: str | Path = CODEX_STATE_PATH,
        latest_path: str | Path = CODEX_LATEST_PATH,
        queue_root: str | Path = CODEX_QUEUE_ROOT,
        runs_root: str | Path = CODEX_RUNS_ROOT,
        lock_path: str | Path = CODEX_LOCK_PATH,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        tuner_state_path: str | Path | None = None,
    ) -> None:
        self.state_path = resolve_repo_path(state_path)
        self.latest_path = resolve_repo_path(latest_path)
        self.queue_root = resolve_repo_path(queue_root)
        self.runs_root = resolve_repo_path(runs_root)
        self.lock_path = resolve_repo_path(lock_path)
        self.config_path = resolve_repo_path(config_path)
        self.tuner_state_path = resolve_repo_path(tuner_state_path) if tuner_state_path is not None else None
        ensure_codex_dirs()
        for path in (self.state_path.parent, self.queue_root, self.runs_root, self.lock_path.parent, self.latest_path.parent):
            path.mkdir(parents=True, exist_ok=True)

    def read_state(self) -> dict[str, Any]:
        state = self._default_state()
        if self.state_path.exists():
            try:
                payload = json.loads(self.state_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    state = self._merge_dicts(state, payload)
            except json.JSONDecodeError:
                pass
        state = self._normalize_state(state)
        self._sync_tuner_state(state)
        return state

    def save_state(self, state: dict[str, Any]) -> Path:
        state = self._normalize_state(state)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        return self.state_path

    def snapshot(self) -> dict[str, Any]:
        state = self.read_state()
        runner = dict(state.get("runner") or {})
        active_run = state.get("active_run")
        last_run = state.get("last_run")
        next_queued_job = self._next_queued_job_payload()
        daily_research_metrics = self._latest_daily_refresh_metrics()
        tuner_schedule = dict((state.get("schedules") or {}).get("tuning_proposal") or {})
        latest_candidate = dict(state.get("latest_candidate") or {})
        daily_local_candidate = dict(state.get("daily_local_candidate") or {})
        weekly_ai_candidate = dict(state.get("weekly_ai_candidate") or {})
        weekly_review_bundle = dict(state.get("weekly_review_bundle") or {})
        return {
            "codex": {
                "healthy": bool(runner.get("healthy", False)),
                "last_heartbeat_at": runner.get("last_heartbeat_at"),
                "queue_depth": self.queue_depth(),
                "active_run": active_run,
                "last_run": last_run,
                "next_queued_job": next_queued_job,
                "daily_research_metrics": daily_research_metrics,
                "latest_candidate": latest_candidate,
                "daily_local_candidate": daily_local_candidate,
                "weekly_ai_candidate": weekly_ai_candidate,
                "weekly_review_bundle": weekly_review_bundle,
                "primary_candidate_source": state.get("primary_candidate_source", "none"),
            },
            "tuner": {
                "running": bool(active_run and active_run.get("job_type") == "tuning_proposal"),
                "schedule_enabled": bool(tuner_schedule.get("schedule_enabled", True)),
                "cadence": tuner_schedule.get("cadence", "weekly"),
                "skip_next_auto_run": bool(tuner_schedule.get("skip_next_auto_run", False)),
                "next_auto_run_at": tuner_schedule.get("next_auto_run_at"),
                "last_run_at": tuner_schedule.get("last_run_at"),
                "last_result": tuner_schedule.get("last_result"),
                "daily_research_metrics": daily_research_metrics,
                "daily_local_last_run_at": ((state.get("schedules") or {}).get("daily_research_refresh") or {}).get("last_run_at"),
                "daily_local_last_result": ((state.get("schedules") or {}).get("daily_research_refresh") or {}).get("last_result"),
                "weekly_ai_last_run_at": tuner_schedule.get("last_run_at"),
                "weekly_ai_last_result": tuner_schedule.get("last_result"),
                "daily_local_candidate": daily_local_candidate,
                "weekly_ai_candidate": weekly_ai_candidate,
                "weekly_review_bundle": weekly_review_bundle,
                "primary_candidate_source": state.get("primary_candidate_source", "none"),
                "candidate_available": latest_candidate.get("status") == "ready",
                "candidate_status": latest_candidate.get("status", "none"),
                "candidate_summary": latest_candidate.get("summary", ""),
            },
        }

    def available_actions(self) -> dict[str, bool]:
        return {
            "research_run_now": True,
            "tuner_run_now": True,
            "tuner_schedule_pause": True,
            "tuner_schedule_resume": True,
            "tuner_set_cadence": True,
            "tuner_skip_next": True,
            "tuner_promote_latest": True,
            "tuner_reject_latest": True,
        }

    def enqueue_daily_refresh(self, *, requested_by: str = "dashboard", args: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.enqueue_job("daily_research_refresh", requested_by=requested_by, args=args)

    def enqueue_tuning_proposal(self, *, requested_by: str = "dashboard", args: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.enqueue_job("tuning_proposal", requested_by=requested_by, args=args)

    def enqueue_promote_candidate(self, *, requested_by: str = "dashboard", candidate_id: str | None = None) -> dict[str, Any]:
        tuner = load_tuner_state(self.tuner_state_path or "data/research/tuner_state.json")
        if tuner.get("latest_candidate_status") != "ready":
            raise TuningError("No ready tuning candidate is available for promotion.")
        args = {"candidate_id": candidate_id} if candidate_id else {}
        return self.enqueue_job("promote_candidate", requested_by=requested_by, args=args)

    def enqueue_reject_candidate(
        self,
        *,
        requested_by: str = "dashboard",
        candidate_id: str | None = None,
        reason: str = DEFAULT_TUNER_REASON,
    ) -> dict[str, Any]:
        tuner = load_tuner_state(self.tuner_state_path or "data/research/tuner_state.json")
        if tuner.get("latest_candidate_status") != "ready":
            raise TuningError("No ready tuning candidate is available to reject.")
        args = {"candidate_id": candidate_id, "reason": reason}
        return self.enqueue_job("reject_candidate", requested_by=requested_by, args=args)

    def enqueue_job(
        self,
        job_type: str,
        *,
        requested_by: str,
        args: dict[str, Any] | None = None,
        dedupe: bool = True,
    ) -> dict[str, Any]:
        normalized = str(job_type or "").strip().lower()
        if normalized not in JOB_TYPES:
            raise TuningError(f"Unsupported Codex job type: {job_type}")
        ensure_codex_dirs()
        if dedupe:
            duplicate = self._find_duplicate_job(normalized)
            if duplicate is not None:
                return {"queued": False, "duplicate": True, **duplicate}
        now = self._utcnow()
        request_id = uuid4().hex
        payload = {
            "request_id": request_id,
            "job_type": normalized,
            "requested_at": now.isoformat(),
            "requested_by": str(requested_by or "unknown"),
            "args": dict(args or {}),
        }
        queue_path = self.queue_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}_{normalized}_{request_id}.json"
        queue_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        state = self.read_state()
        state["runner"]["queue_depth"] = self.queue_depth()
        self.save_state(state)
        return {"queued": True, **payload, "queue_path": str(queue_path)}

    def pause_tuner_schedule(self) -> dict[str, Any]:
        state = self.read_state()
        schedule = state["schedules"]["tuning_proposal"]
        schedule["schedule_enabled"] = False
        schedule["next_auto_run_at"] = None
        self.save_state(state)
        return {"ok": True, "message": "Weekly tuning schedule paused."}

    def resume_tuner_schedule(self) -> dict[str, Any]:
        state = self.read_state()
        schedule = state["schedules"]["tuning_proposal"]
        schedule["schedule_enabled"] = True
        schedule["next_auto_run_at"] = self._compute_next_run("tuning_proposal", self._utcnow()).isoformat()
        self.save_state(state)
        return {"ok": True, "message": "Weekly tuning schedule resumed."}

    def set_tuner_cadence(self, cadence: str) -> dict[str, Any]:
        normalized = str(cadence or "").strip().lower()
        if normalized not in {"weekly", "manual"}:
            raise TuningError(f"Unsupported tuner cadence: {cadence}")
        state = self.read_state()
        schedule = state["schedules"]["tuning_proposal"]
        schedule["cadence"] = normalized
        schedule["next_auto_run_at"] = (
            self._compute_next_run("tuning_proposal", self._utcnow()).isoformat() if normalized == "weekly" else None
        )
        self.save_state(state)
        return {"ok": True, "message": f"Tuner cadence set to {normalized}."}

    def skip_next_tuner_run(self) -> dict[str, Any]:
        state = self.read_state()
        schedule = state["schedules"]["tuning_proposal"]
        schedule["skip_next_auto_run"] = True
        self.save_state(state)
        return {"ok": True, "message": "Next automatic tuning run will be skipped."}

    def run_once(self) -> dict[str, Any]:
        ensure_codex_dirs()
        self._clear_stale_lock()
        state = self.read_state()
        if self._clear_orphaned_active_run(state) or self._clear_stale_active_run(state):
            state = self.read_state()
        now = self._utcnow()
        self._queue_due_jobs(state, now)
        self._update_runner_status(state=state, healthy=True)

        next_job = self._next_job_path()
        if next_job is None:
            return {"ok": True, "status": "idle", "queue_depth": 0}
        if not self._acquire_lock():
            return {"ok": False, "status": "busy", "message": "Another Codex runner is already active."}
        try:
            return self._process_job(next_job)
        finally:
            self._release_lock()

    def run_loop(self, *, poll_seconds: float = 15.0) -> None:
        delay = max(1.0, float(poll_seconds))
        while True:
            self.run_once()
            time.sleep(delay)

    def run_tuner_heartbeat(self) -> dict[str, Any]:
        state = self.read_state()
        now = self._utcnow()
        schedule = state["schedules"]["tuning_proposal"]
        next_run_at = self._parse_dt(schedule.get("next_auto_run_at"))
        result = maybe_run_tuner_heartbeat(
            enabled=bool(schedule.get("schedule_enabled", True)),
            cadence=str(schedule.get("cadence") or "weekly"),
            due=bool(next_run_at and next_run_at <= now),
            skip_next=bool(schedule.get("skip_next_auto_run", False)),
            has_recent_daily_refresh=self._has_recent_daily_success(state, now),
            config_path=self.config_path,
            tuner_state_path=self.tuner_state_path or "data/research/tuner_state.json",
        )
        if result.get("status") == "skipped":
            schedule["skip_next_auto_run"] = False
        if result.get("ok"):
            schedule["next_auto_run_at"] = self._compute_next_run("tuning_proposal", now + timedelta(minutes=1)).isoformat()
            schedule["last_run_at"] = now.isoformat()
            schedule["last_result"] = result.get("status") or "success"
            if result.get("candidate_status") in {"ready", "none"}:
                schedule["last_success_at"] = now.isoformat()
        elif result.get("status") == "blocked":
            schedule["next_auto_run_at"] = self._compute_next_run("tuning_proposal", now + timedelta(minutes=1)).isoformat()
            schedule["last_run_at"] = now.isoformat()
            schedule["last_result"] = "blocked"
        self._sync_tuner_state(state)
        self.save_state(state)
        return result

    def _process_job(self, job_path: Path) -> dict[str, Any]:
        job = json.loads(job_path.read_text(encoding="utf-8"))
        now = self._utcnow()
        run_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}-{job['job_type']}-{job['request_id'][:8]}"
        run_dir = self.runs_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "job.json").write_text(json.dumps(job, indent=2, sort_keys=True), encoding="utf-8")

        state = self.read_state()
        state["active_run"] = {
            "run_id": run_id,
            "job_type": job["job_type"],
            "request_id": job["request_id"],
            "requested_by": job.get("requested_by"),
            "started_at": now.isoformat(),
            "phase": "starting",
            "detail": "Preparing job workspace.",
        }
        self._update_runner_status(state=state, healthy=True)

        ok = False
        result: dict[str, Any]
        try:
            result = self._execute_job(job)
            ok = bool(result.get("ok", True))
        except Exception as exc:  # noqa: BLE001
            result = {
                "ok": False,
                "error": {"type": exc.__class__.__name__, "message": str(exc)},
            }

        finished_at = self._utcnow().isoformat()
        result_payload = {
            "run_id": run_id,
            "job_type": job["job_type"],
            "request_id": job["request_id"],
            "started_at": now.isoformat(),
            "finished_at": finished_at,
            "ok": ok,
            "result": result,
        }
        (run_dir / "result.json").write_text(json.dumps(result_payload, indent=2, sort_keys=True), encoding="utf-8")
        try:
            job_path.unlink()
        except FileNotFoundError:
            pass

        state = self.read_state()
        state["active_run"] = None
        state["last_run"] = {
            "run_id": run_id,
            "job_type": job["job_type"],
            "started_at": now.isoformat(),
            "finished_at": finished_at,
            "ok": ok,
            "summary": self._result_summary(job["job_type"], result_payload),
        }
        schedule = state["schedules"].get(job["job_type"])
        if isinstance(schedule, dict):
            schedule["last_run_at"] = finished_at
            if ok:
                schedule["last_result"] = result.get("status") or "success"
            else:
                schedule["last_result"] = result.get("status") or "failed"
            if ok:
                schedule["last_success_at"] = finished_at
        if ok and job["job_type"] in ("daily_research_refresh", "tuning_proposal"):
            latest_payload = self._read_json(self.latest_path, default={})
            latest_payload[job["job_type"]] = {
                "run_id": run_id,
                "finished_at": finished_at,
                "result_path": str(run_dir / "result.json"),
            }
            self.latest_path.write_text(json.dumps(latest_payload, indent=2, sort_keys=True), encoding="utf-8")
        self._sync_tuner_state(state)
        state["runner"]["queue_depth"] = self.queue_depth()
        self.save_state(state)
        return result_payload

    def _execute_job(self, job: dict[str, Any]) -> dict[str, Any]:
        job_type = job["job_type"]
        args = dict(job.get("args") or {})
        if job_type == "daily_research_refresh":
            return self._run_daily_refresh(args)
        if job_type == "tuning_proposal":
            return build_weekly_review_bundle(
                config_path=args.get("config_path", self.config_path),
                tuner_state_path=self.tuner_state_path or "data/research/tuner_state.json",
            )
        if job_type == "promote_candidate":
            return promote_tuning_candidate(
                candidate_id=args.get("candidate_id"),
                config_path=args.get("config_path", self.config_path),
                tuner_state_path=self.tuner_state_path or "data/research/tuner_state.json",
            )
        if job_type == "reject_candidate":
            return reject_tuning_candidate(
                candidate_id=args.get("candidate_id"),
                tuner_state_path=self.tuner_state_path or "data/research/tuner_state.json",
                reason=str(args.get("reason") or DEFAULT_TUNER_REASON),
            )
        if job_type == "repo_task":
            return self._run_repo_task(args)
        raise TuningError(f"Unsupported Codex job type: {job_type}")

    def _run_daily_refresh(self, args: dict[str, Any]) -> dict[str, Any]:
        warehouse = ResearchWarehouse(args.get("warehouse_path", WAREHOUSE_PATH))
        market_source = GammaMarketSource(
            timeout_seconds=float(args.get("http_timeout_seconds", 30.0)),
            retry_attempts=int(args.get("http_retry_attempts", 3)),
            retry_backoff_seconds=float(args.get("http_retry_backoff_seconds", 1.5)),
            retry_max_backoff_seconds=float(args.get("http_retry_max_backoff_seconds", 10.0)),
        )
        fill_source = GoldskyFillSource(
            timeout_seconds=float(args.get("http_timeout_seconds", 30.0)),
            retry_attempts=int(args.get("http_retry_attempts", 3)),
            retry_backoff_seconds=float(args.get("http_retry_backoff_seconds", 1.5)),
            retry_max_backoff_seconds=float(args.get("http_retry_max_backoff_seconds", 10.0)),
        )
        sync_result: dict[str, Any] | None = None
        sync_error: dict[str, str] | None = None
        build_result: dict[str, Any] | None = None
        build_error: dict[str, str] | None = None
        local_tuning_result: dict[str, Any] | None = None
        local_tuning_error: dict[str, str] | None = None
        weekly_context_result: dict[str, Any] | None = None
        weekly_context_error: dict[str, str] | None = None
        try:
            self._refresh_active_run(phase="syncing markets", detail="Refreshing recent Gamma markets.")
            market_result = sync_recent_markets(
                warehouse,
                market_source,
                lookback_days=int(args.get("market_lookback_days", 30)),
                batch_size=int(args.get("market_batch_size", 500)),
                max_batches=self._optional_int(args.get("market_max_batches")),
            )
            self._refresh_active_run(phase="syncing fills", detail="Refreshing recent Goldsky 5m fills.")
            fill_result = sync_recent_5m_fills(
                warehouse,
                fill_source,
                lookback_hours=int(args.get("lookback_hours", 24)),
                history_lookback_days=int(args.get("history_lookback_days", 30)),
                batch_size=int(args.get("batch_size", 1000)),
                asset_chunk_size=int(args.get("asset_chunk_size", 20)),
                bucket_minutes=int(args.get("bucket_minutes", 60)),
                history_bucket_minutes=int(args.get("history_bucket_minutes", 360)),
                bucket_buffer_seconds=int(args.get("bucket_buffer_seconds", 900)),
                max_batches_per_chunk=self._optional_int(args.get("max_batches_per_chunk", 2)),
                max_history_batches_per_chunk=self._optional_int(args.get("max_history_batches_per_chunk", 1)),
            )
            sync_result = {
                "dataset": "daily_research_sync",
                "markets": market_result,
                "fills": fill_result,
            }
        except Exception as exc:  # noqa: BLE001
            sync_error = {"type": exc.__class__.__name__, "message": str(exc)}
        finally:
            self._close_quietly(market_source)
            self._close_quietly(fill_source)
            self._close_quietly(warehouse)
        try:
            self._refresh_active_run(phase="building artifacts", detail="Rebuilding runtime policy and research report.")
            build_result = build_artifacts(
                warehouse_path=args.get("warehouse_path", WAREHOUSE_PATH),
                tracker_db=args.get("tracker_db", LOCAL_TRACKER_DB),
                policy_path=args.get("policy_path", "data/research/runtime_policy.json"),
                lookback_days=int(args.get("lookback_days", 30)),
            )
        except Exception as exc:  # noqa: BLE001
            build_error = {"type": exc.__class__.__name__, "message": str(exc)}
        try:
            self._refresh_active_run(phase="local tuning", detail="Evaluating deterministic tuning candidate.")
            local_tuning_result = propose_tuning(
                config_path=args.get("config_path", self.config_path),
                tracker_db_path=args.get("tracker_db", LOCAL_TRACKER_DB),
                tuner_state_path=self.tuner_state_path or "data/research/tuner_state.json",
                research_report_json_path=args.get("report_json_path", "data/research/research_report.json"),
            )
        except Exception as exc:  # noqa: BLE001
            local_tuning_error = {"type": exc.__class__.__name__, "message": str(exc)}
        try:
            self._refresh_active_run(phase="weekly context", detail="Refreshing weekly desktop review context.")
            weekly_context_result = build_weekly_ai_context(
                config_path=args.get("config_path", self.config_path),
                tuner_state_path=self.tuner_state_path or "data/research/tuner_state.json",
                report_json_path=args.get("report_json_path", "data/research/research_report.json"),
                window_days=int(args.get("weekly_context_days", 7)),
            )
        except Exception as exc:  # noqa: BLE001
            weekly_context_error = {"type": exc.__class__.__name__, "message": str(exc)}
        return {
            "command": "daily-refresh",
            "ok": build_error is None and local_tuning_error is None and weekly_context_error is None,
            "sync": {"ok": sync_error is None, "result": sync_result, "error": sync_error},
            "build": {"ok": build_error is None, "result": build_result, "error": build_error},
            "daily_local_tuning": {
                "ok": local_tuning_error is None,
                "result": local_tuning_result,
                "error": local_tuning_error,
            },
            "weekly_ai_context": {
                "ok": weekly_context_error is None,
                "result": weekly_context_result,
                "error": weekly_context_error,
            },
        }

    def _run_repo_task(self, args: dict[str, Any]) -> dict[str, Any]:
        command = args.get("command")
        if not command:
            raise TuningError("repo_task requires args.command.")
        completed = subprocess.run(
            command if isinstance(command, list) else str(command),
            cwd=resolve_repo_path("."),
            capture_output=True,
            text=True,
            shell=not isinstance(command, list),
            check=False,
        )
        return {
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
        }

    def _queue_due_jobs(self, state: dict[str, Any], now: datetime) -> None:
        for job_type in ("daily_research_refresh", "tuning_proposal"):
            schedule = state["schedules"][job_type]
            if not bool(schedule.get("schedule_enabled", True)):
                continue
            next_run_at = self._parse_dt(schedule.get("next_auto_run_at")) or self._compute_next_run(job_type, now)
            if next_run_at > now:
                schedule["next_auto_run_at"] = next_run_at.isoformat()
                continue
            if job_type == "tuning_proposal":
                if str(schedule.get("cadence") or "weekly").lower() != "weekly":
                    schedule["next_auto_run_at"] = None
                    continue
                if bool(schedule.get("skip_next_auto_run", False)):
                    schedule["skip_next_auto_run"] = False
                    schedule["last_result"] = "skipped"
                    schedule["next_auto_run_at"] = self._compute_next_run(job_type, now + timedelta(minutes=1)).isoformat()
                    continue
                if not self._has_recent_daily_success(state, now):
                    schedule["last_result"] = "blocked"
                    schedule["next_auto_run_at"] = self._compute_next_run(job_type, now + timedelta(minutes=1)).isoformat()
                    continue
            self.enqueue_job(job_type, requested_by="schedule", args={}, dedupe=True)
            schedule["next_auto_run_at"] = self._compute_next_run(job_type, now + timedelta(minutes=1)).isoformat()

    def queue_depth(self) -> int:
        ensure_codex_dirs()
        return len(self._visible_queue_paths())

    def _find_duplicate_job(self, job_type: str) -> dict[str, Any] | None:
        state = self.read_state()
        active = state.get("active_run")
        if active and active.get("job_type") == job_type:
            return {"job_type": job_type, "active_run": active}
        for path in sorted(self.queue_root.glob("*.json")):
            payload = self._read_json(path, default={})
            if payload.get("job_type") == job_type:
                return payload
        return None

    def _next_job_path(self) -> Path | None:
        queue_files = sorted(self.queue_root.glob("*.json"))
        return queue_files[0] if queue_files else None

    def _next_queued_job_payload(self) -> dict[str, Any] | None:
        queue_files = self._visible_queue_paths()
        next_job_path = queue_files[0] if queue_files else None
        if next_job_path is None:
            return None
        payload = self._read_json(next_job_path, default={})
        return payload or None

    def _visible_queue_paths(self, *, active_request_id: str | None = None) -> list[Path]:
        queue_files = sorted(self.queue_root.glob("*.json"))
        if active_request_id is None:
            active = self.read_state().get("active_run") or {}
            active_request_id = str(active.get("request_id") or "").strip()
        if not active_request_id:
            return queue_files
        visible: list[Path] = []
        skipped_active = False
        for path in queue_files:
            payload = self._read_json(path, default={})
            if not skipped_active and str(payload.get("request_id") or "") == active_request_id:
                skipped_active = True
                continue
            visible.append(path)
        return visible

    def _update_runner_status(self, *, state: dict[str, Any] | None = None, healthy: bool = True) -> None:
        state = state or self.read_state()
        active = state.get("active_run") or {}
        active_request_id = str(active.get("request_id") or "").strip()
        state["runner"]["healthy"] = bool(healthy)
        state["runner"]["last_heartbeat_at"] = self._utcnow().isoformat()
        state["runner"]["queue_depth"] = len(self._visible_queue_paths(active_request_id=active_request_id))
        self.save_state(state)

    def _refresh_active_run(self, *, phase: str, detail: str) -> None:
        state = self.read_state()
        active = dict(state.get("active_run") or {})
        if not active:
            return
        active["phase"] = str(phase or "").strip()
        active["detail"] = str(detail or "").strip()
        active["progress_at"] = self._utcnow().isoformat()
        state["active_run"] = active
        self._update_runner_status(state=state, healthy=True)

    def _clear_orphaned_active_run(self, state: dict[str, Any]) -> bool:
        active = state.get("active_run") or {}
        if not active:
            return False
        if self.lock_path.exists():
            return False
        state["active_run"] = None
        self.save_state(state)
        return True

    def _clear_stale_active_run(self, state: dict[str, Any]) -> bool:
        active = state.get("active_run") or {}
        if not active:
            return False
        started_at = self._parse_dt(active.get("started_at"))
        progress_at = self._parse_dt(active.get("progress_at"))
        last_activity = progress_at or started_at
        if last_activity is None:
            return False
        age_s = (self._utcnow() - last_activity).total_seconds()
        if age_s < STALE_ACTIVE_RUN_SECONDS:
            return False
        if self.lock_path.exists():
            payload = self._read_json(self.lock_path, default={})
            pid = self._optional_int((payload or {}).get("pid"))
            if pid is not None and self._pid_is_running(pid):
                return False
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass
        state["active_run"] = None
        self.save_state(state)
        return True

    def _clear_stale_lock(self) -> bool:
        if not self.lock_path.exists():
            return False
        payload = self._read_json(self.lock_path, default={})
        pid = self._optional_int((payload or {}).get("pid"))
        if pid is not None and self._pid_is_running(pid):
            return False
        created_at = self._parse_dt((payload or {}).get("created_at"))
        if pid is None and created_at is not None:
            age_s = (self._utcnow() - created_at).total_seconds()
            if age_s < STALE_RUNNER_LOCK_SECONDS:
                return False
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            return False
        return True

    def _latest_daily_refresh_metrics(self) -> dict[str, Any]:
        latest_payload = self._read_json(self.latest_path, default={})
        daily_refresh = latest_payload.get("daily_research_refresh") or {}
        result_path = daily_refresh.get("result_path")
        if not result_path:
            return {}
        payload = self._read_json(resolve_repo_path(result_path), default={})
        if not payload:
            return {}
        result = payload.get("result") or {}
        build = (result.get("build") or {}).get("result") or {}
        sync = (result.get("sync") or {}).get("result") or {}
        markets = (sync.get("markets") or {}) if isinstance(sync, dict) else {}
        open_markets = (markets.get("open_markets") or {}) if isinstance(markets, dict) else {}
        closed_markets = (markets.get("closed_markets") or {}) if isinstance(markets, dict) else {}
        fills = (sync.get("fills") or {}) if isinstance(sync, dict) else {}
        recent = (fills.get("recent") or {}) if isinstance(fills, dict) else {}
        history = (fills.get("history") or {}) if isinstance(fills, dict) else {}
        local = (result.get("daily_local_tuning") or {}).get("result") or {}
        return {
            "run_id": payload.get("run_id"),
            "finished_at": payload.get("finished_at"),
            "ok": bool(payload.get("ok", False)),
            "cluster_count": int(build.get("cluster_count", 0) or 0),
            "outcome_count": int(build.get("outcome_count", 0) or 0),
            "fill_flow_rows": int(build.get("fill_flow_rows", 0) or 0),
            "market_fetched_count": int(markets.get("fetched", 0) or 0),
            "market_inserted_count": int(markets.get("inserted", 0) or 0),
            "open_market_fetched_count": int(open_markets.get("fetched", 0) or 0),
            "open_market_inserted_count": int(open_markets.get("inserted", 0) or 0),
            "closed_market_fetched_count": int(closed_markets.get("fetched", 0) or 0),
            "closed_market_inserted_count": int(closed_markets.get("inserted", 0) or 0),
            "fetched_fill_count": int(fills.get("fetched", 0) or 0),
            "inserted_fill_count": int(fills.get("inserted", 0) or 0),
            "recent_window_count": int(recent.get("asset_window_count", 0) or 0),
            "recent_asset_count": int(recent.get("asset_count", 0) or 0),
            "recent_fetched_fill_count": int(recent.get("fetched", 0) or 0),
            "recent_inserted_fill_count": int(recent.get("inserted", 0) or 0),
            "history_window_count": int(history.get("asset_window_count", 0) or 0),
            "history_asset_count": int(history.get("asset_count", 0) or 0),
            "history_fetched_fill_count": int(history.get("fetched", 0) or 0),
            "history_inserted_fill_count": int(history.get("inserted", 0) or 0),
            "fills_enriched_rows": int(fills.get("fills_enriched_rows", 0) or 0),
            "market_5m_registry_rows": int(fills.get("market_5m_registry_rows", 0) or 0),
            "candidate_status": str(local.get("candidate_status") or "none"),
        }

    def _sync_tuner_state(self, state: dict[str, Any]) -> None:
        tuner = load_tuner_state(self.tuner_state_path or "data/research/tuner_state.json")
        state["latest_candidate"] = {
            "candidate_id": tuner.get("latest_candidate_id"),
            "status": tuner.get("latest_candidate_status", "none"),
            "summary": tuner.get("latest_candidate_summary", ""),
            "paths": dict(tuner.get("latest_candidate_paths") or {}),
            "source": tuner.get("latest_candidate_source", "none"),
        }
        state["daily_local_candidate"] = dict(tuner.get("daily_local_candidate") or {})
        state["weekly_ai_candidate"] = dict(tuner.get("weekly_ai_candidate") or {})
        state["weekly_review_bundle"] = dict(tuner.get("weekly_review_bundle") or {})
        state["primary_candidate_source"] = tuner.get("primary_candidate_source", "none")
        schedule = state["schedules"]["tuning_proposal"]
        weekly_bundle = dict(tuner.get("weekly_review_bundle") or {})
        schedule["last_run_at"] = weekly_bundle.get("generated_at") or schedule.get("last_run_at")
        schedule["last_result"] = weekly_bundle.get("last_result") or schedule.get("last_result")

    def _has_recent_daily_success(self, state: dict[str, Any], now: datetime) -> bool:
        schedule = state["schedules"]["daily_research_refresh"]
        last_success = self._parse_dt(schedule.get("last_success_at"))
        if last_success is None:
            return False
        return (now - last_success) <= timedelta(hours=36)

    def _default_state(self) -> dict[str, Any]:
        now = self._utcnow()
        return {
            "schema_version": 2,
            "runner": {
                "healthy": False,
                "last_heartbeat_at": None,
                "queue_depth": 0,
                "workspace_root": str(resolve_repo_path(".")),
                "shared_data_root": str(SHARED_DATA_ROOT),
            },
            "active_run": None,
            "last_run": None,
            "latest_candidate": {
                "candidate_id": None,
                "status": "none",
                "summary": "",
                "paths": {},
                "source": "none",
            },
            "daily_local_candidate": {},
            "weekly_ai_candidate": {},
            "weekly_review_bundle": {},
            "primary_candidate_source": "none",
            "schedules": {
                "daily_research_refresh": {
                    **SCHEDULE_DEFAULTS["daily_research_refresh"],
                    "next_auto_run_at": self._compute_next_run("daily_research_refresh", now).isoformat(),
                    "last_run_at": None,
                    "last_success_at": None,
                    "last_result": None,
                },
                "tuning_proposal": {
                    **SCHEDULE_DEFAULTS["tuning_proposal"],
                    "next_auto_run_at": self._compute_next_run("tuning_proposal", now).isoformat(),
                    "last_run_at": None,
                    "last_success_at": None,
                    "last_result": None,
                },
            },
        }

    def _normalize_state(self, state: dict[str, Any]) -> dict[str, Any]:
        base = self._merge_dicts(self._default_state(), state)
        tuner = base["schedules"]["tuning_proposal"]
        tuner["cadence"] = "manual" if str(tuner.get("cadence")).lower() == "manual" else "weekly"
        for job_type, schedule in base["schedules"].items():
            enabled = bool(schedule.get("schedule_enabled", True))
            if not enabled:
                schedule["next_auto_run_at"] = None
            elif job_type == "tuning_proposal" and str(schedule.get("cadence")).lower() == "manual":
                schedule["next_auto_run_at"] = None
            elif schedule.get("next_auto_run_at") is None:
                schedule["next_auto_run_at"] = self._compute_next_run(job_type, self._utcnow()).isoformat()
        return base

    def _compute_next_run(self, job_type: str, now: datetime) -> datetime:
        tz = self._local_timezone()
        local_now = now.astimezone(tz)
        schedule = SCHEDULE_DEFAULTS[job_type]
        hour = int(schedule["hour_local"])
        minute = int(schedule["minute_local"])
        target = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if job_type == "daily_research_refresh":
            if target <= local_now:
                target += timedelta(days=1)
        else:
            days_ahead = (0 - target.weekday()) % 7
            target += timedelta(days=days_ahead)
            if target <= local_now:
                target += timedelta(days=7)
        return target.astimezone(timezone.utc)

    def _local_timezone(self):
        tz_name = str(os.getenv("EDEC_LOCAL_TIMEZONE", "")).strip()
        if tz_name:
            try:
                return ZoneInfo(tz_name)
            except Exception:
                pass
        return datetime.now().astimezone().tzinfo or timezone.utc

    def _acquire_lock(self) -> bool:
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"created_at": self._utcnow().isoformat(), "pid": os.getpid()}))
        return True

    def _release_lock(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def _pid_is_running(self, pid: int) -> bool:
        try:
            os.kill(int(pid), 0)
        except (OSError, ValueError):
            return False
        return True

    def _result_summary(self, job_type: str, payload: dict[str, Any]) -> str:
        if not payload.get("ok"):
            error = payload.get("result", {}).get("error") or {}
            return f"{job_type} failed: {error.get('message', 'unknown error')}"
        if job_type == "daily_research_refresh":
            build_result = payload["result"].get("build", {}).get("result") or {}
            sync_result = payload["result"].get("sync", {}).get("result") or {}
            fill_result = (sync_result.get("fills") or {}) if isinstance(sync_result, dict) else {}
            local = payload["result"].get("daily_local_tuning", {}).get("result") or {}
            return (
                f"Daily refresh built {int(build_result.get('cluster_count', 0))} clusters; "
                f"warehouse fetched {int(fill_result.get('fetched', 0))} fills; "
                f"local candidate {str(local.get('candidate_status') or 'unknown')}."
            )
        if job_type == "tuning_proposal":
            return f"Weekly desktop review bundle status: {payload['result'].get('status', 'unknown')}."
        if job_type == "promote_candidate":
            return f"Promoted candidate {payload['result'].get('candidate_id', '')}."
        if job_type == "reject_candidate":
            return f"Rejected candidate {payload['result'].get('candidate_id', '')}."
        return f"{job_type} completed."

    @staticmethod
    def _close_quietly(resource: object) -> None:
        close = getattr(resource, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass

    @staticmethod
    def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = CodexAutomationManager._merge_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _read_json(path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return dict(default)
        return payload if isinstance(payload, dict) else dict(default)

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value in (None, "", False):
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _parse_dt(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)
