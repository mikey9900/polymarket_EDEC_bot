"""Deterministic daily tuning and weekly AI proposal helpers."""

from __future__ import annotations

import csv
import difflib
import gzip
import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import yaml

from ._openai import require_openai
from .paths import (
    CONFIG_CANDIDATES_ROOT,
    DEFAULT_CONFIG_PATH,
    DEFAULT_REPORT_JSON_PATH,
    LOCAL_TRACKER_DB,
    TUNER_ACTIVE_PATCH_PATH,
    TUNER_REPORT_JSON_PATH,
    TUNER_REPORT_MD_PATH,
    TUNER_STATE_PATH,
    WEEKLY_AI_CONTEXT_PATH,
    WEEKLY_AI_PATCH_PATH,
    WEEKLY_AI_PROMPT_BUNDLE_PATH,
    WEEKLY_AI_REPORT_JSON_PATH,
    WEEKLY_AI_REPORT_MD_PATH,
    WEEKLY_AI_RESPONSE_PATH,
    WEEKLY_DESKTOP_PROMPT_PATH,
    WEEKLY_REVIEW_BUNDLE_JSON_PATH,
    WEEKLY_REVIEW_BUNDLE_MD_PATH,
    discover_session_export_roots,
    ensure_tuner_dirs,
    resolve_repo_path,
)


SAFE_CONFIG_PREFIXES = (
    "dual_leg.",
    "single_leg.",
    "lead_lag.",
    "swing_leg.",
    "research.",
    "risk.",
)
SAFE_ROOT_KEYS = tuple(sorted({prefix.split(".", 1)[0] for prefix in SAFE_CONFIG_PREFIXES}))
TUNER_STATUSES = {"none", "ready", "promoted", "rejected"}
CANDIDATE_SOURCES = ("daily_local", "weekly_ai")
PRIMARY_SOURCE_ORDER = ("weekly_ai", "daily_local")
DEFAULT_WEEKLY_AI_MODEL = "gpt-5.4-mini"
DEFAULT_WEEKLY_CONTEXT_DAYS = 7


class TuningError(RuntimeError):
    """Raised when tuning proposal inputs are missing or invalid."""


@dataclass(frozen=True)
class ProposedChange:
    path: str
    current: Any
    recommended: Any
    evidence: str
    confidence: float | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "path": self.path,
            "current": self.current,
            "recommended": self.recommended,
            "evidence": self.evidence,
        }
        if self.confidence is not None:
            payload["confidence"] = round(float(self.confidence), 4)
        return payload


@dataclass(frozen=True)
class SessionBundle:
    export_id: str
    trades_path: Path
    signals_path: Path
    bundle_time: datetime | None


@dataclass(frozen=True)
class TuningInputBundle:
    source: str
    export_id: str
    trades_path: Path | None
    signals_path: Path | None
    tracker_db_path: Path | None
    analysis: dict[str, Any]


def load_tuner_state(path: str | Path = TUNER_STATE_PATH) -> dict[str, Any]:
    tuner_path = resolve_repo_path(path)
    if not tuner_path.exists():
        return _default_tuner_state()
    try:
        payload = json.loads(tuner_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _default_tuner_state()
    state = _normalize_tuner_state(payload if isinstance(payload, dict) else {})
    return state


def save_tuner_state(state: dict[str, Any], path: str | Path = TUNER_STATE_PATH) -> Path:
    tuner_path = resolve_repo_path(path)
    tuner_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_tuner_state(state)
    tuner_path.write_text(json.dumps(normalized, indent=2, sort_keys=True), encoding="utf-8")
    return tuner_path


def tuner_status(
    *,
    path: str | Path = TUNER_STATE_PATH,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> dict[str, Any]:
    state = load_tuner_state(path)
    return {
        **state,
        "config_path": str(resolve_repo_path(config_path)),
    }


def propose_tuning(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    tracker_db_path: str | Path = LOCAL_TRACKER_DB,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    report_json_path: str | Path = TUNER_REPORT_JSON_PATH,
    report_md_path: str | Path = TUNER_REPORT_MD_PATH,
    patch_path: str | Path = TUNER_ACTIVE_PATCH_PATH,
    candidates_root: str | Path = CONFIG_CANDIDATES_ROOT,
    research_report_json_path: str | Path = DEFAULT_REPORT_JSON_PATH,
) -> dict[str, Any]:
    ensure_tuner_dirs()
    now = _utcnow()
    config_path = resolve_repo_path(config_path)
    report_json_path = resolve_repo_path(report_json_path)
    report_md_path = resolve_repo_path(report_md_path)
    patch_path = resolve_repo_path(patch_path)
    candidates_root = resolve_repo_path(candidates_root)
    research_report = _load_json_file(research_report_json_path, default={})

    input_bundle = _build_tuning_input_bundle(tracker_db_path)
    analysis = input_bundle.analysis
    current_config = _load_yaml(config_path)
    candidate_config = _deep_copy(current_config)

    changes, advisories, no_change = _recommend_changes(candidate_config, analysis, research_report=research_report)
    _enforce_safe_change_surface(changes)

    candidate_id = now.strftime("%Y%m%dT%H%M%SZ")
    candidate_path: Path | None = None
    if changes:
        candidate_path = candidates_root / f"{candidate_id}_tuned.yaml"
        candidate_text = _dump_yaml(candidate_config)
        candidate_path.write_text(candidate_text, encoding="utf-8")
        patch_path.write_text(
            _diff_text(
                config_path.read_text(encoding="utf-8"),
                candidate_text,
                config_path,
                candidate_path,
            ),
            encoding="utf-8",
        )
    else:
        patch_path.write_text("", encoding="utf-8")

    report_payload = {
        "generated_at": now.isoformat(),
        "source": "daily_local",
        "candidate_id": candidate_id if changes else None,
        "candidate_status": "ready" if changes else "none",
        "config_name": config_path.name,
        "inputs": {
            "source": input_bundle.source,
            "export_id": input_bundle.export_id,
            "trades_file": input_bundle.trades_path.name if input_bundle.trades_path else "",
            "signals_file": input_bundle.signals_path.name if input_bundle.signals_path else "",
            "tracker_db": str(input_bundle.tracker_db_path) if input_bundle.tracker_db_path else "",
        },
        "data": analysis["overall"],
        "research_rollups": _compact_research_report(research_report),
        "changes": [change.as_dict() for change in changes],
        "advisories": advisories,
        "no_change": no_change,
    }
    report_json_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")
    report_md_path.write_text(_render_tuner_markdown(report_payload), encoding="utf-8")

    state = load_tuner_state(tuner_state_path)
    _set_candidate_record(
        state,
        source="daily_local",
        candidate_id=candidate_id if changes else None,
        status="ready" if changes else "none",
        summary=_candidate_summary(changes, analysis["overall"]),
        paths={
            "report_json": str(report_json_path),
            "report_md": str(report_md_path),
            "patch": str(patch_path),
            "candidate_config": str(candidate_path) if candidate_path else "",
        },
        generated_at=now.isoformat(),
        last_result="ready" if changes else "no_change",
        change_count=len(changes),
        top_changes=[change.path for change in changes[:5]],
    )
    state["running"] = False
    state["last_run_at"] = now.isoformat()
    state["last_result"] = "ready" if changes else "no_change"
    save_tuner_state(state, tuner_state_path)

    return {
        "command": "propose-tuning",
        "ok": True,
        "candidate_id": state["daily_local_candidate"]["candidate_id"],
        "candidate_status": state["daily_local_candidate"]["status"],
        "candidate_source": "daily_local",
        "config_path": str(config_path),
        "input_source": input_bundle.source,
        "export_id": input_bundle.export_id,
        "change_count": len(changes),
        "report_json_path": str(report_json_path),
        "report_md_path": str(report_md_path),
        "patch_path": str(patch_path),
        "candidate_config_path": str(candidate_path) if candidate_path else "",
    }


def build_weekly_ai_context(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    context_path: str | Path = WEEKLY_AI_CONTEXT_PATH,
    report_json_path: str | Path = DEFAULT_REPORT_JSON_PATH,
    window_days: int = DEFAULT_WEEKLY_CONTEXT_DAYS,
) -> dict[str, Any]:
    ensure_tuner_dirs()
    now = _utcnow()
    config_path = resolve_repo_path(config_path)
    context_path = resolve_repo_path(context_path)
    state = load_tuner_state(tuner_state_path)
    current_config = _load_yaml(config_path)
    research_report = _load_json_file(report_json_path, default={})
    daily_candidate = _candidate_overview(state.get("daily_local_candidate") or {})

    snapshots, raw_refs = _build_daily_snapshots(current_config, window_days=window_days)
    payload = {
        "generated_at": now.isoformat(),
        "window_days": int(window_days),
        "window_start": (now - timedelta(days=int(window_days))).date().isoformat(),
        "window_end": now.date().isoformat(),
        "config": {
            "name": config_path.name,
            "version": _read_repo_version(),
            "hash": _config_hash(config_path),
            "safe_params": _extract_safe_params(current_config),
        },
        "daily_local_candidate": daily_candidate,
        "daily_snapshots": snapshots,
        "research_rollups": _compact_research_report(research_report),
        "raw_export_refs": raw_refs,
    }
    context_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    state["weekly_ai_context"] = {
        "generated_at": now.isoformat(),
        "path": str(context_path),
        "window_days": int(window_days),
        "window_start": payload["window_start"],
        "window_end": payload["window_end"],
        "raw_ref_count": len(raw_refs),
    }
    save_tuner_state(state, tuner_state_path)

    return {
        "command": "build-weekly-ai-context",
        "ok": True,
        "context_path": str(context_path),
        "snapshot_count": len(snapshots),
        "raw_ref_count": len(raw_refs),
        "window_days": int(window_days),
    }


def build_weekly_review_bundle(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    context_path: str | Path = WEEKLY_AI_CONTEXT_PATH,
    bundle_json_path: str | Path = WEEKLY_REVIEW_BUNDLE_JSON_PATH,
    bundle_md_path: str | Path = WEEKLY_REVIEW_BUNDLE_MD_PATH,
    prompt_path: str | Path = WEEKLY_DESKTOP_PROMPT_PATH,
) -> dict[str, Any]:
    ensure_tuner_dirs()
    build_weekly_ai_context(
        config_path=config_path,
        tuner_state_path=tuner_state_path,
        context_path=context_path,
    )
    now = _utcnow()
    config_path = resolve_repo_path(config_path)
    bundle_json_path = resolve_repo_path(bundle_json_path)
    bundle_md_path = resolve_repo_path(bundle_md_path)
    prompt_path = resolve_repo_path(prompt_path)
    context_payload = _load_json_file(context_path, default={})
    state = load_tuner_state(tuner_state_path)
    daily_local = dict(state.get("daily_local_candidate") or {})
    resolved_context_path = Path(resolve_repo_path(context_path))
    desktop_prompt = _desktop_review_prompt(bundle_md_path.name, resolved_context_path.name)
    short_prompt = _desktop_weekly_prompt(
        bundle_md_path=bundle_md_path,
        config_path=config_path,
        context_path=resolved_context_path,
    )

    bundle_payload = {
        "generated_at": now.isoformat(),
        "review_mode": "desktop_manual",
        "config": {
            "name": config_path.name,
            "version": _read_repo_version(),
            "hash": _config_hash(config_path),
        },
        "weekly_context": {
            "generated_at": context_payload.get("generated_at"),
            "window_start": context_payload.get("window_start"),
            "window_end": context_payload.get("window_end"),
            "snapshot_count": len(context_payload.get("daily_snapshots") or []),
            "raw_ref_count": len(context_payload.get("raw_export_refs") or []),
        },
        "daily_local_candidate": _candidate_overview(daily_local),
        "safe_config_surface": list(SAFE_CONFIG_PREFIXES),
        "artifacts": {
            "context_json": Path(resolve_repo_path(context_path)).name,
            "bundle_json": bundle_json_path.name,
            "bundle_md": bundle_md_path.name,
            "desktop_prompt": prompt_path.name,
        },
        "desktop_review_prompt": desktop_prompt,
        "desktop_weekly_prompt": short_prompt,
        "review_checklist": [
            "Use only the compact weekly context and the review bundle.",
            "Stay inside the safe config surface.",
            "Prefer no change when evidence is weak.",
            "Explain evidence, risks, and follow-ups in plain language.",
            "Produce a candidate patch against the current active config only.",
        ],
        "research_rollups": context_payload.get("research_rollups") or {},
        "daily_snapshots": context_payload.get("daily_snapshots") or [],
        "raw_export_refs": context_payload.get("raw_export_refs") or [],
    }
    bundle_json_path.write_text(json.dumps(bundle_payload, indent=2, sort_keys=True), encoding="utf-8")
    bundle_md_path.write_text(_render_weekly_review_bundle_markdown(bundle_payload), encoding="utf-8")
    prompt_path.write_text(short_prompt + "\n", encoding="utf-8")

    state["weekly_review_bundle"] = {
        "generated_at": now.isoformat(),
        "status": "ready",
        "summary": (
            f"Weekly desktop review bundle ready with {len(bundle_payload['daily_snapshots'])} daily snapshots and "
            f"{len(bundle_payload['raw_export_refs'])} export refs."
        ),
        "paths": {
            "bundle_json": str(bundle_json_path),
            "bundle_md": str(bundle_md_path),
            "context_json": str(resolve_repo_path(context_path)),
            "desktop_prompt": str(prompt_path),
        },
        "last_result": "ready",
    }
    save_tuner_state(state, tuner_state_path)
    return {
        "command": "build-weekly-review-bundle",
        "ok": True,
        "status": "ready",
        "bundle_json_path": str(bundle_json_path),
        "bundle_md_path": str(bundle_md_path),
        "context_path": str(resolve_repo_path(context_path)),
        "desktop_prompt_path": str(prompt_path),
    }


def propose_weekly_ai_tuning(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    context_path: str | Path = WEEKLY_AI_CONTEXT_PATH,
    report_json_path: str | Path = WEEKLY_AI_REPORT_JSON_PATH,
    report_md_path: str | Path = WEEKLY_AI_REPORT_MD_PATH,
    prompt_bundle_path: str | Path = WEEKLY_AI_PROMPT_BUNDLE_PATH,
    response_path: str | Path = WEEKLY_AI_RESPONSE_PATH,
    patch_path: str | Path = WEEKLY_AI_PATCH_PATH,
    candidates_root: str | Path = CONFIG_CANDIDATES_ROOT,
    model: str = DEFAULT_WEEKLY_AI_MODEL,
    api_key: str | None = None,
    max_output_tokens: int = 4000,
) -> dict[str, Any]:
    ensure_tuner_dirs()
    now = _utcnow()
    config_path = resolve_repo_path(config_path)
    report_json_path = resolve_repo_path(report_json_path)
    report_md_path = resolve_repo_path(report_md_path)
    prompt_bundle_path = resolve_repo_path(prompt_bundle_path)
    response_path = resolve_repo_path(response_path)
    patch_path = resolve_repo_path(patch_path)
    candidates_root = resolve_repo_path(candidates_root)

    effective_api_key = str(api_key or os.getenv("OPENAI_API_KEY") or "").strip()
    if not effective_api_key:
        state = load_tuner_state(tuner_state_path)
        state["running"] = False
        state["last_run_at"] = now.isoformat()
        state["last_result"] = "blocked"
        save_tuner_state(state, tuner_state_path)
        return {
            "command": "propose-weekly-ai-tuning",
            "ok": False,
            "status": "blocked",
            "message": "OPENAI_API_KEY is not configured.",
        }

    build_weekly_ai_context(
        config_path=config_path,
        tuner_state_path=tuner_state_path,
        context_path=context_path,
    )
    context_payload = _load_json_file(context_path, default={})
    current_config = _load_yaml(config_path)
    prompt_bundle = {
        "generated_at": now.isoformat(),
        "model": model,
        "system_instructions": _weekly_ai_system_instructions(),
        "compact_context": context_payload,
        "expanded_export_facts": _build_weekly_ai_expansion(context_payload),
    }
    prompt_bundle_path.write_text(json.dumps(prompt_bundle, indent=2, sort_keys=True), encoding="utf-8")

    openai = require_openai()
    client = openai.OpenAI(api_key=effective_api_key)
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": _weekly_ai_system_instructions()},
            {"role": "user", "content": json.dumps(prompt_bundle, indent=2, sort_keys=True)},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "weekly_ai_tuning",
                "schema": _weekly_ai_response_schema(),
                "strict": True,
            }
        },
        max_output_tokens=int(max_output_tokens),
    )

    response_payload = {
        "id": getattr(response, "id", None),
        "model": getattr(response, "model", model),
        "output_text": getattr(response, "output_text", "") or "",
        "usage": _jsonable(getattr(response, "usage", None)),
        "refusal": _extract_response_refusal(response),
    }
    response_path.write_text(json.dumps(response_payload, indent=2, sort_keys=True), encoding="utf-8")

    response_text = str(response_payload.get("output_text") or "").strip()
    if not response_text:
        raise TuningError(response_payload.get("refusal") or "Weekly AI response did not include structured output.")
    raw_payload = json.loads(response_text)
    parsed = _parse_weekly_ai_output(raw_payload)

    candidate_config = _deep_copy(current_config)
    changes: list[ProposedChange] = []
    for item in parsed["recommended_changes"]:
        path = item["path"]
        if not path.startswith(SAFE_CONFIG_PREFIXES):
            raise TuningError(f"Unsafe weekly AI path outside tuning surface: {path}")
        current_value = _get_nested(current_config, path)
        recommended_value = item["recommended"]
        if current_value == recommended_value:
            continue
        _set_nested(candidate_config, path, recommended_value)
        changes.append(
            ProposedChange(
                path=path,
                current=current_value,
                recommended=recommended_value,
                evidence=item["evidence"],
                confidence=item["confidence"],
            )
        )
    _enforce_safe_change_surface(changes)

    candidate_id = now.strftime("%Y%m%dT%H%M%SZ")
    candidate_path: Path | None = None
    if changes:
        candidate_path = candidates_root / f"{candidate_id}_weekly_ai.yaml"
        candidate_text = _dump_yaml(candidate_config)
        candidate_path.write_text(candidate_text, encoding="utf-8")
        patch_path.write_text(
            _diff_text(
                config_path.read_text(encoding="utf-8"),
                candidate_text,
                config_path,
                candidate_path,
            ),
            encoding="utf-8",
        )
    else:
        patch_path.write_text("", encoding="utf-8")

    report_payload = {
        "generated_at": now.isoformat(),
        "source": "weekly_ai",
        "model": model,
        "candidate_id": candidate_id if changes else None,
        "candidate_status": "ready" if changes else "none",
        "config_name": config_path.name,
        "summary": parsed["summary"],
        "inputs": {
            "context_generated_at": context_payload.get("generated_at"),
            "window_start": context_payload.get("window_start"),
            "window_end": context_payload.get("window_end"),
            "raw_ref_count": len(context_payload.get("raw_export_refs") or []),
            "expanded_ref_count": len((prompt_bundle.get("expanded_export_facts") or {}).get("export_summaries") or []),
        },
        "changes": [change.as_dict() for change in changes],
        "risks": parsed["risks"],
        "followups": parsed["followups"],
        "raw_refs_used": [
            ref
            for ref in parsed["raw_refs_used"]
            if ref in {item.get("export_id") for item in (context_payload.get("raw_export_refs") or [])}
        ],
        "no_change": [] if changes else ["Weekly AI review did not produce any config deltas."],
    }
    report_json_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")
    report_md_path.write_text(_render_weekly_ai_markdown(report_payload), encoding="utf-8")

    state = load_tuner_state(tuner_state_path)
    _set_candidate_record(
        state,
        source="weekly_ai",
        candidate_id=candidate_id if changes else None,
        status="ready" if changes else "none",
        summary=parsed["summary"].strip() or _candidate_summary(changes, {}),
        paths={
            "report_json": str(report_json_path),
            "report_md": str(report_md_path),
            "prompt_bundle": str(prompt_bundle_path),
            "response_json": str(response_path),
            "patch": str(patch_path),
            "candidate_config": str(candidate_path) if candidate_path else "",
        },
        generated_at=now.isoformat(),
        last_result="ready" if changes else "no_change",
        change_count=len(changes),
        top_changes=[change.path for change in changes[:5]],
    )
    state["running"] = False
    state["last_run_at"] = now.isoformat()
    state["last_result"] = "ready" if changes else "no_change"
    save_tuner_state(state, tuner_state_path)

    return {
        "command": "propose-weekly-ai-tuning",
        "ok": True,
        "status": "success",
        "candidate_id": state["weekly_ai_candidate"]["candidate_id"],
        "candidate_status": state["weekly_ai_candidate"]["status"],
        "candidate_source": "weekly_ai",
        "change_count": len(changes),
        "report_json_path": str(report_json_path),
        "report_md_path": str(report_md_path),
        "prompt_bundle_path": str(prompt_bundle_path),
        "response_json_path": str(response_path),
        "patch_path": str(patch_path),
        "candidate_config_path": str(candidate_path) if candidate_path else "",
    }


def promote_tuning_candidate(
    *,
    candidate_id: str | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    version_path: str | Path | None = None,
    addon_config_path: str | Path | None = None,
) -> dict[str, Any]:
    state = load_tuner_state(tuner_state_path)
    source = _resolve_ready_candidate_source(state, candidate_id=candidate_id)
    candidate = dict(state[f"{source}_candidate"] or {})
    candidate_path = resolve_repo_path(candidate.get("paths", {}).get("candidate_config") or "")
    if not candidate_path.exists():
        raise TuningError(f"Candidate config is missing: {candidate_path}")

    config_path = resolve_repo_path(config_path)
    config_path.write_text(candidate_path.read_text(encoding="utf-8"), encoding="utf-8")

    repo_root = resolve_repo_path("edec_bot")
    version_path = resolve_repo_path(version_path or (repo_root / "version.py"))
    addon_config_path = resolve_repo_path(addon_config_path or (repo_root / "config.json"))
    new_version = _bump_patch_version(version_path, addon_config_path)

    now = _utcnow().isoformat()
    _set_candidate_record(
        state,
        source=source,
        candidate_id=candidate.get("candidate_id"),
        status="promoted",
        summary=f"Promoted {candidate.get('candidate_id')} to {config_path.name} ({new_version}).",
        paths=dict(candidate.get("paths") or {}),
        generated_at=candidate.get("generated_at"),
        promoted_at=now,
        last_result="promoted",
        change_count=int(candidate.get("change_count") or 0),
        top_changes=list(candidate.get("top_changes") or []),
    )
    _invalidate_other_candidate_after_promotion(state, promoted_source=source, promoted_id=str(candidate.get("candidate_id") or ""))
    state["running"] = False
    state["last_run_at"] = now
    state["last_result"] = "promoted"
    save_tuner_state(state, tuner_state_path)

    return {
        "command": "promote-tuning-candidate",
        "ok": True,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_source": source,
        "config_path": str(config_path),
        "version": new_version,
    }


def reject_tuning_candidate(
    *,
    candidate_id: str | None = None,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    reason: str = "Rejected by operator.",
) -> dict[str, Any]:
    state = load_tuner_state(tuner_state_path)
    source = _resolve_ready_candidate_source(state, candidate_id=candidate_id)
    candidate = dict(state[f"{source}_candidate"] or {})
    now = _utcnow().isoformat()
    _set_candidate_record(
        state,
        source=source,
        candidate_id=candidate.get("candidate_id"),
        status="rejected",
        summary=reason.strip() or "Rejected by operator.",
        paths=dict(candidate.get("paths") or {}),
        generated_at=candidate.get("generated_at"),
        rejected_at=now,
        last_result="rejected",
        change_count=int(candidate.get("change_count") or 0),
        top_changes=list(candidate.get("top_changes") or []),
    )
    state["running"] = False
    state["last_run_at"] = now
    state["last_result"] = "rejected"
    save_tuner_state(state, tuner_state_path)
    return {
        "command": "reject-tuning-candidate",
        "ok": True,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_source": source,
        "status": "rejected",
    }


def maybe_run_tuner_heartbeat(
    *,
    enabled: bool,
    cadence: str,
    due: bool,
    skip_next: bool,
    has_recent_daily_refresh: bool,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    model: str = DEFAULT_WEEKLY_AI_MODEL,
) -> dict[str, Any]:
    if not enabled:
        return {"command": "tuner-heartbeat", "ok": True, "status": "paused"}
    if str(cadence or "").lower() != "weekly":
        return {"command": "tuner-heartbeat", "ok": True, "status": "manual"}
    if skip_next:
        return {"command": "tuner-heartbeat", "ok": True, "status": "skipped"}
    if not due:
        return {"command": "tuner-heartbeat", "ok": True, "status": "idle"}
    if not has_recent_daily_refresh:
        return {
            "command": "tuner-heartbeat",
            "ok": False,
            "status": "blocked",
            "message": "Daily research refresh is stale.",
        }
    result = build_weekly_review_bundle(
        config_path=config_path,
        tuner_state_path=tuner_state_path,
    )
    result.setdefault("command", "tuner-heartbeat")
    return result


def _default_tuner_state() -> dict[str, Any]:
    state = {
        "running": False,
        "last_run_at": None,
        "last_result": None,
        "latest_candidate_id": None,
        "latest_candidate_status": "none",
        "latest_candidate_paths": {},
        "latest_candidate_summary": "",
        "latest_candidate_source": "none",
        "primary_candidate_source": "none",
        "daily_local_candidate": _default_candidate_payload("daily_local"),
        "weekly_ai_candidate": _default_candidate_payload("weekly_ai"),
        "weekly_review_bundle": {
            "generated_at": None,
            "status": "none",
            "summary": "",
            "paths": {},
            "last_result": None,
        },
        "weekly_ai_context": {
            "generated_at": None,
            "path": str(resolve_repo_path(WEEKLY_AI_CONTEXT_PATH)),
            "window_days": DEFAULT_WEEKLY_CONTEXT_DAYS,
            "window_start": None,
            "window_end": None,
            "raw_ref_count": 0,
        },
    }
    _sync_primary_candidate_fields(state)
    return state


def _default_candidate_payload(source: str) -> dict[str, Any]:
    return {
        "source": source,
        "candidate_id": None,
        "status": "none",
        "summary": "",
        "paths": {},
        "generated_at": None,
        "promoted_at": None,
        "rejected_at": None,
        "last_result": None,
        "change_count": 0,
        "top_changes": [],
    }


def _normalize_tuner_state(payload: dict[str, Any]) -> dict[str, Any]:
    state = _deep_merge(_default_tuner_state(), payload)
    legacy_candidate = {
        "candidate_id": state.get("latest_candidate_id"),
        "status": state.get("latest_candidate_status"),
        "summary": state.get("latest_candidate_summary"),
        "paths": dict(state.get("latest_candidate_paths") or {}),
        "generated_at": state.get("last_run_at"),
        "last_result": state.get("last_result"),
    }
    daily_payload = payload.get("daily_local_candidate") if isinstance(payload, dict) else None
    weekly_payload = payload.get("weekly_ai_candidate") if isinstance(payload, dict) else None
    state["daily_local_candidate"] = _normalize_candidate_payload(
        "daily_local",
        daily_payload if isinstance(daily_payload, dict) and daily_payload else legacy_candidate,
    )
    state["weekly_ai_candidate"] = _normalize_candidate_payload(
        "weekly_ai",
        weekly_payload if isinstance(weekly_payload, dict) else {},
    )
    state["weekly_review_bundle"] = _normalize_weekly_review_bundle(state.get("weekly_review_bundle") or {})
    state["weekly_ai_context"] = _normalize_weekly_context(state.get("weekly_ai_context") or {})
    _sync_primary_candidate_fields(state)
    return state


def _normalize_candidate_payload(source: str, payload: dict[str, Any]) -> dict[str, Any]:
    candidate = _deep_merge(_default_candidate_payload(source), payload if isinstance(payload, dict) else {})
    candidate["source"] = source
    candidate["status"] = _normalize_status(candidate.get("status"))
    candidate["paths"] = dict(candidate.get("paths") or {})
    candidate["change_count"] = int(candidate.get("change_count") or 0)
    candidate["top_changes"] = [str(item) for item in (candidate.get("top_changes") or []) if str(item).strip()]
    return candidate


def _normalize_weekly_context(payload: dict[str, Any]) -> dict[str, Any]:
    base = {
        "generated_at": None,
        "path": str(resolve_repo_path(WEEKLY_AI_CONTEXT_PATH)),
        "window_days": DEFAULT_WEEKLY_CONTEXT_DAYS,
        "window_start": None,
        "window_end": None,
        "raw_ref_count": 0,
    }
    base.update(payload if isinstance(payload, dict) else {})
    return base


def _normalize_weekly_review_bundle(payload: dict[str, Any]) -> dict[str, Any]:
    base = {
        "generated_at": None,
        "status": "none",
        "summary": "",
        "paths": {},
        "last_result": None,
    }
    base.update(payload if isinstance(payload, dict) else {})
    base["paths"] = dict(base.get("paths") or {})
    return base


def _sync_primary_candidate_fields(state: dict[str, Any]) -> None:
    primary_source = _select_primary_candidate_source(state)
    display_source = _select_display_candidate_source(state, primary_source=primary_source)
    state["primary_candidate_source"] = primary_source
    if display_source == "none":
        state["latest_candidate_id"] = None
        state["latest_candidate_status"] = "none"
        state["latest_candidate_paths"] = {}
        state["latest_candidate_summary"] = ""
        state["latest_candidate_source"] = "none"
        return
    candidate = dict(state.get(f"{display_source}_candidate") or {})
    state["latest_candidate_id"] = candidate.get("candidate_id")
    state["latest_candidate_status"] = candidate.get("status", "none")
    state["latest_candidate_paths"] = dict(candidate.get("paths") or {})
    state["latest_candidate_summary"] = candidate.get("summary", "")
    state["latest_candidate_source"] = display_source


def _select_primary_candidate_source(state: dict[str, Any]) -> str:
    for source in PRIMARY_SOURCE_ORDER:
        candidate = dict(state.get(f"{source}_candidate") or {})
        if candidate.get("status") == "ready" and candidate.get("candidate_id"):
            return source
    return "none"


def _select_display_candidate_source(state: dict[str, Any], *, primary_source: str) -> str:
    if primary_source != "none":
        return primary_source
    candidates = []
    for source in CANDIDATE_SOURCES:
        candidate = dict(state.get(f"{source}_candidate") or {})
        ts = candidate.get("generated_at") or candidate.get("promoted_at") or candidate.get("rejected_at")
        candidates.append((ts or "", source))
    candidates.sort(reverse=True)
    for _, source in candidates:
        candidate = dict(state.get(f"{source}_candidate") or {})
        if candidate.get("candidate_id"):
            return source
    return "none"


def _set_candidate_record(
    state: dict[str, Any],
    *,
    source: str,
    candidate_id: str | None,
    status: str,
    summary: str,
    paths: dict[str, Any],
    generated_at: str | None,
    last_result: str | None,
    change_count: int = 0,
    top_changes: list[str] | None = None,
    promoted_at: str | None = None,
    rejected_at: str | None = None,
) -> None:
    key = f"{source}_candidate"
    current = _normalize_candidate_payload(source, state.get(key) or {})
    current.update(
        {
            "candidate_id": candidate_id,
            "status": _normalize_status(status),
            "summary": str(summary or ""),
            "paths": dict(paths or {}),
            "generated_at": generated_at,
            "last_result": last_result,
            "change_count": int(change_count or 0),
            "top_changes": [str(item) for item in (top_changes or []) if str(item).strip()],
        }
    )
    if promoted_at is not None:
        current["promoted_at"] = promoted_at
    if rejected_at is not None:
        current["rejected_at"] = rejected_at
    state[key] = _normalize_candidate_payload(source, current)
    _sync_primary_candidate_fields(state)


def _resolve_ready_candidate_source(state: dict[str, Any], *, candidate_id: str | None) -> str:
    if candidate_id:
        for source in PRIMARY_SOURCE_ORDER:
            candidate = dict(state.get(f"{source}_candidate") or {})
            if candidate.get("candidate_id") == candidate_id:
                if candidate.get("status") != "ready":
                    raise TuningError(f"Candidate {candidate_id} is not ready for promotion or rejection.")
                return source
        raise TuningError(f"No ready tuning candidate matches {candidate_id}.")
    primary = _select_primary_candidate_source(state)
    if primary == "none":
        raise TuningError("No ready tuning candidate is available.")
    return primary


def _invalidate_other_candidate_after_promotion(state: dict[str, Any], *, promoted_source: str, promoted_id: str) -> None:
    for source in CANDIDATE_SOURCES:
        if source == promoted_source:
            continue
        candidate = dict(state.get(f"{source}_candidate") or {})
        if candidate.get("status") != "ready":
            continue
        _set_candidate_record(
            state,
            source=source,
            candidate_id=candidate.get("candidate_id"),
            status="rejected",
            summary=f"Superseded after promoting {promoted_id}; rebuild this candidate on the new config base.",
            paths=dict(candidate.get("paths") or {}),
            generated_at=candidate.get("generated_at"),
            rejected_at=_utcnow().isoformat(),
            last_result="rejected",
            change_count=int(candidate.get("change_count") or 0),
            top_changes=list(candidate.get("top_changes") or []),
        )


def _normalize_status(value: Any) -> str:
    text = str(value or "none").strip().lower()
    return text if text in TUNER_STATUSES else "none"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _discover_latest_session_bundle() -> SessionBundle:
    bundles = _discover_session_bundles()
    if not bundles:
        raise TuningError("No paired session export trades/signals files were found.")
    return bundles[0]


def _build_tuning_input_bundle(tracker_db_path: str | Path) -> TuningInputBundle:
    tracker_path = resolve_repo_path(tracker_db_path)
    if tracker_path.exists():
        try:
            analysis = _analyze_tracker_db(tracker_path)
        except sqlite3.DatabaseError:
            analysis = {}
        if analysis:
            closed = int(((analysis.get("overall") or {}).get("closed")) or 0)
            signal_count = int(((analysis.get("filter_analysis") or {}).get("total_signals")) or 0)
            if closed > 0 or signal_count > 0:
                return TuningInputBundle(
                    source="tracker_db",
                    export_id=f"tracker_db:{tracker_path.name}",
                    trades_path=None,
                    signals_path=None,
                    tracker_db_path=tracker_path,
                    analysis=analysis,
                )
    try:
        bundle = _discover_latest_session_bundle()
    except TuningError:
        if tracker_path.exists():
            raise TuningError(
                f"No usable tracker DB outcomes or paired session exports were found for {tracker_path}."
            )
        raise TuningError("No usable tracker DB or paired session exports were found.")
    return TuningInputBundle(
        source="session_export",
        export_id=bundle.export_id,
        trades_path=bundle.trades_path,
        signals_path=bundle.signals_path,
        tracker_db_path=tracker_path if tracker_path.exists() else None,
        analysis=_analyze_session_exports(bundle.trades_path, bundle.signals_path),
    )


def _discover_session_bundles() -> list[SessionBundle]:
    bundles: list[SessionBundle] = []
    seen_export_ids: set[str] = set()
    for root in discover_session_export_roots():
        trades_files = sorted(root.rglob("*_session_trades.csv")) + sorted(root.rglob("*_session_trades.csv.gz"))
        for trades_path in trades_files:
            signals_path = _matching_signals_path(trades_path)
            if signals_path is None:
                continue
            export_id = trades_path.parent.name
            if export_id in seen_export_ids:
                continue
            seen_export_ids.add(export_id)
            bundles.append(
                SessionBundle(
                    export_id=export_id,
                    trades_path=trades_path.resolve(),
                    signals_path=signals_path.resolve(),
                    bundle_time=_parse_bundle_time(export_id),
                )
            )
    bundles.sort(
        key=lambda item: (
            item.bundle_time.isoformat() if item.bundle_time else "",
            item.export_id,
        ),
        reverse=True,
    )
    return bundles


def _matching_signals_path(trades_path: Path) -> Path | None:
    name = trades_path.name
    for suffix in ("_session_trades.csv.gz", "_session_trades.csv"):
        if not name.endswith(suffix):
            continue
        signal_name = name.replace("_session_trades", "_session_signals")
        for candidate in (
            trades_path.with_name(signal_name),
            trades_path.with_name(signal_name.replace(".csv.gz", ".csv")),
            trades_path.with_name(signal_name.replace(".csv", ".csv.gz")),
        ):
            if candidate.exists():
                return candidate
    return None


def _parse_bundle_time(export_id: str) -> datetime | None:
    prefix = str(export_id or "").split("_EDEC", 1)[0]
    for fmt in ("%Y-%m-%d_%H%M%S", "%Y-%m-%d_%H%M"):
        try:
            return datetime.strptime(prefix, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _open_csv(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with _open_csv(path) as fh:
        rows.extend(csv.DictReader(fh))
    return rows


def _analyze_session_exports(trades_path: Path, signals_path: Path) -> dict[str, Any]:
    trades = _read_csv_rows(trades_path)
    signals = _read_csv_rows(signals_path)
    return _analyze_rows(trades, signals, folder_ts=trades_path.parent.name)


def _analyze_tracker_db(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        reset_at = _tracker_reset_at(conn)
        trades_rows = conn.execute(
            """
            SELECT
                p.timestamp,
                p.coin,
                p.strategy_type,
                p.status,
                p.pnl,
                p.exit_reason,
                p.depth_ratio,
                p.entry_price,
                p.max_bid_seen,
                p.mae,
                d.coin_velocity_30s,
                d.coin_velocity_60s
            FROM paper_trades p
            LEFT JOIN decisions d ON d.id = p.decision_id
            WHERE p.timestamp >= ?
            ORDER BY p.timestamp DESC
            """,
            (reset_at,),
        ).fetchall()
        signal_rows = conn.execute(
            """
            SELECT
                timestamp,
                coin,
                strategy_type,
                action,
                entry_price,
                coin_velocity_30s,
                coin_velocity_60s,
                time_remaining_s,
                entry_depth_side_usd,
                opposite_depth_usd,
                signal_score,
                filter_passed,
                filter_failed,
                reason
            FROM decisions
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
            """,
            (reset_at,),
        ).fetchall()
    finally:
        conn.close()

    trades = [
        {
            "ts": row["timestamp"],
            "c": row["coin"],
            "st": row["strategy_type"],
            "status": row["status"],
            "pnl": row["pnl"],
            "er": row["exit_reason"],
            "drt": row["depth_ratio"],
            "ep": row["entry_price"],
            "maxb": row["max_bid_seen"],
            "mae": row["mae"],
            "v30": row["coin_velocity_30s"],
            "v60": row["coin_velocity_60s"],
        }
        for row in trades_rows
    ]
    signals = [
        {
            "ts": row["timestamp"],
            "c": row["coin"],
            "st": row["strategy_type"],
            "act": row["action"],
            "ep": row["entry_price"],
            "v30": row["coin_velocity_30s"],
            "v60": row["coin_velocity_60s"],
            "te": row["time_remaining_s"],
            "eds": row["entry_depth_side_usd"],
            "ods": row["opposite_depth_usd"],
            "sg": row["signal_score"],
            "fp": row["filter_passed"],
            "ff": row["filter_failed"],
            "why": row["reason"],
        }
        for row in signal_rows
    ]
    latest_ts = max(
        [str(item.get("ts") or "") for item in trades] + [str(item.get("ts") or "") for item in signals],
        default="",
    )
    folder_ts = latest_ts or f"tracker_db:{db_path.name}"
    return _analyze_rows(trades, signals, folder_ts=folder_ts)


def _tracker_reset_at(conn: sqlite3.Connection) -> str:
    try:
        row = conn.execute("SELECT reset_at FROM paper_capital WHERE id = 1").fetchone()
    except sqlite3.DatabaseError:
        return "1970-01-01"
    if not row:
        return "1970-01-01"
    value = row["reset_at"] if isinstance(row, sqlite3.Row) else row[0]
    return str(value or "1970-01-01")


def _analyze_rows(trades: list[dict[str, str]], signals: list[dict[str, str]], *, folder_ts: str) -> dict[str, Any]:
    closed = [row for row in trades if row.get("status") in ("closed_win", "closed_loss")]
    wins = [row for row in closed if row.get("status") == "closed_win"]
    losses = [row for row in closed if row.get("status") == "closed_loss"]
    closed_by_strategy: dict[str, list[dict[str, str]]] = {}
    for row in closed:
        strategy_type = str(row.get("st") or row.get("strategy_type") or "").strip().lower()
        closed_by_strategy.setdefault(strategy_type, []).append(row)

    filter_fail_counts: dict[str, int] = {}
    for row in signals:
        for name in str(row.get("ff") or "").split(","):
            key = name.strip()
            if not key:
                continue
            filter_fail_counts[key] = filter_fail_counts.get(key, 0) + 1

    return {
        "overall": {
            "folder_ts": folder_ts,
            "closed": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "open": sum(1 for row in trades if row.get("status") == "open"),
            "win_pct": _pct(len(wins), len(closed)),
            "total_pnl": round(sum(_flt(row.get("pnl"), 0.0) or 0.0 for row in closed), 4),
        },
        "closed_by_strategy": closed_by_strategy,
        "filter_analysis": {
            "total_signals": len(signals),
            "fail_counts": {
                key: {
                    "n": value,
                    "pct_of_signals": round(value / len(signals) * 100.0, 1) if signals else None,
                }
                for key, value in sorted(filter_fail_counts.items(), key=lambda item: (-item[1], item[0]))
            },
        },
        "advisory_groups": {
            "by_exit_reason": _group_stats(closed, lambda row: str(row.get("er") or "null")),
            "by_coin": _group_stats(closed, lambda row: str(row.get("c") or row.get("coin") or "null").lower()),
            "by_strategy": _group_stats(
                closed,
                lambda row: str(row.get("st") or row.get("strategy_type") or "null").lower(),
            ),
        },
    }


def _recommend_changes(
    config: dict[str, Any],
    analysis: dict[str, Any],
    research_report: dict[str, Any] | None = None,
) -> tuple[list[ProposedChange], list[str], list[str]]:
    changes: list[ProposedChange] = []
    advisories: list[str] = []
    no_change: list[str] = []

    single_rows = list(analysis["closed_by_strategy"].get("single_leg", []))
    lead_rows = list(analysis["closed_by_strategy"].get("lead_lag", []))
    loss_rows = [
        row for rows in (single_rows, lead_rows) for row in rows if row.get("status") == "closed_loss"
    ]

    _recommend_velocity(changes, no_change, config, single_rows, "single_leg.min_velocity_30s")
    _recommend_velocity(changes, no_change, config, lead_rows, "lead_lag.min_velocity_30s")
    _recommend_entry_band(changes, no_change, config, single_rows)
    _recommend_loss_cut(changes, no_change, config, single_rows, "single_leg.loss_cut_pct")
    _recommend_loss_cut(changes, no_change, config, lead_rows, "lead_lag.hard_stop_loss_pct")
    _recommend_high_confidence_bid(changes, no_change, config, single_rows)
    _recommend_disabled_coins(changes, no_change, config, single_rows, "single_leg.disabled_coins")
    _recommend_disabled_coins(changes, no_change, config, lead_rows, "lead_lag.disabled_coins")

    for exit_reason, payload in analysis["advisory_groups"]["by_exit_reason"].items():
        n = int(payload.get("n") or 0)
        win_pct = payload.get("win_pct")
        if n >= 5 and win_pct is not None and win_pct < 35.0:
            advisories.append(f"Exit reason '{exit_reason}' is losing at {win_pct:.1f}% (n={n}) - review exit logic.")

    total_signals = int(analysis["filter_analysis"].get("total_signals") or 0)
    for filter_name, payload in analysis["filter_analysis"]["fail_counts"].items():
        pct_of_signals = payload.get("pct_of_signals")
        if total_signals > 0 and pct_of_signals is not None and pct_of_signals > 30.0:
            advisories.append(
                f"Filter '{filter_name}' is rejecting {pct_of_signals:.1f}% of signals - verify that is intentional."
            )

    if len(loss_rows) >= 10:
        loss_drt = [_flt(row.get("drt")) for row in loss_rows if _flt(row.get("drt")) is not None]
        drt_p75 = _ptile(loss_drt, 75)
        if drt_p75 is not None and drt_p75 > 2.0:
            advisories.append(
                f"Losses have p75 depth ratio {drt_p75:.2f} - consider tightening min_book_depth_usd."
            )

    _add_research_context_advisories(advisories, no_change, research_report or {})

    if not changes:
        no_change.append("No config changes met the evidence thresholds in the latest local tuning inputs.")
    return changes, advisories, no_change


def _add_research_context_advisories(
    advisories: list[str],
    no_change: list[str],
    research_report: dict[str, Any],
) -> None:
    if not research_report:
        no_change.append("Warehouse context unavailable for this tuning pass.")
        return
    policy = dict(research_report.get("policy") or {})
    cluster_count = int(policy.get("cluster_count") or 0)
    outcome_count = int(policy.get("outcome_count") or 0)
    if cluster_count <= 0 or outcome_count <= 0:
        no_change.append("Warehouse context is present but has no recent cluster rollups.")
    blocked = [
        f"{str(item.get('name') or '').upper()} ({int(item.get('paper_blocked_clusters') or 0)})"
        for item in (research_report.get("by_coin") or [])
        if int(item.get("paper_blocked_clusters") or 0) > 0
    ]
    if blocked:
        advisories.append(f"Warehouse paper-blocked clusters: {', '.join(blocked[:4])}.")
    flow_rows = research_report.get("fill_flow_5m_1d") or []
    if flow_rows:
        top_flow = ", ".join(
            f"{str(item.get('coin') or '').upper()} {int(item.get('fill_count') or 0)} fills / ${float(item.get('usd_volume') or 0.0):.2f}"
            for item in flow_rows[:3]
        )
        advisories.append(f"Warehouse 24h 5m flow: {top_flow}.")
    crowded = [
        item
        for item in (research_report.get("trader_concentration_5m_1d") or [])
        if float(item.get("top_3_share_pct") or 0.0) >= 80.0 or float(item.get("top_trader_share_pct") or 0.0) >= 50.0
    ]
    if crowded:
        summary = ", ".join(
            f"{str(item.get('coin') or '').upper()} top-3 {float(item.get('top_3_share_pct') or 0.0):.1f}%"
            for item in crowded[:3]
        )
        advisories.append(f"Warehouse crowding is elevated: {summary}.")


def _recommend_velocity(
    changes: list[ProposedChange],
    no_change: list[str],
    config: dict[str, Any],
    rows: list[dict[str, str]],
    path: str,
) -> None:
    if len(rows) < 5:
        no_change.append(f"{path}: insufficient closed trades (n={len(rows)}).")
        return
    buckets = _group_stats(rows, lambda row: _v30_bucket(_flt(row.get("v30"))))
    candidate: float | None = None
    for bucket_name, payload in _sorted_range_buckets(buckets):
        if int(payload.get("n") or 0) < 5:
            continue
        if (payload.get("win_pct") or 0.0) >= 45.0:
            candidate = _bucket_lower_bound(bucket_name)
            break
    current = float(_get_nested(config, path) or 0.0)
    if candidate is None:
        no_change.append(f"{path}: no velocity bucket reached the win-rate threshold.")
        return
    if candidate > current + 0.01:
        _set_nested(config, path, round(candidate, 2))
        changes.append(
            ProposedChange(
                path=path,
                current=current,
                recommended=round(candidate, 2),
                evidence=f"Lowest viable velocity bucket starts at {candidate:.2f}.",
            )
        )
        return
    no_change.append(f"{path}: current threshold {current:.2f} already matches the viable bucket floor.")


def _recommend_entry_band(
    changes: list[ProposedChange],
    no_change: list[str],
    config: dict[str, Any],
    rows: list[dict[str, str]],
) -> None:
    if len(rows) < 5:
        no_change.append(f"single_leg.entry_min / single_leg.entry_max: insufficient closed trades (n={len(rows)}).")
        return
    buckets = _group_stats(rows, lambda row: _ep_bucket(_flt(row.get("ep"))))
    viable = [
        (bucket_name, payload)
        for bucket_name, payload in _sorted_range_buckets(buckets)
        if int(payload.get("n") or 0) >= 5 and (payload.get("win_pct") or 0.0) >= 45.0
    ]
    current_min = float(_get_nested(config, "single_leg.entry_min") or 0.0)
    current_max = float(_get_nested(config, "single_leg.entry_max") or 0.0)
    if not viable:
        no_change.append(
            "single_leg.entry_min / single_leg.entry_max: no entry-price bucket cleared the win-rate threshold."
        )
        return
    changed = False
    new_min = _bucket_lower_bound(viable[0][0])
    new_max = _bucket_upper_bound(viable[-1][0])
    if new_min > current_min + 0.01:
        _set_nested(config, "single_leg.entry_min", round(new_min, 2))
        changes.append(
            ProposedChange(
                path="single_leg.entry_min",
                current=current_min,
                recommended=round(new_min, 2),
                evidence=f"First viable entry bucket begins at {new_min:.2f}.",
            )
        )
        changed = True
    if new_max < current_max - 0.01:
        _set_nested(config, "single_leg.entry_max", round(new_max, 2))
        changes.append(
            ProposedChange(
                path="single_leg.entry_max",
                current=current_max,
                recommended=round(new_max, 2),
                evidence=f"Highest viable entry bucket tops out at {new_max:.2f}.",
            )
        )
        changed = True
    if not changed:
        no_change.append(
            "single_leg.entry_min / single_leg.entry_max: current band already covers the viable entry buckets."
        )


def _recommend_loss_cut(
    changes: list[ProposedChange],
    no_change: list[str],
    config: dict[str, Any],
    rows: list[dict[str, str]],
    path: str,
) -> None:
    if len(rows) < 10:
        no_change.append(f"{path}: insufficient closed trades (n={len(rows)}).")
        return
    mae_as_pct = []
    for row in rows:
        mae = _flt(row.get("mae"))
        entry_price = _flt(row.get("ep"))
        if mae is None or not entry_price or entry_price <= 0:
            continue
        mae_as_pct.append(abs(mae) / entry_price)
    mae_p75 = _ptile(mae_as_pct, 75)
    current = float(_get_nested(config, path) or 0.0)
    if mae_p75 is None:
        no_change.append(f"{path}: MAE distribution is incomplete in the latest local inputs.")
        return
    calibrated = round(mae_p75 + 0.02, 2)
    if abs(calibrated - current) >= 0.02:
        _set_nested(config, path, calibrated)
        changes.append(
            ProposedChange(
                path=path,
                current=current,
                recommended=calibrated,
                evidence=f"MAE p75 is {mae_p75:.1%}; calibrated stop adds a 2% buffer.",
            )
        )
        return
    no_change.append(f"{path}: current stop {current:.2f} is already close to MAE p75 calibration.")


def _recommend_high_confidence_bid(
    changes: list[ProposedChange],
    no_change: list[str],
    config: dict[str, Any],
    rows: list[dict[str, str]],
) -> None:
    path = "single_leg.high_confidence_bid"
    if len(rows) < 10:
        no_change.append(f"{path}: insufficient closed trades (n={len(rows)}).")
        return
    maxb = [_flt(row.get("maxb")) for row in rows if _flt(row.get("maxb")) is not None]
    maxb_p50 = _ptile(maxb, 50)
    current = float(_get_nested(config, path) or 0.0)
    if maxb_p50 is None:
        no_change.append(f"{path}: max bid distribution is incomplete in the latest local inputs.")
        return
    if maxb_p50 > current + 0.04:
        recommended = round(current + 0.02, 2)
        _set_nested(config, path, recommended)
        changes.append(
            ProposedChange(
                path=path,
                current=current,
                recommended=recommended,
                evidence=f"Median max bid {maxb_p50:.3f} sits well above the current HC threshold.",
            )
        )
        return
    if maxb_p50 < current - 0.03:
        recommended = round(max(0.0, current - 0.02), 2)
        _set_nested(config, path, recommended)
        changes.append(
            ProposedChange(
                path=path,
                current=current,
                recommended=recommended,
                evidence=f"Median max bid {maxb_p50:.3f} is below the current HC threshold.",
            )
        )
        return
    no_change.append(f"{path}: median max bid {maxb_p50:.3f} does not justify a threshold move.")


def _recommend_disabled_coins(
    changes: list[ProposedChange],
    no_change: list[str],
    config: dict[str, Any],
    rows: list[dict[str, str]],
    path: str,
) -> None:
    if len(rows) < 5:
        no_change.append(f"{path}: insufficient closed trades (n={len(rows)}).")
        return
    grouped = _group_stats(rows, lambda row: str(row.get("c") or row.get("coin") or "").lower())
    current = [str(item).lower() for item in (_get_nested(config, path) or [])]
    additions = []
    for coin, payload in sorted(grouped.items()):
        if not coin or coin in current:
            continue
        n = int(payload.get("n") or 0)
        win_pct = payload.get("win_pct")
        if n >= 5 and win_pct is not None and win_pct < 40.0:
            additions.append((coin, n, win_pct))
    if not additions:
        no_change.append(f"{path}: no coin failed the disable threshold.")
        return
    updated = sorted(set(current + [coin for coin, _, _ in additions]))
    _set_nested(config, path, updated)
    evidence = "; ".join(f"{coin}: {win_pct:.1f}% wins (n={n})" for coin, n, win_pct in additions)
    changes.append(
        ProposedChange(
            path=path,
            current=current,
            recommended=updated,
            evidence=evidence,
        )
    )


def _build_daily_snapshots(current_config: dict[str, Any], *, window_days: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    bundles = _discover_session_bundles()
    if not bundles:
        return [], []
    now = _utcnow()
    day_cutoff = now.date() - timedelta(days=max(int(window_days) - 1, 0))
    grouped: dict[str, dict[str, Any]] = {}
    raw_refs: list[dict[str, Any]] = []
    for bundle in bundles:
        bundle_day = (bundle.bundle_time or now).date()
        if bundle_day < day_cutoff:
            continue
        analysis = _analyze_session_exports(bundle.trades_path, bundle.signals_path)
        candidate_config = _deep_copy(current_config)
        changes, advisories, _ = _recommend_changes(candidate_config, analysis)
        key = bundle_day.isoformat()
        bucket = grouped.setdefault(
            key,
            {
                "day": key,
                "export_count": 0,
                "closed_trade_count": 0,
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "top_advisories": [],
                "top_changed_params": [],
                "_export_ids": [],
            },
        )
        bucket["export_count"] += 1
        bucket["closed_trade_count"] += int(analysis["overall"].get("closed") or 0)
        bucket["wins"] += int(analysis["overall"].get("wins") or 0)
        bucket["losses"] += int(analysis["overall"].get("losses") or 0)
        bucket["total_pnl"] += float(analysis["overall"].get("total_pnl") or 0.0)
        bucket["top_advisories"].extend(advisories[:3])
        bucket["top_changed_params"].extend(change.path for change in changes[:3])
        bucket["_export_ids"].append(bundle.export_id)
        raw_refs.append(_build_export_ref_payload(bundle, analysis))

    snapshots = []
    for day in sorted(grouped.keys(), reverse=True)[: int(window_days)]:
        bucket = grouped[day]
        closed = int(bucket["closed_trade_count"] or 0)
        wins = int(bucket["wins"] or 0)
        snapshots.append(
            {
                "day": bucket["day"],
                "export_count": int(bucket["export_count"] or 0),
                "closed_trade_count": closed,
                "win_pct": _pct(wins, closed),
                "total_pnl": round(float(bucket["total_pnl"] or 0.0), 4),
                "top_advisories": _unique_trim(bucket["top_advisories"], limit=3),
                "top_changed_params": _unique_trim(bucket["top_changed_params"], limit=5),
                "export_refs": list(bucket["_export_ids"]),
            }
        )

    selected_export_ids = {export_id for snap in snapshots for export_id in snap["export_refs"]}
    raw_refs = [item for item in raw_refs if item["export_id"] in selected_export_ids]
    raw_refs.sort(key=lambda item: item["export_id"], reverse=True)
    return snapshots, raw_refs


def _build_export_ref_payload(bundle: SessionBundle, analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "export_id": bundle.export_id,
        "bundle_day": (bundle.bundle_time.date().isoformat() if bundle.bundle_time else bundle.export_id[:10]),
        "closed_trade_count": int(analysis["overall"].get("closed") or 0),
        "signal_count": int((analysis.get("filter_analysis") or {}).get("total_signals") or 0),
        "win_pct": float(analysis["overall"].get("win_pct") or 0.0),
        "total_pnl": float(analysis["overall"].get("total_pnl") or 0.0),
        "coins": [key for key in sorted((analysis["advisory_groups"]["by_coin"] or {}).keys()) if key and key != "null"],
        "strategies": [
            key
            for key in sorted((analysis["advisory_groups"]["by_strategy"] or {}).keys())
            if key and key != "null"
        ],
    }


def _candidate_overview(candidate: dict[str, Any]) -> dict[str, Any]:
    report_payload = _load_json_file((candidate.get("paths") or {}).get("report_json") or "", default={})
    return {
        "source": candidate.get("source"),
        "candidate_id": candidate.get("candidate_id"),
        "status": candidate.get("status", "none"),
        "summary": candidate.get("summary", ""),
        "generated_at": candidate.get("generated_at"),
        "change_count": int(candidate.get("change_count") or 0),
        "top_changes": [item.get("path") for item in (report_payload.get("changes") or [])[:5] if item.get("path")],
    }


def _compact_research_report(report: dict[str, Any]) -> dict[str, Any]:
    policy = dict(report.get("policy") or {})
    return {
        "generated_at": report.get("generated_at"),
        "cluster_count": int(policy.get("cluster_count") or 0),
        "outcome_count": int(policy.get("outcome_count") or 0),
        "cluster_winners": list(report.get("cluster_winners") or [])[:5],
        "cluster_losers": list(report.get("cluster_losers") or [])[:5],
        "by_coin": list(report.get("by_coin") or [])[:5],
        "by_strategy": list(report.get("by_strategy") or [])[:5],
        "fill_flow_5m_1d": list(report.get("fill_flow_5m_1d") or [])[:5],
        "trader_concentration_5m_1d": list(report.get("trader_concentration_5m_1d") or [])[:5],
    }


def _extract_safe_params(config: dict[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for root_key in SAFE_ROOT_KEYS:
        payload = config.get(root_key)
        if isinstance(payload, dict):
            _flatten_payload(flattened, root_key, payload)
        elif payload is not None:
            flattened[root_key] = payload
    return dict(sorted(flattened.items()))


def _flatten_payload(target: dict[str, Any], prefix: str, payload: dict[str, Any]) -> None:
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            _flatten_payload(target, path, value)
        else:
            target[path] = value


def _read_repo_version() -> str:
    version_path = resolve_repo_path("edec_bot/version.py")
    try:
        text = version_path.read_text(encoding="utf-8")
    except OSError:
        return "unknown"
    for line in text.splitlines():
        if "__version__" not in line:
            continue
        return line.split("=", 1)[-1].strip().strip('"').strip("'")
    return "unknown"


def _config_hash(config_path: Path) -> str:
    return hashlib.sha1(config_path.read_bytes()).hexdigest()[:12]


def _build_weekly_ai_expansion(context_payload: dict[str, Any]) -> dict[str, Any]:
    ref_index = {bundle.export_id: bundle for bundle in _discover_session_bundles()}
    export_summaries: list[dict[str, Any]] = []
    for ref in list(context_payload.get("raw_export_refs") or [])[:3]:
        export_id = str(ref.get("export_id") or "").strip()
        bundle = ref_index.get(export_id)
        if bundle is None:
            continue
        analysis = _analyze_session_exports(bundle.trades_path, bundle.signals_path)
        fail_counts = analysis["filter_analysis"].get("fail_counts") or {}
        top_fail_filters = []
        for name, payload in list(fail_counts.items())[:5]:
            top_fail_filters.append(
                {
                    "name": name,
                    "n": int(payload.get("n") or 0),
                    "pct_of_signals": payload.get("pct_of_signals"),
                }
            )
        export_summaries.append(
            {
                "export_id": export_id,
                "by_coin": _top_group_rows(analysis["advisory_groups"]["by_coin"]),
                "by_strategy": _top_group_rows(analysis["advisory_groups"]["by_strategy"]),
                "by_exit_reason": _top_group_rows(analysis["advisory_groups"]["by_exit_reason"]),
                "top_fail_filters": top_fail_filters,
            }
        )
    return {
        "generated_at": _utcnow().isoformat(),
        "export_summaries": export_summaries,
    }


def _top_group_rows(payload: dict[str, dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
    rows = []
    for name, item in (payload or {}).items():
        rows.append(
            {
                "name": name,
                "n": int(item.get("n") or 0),
                "win_pct": item.get("win_pct"),
                "total_pnl": item.get("total_pnl"),
            }
        )
    rows.sort(key=lambda row: (float(row.get("total_pnl") or 0.0), int(row.get("n") or 0)), reverse=True)
    return rows[:limit]


def _weekly_ai_system_instructions() -> str:
    allowed = ", ".join(SAFE_CONFIG_PREFIXES)
    return (
        "You are producing a weekly config proposal for the EDEC Polymarket bot. "
        "Use only the compact research context and expanded export facts provided. "
        "Recommend changes only when the evidence in the supplied payload is explicit. "
        f"Only recommend config paths under these prefixes: {allowed}. "
        "Do not mention filesystem paths or raw CSV files. "
        "If evidence is insufficient, return an empty recommended_changes list, explain the no-op in summary, "
        "and add next-step checks to followups. "
        "Use raw_refs_used only with export_id values that appear in raw_export_refs."
    )


def _weekly_ai_response_schema() -> dict[str, Any]:
    scalar_or_list = {
        "anyOf": [
            {"type": "number"},
            {"type": "string"},
            {"type": "boolean"},
            {"type": "null"},
            {
                "type": "array",
                "items": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "string"},
                        {"type": "boolean"},
                        {"type": "null"},
                    ]
                },
            },
        ]
    }
    return {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "recommended_changes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "current": scalar_or_list,
                        "recommended": scalar_or_list,
                        "evidence": {"type": "string"},
                        "confidence": {"type": "number"},
                    },
                    "required": ["path", "current", "recommended", "evidence", "confidence"],
                    "additionalProperties": False,
                },
            },
            "risks": {"type": "array", "items": {"type": "string"}},
            "followups": {"type": "array", "items": {"type": "string"}},
            "raw_refs_used": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["summary", "recommended_changes", "risks", "followups", "raw_refs_used"],
        "additionalProperties": False,
    }


def _parse_weekly_ai_output(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TuningError("Weekly AI output must be a JSON object.")
    parsed = {
        "summary": str(payload.get("summary") or "").strip(),
        "recommended_changes": [],
        "risks": [str(item).strip() for item in (payload.get("risks") or []) if str(item).strip()],
        "followups": [str(item).strip() for item in (payload.get("followups") or []) if str(item).strip()],
        "raw_refs_used": [str(item).strip() for item in (payload.get("raw_refs_used") or []) if str(item).strip()],
    }
    changes = payload.get("recommended_changes") or []
    if not isinstance(changes, list):
        raise TuningError("Weekly AI output field `recommended_changes` must be a list.")
    for item in changes:
        if not isinstance(item, dict):
            raise TuningError("Weekly AI output contained a non-object recommended change.")
        path = str(item.get("path") or "").strip()
        evidence = str(item.get("evidence") or "").strip()
        if not path or not evidence:
            raise TuningError("Weekly AI recommended changes require non-empty path and evidence values.")
        confidence = float(item.get("confidence") or 0.0)
        parsed["recommended_changes"].append(
            {
                "path": path,
                "current": item.get("current"),
                "recommended": item.get("recommended"),
                "evidence": evidence,
                "confidence": _clamp(confidence, 0.0, 1.0),
            }
        )
    return parsed


def _render_tuner_markdown(report: dict[str, Any]) -> str:
    research_rollups = report.get("research_rollups") or {}
    lines = [
        f"# Daily Local Tuning Proposal - {report['inputs']['export_id']}",
        "",
        "## Data",
        f"- Trades analysed: {report['data']['closed']} closed ({report['data']['wins']}W / {report['data']['losses']}L)",
        f"- Session win rate: {report['data']['win_pct']:.1f}%",
        f"- Session PnL: ${report['data']['total_pnl']:.4f}",
        f"- Config file: {report['config_name']}",
        "",
        "## Warehouse Context",
        f"- Research clusters: {int(research_rollups.get('cluster_count') or 0)}",
        f"- Research outcomes: {int(research_rollups.get('outcome_count') or 0)}",
        "",
        "## Proposed Config Changes",
    ]
    if report["changes"]:
        lines.extend(["| Parameter | Current | Recommended | Evidence |", "| --- | --- | --- | --- |"])
        for change in report["changes"]:
            lines.append(
                f"| {change['path']} | {change['current']} | {change['recommended']} | {change['evidence']} |"
            )
    else:
        lines.append("None.")
    lines.extend(["", "## Advisory Flags"])
    if report["advisories"]:
        lines.extend(f"- {item}" for item in report["advisories"])
    else:
        lines.append("None.")
    lines.extend(["", "## Parameters With No Change Recommended"])
    lines.extend(f"- {item}" for item in (report["no_change"] or ["None."]))
    return "\n".join(lines).strip() + "\n"


def _render_weekly_ai_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Weekly AI Tuning Proposal",
        "",
        f"- Generated: {report['generated_at']}",
        f"- Model: {report.get('model') or DEFAULT_WEEKLY_AI_MODEL}",
        f"- Config file: {report['config_name']}",
        "",
        "## Summary",
        report.get("summary") or "No summary provided.",
        "",
        "## Proposed Config Changes",
    ]
    if report["changes"]:
        lines.extend(["| Parameter | Current | Recommended | Confidence | Evidence |", "| --- | --- | --- | ---: | --- |"])
        for change in report["changes"]:
            lines.append(
                f"| {change['path']} | {change['current']} | {change['recommended']} | "
                f"{float(change.get('confidence') or 0.0):.2f} | {change['evidence']} |"
            )
    else:
        lines.append("None.")
    lines.extend(["", "## Risks"])
    lines.extend(f"- {item}" for item in (report.get("risks") or ["None."]))
    lines.extend(["", "## Follow-ups"])
    lines.extend(f"- {item}" for item in (report.get("followups") or ["None."]))
    lines.extend(["", "## Raw Export Refs Used"])
    lines.extend(f"- {item}" for item in (report.get("raw_refs_used") or ["None."]))
    return "\n".join(lines).strip() + "\n"


def _desktop_review_prompt(bundle_md_name: str, context_name: str) -> str:
    return (
        f"Read {bundle_md_name} and {context_name}, compare them to the active config, "
        "and propose only safe-surface config changes. Prefer no change when evidence is weak. "
        "If you recommend changes, explain the evidence, risks, and follow-ups, then produce a candidate patch."
    )


def _desktop_weekly_prompt(*, bundle_md_path: Path, config_path: Path, context_path: Path) -> str:
    return "\n".join(
        [
            "Open these files in Codex desktop:",
            f"- {bundle_md_path}",
            f"- {context_path}",
            f"- {config_path}",
            "",
            "Paste this prompt:",
            (
                f"Read {bundle_md_path.name}, {context_path.name}, and {config_path.name}. "
                "Stay inside the safe config surface only: dual_leg.*, single_leg.*, lead_lag.*, "
                "swing_leg.*, research.*, risk.*. Prefer no change when evidence is weak. "
                "Start with a short conclusion on whether any config change is warranted. "
                "If changes are justified, propose only safe-surface config changes and produce "
                "a candidate patch against the active config with concise evidence, risks, and follow-ups. "
                "Do not touch HA or add-on files."
            ),
        ]
    )


def _render_weekly_review_bundle_markdown(bundle: dict[str, Any]) -> str:
    config = bundle.get("config") or {}
    weekly_context = bundle.get("weekly_context") or {}
    daily_candidate = bundle.get("daily_local_candidate") or {}
    artifacts = bundle.get("artifacts") or {}
    lines = [
        "# Weekly Desktop Review Bundle",
        "",
        f"- Generated: {bundle.get('generated_at')}",
        f"- Config: {config.get('name')} ({config.get('version')} / {config.get('hash')})",
        f"- Context window: {weekly_context.get('window_start')} -> {weekly_context.get('window_end')}",
        f"- Daily snapshots: {weekly_context.get('snapshot_count')}",
        f"- Raw export refs: {weekly_context.get('raw_ref_count')}",
        f"- Copy-paste prompt file: {artifacts.get('desktop_prompt') or 'weekly_desktop_prompt.txt'}",
        "",
        "## Desktop Prompt",
        bundle.get("desktop_review_prompt") or "",
        "",
        "## Quick Paste Prompt",
        bundle.get("desktop_weekly_prompt") or "",
        "",
        "## Open These Files",
        f"- {artifacts.get('bundle_md') or 'weekly_review_bundle.md'}",
        f"- {artifacts.get('context_json') or 'weekly_ai_context.json'}",
        f"- {config.get('name') or 'config_phase_a_single.yaml'}",
        "",
        "## Safe Config Surface",
    ]
    lines.extend(f"- {item}" for item in (bundle.get("safe_config_surface") or []))
    lines.extend(
        [
            "",
            "## Daily Local Candidate",
            f"- Status: {daily_candidate.get('status')}",
            f"- Summary: {daily_candidate.get('summary') or 'None'}",
            f"- Top changes: {', '.join(daily_candidate.get('top_changes') or []) or 'None'}",
            "",
            "## Review Checklist",
        ]
    )
    lines.extend(f"- {item}" for item in (bundle.get("review_checklist") or []))
    lines.extend(["", "## Daily Snapshots"])
    snapshots = bundle.get("daily_snapshots") or []
    if snapshots:
        lines.extend(["| Day | Closed | Win % | PnL | Top Params |", "| --- | ---: | ---: | ---: | --- |"])
        for item in snapshots:
            lines.append(
                f"| {item.get('day')} | {int(item.get('closed_trade_count') or 0)} | "
                f"{float(item.get('win_pct') or 0.0):.1f} | {float(item.get('total_pnl') or 0.0):.4f} | "
                f"{', '.join(item.get('top_changed_params') or []) or 'None'} |"
            )
    else:
        lines.append("No daily snapshots available.")
    lines.extend(["", "## Raw Export Refs"])
    refs = bundle.get("raw_export_refs") or []
    if refs:
        lines.extend(["| Export ID | Closed | Win % | PnL | Coins | Strategies |", "| --- | ---: | ---: | ---: | --- | --- |"])
        for item in refs:
            lines.append(
                f"| {item.get('export_id')} | {int(item.get('closed_trade_count') or 0)} | "
                f"{float(item.get('win_pct') or 0.0):.1f} | {float(item.get('total_pnl') or 0.0):.4f} | "
                f"{', '.join(item.get('coins') or []) or 'None'} | {', '.join(item.get('strategies') or []) or 'None'} |"
            )
    else:
        lines.append("No raw export refs available.")
    return "\n".join(lines).strip() + "\n"


def _enforce_safe_change_surface(changes: list[ProposedChange]) -> None:
    for change in changes:
        if not change.path.startswith(SAFE_CONFIG_PREFIXES):
            raise TuningError(f"Unsafe config path outside tuning surface: {change.path}")


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise TuningError(f"Expected mapping at root of config: {path}")
    return data


def _dump_yaml(payload: dict[str, Any]) -> str:
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)


def _diff_text(current_text: str, candidate_text: str, current_path: Path, candidate_path: Path) -> str:
    return "".join(
        difflib.unified_diff(
            current_text.splitlines(keepends=True),
            candidate_text.splitlines(keepends=True),
            fromfile=str(current_path),
            tofile=str(candidate_path),
        )
    )


def _get_nested(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for segment in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    return current


def _set_nested(payload: dict[str, Any], path: str, value: Any) -> None:
    current = payload
    parts = path.split(".")
    for segment in parts[:-1]:
        next_payload = current.get(segment)
        if not isinstance(next_payload, dict):
            next_payload = {}
            current[segment] = next_payload
        current = next_payload
    current[parts[-1]] = value


def _flt(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _pct(wins: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(wins / total * 100.0, 1)


def _ptile(values: list[float | None], percentile: float) -> float | None:
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        return None
    k = (len(ordered) - 1) * float(percentile) / 100.0
    lower = int(k)
    upper = min(lower + 1, len(ordered) - 1)
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (k - lower), 4)


def _group_stats(rows: list[dict[str, str]], key_fn) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(key_fn(row) or "null")
        bucket = grouped.setdefault(key, {"n": 0, "wins": 0, "losses": 0, "pnl": []})
        bucket["n"] += 1
        if row.get("status") == "closed_win":
            bucket["wins"] += 1
        if row.get("status") == "closed_loss":
            bucket["losses"] += 1
        pnl = _flt(row.get("pnl"))
        if pnl is not None:
            bucket["pnl"].append(pnl)
    result: dict[str, dict[str, Any]] = {}
    for key, payload in grouped.items():
        pnl_rows = payload["pnl"]
        result[key] = {
            "n": int(payload["n"]),
            "wins": int(payload["wins"]),
            "losses": int(payload["losses"]),
            "win_pct": _pct(int(payload["wins"]), int(payload["wins"]) + int(payload["losses"])),
            "total_pnl": round(sum(pnl_rows), 4),
            "avg_pnl": round(mean(pnl_rows), 4) if pnl_rows else None,
        }
    return result


def _sorted_range_buckets(buckets: dict[str, dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    return sorted(buckets.items(), key=lambda item: (_bucket_lower_bound(item[0]), item[0]))


def _bucket_lower_bound(bucket: str) -> float:
    text = str(bucket or "").strip()
    if text.startswith("<"):
        return 0.0
    if text.endswith("+"):
        return float(text[:-1])
    if "-" in text:
        return float(text.split("-", 1)[0])
    return 0.0


def _bucket_upper_bound(bucket: str) -> float:
    text = str(bucket or "").strip()
    if text.startswith("<"):
        return float(text[1:])
    if text.endswith("+"):
        return float(text[:-1])
    if "-" in text:
        return float(text.split("-", 1)[1])
    return 0.0


def _ep_bucket(entry_price: float | None) -> str:
    if entry_price is None:
        return "null"
    if entry_price < 0.50:
        return "<0.50"
    if entry_price < 0.52:
        return "0.50-0.52"
    if entry_price < 0.54:
        return "0.52-0.54"
    if entry_price < 0.56:
        return "0.54-0.56"
    if entry_price < 0.58:
        return "0.56-0.58"
    if entry_price < 0.60:
        return "0.58-0.60"
    if entry_price < 0.63:
        return "0.60-0.63"
    if entry_price < 0.66:
        return "0.63-0.66"
    return "0.66+"


def _v30_bucket(velocity: float | None) -> str:
    if velocity is None:
        return "null"
    absolute = abs(velocity)
    if absolute < 0.04:
        return "<0.04"
    if absolute < 0.06:
        return "0.04-0.06"
    if absolute < 0.08:
        return "0.06-0.08"
    if absolute < 0.10:
        return "0.08-0.10"
    if absolute < 0.12:
        return "0.10-0.12"
    if absolute < 0.15:
        return "0.12-0.15"
    if absolute < 0.20:
        return "0.15-0.20"
    return "0.20+"


def _candidate_summary(changes: list[ProposedChange], overall: dict[str, Any]) -> str:
    closed = int(overall.get("closed") or 0)
    win_pct = overall.get("win_pct") or 0.0
    if not changes:
        if closed:
            return f"No config changes proposed from {closed} closed trades."
        return "No config changes proposed."
    return f"{len(changes)} config changes proposed from {closed} closed trades at {win_pct:.1f}% win rate."


def _load_json_file(path: str | Path, *, default: dict[str, Any]) -> dict[str, Any]:
    resolved = resolve_repo_path(path) if path else None
    if not resolved or not resolved.exists():
        return dict(default)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return dict(default)
    return payload if isinstance(payload, dict) else dict(default)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    if hasattr(value, "__dict__"):
        return _jsonable(vars(value))
    return str(value)


def _extract_response_refusal(response: Any) -> str | None:
    output = getattr(response, "output", None) or []
    for item in output:
        content = getattr(item, "content", None) or []
        for chunk in content:
            if getattr(chunk, "type", None) == "refusal":
                return str(getattr(chunk, "refusal", "") or "").strip() or None
    return None


def _unique_trim(values: list[str], *, limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        key = str(item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= limit:
            break
    return out


def _deep_copy(payload: Any) -> Any:
    return json.loads(json.dumps(payload))


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _bump_patch_version(version_path: Path, addon_config_path: Path) -> str:
    version_text = version_path.read_text(encoding="utf-8").strip()
    current = version_text.split("=", 1)[-1].strip().strip('"').strip("'")
    parts = [int(part) for part in current.split(".")]
    if len(parts) != 3:
        raise TuningError(f"Unsupported version format: {current}")
    parts[2] += 1
    if parts[2] % 10 == 0:
        parts[2] += 1
    next_version = ".".join(str(part) for part in parts)
    version_path.write_text(f'__version__ = "{next_version}"\n', encoding="utf-8")

    addon_payload = json.loads(addon_config_path.read_text(encoding="utf-8"))
    addon_payload["version"] = next_version
    addon_config_path.write_text(json.dumps(addon_payload, indent=2) + "\n", encoding="utf-8")
    return next_version
