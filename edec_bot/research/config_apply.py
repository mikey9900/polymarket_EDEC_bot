"""Shared helpers for applying, approving, and rolling back live config changes."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import (
    CONFIG_HISTORY_ROOT,
    DEFAULT_CONFIG_PATH,
    LAST_CONFIG_APPLY_RECEIPT_PATH,
    LOOSE_PAPER_BASELINE_PATCH_PATH,
    CODEX_RESTART_REQUEST_PATH,
    discover_local_data_repo_root,
    ensure_runtime_config,
    resolve_repo_path,
)

try:  # Package-safe import for `python -m edec_bot.research`
    from ..bot.config import load_config
except ImportError:  # pragma: no cover - local test path
    from bot.config import load_config


APPROVED_MANIFEST_NAME = "approved_manifest.json"
APPROVED_PATCH_NAME = "approved_patch.json"
APPROVED_CONFIG_NAME = "approved_active_config.yaml"


class ConfigApplyError(Exception):
    """Raised when a reviewed config artifact or apply step is invalid."""


def _tuner_attr(name: str):
    from . import tuner as tuner_module

    return getattr(tuner_module, name)


def _config_hash(path: str | Path) -> str:
    return _tuner_attr("_config_hash")(path)


def _deep_copy(value: Any) -> Any:
    return _tuner_attr("_deep_copy")(value)


def _dump_yaml(payload: dict[str, Any]) -> str:
    return _tuner_attr("_dump_yaml")(payload)


def _enforce_safe_change_surface(changes: list[Any]) -> None:
    _tuner_attr("_enforce_safe_change_surface")(changes)


def _get_nested(payload: dict[str, Any], path: str) -> Any:
    return _tuner_attr("_get_nested")(payload, path)


def _load_yaml(path: str | Path) -> dict[str, Any]:
    return _tuner_attr("_load_yaml")(path)


def _set_nested(payload: dict[str, Any], path: str, value: Any) -> None:
    _tuner_attr("_set_nested")(payload, path, value)


def _safe_config_prefixes() -> list[str]:
    return list(_tuner_attr("SAFE_CONFIG_PREFIXES"))


def publish_reviewed_config(
    *,
    approved_config_path: str | Path,
    data_repo_root: str | Path | None = None,
    live_config_path: str | Path | None = None,
    source_type: str = "manual_review",
    source_ref: str = "",
    summary: str = "",
    apply_mode: str = "manual",
    allow_mismatch: bool = False,
    restart_required: bool = True,
    approval_id: str | None = None,
) -> dict[str, Any]:
    repo_root = _resolve_local_data_repo_root(data_repo_root)
    approved_root = repo_root / "research_exports" / "approved"
    approved_root.mkdir(parents=True, exist_ok=True)

    current_path = Path(live_config_path) if live_config_path else repo_root / "research_exports" / "latest" / "config" / "active_config.yaml"
    reviewed_path = Path(approved_config_path)
    if not current_path.exists():
        raise ConfigApplyError(f"Live config snapshot is missing: {current_path}")
    if not reviewed_path.exists():
        raise ConfigApplyError(f"Reviewed config is missing: {reviewed_path}")

    current_config = _load_yaml(current_path)
    reviewed_config = _load_yaml(reviewed_path)
    changes = _diff_safe_surface(current_config, reviewed_config)
    if not changes:
        raise ConfigApplyError("Reviewed config does not change the safe config surface.")

    _validate_config_payload(reviewed_config, current_path)
    normalized_changes = _normalize_changes(changes)
    approval_id = str(approval_id or _approval_id()).strip()
    manifest = {
        "approval_id": approval_id,
        "created_at": _utcnow().isoformat(),
        "source_type": str(source_type or "manual_review"),
        "source_ref": str(source_ref or reviewed_path.name),
        "summary": str(summary or _approval_summary(normalized_changes)),
        "apply_mode": _normalize_apply_mode(apply_mode),
        "base_config_hash": _hash_text(current_path.read_text(encoding="utf-8")),
        "allow_mismatch": bool(allow_mismatch),
        "restart_required": bool(restart_required),
        "change_count": len(normalized_changes),
    }

    manifest_path = approved_root / APPROVED_MANIFEST_NAME
    patch_path = approved_root / APPROVED_PATCH_NAME
    approved_config_out = approved_root / APPROVED_CONFIG_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    patch_path.write_text(json.dumps(normalized_changes, indent=2, sort_keys=True), encoding="utf-8")
    approved_config_out.write_text(_dump_yaml(reviewed_config), encoding="utf-8")
    return {
        "ok": True,
        "approval_id": approval_id,
        "approved_root": str(approved_root),
        "manifest_path": str(manifest_path),
        "patch_path": str(patch_path),
        "approved_config_path": str(approved_config_out),
        "change_count": len(normalized_changes),
    }


def apply_reviewed_patch(
    *,
    changes: list[dict[str, Any]],
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    action: str,
    summary: str,
    source_type: str,
    source_ref: str,
    approval_id: str | None = None,
    apply_mode: str = "manual",
    allow_mismatch: bool = False,
    restart_required: bool = True,
    requested_by: str = "system",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config_path = ensure_runtime_config(config_path)
    current_config = _load_yaml(config_path)
    normalized_changes = _normalize_changes(changes)
    _enforce_safe_change_surface([_change_obj(change) for change in normalized_changes])

    conflict_paths: list[str] = []
    merged = _deep_copy(current_config)
    for change in normalized_changes:
        path = str(change["path"])
        current_value = _get_nested(current_config, path)
        expected_value = change.get("current", None)
        if change.get("_has_current", False) and not allow_mismatch and not _values_match(current_value, expected_value):
            conflict_paths.append(path)
            continue
        _set_nested(merged, path, _deep_copy(change.get("recommended")))
        change["current"] = _deep_copy(current_value)
    if conflict_paths:
        raise ConfigConflictError(conflict_paths)
    _validate_config_payload(merged, config_path)
    return commit_config_payload(
        target_config=merged,
        config_path=config_path,
        action=action,
        summary=summary,
        source_type=source_type,
        source_ref=source_ref,
        approval_id=approval_id,
        apply_mode=apply_mode,
        restart_required=restart_required,
        requested_by=requested_by,
        changes=normalized_changes,
        metadata=metadata,
    )


def apply_loose_paper_baseline(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    requested_by: str = "dashboard",
) -> dict[str, Any]:
    baseline_changes = json.loads(LOOSE_PAPER_BASELINE_PATCH_PATH.read_text(encoding="utf-8"))
    return apply_reviewed_patch(
        changes=list(baseline_changes),
        config_path=config_path,
        action="reset_loose_baseline",
        summary="Applied loose paper exploration baseline.",
        source_type="baseline_preset",
        source_ref="loose_paper_baseline_v1",
        allow_mismatch=True,
        restart_required=True,
        requested_by=requested_by,
    )


def set_paper_gate_enabled(
    enabled: bool,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    requested_by: str = "dashboard",
) -> dict[str, Any]:
    desired = bool(enabled)
    return apply_reviewed_patch(
        changes=[
            {
                "path": "research.paper_gate_enabled",
                "recommended": desired,
                "evidence": "Operator toggled paper gate from the dashboard.",
            }
        ],
        config_path=config_path,
        action="set_paper_gate",
        summary=f"Paper gate {'enabled' if desired else 'disabled'}.",
        source_type="dashboard_toggle",
        source_ref=f"paper_gate_{'enabled' if desired else 'disabled'}",
        allow_mismatch=True,
        restart_required=True,
        requested_by=requested_by,
        metadata={"paper_gate_enabled": desired},
    )


def rollback_last_config_apply(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    receipt_path: str | Path = LAST_CONFIG_APPLY_RECEIPT_PATH,
    requested_by: str = "dashboard",
) -> dict[str, Any]:
    last_receipt = load_last_config_apply_receipt(receipt_path)
    if not last_receipt:
        raise ConfigApplyError("No previous config apply receipt is available.")
    previous_path = resolve_repo_path(last_receipt.get("previous_config_path") or "")
    if not previous_path.exists():
        raise ConfigApplyError(f"Previous config snapshot is missing: {previous_path}")
    previous_config = _load_yaml(previous_path)
    _validate_config_payload(previous_config, ensure_runtime_config(config_path))
    return commit_config_payload(
        target_config=previous_config,
        config_path=config_path,
        action="rollback_config",
        summary=f"Rolled back config from {last_receipt.get('receipt_id') or 'the latest receipt'}.",
        source_type="rollback",
        source_ref=str(last_receipt.get("receipt_id") or ""),
        restart_required=True,
        requested_by=requested_by,
        metadata={"rollback_of_receipt_id": last_receipt.get("receipt_id")},
    )


def commit_config_payload(
    *,
    target_config: dict[str, Any],
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    action: str,
    summary: str,
    source_type: str,
    source_ref: str,
    approval_id: str | None = None,
    apply_mode: str = "manual",
    restart_required: bool = True,
    requested_by: str = "system",
    changes: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config_path = ensure_runtime_config(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    current_exists = config_path.exists()
    current_text = config_path.read_text(encoding="utf-8") if current_exists else ""
    current_hash = _config_hash(config_path) if current_exists else _hash_text(current_text)
    target_text = _dump_yaml(target_config)
    target_hash = _hash_text(target_text)
    changed = current_text != target_text

    stamp = _utcnow().strftime("%Y%m%dT%H%M%SZ")
    slug = _slugify(source_ref or approval_id or action or "config")
    previous_path = CONFIG_HISTORY_ROOT / f"{stamp}_{action}_{slug}_previous.yaml"
    applied_path = CONFIG_HISTORY_ROOT / f"{stamp}_{action}_{slug}_applied.yaml"
    receipt_path = CONFIG_HISTORY_ROOT / f"{stamp}_{action}_{slug}_receipt.json"

    previous_path.write_text(current_text or target_text, encoding="utf-8")
    applied_path.write_text(target_text, encoding="utf-8")
    config_path.write_text(target_text, encoding="utf-8")

    restart_request = {}
    if restart_required and changed:
        restart_request = write_restart_request(
            action=action,
            config_path=config_path,
            config_hash=target_hash,
            requested_by=requested_by,
            approval_id=approval_id,
        )

    receipt = {
        "receipt_id": f"{stamp}-{action}-{slug}",
        "applied_at": _utcnow().isoformat(),
        "action": str(action or "config_apply"),
        "summary": str(summary or ""),
        "source_type": str(source_type or "unknown"),
        "source_ref": str(source_ref or ""),
        "approval_id": str(approval_id or ""),
        "apply_mode": _normalize_apply_mode(apply_mode),
        "requested_by": str(requested_by or "unknown"),
        "restart_required": bool(restart_required),
        "restart_requested": bool(restart_request),
        "changed": bool(changed),
        "config_path": str(config_path),
        "previous_config_path": str(previous_path),
        "applied_config_path": str(applied_path),
        "config_hash_before": current_hash,
        "config_hash_after": target_hash,
        "change_count": len(changes or []),
        "changes": list(changes or []),
        "restart_request": restart_request,
        "metadata": dict(metadata or {}),
    }
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
    resolve_repo_path(LAST_CONFIG_APPLY_RECEIPT_PATH).write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "ok": True,
        "status": "applied" if changed else "no_change",
        "receipt": receipt,
        "receipt_path": str(receipt_path),
        "history_config_path": str(previous_path),
        "config_path": str(config_path),
        "config_hash": target_hash,
        "restart_requested": bool(restart_request),
    }


def write_restart_request(
    *,
    action: str,
    config_path: str | Path,
    config_hash: str,
    requested_by: str,
    approval_id: str | None = None,
) -> dict[str, Any]:
    request_payload = {
        "request_id": _approval_id(),
        "requested_at": _utcnow().isoformat(),
        "action": str(action or "config_apply"),
        "config_path": str(config_path),
        "config_hash": str(config_hash or ""),
        "requested_by": str(requested_by or "unknown"),
        "approval_id": str(approval_id or ""),
    }
    request_path = resolve_repo_path(CODEX_RESTART_REQUEST_PATH)
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(json.dumps(request_payload, indent=2, sort_keys=True), encoding="utf-8")
    return request_payload


def load_last_config_apply_receipt(path: str | Path = LAST_CONFIG_APPLY_RECEIPT_PATH) -> dict[str, Any]:
    resolved = resolve_repo_path(path)
    if not resolved.exists():
        return {}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


class ConfigConflictError(ConfigApplyError):
    """Raised when an approval expects stale live values."""

    def __init__(self, conflict_paths: list[str]):
        self.conflict_paths = [str(path) for path in conflict_paths if str(path).strip()]
        super().__init__(
            "Approved patch no longer matches the live config. "
            f"Conflicts: {', '.join(self.conflict_paths) or 'unknown'}"
        )


def _resolve_local_data_repo_root(path_value: str | Path | None) -> Path:
    if path_value:
        path = Path(path_value).expanduser()
        if not path.exists():
            raise ConfigApplyError(f"Local data repo root does not exist: {path}")
        return path
    discovered = discover_local_data_repo_root()
    if discovered is None:
        raise ConfigApplyError("Local edec-bot-data checkout was not found.")
    return discovered


def _normalize_changes(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for change in changes or []:
        path = str(change.get("path") or "").strip()
        if not path:
            raise ConfigApplyError("Each approved change must include a path.")
        payload = {
            "path": path,
            "recommended": _deep_copy(change.get("recommended")),
            "evidence": str(change.get("evidence") or "Reviewed config change."),
            "_has_current": "current" in change,
        }
        if "current" in change:
            payload["current"] = _deep_copy(change.get("current"))
        normalized.append(payload)
    return normalized


def _change_obj(change: dict[str, Any]):
    from .tuner import ProposedChange

    return ProposedChange(
        path=str(change["path"]),
        current=change.get("current"),
        recommended=change.get("recommended"),
        evidence=str(change.get("evidence") or "Reviewed config change."),
    )


def _diff_safe_surface(current_config: dict[str, Any], reviewed_config: dict[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for prefix in _safe_config_prefixes():
        root_key = prefix.split(".", 1)[0]
        if root_key not in current_config and root_key not in reviewed_config:
            continue
        _diff_payloads(
            changes,
            current_config.get(root_key),
            reviewed_config.get(root_key),
            prefix=root_key,
        )
    _enforce_safe_change_surface([_change_obj(change) for change in changes])
    return changes


def _diff_payloads(changes: list[dict[str, Any]], current_value: Any, reviewed_value: Any, *, prefix: str) -> None:
    if isinstance(current_value, dict) or isinstance(reviewed_value, dict):
        current_map = current_value if isinstance(current_value, dict) else {}
        reviewed_map = reviewed_value if isinstance(reviewed_value, dict) else {}
        for key in sorted(set(current_map) | set(reviewed_map)):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            _diff_payloads(changes, current_map.get(key), reviewed_map.get(key), prefix=child_prefix)
        return
    if _values_match(current_value, reviewed_value):
        return
    changes.append(
        {
            "path": prefix,
            "current": _deep_copy(current_value),
            "recommended": _deep_copy(reviewed_value),
            "evidence": "Reviewed config change.",
        }
    )


def _validate_config_payload(payload: dict[str, Any], reference_path: Path) -> None:
    target_dir = reference_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    temp_handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yaml",
        prefix="config-validate-",
        dir=target_dir,
        delete=False,
    )
    temp_path = Path(temp_handle.name)
    try:
        temp_handle.write(_dump_yaml(payload))
        temp_handle.close()
        load_config(str(temp_path))
    except Exception as exc:  # noqa: BLE001
        raise ConfigApplyError(f"Config validation failed: {exc}") from exc
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _values_match(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return abs(float(left) - float(right)) <= 1e-9
    return left == right


def _approval_id() -> str:
    return _utcnow().strftime("%Y%m%dT%H%M%SZ")


def _approval_summary(changes: list[dict[str, Any]]) -> str:
    count = len(changes or [])
    if count <= 0:
        return "Reviewed config approval."
    if count == 1:
        return f"Reviewed config approval for {changes[0]['path']}."
    return f"Reviewed config approval for {count} changes."


def _normalize_apply_mode(value: Any) -> str:
    mode = str(value or "manual").strip().lower()
    return "auto" if mode == "auto" else "manual"


def _hash_text(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:12]


def _slugify(value: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    while "--" in text:
        text = text.replace("--", "-")
    return text.strip("-") or "config"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
