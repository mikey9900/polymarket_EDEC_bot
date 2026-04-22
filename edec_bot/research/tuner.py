"""Deterministic tuning proposals and promotion helpers."""

from __future__ import annotations

import csv
import difflib
import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import yaml

from .paths import (
    CONFIG_CANDIDATES_ROOT,
    DEFAULT_CONFIG_PATH,
    TUNER_ACTIVE_PATCH_PATH,
    TUNER_REPORT_JSON_PATH,
    TUNER_REPORT_MD_PATH,
    TUNER_STATE_PATH,
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
TUNER_STATUSES = {"none", "ready", "promoted", "rejected"}


class TuningError(RuntimeError):
    """Raised when tuning proposal inputs are missing or invalid."""


@dataclass(frozen=True)
class ProposedChange:
    path: str
    current: Any
    recommended: Any
    evidence: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "current": self.current,
            "recommended": self.recommended,
            "evidence": self.evidence,
        }


def load_tuner_state(path: str | Path = TUNER_STATE_PATH) -> dict[str, Any]:
    tuner_path = resolve_repo_path(path)
    if not tuner_path.exists():
        return _default_tuner_state()
    try:
        payload = json.loads(tuner_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _default_tuner_state()
    state = _default_tuner_state()
    state.update(payload if isinstance(payload, dict) else {})
    state["latest_candidate_status"] = _normalize_status(state.get("latest_candidate_status"))
    state["latest_candidate_paths"] = dict(state.get("latest_candidate_paths") or {})
    return state


def save_tuner_state(state: dict[str, Any], path: str | Path = TUNER_STATE_PATH) -> Path:
    tuner_path = resolve_repo_path(path)
    tuner_path.parent.mkdir(parents=True, exist_ok=True)
    tuner_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
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
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    report_json_path: str | Path = TUNER_REPORT_JSON_PATH,
    report_md_path: str | Path = TUNER_REPORT_MD_PATH,
    patch_path: str | Path = TUNER_ACTIVE_PATCH_PATH,
    candidates_root: str | Path = CONFIG_CANDIDATES_ROOT,
) -> dict[str, Any]:
    ensure_tuner_dirs()
    now = _utcnow()
    config_path = resolve_repo_path(config_path)
    report_json_path = resolve_repo_path(report_json_path)
    report_md_path = resolve_repo_path(report_md_path)
    patch_path = resolve_repo_path(patch_path)
    candidates_root = resolve_repo_path(candidates_root)

    trades_path, signals_path = _discover_latest_session_bundle()
    analysis = _analyze_session_exports(trades_path, signals_path)
    current_config = _load_yaml(config_path)
    candidate_config = json.loads(json.dumps(current_config))

    changes, advisories, no_change = _recommend_changes(candidate_config, analysis)
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
        "candidate_id": candidate_id if changes else None,
        "candidate_status": "ready" if changes else "none",
        "config_path": str(config_path),
        "inputs": {
            "trades_csv": str(trades_path),
            "signals_csv": str(signals_path),
            "folder_ts": trades_path.parent.name,
        },
        "data": analysis["overall"],
        "changes": [change.as_dict() for change in changes],
        "advisories": advisories,
        "no_change": no_change,
    }
    report_json_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")
    report_md_path.write_text(_render_tuner_markdown(report_payload), encoding="utf-8")

    state = load_tuner_state(tuner_state_path)
    state.update(
        {
            "last_run_at": now.isoformat(),
            "last_result": "ready" if changes else "no_change",
            "running": False,
            "latest_candidate_id": candidate_id if changes else None,
            "latest_candidate_status": "ready" if changes else "none",
            "latest_candidate_paths": {
                "report_json": str(report_json_path),
                "report_md": str(report_md_path),
                "patch": str(patch_path),
                "candidate_config": str(candidate_path) if candidate_path else "",
            },
            "latest_candidate_summary": _candidate_summary(changes, analysis["overall"]),
        }
    )
    save_tuner_state(state, tuner_state_path)

    return {
        "command": "propose-tuning",
        "ok": True,
        "candidate_id": state["latest_candidate_id"],
        "candidate_status": state["latest_candidate_status"],
        "config_path": str(config_path),
        "trades_csv": str(trades_path),
        "signals_csv": str(signals_path),
        "change_count": len(changes),
        "report_json_path": str(report_json_path),
        "report_md_path": str(report_md_path),
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
    latest_id = str(state.get("latest_candidate_id") or "")
    latest_status = _normalize_status(state.get("latest_candidate_status"))
    latest_paths = dict(state.get("latest_candidate_paths") or {})
    if latest_status != "ready" or not latest_id:
        raise TuningError("No ready tuning candidate is available for promotion.")
    if candidate_id and candidate_id != latest_id:
        raise TuningError(f"Latest ready candidate is {latest_id}, not {candidate_id}.")

    candidate_path = resolve_repo_path(latest_paths.get("candidate_config") or "")
    if not candidate_path.exists():
        raise TuningError(f"Candidate config is missing: {candidate_path}")

    config_path = resolve_repo_path(config_path)
    config_path.write_text(candidate_path.read_text(encoding="utf-8"), encoding="utf-8")

    repo_root = resolve_repo_path("edec_bot")
    version_path = resolve_repo_path(version_path or (repo_root / "version.py"))
    addon_config_path = resolve_repo_path(addon_config_path or (repo_root / "config.json"))
    new_version = _bump_patch_version(version_path, addon_config_path)

    now = _utcnow().isoformat()
    state.update(
        {
            "last_run_at": now,
            "last_result": "promoted",
            "latest_candidate_status": "promoted",
            "latest_candidate_promoted_at": now,
            "latest_candidate_summary": f"Promoted {latest_id} to {config_path.name} ({new_version}).",
        }
    )
    save_tuner_state(state, tuner_state_path)

    return {
        "command": "promote-tuning-candidate",
        "ok": True,
        "candidate_id": latest_id,
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
    latest_id = str(state.get("latest_candidate_id") or "")
    latest_status = _normalize_status(state.get("latest_candidate_status"))
    if latest_status != "ready" or not latest_id:
        raise TuningError("No ready tuning candidate is available to reject.")
    if candidate_id and candidate_id != latest_id:
        raise TuningError(f"Latest ready candidate is {latest_id}, not {candidate_id}.")

    now = _utcnow().isoformat()
    state.update(
        {
            "last_run_at": now,
            "last_result": "rejected",
            "latest_candidate_status": "rejected",
            "latest_candidate_rejected_at": now,
            "latest_candidate_summary": reason.strip() or "Rejected by operator.",
        }
    )
    save_tuner_state(state, tuner_state_path)
    return {
        "command": "reject-tuning-candidate",
        "ok": True,
        "candidate_id": latest_id,
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
    return propose_tuning(config_path=config_path, tuner_state_path=tuner_state_path)


def _default_tuner_state() -> dict[str, Any]:
    return {
        "running": False,
        "last_run_at": None,
        "last_result": None,
        "latest_candidate_id": None,
        "latest_candidate_status": "none",
        "latest_candidate_paths": {},
        "latest_candidate_summary": "",
    }


def _normalize_status(value: Any) -> str:
    text = str(value or "none").strip().lower()
    return text if text in TUNER_STATUSES else "none"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _discover_latest_session_bundle() -> tuple[Path, Path]:
    bundles: list[tuple[str, Path, Path]] = []
    for root in discover_session_export_roots():
        trades_files = sorted(root.rglob("*_session_trades.csv")) + sorted(root.rglob("*_session_trades.csv.gz"))
        for trades_path in trades_files:
            signals_path = _matching_signals_path(trades_path)
            if signals_path is None:
                continue
            bundles.append((trades_path.parent.name, trades_path.resolve(), signals_path.resolve()))
    if not bundles:
        raise TuningError("No paired session export trades/signals files were found.")
    bundles.sort(key=lambda item: item[0], reverse=True)
    _, trades_path, signals_path = bundles[0]
    return trades_path, signals_path


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


def _open_csv(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")


def _analyze_session_exports(trades_path: Path, signals_path: Path) -> dict[str, Any]:
    trades: list[dict[str, str]] = []
    with _open_csv(trades_path) as fh:
        trades.extend(csv.DictReader(fh))

    closed = [row for row in trades if row.get("status") in ("closed_win", "closed_loss")]
    wins = [row for row in closed if row.get("status") == "closed_win"]
    losses = [row for row in closed if row.get("status") == "closed_loss"]
    closed_by_strategy: dict[str, list[dict[str, str]]] = {}
    for row in closed:
        strategy_type = str(row.get("st") or row.get("strategy_type") or "").strip().lower()
        closed_by_strategy.setdefault(strategy_type, []).append(row)

    signals: list[dict[str, str]] = []
    with _open_csv(signals_path) as fh:
        signals.extend(csv.DictReader(fh))

    filter_fail_counts: dict[str, int] = {}
    for row in signals:
        for name in str(row.get("ff") or "").split(","):
            key = name.strip()
            if not key:
                continue
            filter_fail_counts[key] = filter_fail_counts.get(key, 0) + 1

    return {
        "overall": {
            "folder_ts": trades_path.parent.name,
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

    if not changes:
        no_change.append("No config changes met the evidence thresholds in the latest session export.")
    return changes, advisories, no_change


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
        no_change.append(f"{path}: MAE distribution is incomplete in the latest export.")
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
        no_change.append(f"{path}: max bid distribution is incomplete in the latest export.")
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
        return f"No config changes proposed from {closed} closed trades."
    return f"{len(changes)} config changes proposed from {closed} closed trades at {win_pct:.1f}% win rate."


def _render_tuner_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Tuning Proposal - {report['inputs']['folder_ts']}",
        "",
        "## Data",
        f"- Trades analysed: {report['data']['closed']} closed ({report['data']['wins']}W / {report['data']['losses']}L)",
        f"- Session win rate: {report['data']['win_pct']:.1f}%",
        f"- Session PnL: ${report['data']['total_pnl']:.4f}",
        f"- Config file: {report['config_path']}",
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
