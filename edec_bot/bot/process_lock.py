"""Single-process PID lock helpers for runtime safety."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class ProcessLock:
    path: Path
    pid: int

    def release(self) -> None:
        release_pid_lock(self.path, self.pid)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def runtime_dir(base_dir: str | Path = "data/runtime") -> Path:
    path = Path(base_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_lock_path(base_dir: str | Path = "data/runtime") -> Path:
    return runtime_dir(base_dir) / "edec.pid"


def read_pid_lock(lock_path: str | Path) -> dict[str, object] | None:
    path = Path(lock_path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def is_pid_running(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        normalized = int(pid)
    except (TypeError, ValueError):
        return False
    if normalized <= 0:
        return False
    try:
        os.kill(normalized, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def acquire_pid_lock(lock_path: str | Path) -> ProcessLock:
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    current_pid = os.getpid()
    existing = read_pid_lock(path)
    if existing:
        existing_pid = existing.get("pid")
        if int(existing_pid or 0) == int(current_pid):
            existing_pid = None
        if is_pid_running(existing_pid):
            raise RuntimeError(f"Bot is already running with pid={existing_pid}")
    payload = {
        "pid": current_pid,
        "created_at": _utc_now_iso(),
        "cwd": str(Path.cwd()),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return ProcessLock(path=path, pid=int(payload["pid"]))


def release_pid_lock(lock_path: str | Path, owner_pid: int | None = None) -> None:
    path = Path(lock_path)
    if not path.exists():
        return
    if owner_pid is not None:
        data = read_pid_lock(path)
        if data and int(data.get("pid") or 0) != int(owner_pid):
            return
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def clear_stale_pid_lock(lock_path: str | Path) -> bool:
    data = read_pid_lock(lock_path)
    if not data:
        return False
    if is_pid_running(data.get("pid")):
        raise RuntimeError(f"Lock belongs to active pid={data.get('pid')}")
    release_pid_lock(lock_path)
    return True
