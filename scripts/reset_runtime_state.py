"""Clear persisted runtime state and remove only stale PID locks."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.process_lock import clear_stale_pid_lock, default_lock_path, read_pid_lock  # noqa: E402


def reset_runtime_state(*, db_path: str, lock_path: str) -> dict[str, object]:
    lock_data = read_pid_lock(lock_path)
    lock_removed = False
    if lock_data:
        lock_removed = clear_stale_pid_lock(lock_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL,
                state_json TEXT NOT NULL
            )
            """
        )
        cursor = conn.execute("DELETE FROM runtime_state WHERE id = 1")
        conn.commit()
    finally:
        conn.close()

    return {
        "runtime_state_cleared": cursor.rowcount > 0,
        "stale_lock_removed": lock_removed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Clear runtime_state and remove stale PID locks.")
    parser.add_argument("--db", default=str(ROOT / "data" / "decisions.db"))
    parser.add_argument("--lock", default=str(default_lock_path(ROOT / "data" / "runtime")))
    args = parser.parse_args()
    try:
        result = reset_runtime_state(db_path=args.db, lock_path=args.lock)
    except RuntimeError as exc:
        print(f"refused: {exc}")
        return 1
    print(
        "runtime_state_cleared={runtime_state_cleared} stale_lock_removed={stale_lock_removed}".format(
            **result
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
