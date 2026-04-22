import json
import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.process_lock import acquire_pid_lock, clear_stale_pid_lock, read_pid_lock


class ProcessLockTests(unittest.TestCase):
    def test_acquire_and_release_lock(self):
        scratch = ROOT / ".tmp_testdata" / "process_lock_acquire"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        lock_path = scratch / "edec.pid"

        lock = acquire_pid_lock(lock_path)

        self.assertEqual(read_pid_lock(lock_path)["pid"], os.getpid())
        lock.release()
        self.assertFalse(lock_path.exists())

    def test_stale_lock_is_replaced(self):
        scratch = ROOT / ".tmp_testdata" / "process_lock_stale"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        lock_path = scratch / "edec.pid"
        lock_path.write_text(json.dumps({"pid": 0, "created_at": "old"}), encoding="utf-8")

        lock = acquire_pid_lock(lock_path)

        self.assertEqual(read_pid_lock(lock_path)["pid"], os.getpid())
        lock.release()

    def test_same_pid_lock_is_reclaimed(self):
        scratch = ROOT / ".tmp_testdata" / "process_lock_active"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        lock_path = scratch / "edec.pid"
        lock_path.write_text(json.dumps({"pid": os.getpid(), "created_at": "now"}), encoding="utf-8")

        lock = acquire_pid_lock(lock_path)

        self.assertEqual(read_pid_lock(lock_path)["pid"], os.getpid())
        lock.release()

    def test_active_lock_is_rejected(self):
        scratch = ROOT / ".tmp_testdata" / "process_lock_active_other"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        lock_path = scratch / "edec.pid"
        lock_path.write_text(json.dumps({"pid": 424242, "created_at": "now"}), encoding="utf-8")

        with mock.patch("bot.process_lock.is_pid_running", return_value=True):
            with self.assertRaises(RuntimeError):
                acquire_pid_lock(lock_path)

    def test_clear_stale_lock_refuses_active_pid(self):
        scratch = ROOT / ".tmp_testdata" / "process_lock_clear_active"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        lock_path = scratch / "edec.pid"
        lock_path.write_text(json.dumps({"pid": 424242, "created_at": "now"}), encoding="utf-8")

        with mock.patch("bot.process_lock.is_pid_running", return_value=True):
            with self.assertRaises(RuntimeError):
                clear_stale_pid_lock(lock_path)


if __name__ == "__main__":
    unittest.main()
