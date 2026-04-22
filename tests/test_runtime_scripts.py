import json
import os
import shutil
import sqlite3
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "edec_bot"))

from scripts.orderbook_monitor import render_monitor_snapshot
from scripts.reset_runtime_state import reset_runtime_state


class RuntimeScriptTests(unittest.TestCase):
    def test_render_monitor_snapshot_contains_core_fields(self):
        snapshot = {
            "coin": "btc",
            "market": SimpleNamespace(slug="btc-updown-5m-123"),
            "agg": SimpleNamespace(price=87000.5),
            "up_book": SimpleNamespace(best_bid=0.48, best_ask=0.49, bid_depth_usd=120.0, ask_depth_usd=140.0),
            "down_book": SimpleNamespace(best_bid=0.51, best_ask=0.52, bid_depth_usd=130.0, ask_depth_usd=150.0),
            "reference_price": 86950.0,
            "gap": 50.5,
            "time_remaining_s": 87.0,
            "sources": {"binance": 87001.0, "coinbase": 87000.0},
            "source_ages": {"binance": 0.3, "coinbase": 0.7},
        }

        rendered = render_monitor_snapshot(snapshot)

        self.assertIn("active slug: btc-updown-5m-123", rendered)
        self.assertIn("aggregated price: 87000.5000", rendered)
        self.assertIn("price to beat: 86950.0000", rendered)
        self.assertIn("current gap: 50.5000", rendered)
        self.assertIn("sources: binance=87001.00, coinbase=87000.00", rendered)

    def test_reset_runtime_state_clears_row_and_stale_lock(self):
        scratch = ROOT / ".tmp_testdata" / "runtime_reset"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        db_path = scratch / "decisions.db"
        lock_path = scratch / "edec.pid"
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE runtime_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL,
                state_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO runtime_state (id, version, updated_at, state_json) VALUES (1, 1, '2026-04-21T00:00:00Z', ?)",
            (json.dumps({"paused": True}),),
        )
        conn.commit()
        conn.close()
        lock_path.write_text(json.dumps({"pid": 0, "created_at": "old"}), encoding="utf-8")

        result = reset_runtime_state(db_path=str(db_path), lock_path=str(lock_path))

        self.assertTrue(result["runtime_state_cleared"])
        self.assertTrue(result["stale_lock_removed"])
        self.assertFalse(lock_path.exists())
        conn = sqlite3.connect(db_path)
        try:
            remaining = conn.execute("SELECT COUNT(*) FROM runtime_state").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(remaining, 0)

    def test_reset_runtime_state_refuses_active_lock(self):
        scratch = ROOT / ".tmp_testdata" / "runtime_reset_active"
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(scratch, ignore_errors=True))
        db_path = scratch / "decisions.db"
        lock_path = scratch / "edec.pid"
        sqlite3.connect(db_path).close()
        lock_path.write_text(json.dumps({"pid": os.getpid(), "created_at": "now"}), encoding="utf-8")

        with self.assertRaises(RuntimeError):
            reset_runtime_state(db_path=str(db_path), lock_path=str(lock_path))


if __name__ == "__main__":
    unittest.main()
