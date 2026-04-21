import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from research import artifacts
from research import _duckdb as optional_duckdb
from research.cli import build_parser


class ResearchOptionalDependencyTests(unittest.TestCase):
    def test_require_duckdb_raises_clear_error(self):
        with (
            mock.patch.object(optional_duckdb, "_duckdb", None),
            mock.patch.object(optional_duckdb, "_IMPORT_ERROR", ModuleNotFoundError("No module named 'duckdb'")),
        ):
            with self.assertRaisesRegex(ModuleNotFoundError, "duckdb is required for research warehouse commands"):
                optional_duckdb.require_duckdb()

    def test_build_fill_summary_skips_duckdb_when_warehouse_is_missing(self):
        missing_path = ROOT / ".tmp_testdata" / "missing_warehouse.duckdb"
        with mock.patch.object(optional_duckdb, "_duckdb", None):
            summary = artifacts._build_fill_summary(missing_path, cutoff=datetime.now(timezone.utc))

        self.assertEqual(summary, {"fill_flow_5m_1d": [], "trader_concentration_5m_1d": []})

    def test_build_parser_does_not_need_duckdb(self):
        with mock.patch.object(optional_duckdb, "_duckdb", None):
            parser = build_parser()

        self.assertEqual(parser.prog, "python -m edec_bot.research")
