import json
import io
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from research import artifacts
from research import _duckdb as optional_duckdb
from research.cli import build_parser, main


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

    def test_build_parser_exposes_http_retry_flags(self):
        with mock.patch.object(optional_duckdb, "_duckdb", None):
            parser = build_parser()

        args = parser.parse_args(
            [
                "sync-recent-5m-fills",
                "--http-timeout-seconds",
                "12.5",
                "--http-retry-attempts",
                "4",
                "--http-retry-backoff-seconds",
                "0.75",
                "--http-retry-max-backoff-seconds",
                "9",
                "--lookback-hours",
                "1",
            ]
        )

        self.assertEqual(args.command, "sync-recent-5m-fills")
        self.assertEqual(args.http_timeout_seconds, 12.5)
        self.assertEqual(args.http_retry_attempts, 4)
        self.assertEqual(args.http_retry_backoff_seconds, 0.75)
        self.assertEqual(args.http_retry_max_backoff_seconds, 9.0)

    def test_build_parser_exposes_tuning_and_runner_commands(self):
        with mock.patch.object(optional_duckdb, "_duckdb", None):
            parser = build_parser()

        propose_args = parser.parse_args(["propose-tuning"])
        runner_args = parser.parse_args(["codex-runner", "--run-once"])

        self.assertEqual(propose_args.command, "propose-tuning")
        self.assertEqual(runner_args.command, "codex-runner")
        self.assertTrue(runner_args.run_once)

    def test_sync_recent_main_passes_http_retry_flags(self):
        fake_warehouse = mock.MagicMock()
        fake_source = mock.MagicMock()
        fake_source.close = mock.MagicMock()
        fake_warehouse.close = mock.MagicMock()

        with (
            mock.patch("research.warehouse.ResearchWarehouse", return_value=fake_warehouse) as warehouse_ctor,
            mock.patch("research.cli.GoldskyFillSource", return_value=fake_source) as source_ctor,
            mock.patch("research.cli.sync_recent_5m_fills", return_value={"dataset": "recent_5m_fills"}) as sync_mock,
            mock.patch("sys.stdout", new=io.StringIO()),
        ):
            exit_code = main(
                [
                    "sync-recent-5m-fills",
                    "--lookback-hours",
                    "24",
                    "--asset-chunk-size",
                    "20",
                    "--bucket-minutes",
                    "60",
                    "--bucket-buffer-seconds",
                    "900",
                    "--batch-size",
                    "1000",
                    "--max-batches-per-chunk",
                    "2",
                    "--http-timeout-seconds",
                    "12.5",
                    "--http-retry-attempts",
                    "4",
                    "--http-retry-backoff-seconds",
                    "0.75",
                    "--http-retry-max-backoff-seconds",
                    "9",
                ]
            )

        self.assertEqual(exit_code, 0)
        warehouse_ctor.assert_called_once()
        source_ctor.assert_called_once_with(
            timeout_seconds=12.5,
            retry_attempts=4,
            retry_backoff_seconds=0.75,
            retry_max_backoff_seconds=9.0,
        )
        sync_mock.assert_called_once_with(
            fake_warehouse,
            fake_source,
            lookback_hours=24,
            batch_size=1000,
            asset_chunk_size=20,
            bucket_minutes=60,
            bucket_buffer_seconds=900,
            max_batches_per_chunk=2,
        )
        fake_source.close.assert_called_once()
        fake_warehouse.close.assert_called_once()

    def test_daily_refresh_runs_build_even_if_sync_fails(self):
        fake_warehouse = mock.MagicMock()
        fake_source = mock.MagicMock()
        fake_source.close = mock.MagicMock()
        fake_warehouse.close = mock.MagicMock()

        with (
            mock.patch("research.warehouse.ResearchWarehouse", return_value=fake_warehouse),
            mock.patch("research.cli.GoldskyFillSource", return_value=fake_source),
            mock.patch("research.cli.sync_recent_5m_fills", side_effect=RuntimeError("boom")),
            mock.patch("research.artifacts.build_artifacts", return_value={"cluster_count": 1, "outcome_count": 2}),
            mock.patch("sys.stdout", new=io.StringIO()) as stdout,
        ):
            exit_code = main(
                [
                    "daily-refresh",
                    "--lookback-hours",
                    "24",
                    "--asset-chunk-size",
                    "20",
                    "--bucket-minutes",
                    "60",
                    "--bucket-buffer-seconds",
                    "900",
                    "--batch-size",
                    "1000",
                    "--max-batches-per-chunk",
                    "2",
                ]
            )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertFalse(payload["sync"]["ok"])
        self.assertEqual(payload["sync"]["error"]["type"], "RuntimeError")
        self.assertEqual(payload["sync"]["error"]["message"], "boom")
        self.assertTrue(payload["build"]["ok"])
        self.assertEqual(payload["build"]["result"], {"cluster_count": 1, "outcome_count": 2})
        fake_source.close.assert_called_once()
        fake_warehouse.close.assert_called_once()
