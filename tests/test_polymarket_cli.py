import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "edec_bot"))

from bot.polymarket_cli import (  # noqa: E402
    PolymarketCli,
    PolymarketCliCommandBlocked,
    PolymarketCliCommandFailed,
    PolymarketCliParseError,
    PolymarketCliTimeout,
    PolymarketCliUnavailable,
)


class FakeProcess:
    def __init__(self, *, stdout=b"{}", stderr=b"", returncode=0, delay=0.0):
        self._stdout = stdout
        self._stderr = stderr
        self.returncode = returncode
        self.delay = delay
        self.killed = False

    async def communicate(self):
        if self.delay:
            await asyncio.sleep(self.delay)
        return self._stdout, self._stderr

    def kill(self):
        self.killed = True


def make_config(*, allow_mutating=False, timeout_s=0.05, enabled=True):
    return SimpleNamespace(
        cli=SimpleNamespace(
            enabled=enabled,
            binary_path="polymarket",
            timeout_s=timeout_s,
            signature_type="proxy",
            allow_mutating_commands=allow_mutating,
            startup_check=False,
        ),
        private_key="",
    )


class PolymarketCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_binary_missing_raises_unavailable(self):
        with patch("bot.polymarket_cli.shutil.which", return_value=None):
            cli = PolymarketCli(make_config())

        self.assertFalse(cli.is_available)
        with self.assertRaises(PolymarketCliUnavailable):
            await cli.get_wallet_info()

    async def test_timeout_raises_specific_error(self):
        process = FakeProcess(delay=0.2)
        with patch("bot.polymarket_cli.shutil.which", return_value="polymarket"):
            cli = PolymarketCli(make_config(timeout_s=0.01))

        with patch("bot.polymarket_cli.asyncio.create_subprocess_exec", return_value=process):
            with self.assertRaises(PolymarketCliTimeout):
                await cli.get_wallet_info()

        self.assertTrue(process.killed)

    async def test_non_zero_exit_parses_json_error(self):
        process = FakeProcess(stdout=b'{"error":"boom"}', returncode=1)
        with patch("bot.polymarket_cli.shutil.which", return_value="polymarket"):
            cli = PolymarketCli(make_config())

        with patch("bot.polymarket_cli.asyncio.create_subprocess_exec", return_value=process):
            with self.assertRaises(PolymarketCliCommandFailed) as ctx:
                await cli.get_open_orders()

        self.assertIn("boom", str(ctx.exception))

    async def test_malformed_json_raises_parse_error(self):
        process = FakeProcess(stdout=b"not-json", returncode=0)
        with patch("bot.polymarket_cli.shutil.which", return_value="polymarket"):
            cli = PolymarketCli(make_config())

        with patch("bot.polymarket_cli.asyncio.create_subprocess_exec", return_value=process):
            with self.assertRaises(PolymarketCliParseError):
                await cli.get_wallet_info()

    async def test_mutating_command_blocked(self):
        with patch("bot.polymarket_cli.shutil.which", return_value="polymarket"):
            cli = PolymarketCli(make_config(allow_mutating=False))

        with self.assertRaises(PolymarketCliCommandBlocked):
            await cli.cancel_all_orders()
