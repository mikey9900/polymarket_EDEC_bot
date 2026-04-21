"""Polymarket CLI adapter for optional operator-facing account tooling."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class PolymarketCliError(RuntimeError):
    """Base error for CLI adapter failures."""


class PolymarketCliUnavailable(PolymarketCliError):
    """Raised when the CLI binary is not available in the runtime."""


class PolymarketCliCommandBlocked(PolymarketCliError):
    """Raised when a mutating command is disabled by configuration."""


class PolymarketCliTimeout(PolymarketCliError):
    """Raised when a CLI call exceeds the configured timeout."""


class PolymarketCliParseError(PolymarketCliError):
    """Raised when the CLI returns invalid JSON."""


class PolymarketCliCommandFailed(PolymarketCliError):
    """Raised when the CLI exits with a non-zero status."""

    def __init__(
        self,
        message: str,
        *,
        args: list[str],
        exit_code: int,
        stdout: str,
        stderr: str,
        payload: Any = None,
    ):
        super().__init__(message)
        self.command_args = args
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.payload = payload


@dataclass(frozen=True)
class CliHealthStatus:
    enabled: bool
    available: bool
    healthy: bool
    binary_path: str | None = None
    message: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WalletInfo:
    configured: bool
    address: str | None = None
    proxy_address: str | None = None
    signature_type: str | None = None
    config_path: str | None = None
    source: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AccountStatusInfo:
    closed_only: bool | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BalanceInfo:
    balance: str | None = None
    allowances: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrdersInfo:
    data: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradesInfo:
    data: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CancelAllResult:
    canceled: list[str] = field(default_factory=list)
    not_canceled: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


class PolymarketCli:
    """Thin, whitelist-based JSON adapter over the Polymarket CLI."""

    def __init__(self, config):
        self.enabled = bool(getattr(config.cli, "enabled", True))
        self.binary_path = getattr(config.cli, "binary_path", "polymarket")
        self.timeout_s = float(getattr(config.cli, "timeout_s", 8))
        self.signature_type = str(getattr(config.cli, "signature_type", "proxy")).strip() or "proxy"
        self.allow_mutating_commands = bool(getattr(config.cli, "allow_mutating_commands", False))
        self.startup_check = bool(getattr(config.cli, "startup_check", True))
        self.private_key = (config.private_key or "").strip()
        self._resolved_binary = self._discover_binary()

    @property
    def is_available(self) -> bool:
        return self.enabled and self._resolved_binary is not None

    def unavailable_reason(self) -> str:
        if not self.enabled:
            return "Polymarket CLI is disabled in config."
        if self._resolved_binary is None:
            return (
                "Polymarket CLI not installed in this runtime. "
                "It becomes available when the container image includes the `polymarket` binary."
            )
        return "Polymarket CLI is unavailable."

    async def startup_healthcheck(self) -> CliHealthStatus:
        if not self.enabled:
            return CliHealthStatus(
                enabled=False,
                available=False,
                healthy=False,
                binary_path=None,
                message="Polymarket CLI disabled via config.",
            )

        if not self._resolved_binary:
            return CliHealthStatus(
                enabled=True,
                available=False,
                healthy=False,
                binary_path=None,
                message=self.unavailable_reason(),
            )

        if not self.startup_check:
            return CliHealthStatus(
                enabled=True,
                available=True,
                healthy=True,
                binary_path=self._resolved_binary,
                message=f"Polymarket CLI available at {self._resolved_binary} (startup check skipped).",
            )

        try:
            payload = await self._run_json(["status"])
            status_text = payload.get("status") if isinstance(payload, dict) else None
            message = f"Polymarket CLI healthy at {self._resolved_binary}"
            if status_text:
                message += f" ({status_text})"
            return CliHealthStatus(
                enabled=True,
                available=True,
                healthy=True,
                binary_path=self._resolved_binary,
                message=message,
                raw=payload if isinstance(payload, dict) else {},
            )
        except PolymarketCliError as exc:
            return CliHealthStatus(
                enabled=True,
                available=True,
                healthy=False,
                binary_path=self._resolved_binary,
                message=f"Polymarket CLI startup check failed: {exc}",
            )

    async def get_wallet_info(self) -> WalletInfo:
        payload = await self._run_json(["wallet", "show"])
        data = self._ensure_mapping(payload, "wallet show")
        return WalletInfo(
            configured=bool(data.get("configured", False)),
            address=self._as_str_or_none(data.get("address")),
            proxy_address=self._as_str_or_none(data.get("proxy_address")),
            signature_type=self._as_str_or_none(data.get("signature_type")),
            config_path=self._as_str_or_none(data.get("config_path")),
            source=self._as_str_or_none(data.get("source")),
            raw=data,
        )

    async def get_account_status(self) -> AccountStatusInfo:
        payload = await self._run_json(["clob", "account-status"])
        data = self._ensure_mapping(payload, "clob account-status")
        closed_only = data.get("closed_only")
        return AccountStatusInfo(
            closed_only=bool(closed_only) if isinstance(closed_only, bool) else None,
            raw=data,
        )

    async def get_collateral_balance(self) -> BalanceInfo:
        payload = await self._run_json(["clob", "balance", "--asset-type", "collateral"])
        data = self._ensure_mapping(payload, "clob balance")
        raw_allowances = data.get("allowances", {})
        allowances = {}
        if isinstance(raw_allowances, dict):
            allowances = {str(key): str(value) for key, value in raw_allowances.items()}
        return BalanceInfo(
            balance=self._as_str_or_none(data.get("balance")),
            allowances=allowances,
            raw=data,
        )

    async def get_open_orders(self, limit: int = 10) -> OrdersInfo:
        payload = await self._run_json(["clob", "orders"])
        data = self._ensure_mapping(payload, "clob orders")
        items = data.get("data", [])
        if not isinstance(items, list):
            raise PolymarketCliParseError("Unexpected JSON shape from `polymarket clob orders`.")
        normalized = [item for item in items if isinstance(item, dict)]
        return OrdersInfo(
            data=normalized[:limit],
            next_cursor=self._as_str_or_none(data.get("next_cursor")),
            raw=data,
        )

    async def get_trades(self, limit: int = 10) -> TradesInfo:
        payload = await self._run_json(["clob", "trades"])
        data = self._ensure_mapping(payload, "clob trades")
        items = data.get("data", [])
        if not isinstance(items, list):
            raise PolymarketCliParseError("Unexpected JSON shape from `polymarket clob trades`.")
        normalized = [item for item in items if isinstance(item, dict)]
        return TradesInfo(
            data=normalized[:limit],
            next_cursor=self._as_str_or_none(data.get("next_cursor")),
            raw=data,
        )

    async def cancel_all_orders(self) -> CancelAllResult:
        payload = await self._run_json(["clob", "cancel-all"], mutating=True)
        data = self._ensure_mapping(payload, "clob cancel-all")
        canceled = data.get("canceled", [])
        not_canceled = data.get("not_canceled", {})
        return CancelAllResult(
            canceled=[str(item) for item in canceled] if isinstance(canceled, list) else [],
            not_canceled=(
                {str(key): str(value) for key, value in not_canceled.items()}
                if isinstance(not_canceled, dict)
                else {}
            ),
            raw=data,
        )

    def _discover_binary(self) -> str | None:
        if not self.enabled:
            return None

        candidate = (self.binary_path or "").strip()
        if not candidate:
            return None

        expanded = os.path.expanduser(candidate)
        if any(sep in expanded for sep in (os.sep, "/", "\\")):
            path = Path(expanded)
            return str(path) if path.exists() else None

        return shutil.which(expanded)

    async def _run_json(self, args: list[str], *, mutating: bool = False) -> Any:
        if mutating and not self.allow_mutating_commands:
            raise PolymarketCliCommandBlocked(
                "Polymarket CLI mutating commands are disabled. "
                "Set `cli.allow_mutating_commands: true` to enable them."
            )

        binary = self._resolved_binary or self._discover_binary()
        self._resolved_binary = binary
        if not binary:
            raise PolymarketCliUnavailable(self.unavailable_reason())

        command = [binary, "--output", "json", "--signature-type", self.signature_type, *args]
        env = os.environ.copy()
        if self.private_key:
            env["POLYMARKET_PRIVATE_KEY"] = self.private_key
        env["POLYMARKET_SIGNATURE_TYPE"] = self.signature_type

        logger.debug("Running Polymarket CLI command: %s", " ".join(command[1:]))

        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), timeout=self.timeout_s)
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.communicate()
            raise PolymarketCliTimeout(
                f"`polymarket {' '.join(args)}` timed out after {self.timeout_s:.1f}s."
            ) from exc

        stdout_text = stdout_bytes.decode("utf-8", errors="replace").strip()
        stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
        payload = None
        if stdout_text:
            try:
                payload = json.loads(stdout_text)
            except json.JSONDecodeError as exc:
                raise PolymarketCliParseError(
                    f"`polymarket {' '.join(args)}` returned invalid JSON."
                ) from exc

        if process.returncode != 0:
            message = self._extract_error_message(payload, stdout_text, stderr_text)
            raise PolymarketCliCommandFailed(
                message,
                args=args,
                exit_code=process.returncode,
                stdout=stdout_text,
                stderr=stderr_text,
                payload=payload,
            )

        if payload is None:
            raise PolymarketCliParseError(f"`polymarket {' '.join(args)}` returned no JSON output.")

        return payload

    @staticmethod
    def _ensure_mapping(payload: Any, command_name: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise PolymarketCliParseError(f"Unexpected JSON shape from `polymarket {command_name}`.")
        return payload

    @staticmethod
    def _extract_error_message(payload: Any, stdout_text: str, stderr_text: str) -> str:
        if isinstance(payload, dict):
            error_message = payload.get("error")
            if error_message:
                return str(error_message)
        if stderr_text:
            return stderr_text
        if stdout_text:
            return stdout_text
        return "Polymarket CLI command failed."

    @staticmethod
    def _as_str_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
