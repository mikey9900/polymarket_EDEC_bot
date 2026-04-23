"""External source adapters for historical market and fill sync."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Protocol

import httpx


DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0
DEFAULT_HTTP_RETRY_ATTEMPTS = 3
DEFAULT_HTTP_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_HTTP_RETRY_MAX_BACKOFF_SECONDS = 10.0

ENV_GAMMA_MARKETS_URL = "EDEC_RESEARCH_GAMMA_MARKETS_URL"
ENV_GOLDSKY_ORDERBOOK_URL = "EDEC_RESEARCH_GOLDSKY_ORDERBOOK_URL"
ENV_HTTP_TIMEOUT_SECONDS = "EDEC_RESEARCH_HTTP_TIMEOUT_SECONDS"
ENV_HTTP_RETRY_ATTEMPTS = "EDEC_RESEARCH_HTTP_RETRY_ATTEMPTS"
ENV_HTTP_RETRY_BACKOFF_SECONDS = "EDEC_RESEARCH_HTTP_RETRY_BACKOFF_SECONDS"
ENV_HTTP_RETRY_MAX_BACKOFF_SECONDS = "EDEC_RESEARCH_HTTP_RETRY_MAX_BACKOFF_SECONDS"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GOLDSKY_ORDERBOOK_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/orderbook-subgraph/0.0.1/gn"
)

_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_RETRYABLE_REQUEST_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.WriteTimeout,
)


class MarketSource(Protocol):
    def fetch_markets(
        self,
        *,
        offset: int,
        limit: int,
        ascending: bool = True,
        closed: bool | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch one page of market metadata."""


class FillSource(Protocol):
    def fetch_fills(self, *, cursor: "FillCursor", limit: int) -> tuple[list[dict[str, Any]], "FillCursor"]:
        """Fetch one page of fills and return the next cursor."""


@dataclass(frozen=True)
class FillCursor:
    last_timestamp: int = 0
    last_id: str | None = None
    sticky_timestamp: int | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "FillCursor":
        payload = payload or {}
        return cls(
            last_timestamp=int(payload.get("last_timestamp") or 0),
            last_id=payload.get("last_id") or None,
            sticky_timestamp=(
                int(payload["sticky_timestamp"])
                if payload.get("sticky_timestamp") not in (None, "")
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_timestamp": int(self.last_timestamp or 0),
            "last_id": self.last_id or "",
            "sticky_timestamp": self.sticky_timestamp,
        }


class _RetryingHttpSource:
    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        timeout_seconds: float | None = None,
        retry_attempts: int | None = None,
        retry_backoff_seconds: float | None = None,
        retry_max_backoff_seconds: float | None = None,
    ):
        timeout_seconds = _env_float(ENV_HTTP_TIMEOUT_SECONDS, DEFAULT_HTTP_TIMEOUT_SECONDS) if timeout_seconds is None else timeout_seconds
        retry_attempts = _env_int(ENV_HTTP_RETRY_ATTEMPTS, DEFAULT_HTTP_RETRY_ATTEMPTS) if retry_attempts is None else retry_attempts
        retry_backoff_seconds = (
            _env_float(ENV_HTTP_RETRY_BACKOFF_SECONDS, DEFAULT_HTTP_RETRY_BACKOFF_SECONDS)
            if retry_backoff_seconds is None
            else retry_backoff_seconds
        )
        retry_max_backoff_seconds = (
            _env_float(ENV_HTTP_RETRY_MAX_BACKOFF_SECONDS, DEFAULT_HTTP_RETRY_MAX_BACKOFF_SECONDS)
            if retry_max_backoff_seconds is None
            else retry_max_backoff_seconds
        )

        self._client = client or httpx.Client(timeout=max(float(timeout_seconds), 0.1))
        self._owns_client = client is None
        self._retry_attempts = max(1, int(retry_attempts))
        self._retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self._retry_max_backoff_seconds = max(self._retry_backoff_seconds, float(retry_max_backoff_seconds))

    def _request_json(self, *, method: str, url: str, label: str, **kwargs: Any) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                response = self._client.request(method, url, **kwargs)
            except _RETRYABLE_REQUEST_ERRORS as exc:
                last_error = exc
                if attempt >= self._retry_attempts:
                    raise RuntimeError(self._retry_error_message(label, method, url, attempt, exc)) from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < self._retry_attempts:
                self._sleep_before_retry(attempt)
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(self._retry_error_message(label, method, url, attempt, exc)) from exc
            return response.json()

        if last_error is not None:
            raise RuntimeError(self._retry_error_message(label, method, url, self._retry_attempts, last_error)) from last_error
        raise RuntimeError(f"{label} request to {url} exhausted retries")

    def _sleep_before_retry(self, attempt: int) -> None:
        delay = min(self._retry_backoff_seconds * (2 ** (attempt - 1)), self._retry_max_backoff_seconds)
        if delay > 0:
            time.sleep(delay)

    def _retry_error_message(self, label: str, method: str, url: str, attempt: int, exc: Exception) -> str:
        method = method.upper()
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            return f"{label} {method} {url} failed with HTTP {status_code} on attempt {attempt}/{self._retry_attempts}"
        return (
            f"{label} {method} {url} failed on attempt {attempt}/{self._retry_attempts}: "
            f"{exc.__class__.__name__}: {exc}"
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class GammaMarketSource(_RetryingHttpSource):
    """Gamma market metadata adapter."""

    def __init__(
        self,
        base_url: str | None = None,
        client: httpx.Client | None = None,
        *,
        timeout_seconds: float | None = None,
        retry_attempts: int | None = None,
        retry_backoff_seconds: float | None = None,
        retry_max_backoff_seconds: float | None = None,
    ):
        super().__init__(
            client=client,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_max_backoff_seconds=retry_max_backoff_seconds,
        )
        self.base_url = base_url or os.getenv(ENV_GAMMA_MARKETS_URL, GAMMA_MARKETS_URL)

    def fetch_markets(
        self,
        *,
        offset: int,
        limit: int,
        ascending: bool = True,
        closed: bool | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            "order": "createdAt",
            "ascending": "true" if ascending else "false",
            "limit": limit,
            "offset": offset,
        }
        if closed is True:
            params["closed"] = "true"
        payload = self._request_json(
            method="GET",
            url=self.base_url,
            label="Gamma markets",
            params=params,
        )
        if not isinstance(payload, list):
            raise ValueError("Gamma markets response was not a list")
        return payload

class GoldskyFillSource(_RetryingHttpSource):
    """Goldsky order-filled adapter with sticky cursor pagination."""

    def __init__(
        self,
        url: str | None = None,
        client: httpx.Client | None = None,
        *,
        timeout_seconds: float | None = None,
        retry_attempts: int | None = None,
        retry_backoff_seconds: float | None = None,
        retry_max_backoff_seconds: float | None = None,
    ):
        super().__init__(
            client=client,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_max_backoff_seconds=retry_max_backoff_seconds,
        )
        self.url = url or os.getenv(ENV_GOLDSKY_ORDERBOOK_URL, GOLDSKY_ORDERBOOK_URL)

    def fetch_fills(self, *, cursor: FillCursor, limit: int) -> tuple[list[dict[str, Any]], FillCursor]:
        payload = self._request_json(
            method="POST",
            url=self.url,
            label="Goldsky fills",
            json={"query": build_goldsky_query(cursor=cursor, limit=limit)},
        )
        if payload.get("errors"):
            raise ValueError(f"Goldsky query error: {payload['errors']}")
        rows = payload.get("data", {}).get("orderFilledEvents", [])
        if not isinstance(rows, list):
            raise ValueError("Goldsky fills response was not a list")
        rows = sorted(rows, key=lambda item: (int(item.get("timestamp") or 0), str(item.get("id") or "")))
        return rows, advance_fill_cursor(rows, prior=cursor, limit=limit)

    def fetch_fills_for_assets(
        self,
        *,
        asset_ids: list[str],
        cursor: FillCursor,
        limit: int,
        until_timestamp: int | None = None,
    ) -> tuple[list[dict[str, Any]], FillCursor]:
        payload = self._request_json(
            method="POST",
            url=self.url,
            label="Goldsky fills",
            json={
                "query": build_goldsky_query(
                    cursor=cursor,
                    limit=limit,
                    asset_ids=asset_ids,
                    until_timestamp=until_timestamp,
                )
            },
        )
        if payload.get("errors"):
            raise ValueError(f"Goldsky query error: {payload['errors']}")
        rows = payload.get("data", {}).get("orderFilledEvents", [])
        if not isinstance(rows, list):
            raise ValueError("Goldsky fills response was not a list")
        rows = sorted(rows, key=lambda item: (int(item.get("timestamp") or 0), str(item.get("id") or "")))
        return rows, advance_fill_cursor(rows, prior=cursor, limit=limit)

    def close(self) -> None:
        super().close()


def build_goldsky_query(
    *,
    cursor: FillCursor,
    limit: int,
    asset_ids: list[str] | None = None,
    until_timestamp: int | None = None,
) -> str:
    temporal_filters: list[str] = []
    if cursor.sticky_timestamp is not None and cursor.last_id:
        temporal_filters.extend([f'timestamp: "{int(cursor.sticky_timestamp)}"', f'id_gt: "{cursor.last_id}"'])
    else:
        temporal_filters.append(f'timestamp_gt: "{int(cursor.last_timestamp or 0)}"')
    if until_timestamp is not None:
        temporal_filters.append(f'timestamp_lt: "{int(until_timestamp)}"')
    if asset_ids:
        where_clause = _goldsky_asset_filter(asset_ids, temporal_filters)
    else:
        where_clause = ", ".join(temporal_filters)
    return f"""
query OrderFilledEvents {{
  orderFilledEvents(
    orderBy: timestamp
    orderDirection: asc
    first: {int(limit)}
    where: {{{where_clause}}}
  ) {{
    fee
    id
    maker
    makerAmountFilled
    makerAssetId
    orderHash
    taker
    takerAmountFilled
    takerAssetId
    timestamp
    transactionHash
  }}
}}
""".strip()


def advance_fill_cursor(rows: list[dict[str, Any]], *, prior: FillCursor, limit: int) -> FillCursor:
    if not rows:
        if prior.sticky_timestamp is not None:
            return FillCursor(last_timestamp=int(prior.sticky_timestamp), last_id=None, sticky_timestamp=None)
        return prior

    first_timestamp = int(rows[0].get("timestamp") or 0)
    last_timestamp = int(rows[-1].get("timestamp") or 0)
    last_id = str(rows[-1].get("id") or "")
    if len(rows) >= int(limit):
        return FillCursor(
            last_timestamp=int(prior.last_timestamp or 0),
            last_id=last_id or None,
            sticky_timestamp=last_timestamp,
        )
    if prior.sticky_timestamp is not None:
        return FillCursor(last_timestamp=int(prior.sticky_timestamp), last_id=None, sticky_timestamp=None)
    return FillCursor(last_timestamp=last_timestamp, last_id=None, sticky_timestamp=None)


def normalize_gamma_market(raw: dict[str, Any]) -> dict[str, Any]:
    outcomes = _as_list(raw.get("outcomes"))
    tokens = _as_list(raw.get("clobTokenIds"))
    events = raw.get("events") or []
    event = events[0] if isinstance(events, list) and events else {}
    fee_schedule = raw.get("feeSchedule")
    if isinstance(fee_schedule, str):
        try:
            fee_schedule = json.loads(fee_schedule)
        except json.JSONDecodeError:
            fee_schedule = {}
    fee_schedule = fee_schedule or {}
    question = raw.get("question") or raw.get("title") or event.get("title") or ""
    return {
        "market_id": str(raw.get("id") or ""),
        "created_at": raw.get("createdAt") or "",
        "market_slug": raw.get("slug") or event.get("slug") or "",
        "question": question,
        "answer1": str(outcomes[0]) if len(outcomes) > 0 else "",
        "answer2": str(outcomes[1]) if len(outcomes) > 1 else "",
        "token1": str(tokens[0]) if len(tokens) > 0 else "",
        "token2": str(tokens[1]) if len(tokens) > 1 else "",
        "condition_id": str(raw.get("conditionId") or ""),
        "volume": _as_float(raw.get("volumeClob") or raw.get("volumeNum") or raw.get("volume") or event.get("volume")),
        "ticker": str(event.get("ticker") or raw.get("ticker") or ""),
        "closed_time": raw.get("closedTime") or "",
        "start_time": raw.get("eventStartTime") or raw.get("startDate") or "",
        "end_time": raw.get("endDate") or event.get("endDate") or "",
        "active": bool(raw.get("active", True)),
        "accepting_orders": bool(raw.get("acceptingOrders", True)),
        "neg_risk": bool(raw.get("negRisk") or raw.get("negRiskAugmented") or raw.get("negRiskOther")),
        "fee_rate": _as_float(fee_schedule.get("rate"), 0.072),
        "raw_json": json.dumps(raw, sort_keys=True),
    }


def normalize_goldsky_fill(raw: dict[str, Any], *, resume_cursor: FillCursor) -> dict[str, Any]:
    return {
        "event_id": str(raw.get("id") or ""),
        "event_timestamp": int(raw.get("timestamp") or 0),
        "transaction_hash": str(raw.get("transactionHash") or ""),
        "maker": str(raw.get("maker") or ""),
        "maker_asset_id": str(raw.get("makerAssetId") or ""),
        "maker_amount_filled": _as_float(raw.get("makerAmountFilled")),
        "taker": str(raw.get("taker") or ""),
        "taker_asset_id": str(raw.get("takerAssetId") or ""),
        "taker_amount_filled": _as_float(raw.get("takerAmountFilled")),
        "fee": _as_float(raw.get("fee")),
        "order_hash": str(raw.get("orderHash") or ""),
        "resume_timestamp": int(resume_cursor.last_timestamp or 0),
        "resume_last_id": resume_cursor.last_id or "",
        "resume_sticky_timestamp": resume_cursor.sticky_timestamp,
        "raw_json": json.dumps(raw, sort_keys=True),
    }


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _as_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _goldsky_asset_filter(asset_ids: list[str], temporal_filters: list[str]) -> str:
    cleaned = sorted({str(asset_id) for asset_id in asset_ids if str(asset_id or "").strip()})
    if not cleaned:
        return ""
    quoted = ", ".join(json.dumps(asset_id) for asset_id in cleaned)
    maker_branch = ", ".join([*temporal_filters, f"makerAssetId_in: [{quoted}]"])
    taker_branch = ", ".join([*temporal_filters, f"takerAssetId_in: [{quoted}]"])
    return f"or: [{{{maker_branch}}}, {{{taker_branch}}}]"
