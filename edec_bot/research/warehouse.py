"""DuckDB-backed storage for historical research datasets."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .paths import PARQUET_ROOT, WAREHOUSE_PATH, ensure_research_dirs, resolve_repo_path
from .sources import FillCursor


USDC_ASSET_ID = "0"


class ResearchWarehouse:
    """Owns the research DuckDB connection and canonical datasets."""

    def __init__(self, db_path: str | Path = WAREHOUSE_PATH):
        ensure_research_dirs()
        self.db_path = resolve_repo_path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._create_schema()

    def close(self) -> None:
        self.conn.close()

    def _create_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                name VARCHAR PRIMARY KEY,
                cursor_text VARCHAR,
                updated_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS markets (
                market_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP,
                created_date DATE,
                market_slug VARCHAR,
                question VARCHAR,
                answer1 VARCHAR,
                answer2 VARCHAR,
                token1 VARCHAR,
                token2 VARCHAR,
                condition_id VARCHAR,
                volume DOUBLE,
                ticker VARCHAR,
                closed_time TIMESTAMP,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                active BOOLEAN,
                accepting_orders BOOLEAN,
                neg_risk BOOLEAN,
                fee_rate DOUBLE,
                raw_json VARCHAR,
                synced_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS fills_raw (
                event_id VARCHAR PRIMARY KEY,
                event_timestamp BIGINT,
                event_time TIMESTAMP,
                event_date DATE,
                transaction_hash VARCHAR,
                maker VARCHAR,
                maker_asset_id VARCHAR,
                maker_amount_filled DOUBLE,
                taker VARCHAR,
                taker_asset_id VARCHAR,
                taker_amount_filled DOUBLE,
                fee DOUBLE,
                order_hash VARCHAR,
                resume_timestamp BIGINT,
                resume_last_id VARCHAR,
                resume_sticky_timestamp BIGINT,
                raw_json VARCHAR,
                ingested_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS fills_enriched (
                event_id VARCHAR PRIMARY KEY,
                event_timestamp BIGINT,
                event_time TIMESTAMP,
                event_date DATE,
                transaction_hash VARCHAR,
                maker VARCHAR,
                taker VARCHAR,
                maker_asset_id VARCHAR,
                taker_asset_id VARCHAR,
                market_id VARCHAR,
                market_slug VARCHAR,
                coin VARCHAR,
                token_side VARCHAR,
                token_id VARCHAR,
                price DOUBLE,
                usd_amount DOUBLE,
                token_amount DOUBLE,
                is_5m_updown BOOLEAN
            );

            CREATE TABLE IF NOT EXISTS market_5m_registry (
                market_id VARCHAR PRIMARY KEY,
                market_slug VARCHAR,
                coin VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                window_date DATE,
                up_token_id VARCHAR,
                down_token_id VARCHAR
            );
            """
        )

    def get_market_offset(self) -> int:
        return int((self._get_state("markets_offset") or {}).get("offset") or 0)

    def set_market_offset(self, offset: int) -> None:
        self._set_state("markets_offset", {"offset": int(offset)})

    def get_fill_cursor(self) -> FillCursor:
        return FillCursor.from_dict(self._get_state("fills_cursor"))

    def set_fill_cursor(self, cursor: FillCursor) -> None:
        self._set_state("fills_cursor", cursor.to_dict())

    def _get_state(self, name: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT cursor_text FROM sync_state WHERE name = ?",
            [name],
        ).fetchone()
        if not row or row[0] in (None, ""):
            return None
        return json.loads(row[0])

    def _set_state(self, name: str, payload: dict[str, Any]) -> None:
        now = _utc_now()
        self.conn.execute(
            """
            INSERT INTO sync_state(name, cursor_text, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                cursor_text = excluded.cursor_text,
                updated_at = excluded.updated_at
            """,
            [name, json.dumps(payload, sort_keys=True), now],
        )

    def insert_markets(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payload = []
        now = _utc_now()
        for row in rows:
            created_at = _parse_ts(row.get("created_at"))
            closed_time = _parse_ts(row.get("closed_time"))
            start_time = _parse_ts(row.get("start_time"))
            end_time = _parse_ts(row.get("end_time"))
            payload.append(
                [
                    row.get("market_id"),
                    created_at,
                    created_at.date() if created_at else None,
                    row.get("market_slug"),
                    row.get("question"),
                    row.get("answer1"),
                    row.get("answer2"),
                    row.get("token1"),
                    row.get("token2"),
                    row.get("condition_id"),
                    row.get("volume"),
                    row.get("ticker"),
                    closed_time,
                    start_time,
                    end_time,
                    bool(row.get("active", True)),
                    bool(row.get("accepting_orders", True)),
                    bool(row.get("neg_risk", False)),
                    row.get("fee_rate"),
                    row.get("raw_json"),
                    now,
                ]
            )
        self.conn.executemany(
            """
            INSERT INTO markets (
                market_id, created_at, created_date, market_slug, question, answer1, answer2,
                token1, token2, condition_id, volume, ticker, closed_time, start_time, end_time,
                active, accepting_orders, neg_risk, fee_rate, raw_json, synced_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                created_at = excluded.created_at,
                created_date = excluded.created_date,
                market_slug = excluded.market_slug,
                question = excluded.question,
                answer1 = excluded.answer1,
                answer2 = excluded.answer2,
                token1 = excluded.token1,
                token2 = excluded.token2,
                condition_id = excluded.condition_id,
                volume = excluded.volume,
                ticker = excluded.ticker,
                closed_time = excluded.closed_time,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                active = excluded.active,
                accepting_orders = excluded.accepting_orders,
                neg_risk = excluded.neg_risk,
                fee_rate = excluded.fee_rate,
                raw_json = excluded.raw_json,
                synced_at = excluded.synced_at
            """,
            payload,
        )
        return len(payload)

    def insert_raw_fills(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payload = []
        now = _utc_now()
        for row in rows:
            event_time = _from_epoch(row.get("event_timestamp"))
            payload.append(
                [
                    row.get("event_id"),
                    row.get("event_timestamp"),
                    event_time,
                    event_time.date() if event_time else None,
                    row.get("transaction_hash"),
                    row.get("maker"),
                    row.get("maker_asset_id"),
                    row.get("maker_amount_filled"),
                    row.get("taker"),
                    row.get("taker_asset_id"),
                    row.get("taker_amount_filled"),
                    row.get("fee"),
                    row.get("order_hash"),
                    row.get("resume_timestamp"),
                    row.get("resume_last_id"),
                    row.get("resume_sticky_timestamp"),
                    row.get("raw_json"),
                    now,
                ]
            )
        self.conn.executemany(
            """
            INSERT INTO fills_raw (
                event_id, event_timestamp, event_time, event_date, transaction_hash, maker,
                maker_asset_id, maker_amount_filled, taker, taker_asset_id, taker_amount_filled,
                fee, order_hash, resume_timestamp, resume_last_id, resume_sticky_timestamp,
                raw_json, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO NOTHING
            """,
            payload,
        )
        return len(payload)

    def rebuild_market_5m_registry(self) -> int:
        market_rows = self.conn.execute(
            """
            SELECT market_id, market_slug, answer1, answer2, token1, token2, start_time, end_time
            FROM markets
            WHERE market_slug LIKE '%-updown-5m-%'
            """
        ).fetchall()
        payload = []
        for market_id, market_slug, answer1, answer2, token1, token2, start_time, end_time in market_rows:
            coin = _slug_coin(market_slug)
            up_token_id, down_token_id = _up_down_tokens(answer1, answer2, token1, token2)
            payload.append(
                [
                    market_id,
                    market_slug,
                    coin,
                    start_time,
                    end_time,
                    start_time.date() if start_time else None,
                    up_token_id,
                    down_token_id,
                ]
            )
        self.conn.execute("DELETE FROM market_5m_registry")
        if payload:
            self.conn.executemany(
                """
                INSERT INTO market_5m_registry (
                    market_id, market_slug, coin, window_start, window_end, window_date, up_token_id, down_token_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
        return len(payload)

    def rebuild_fills_enriched(self) -> int:
        market_rows = self.conn.execute(
            """
            SELECT market_id, market_slug, answer1, answer2, token1, token2
            FROM markets
            """
        ).fetchall()
        by_token: dict[str, dict[str, Any]] = {}
        for market_id, market_slug, answer1, answer2, token1, token2 in market_rows:
            coin = _slug_coin(market_slug)
            row = {
                "market_id": market_id,
                "market_slug": market_slug,
                "coin": coin,
                "token_map": {
                    str(token1 or ""): _token_side(answer1, "token1"),
                    str(token2 or ""): _token_side(answer2, "token2"),
                },
            }
            if token1:
                by_token[str(token1)] = row
            if token2:
                by_token[str(token2)] = row

        fill_rows = self.conn.execute(
            """
            SELECT event_id, event_timestamp, event_time, transaction_hash, maker, taker,
                   maker_asset_id, maker_amount_filled, taker_asset_id, taker_amount_filled
            FROM fills_raw
            ORDER BY event_timestamp ASC, event_id ASC
            """
        ).fetchall()
        payload = []
        for row in fill_rows:
            (
                event_id,
                event_timestamp,
                event_time,
                transaction_hash,
                maker,
                taker,
                maker_asset_id,
                maker_amount_filled,
                taker_asset_id,
                taker_amount_filled,
            ) = row
            maker_asset = str(maker_asset_id or "")
            taker_asset = str(taker_asset_id or "")
            token_id = maker_asset if maker_asset != USDC_ASSET_ID else taker_asset
            market = by_token.get(token_id)
            if not market:
                continue
            maker_amount = float(maker_amount_filled or 0.0) / 1_000_000.0
            taker_amount = float(taker_amount_filled or 0.0) / 1_000_000.0
            taker_is_usdc = taker_asset == USDC_ASSET_ID
            usd_amount = taker_amount if taker_is_usdc else maker_amount
            token_amount = maker_amount if taker_is_usdc else taker_amount
            if token_amount <= 0:
                continue
            payload.append(
                [
                    event_id,
                    int(event_timestamp or 0),
                    event_time,
                    event_time.date() if event_time else None,
                    transaction_hash,
                    maker,
                    taker,
                    maker_asset,
                    taker_asset,
                    market["market_id"],
                    market["market_slug"],
                    market["coin"],
                    market["token_map"].get(token_id, "unknown"),
                    token_id,
                    usd_amount / token_amount,
                    usd_amount,
                    token_amount,
                    "-updown-5m-" in str(market["market_slug"] or ""),
                ]
            )
        self.conn.execute("DELETE FROM fills_enriched")
        if payload:
            self.conn.executemany(
                """
                INSERT INTO fills_enriched (
                    event_id, event_timestamp, event_time, event_date, transaction_hash,
                    maker, taker, maker_asset_id, taker_asset_id, market_id, market_slug,
                    coin, token_side, token_id, price, usd_amount, token_amount, is_5m_updown
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
        return len(payload)

    def export_parquet(self, parquet_root: str | Path = PARQUET_ROOT) -> dict[str, str]:
        root = resolve_repo_path(parquet_root)
        root.mkdir(parents=True, exist_ok=True)
        datasets = {
            "markets": "created_date",
            "fills_raw": "event_date",
            "fills_enriched": "event_date",
            "market_5m_registry": "window_date",
        }
        exported: dict[str, str] = {}
        for table_name, partition_column in datasets.items():
            target = root / table_name
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
            target.mkdir(parents=True, exist_ok=True)
            count = int(self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)
            if count <= 0:
                exported[table_name] = str(target)
                continue
            safe_target = str(target).replace("'", "''")
            self.conn.execute(
                f"""
                COPY (SELECT * FROM {table_name})
                TO '{safe_target}'
                (FORMAT PARQUET, PARTITION_BY ({partition_column}))
                """
            )
            exported[table_name] = str(target)
        return exported

    def recent_5m_token_ids(self, *, lookback_hours: int) -> list[str]:
        cutoff = _utc_now() - _hours(lookback_hours)
        rows = self.conn.execute(
            """
            SELECT DISTINCT token_id
            FROM (
                SELECT up_token_id AS token_id
                FROM market_5m_registry
                WHERE window_end >= ?
                UNION ALL
                SELECT down_token_id AS token_id
                FROM market_5m_registry
                WHERE window_end >= ?
            )
            WHERE token_id IS NOT NULL AND token_id <> ''
            ORDER BY token_id ASC
            """,
            [cutoff, cutoff],
        ).fetchall()
        return [str(row[0]) for row in rows if row and row[0]]

    def recent_5m_asset_windows(
        self,
        *,
        lookback_hours: int,
        bucket_minutes: int = 60,
    ) -> list[dict[str, object]]:
        cutoff = _utc_now() - _hours(lookback_hours)
        rows = self.conn.execute(
            """
            SELECT window_start, window_end, up_token_id, down_token_id
            FROM market_5m_registry
            WHERE window_end >= ?
            ORDER BY window_start ASC
            """,
            [cutoff],
        ).fetchall()
        if bucket_minutes <= 0:
            raise ValueError("bucket_minutes must be positive")
        buckets: dict[int, dict[str, object]] = {}
        bucket_span_s = bucket_minutes * 60
        for window_start, window_end, up_token_id, down_token_id in rows:
            if window_start is None:
                continue
            start_aware = _ensure_utc(window_start)
            end_aware = _ensure_utc(window_end or window_start)
            bucket_epoch = int(start_aware.timestamp()) // bucket_span_s * bucket_span_s
            bucket = buckets.setdefault(
                bucket_epoch,
                {
                    "bucket_start": datetime.fromtimestamp(bucket_epoch, tz=timezone.utc),
                    "bucket_end": end_aware,
                    "asset_ids": set(),
                },
            )
            if end_aware > bucket["bucket_end"]:
                bucket["bucket_end"] = end_aware
            if up_token_id:
                bucket["asset_ids"].add(str(up_token_id))
            if down_token_id:
                bucket["asset_ids"].add(str(down_token_id))
        result: list[dict[str, object]] = []
        for bucket_epoch in sorted(buckets):
            bucket = buckets[bucket_epoch]
            result.append(
                {
                    "bucket_start": bucket["bucket_start"],
                    "bucket_end": bucket["bucket_end"],
                    "asset_ids": sorted(bucket["asset_ids"]),
                }
            )
        return result


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _hours(value: int) -> timedelta:
    from datetime import timedelta

    return timedelta(hours=int(value))


def _ensure_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _from_epoch(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _slug_coin(slug: str | None) -> str:
    text = str(slug or "").lower()
    if "-updown-5m-" in text:
        return text.split("-updown-5m-", 1)[0]
    return text.split("-", 1)[0]


def _token_side(answer: str | None, fallback: str) -> str:
    lowered = str(answer or "").strip().lower()
    if lowered in {"up", "yes"}:
        return "up"
    if lowered in {"down", "no"}:
        return "down"
    return fallback


def _up_down_tokens(answer1: str | None, answer2: str | None, token1: str | None, token2: str | None) -> tuple[str, str]:
    side1 = _token_side(answer1, "token1")
    side2 = _token_side(answer2, "token2")
    up_token = token1 if side1 == "up" else token2 if side2 == "up" else token1
    down_token = token2 if side2 == "down" else token1 if side1 == "down" else token2
    return str(up_token or ""), str(down_token or "")
