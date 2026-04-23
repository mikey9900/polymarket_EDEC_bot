"""Incremental sync runners for the research warehouse."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from .sources import FillCursor, FillSource, GammaMarketSource, GoldskyFillSource, normalize_gamma_market, normalize_goldsky_fill

if TYPE_CHECKING:
    from .warehouse import ResearchWarehouse


def sync_markets(
    warehouse: ResearchWarehouse,
    source: GammaMarketSource,
    *,
    batch_size: int = 500,
    max_batches: int | None = None,
) -> dict[str, object]:
    offset = warehouse.get_market_offset()
    fetched = 0
    inserted = 0
    batches = 0
    while True:
        rows = source.fetch_markets(offset=offset, limit=batch_size)
        if not rows:
            break
        normalized = [normalize_gamma_market(row) for row in rows if row.get("id")]
        inserted += warehouse.insert_markets(normalized)
        fetched += len(rows)
        offset += len(rows)
        warehouse.set_market_offset(offset)
        batches += 1
        if len(rows) < batch_size:
            break
        if max_batches is not None and batches >= max_batches:
            break
    registry_rows = warehouse.rebuild_market_5m_registry()
    enriched_rows = warehouse.rebuild_fills_enriched()
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "markets",
        "fetched": fetched,
        "inserted": inserted,
        "offset": offset,
        "batches": batches,
        "market_5m_registry_rows": registry_rows,
        "fills_enriched_rows": enriched_rows,
        "parquet": parquet_paths,
    }


def sync_recent_markets(
    warehouse: ResearchWarehouse,
    source: GammaMarketSource,
    *,
    lookback_days: int = 30,
    batch_size: int = 500,
    max_batches: int | None = None,
) -> dict[str, object]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(lookback_days))
    open_stats = _sync_recent_market_feed(
        warehouse,
        source,
        cutoff=cutoff,
        batch_size=batch_size,
        max_batches=max_batches,
        closed=None,
    )
    closed_stats = _sync_recent_market_feed(
        warehouse,
        source,
        cutoff=cutoff,
        batch_size=batch_size,
        max_batches=max_batches,
        closed=True,
    )
    registry_rows = warehouse.rebuild_market_5m_registry()
    enriched_rows = warehouse.rebuild_fills_enriched()
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "recent_markets",
        "lookback_days": int(lookback_days),
        "cutoff": cutoff.isoformat(),
        "fetched": int(open_stats["fetched"]) + int(closed_stats["fetched"]),
        "inserted": int(open_stats["inserted"]) + int(closed_stats["inserted"]),
        "batches": int(open_stats["batches"]) + int(closed_stats["batches"]),
        "open_markets": open_stats,
        "closed_markets": closed_stats,
        "market_5m_registry_rows": registry_rows,
        "fills_enriched_rows": enriched_rows,
        "parquet": parquet_paths,
    }


def sync_fills(
    warehouse: ResearchWarehouse,
    source: FillSource,
    *,
    batch_size: int = 1000,
    max_batches: int | None = None,
) -> dict[str, object]:
    cursor = warehouse.get_fill_cursor()
    fetched = 0
    inserted = 0
    batches = 0
    while True:
        prior = cursor
        rows, next_cursor = source.fetch_fills(cursor=cursor, limit=batch_size)
        warehouse.set_fill_cursor(next_cursor)
        cursor = next_cursor
        if not rows:
            if cursor != prior:
                continue
            break
        normalized = [normalize_goldsky_fill(row, resume_cursor=cursor) for row in rows if row.get("id")]
        inserted += warehouse.insert_raw_fills(normalized)
        fetched += len(rows)
        batches += 1
        if max_batches is not None and batches >= max_batches:
            break
        if len(rows) < batch_size and prior.sticky_timestamp is None and cursor.sticky_timestamp is None:
            break
    registry_rows = warehouse.rebuild_market_5m_registry()
    enriched_rows = warehouse.rebuild_fills_enriched()
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "fills",
        "fetched": fetched,
        "inserted": inserted,
        "cursor": cursor.to_dict(),
        "batches": batches,
        "market_5m_registry_rows": registry_rows,
        "fills_enriched_rows": enriched_rows,
        "parquet": parquet_paths,
    }


def sync_recent_5m_fills(
    warehouse: ResearchWarehouse,
    source: GoldskyFillSource,
    *,
    lookback_hours: int = 24,
    history_lookback_days: int = 30,
    batch_size: int = 1000,
    asset_chunk_size: int = 50,
    bucket_minutes: int = 60,
    history_bucket_minutes: int = 360,
    bucket_buffer_seconds: int = 900,
    max_batches_per_chunk: int | None = None,
    max_history_batches_per_chunk: int | None = 1,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(hours=int(lookback_hours))
    history_cutoff = now - timedelta(days=int(history_lookback_days))
    recent_windows = warehouse.asset_windows_between(
        since=recent_cutoff,
        until=now,
        bucket_minutes=bucket_minutes,
    )
    history_windows: list[dict[str, object]] = []
    if int(history_lookback_days) > 0 and history_cutoff < recent_cutoff:
        history_windows = warehouse.asset_windows_between(
            since=history_cutoff,
            until=recent_cutoff,
            bucket_minutes=history_bucket_minutes,
        )
    recent_stats = _sync_asset_windows(
        warehouse,
        source,
        asset_windows=recent_windows,
        batch_size=batch_size,
        asset_chunk_size=asset_chunk_size,
        bucket_buffer_seconds=bucket_buffer_seconds,
        max_batches_per_chunk=max_batches_per_chunk,
    )
    history_stats = _sync_asset_windows(
        warehouse,
        source,
        asset_windows=history_windows,
        batch_size=batch_size,
        asset_chunk_size=asset_chunk_size,
        bucket_buffer_seconds=bucket_buffer_seconds,
        max_batches_per_chunk=max_history_batches_per_chunk,
    )
    registry_rows = warehouse.rebuild_market_5m_registry()
    enriched_rows = warehouse.rebuild_fills_enriched()
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "recent_5m_fills",
        "window_end": now.isoformat(),
        "lookback_hours": int(lookback_hours),
        "history_lookback_days": int(history_lookback_days),
        "recent_since_timestamp": int(recent_cutoff.timestamp()) - 1,
        "history_since_timestamp": int(history_cutoff.timestamp()) - 1 if int(history_lookback_days) > 0 else None,
        "asset_window_count": int(recent_stats["asset_window_count"]) + int(history_stats["asset_window_count"]),
        "asset_count": int(recent_stats["asset_count"]) + int(history_stats["asset_count"]),
        "asset_chunk_size": int(asset_chunk_size),
        "bucket_minutes": int(bucket_minutes),
        "history_bucket_minutes": int(history_bucket_minutes),
        "bucket_buffer_seconds": int(bucket_buffer_seconds),
        "chunks_processed": int(recent_stats["chunks_processed"]) + int(history_stats["chunks_processed"]),
        "batches": int(recent_stats["batches"]) + int(history_stats["batches"]),
        "fetched": int(recent_stats["fetched"]) + int(history_stats["fetched"]),
        "inserted": int(recent_stats["inserted"]) + int(history_stats["inserted"]),
        "recent": recent_stats,
        "history": history_stats,
        "market_5m_registry_rows": registry_rows,
        "fills_enriched_rows": enriched_rows,
        "parquet": parquet_paths,
    }


def sync_daily_research_window(
    warehouse: ResearchWarehouse,
    market_source: GammaMarketSource,
    fill_source: GoldskyFillSource,
    *,
    market_lookback_days: int = 30,
    market_batch_size: int = 500,
    market_max_batches: int | None = None,
    lookback_hours: int = 24,
    history_lookback_days: int = 30,
    batch_size: int = 1000,
    asset_chunk_size: int = 50,
    bucket_minutes: int = 60,
    history_bucket_minutes: int = 360,
    bucket_buffer_seconds: int = 900,
    max_batches_per_chunk: int | None = None,
    max_history_batches_per_chunk: int | None = 1,
) -> dict[str, object]:
    market_result = sync_recent_markets(
        warehouse,
        market_source,
        lookback_days=market_lookback_days,
        batch_size=market_batch_size,
        max_batches=market_max_batches,
    )
    fill_result = sync_recent_5m_fills(
        warehouse,
        fill_source,
        lookback_hours=lookback_hours,
        history_lookback_days=history_lookback_days,
        batch_size=batch_size,
        asset_chunk_size=asset_chunk_size,
        bucket_minutes=bucket_minutes,
        history_bucket_minutes=history_bucket_minutes,
        bucket_buffer_seconds=bucket_buffer_seconds,
        max_batches_per_chunk=max_batches_per_chunk,
        max_history_batches_per_chunk=max_history_batches_per_chunk,
    )
    return {
        "dataset": "daily_research_sync",
        "markets": market_result,
        "fills": fill_result,
    }


def _sync_asset_windows(
    warehouse: ResearchWarehouse,
    source: GoldskyFillSource,
    *,
    asset_windows: list[dict[str, object]],
    batch_size: int,
    asset_chunk_size: int,
    bucket_buffer_seconds: int,
    max_batches_per_chunk: int | None,
) -> dict[str, int]:
    fetched = 0
    inserted = 0
    total_batches = 0
    chunks_processed = 0
    asset_count = 0
    for asset_window in asset_windows:
        bucket_start = asset_window["bucket_start"]
        bucket_end = asset_window["bucket_end"]
        window_asset_ids = list(asset_window["asset_ids"])
        asset_count += len(window_asset_ids)
        since_timestamp = int((bucket_start - timedelta(seconds=bucket_buffer_seconds)).timestamp()) - 1
        until_timestamp = int((bucket_end + timedelta(seconds=bucket_buffer_seconds)).timestamp())
        for asset_chunk in _chunked(window_asset_ids, asset_chunk_size):
            cursor = FillCursor(last_timestamp=since_timestamp)
            chunk_batches = 0
            while True:
                prior = cursor
                rows, next_cursor = source.fetch_fills_for_assets(
                    asset_ids=asset_chunk,
                    cursor=cursor,
                    limit=batch_size,
                    until_timestamp=until_timestamp,
                )
                cursor = next_cursor
                if not rows:
                    if cursor != prior:
                        continue
                    break
                normalized = [normalize_goldsky_fill(row, resume_cursor=cursor) for row in rows if row.get("id")]
                inserted += warehouse.insert_raw_fills(normalized)
                fetched += len(rows)
                total_batches += 1
                chunk_batches += 1
                if max_batches_per_chunk is not None and chunk_batches >= max_batches_per_chunk:
                    break
                if len(rows) < batch_size and prior.sticky_timestamp is None and cursor.sticky_timestamp is None:
                    break
            chunks_processed += 1
    return {
        "asset_window_count": len(asset_windows),
        "asset_count": asset_count,
        "chunks_processed": chunks_processed,
        "batches": total_batches,
        "fetched": fetched,
        "inserted": inserted,
    }


def _sync_recent_market_feed(
    warehouse: ResearchWarehouse,
    source: GammaMarketSource,
    *,
    cutoff: datetime,
    batch_size: int,
    max_batches: int | None,
    closed: bool | None,
) -> dict[str, int | bool]:
    fetched = 0
    inserted = 0
    batches = 0
    offset = 0
    reached_cutoff = False
    while True:
        rows = source.fetch_markets(offset=offset, limit=batch_size, ascending=False, closed=closed)
        if not rows:
            break
        normalized: list[dict[str, object]] = []
        for row in rows:
            market = normalize_gamma_market(row)
            created_at = _parse_iso_ts(market.get("created_at"))
            if created_at is not None and created_at < cutoff:
                reached_cutoff = True
                break
            if market.get("market_id"):
                normalized.append(market)
        inserted += warehouse.insert_markets(normalized)
        fetched += len(rows)
        offset += len(rows)
        batches += 1
        if reached_cutoff or len(rows) < batch_size:
            break
        if max_batches is not None and batches >= max_batches:
            break
    return {
        "closed": bool(closed),
        "fetched": fetched,
        "inserted": inserted,
        "batches": batches,
    }


def _parse_iso_ts(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [values[idx:idx + chunk_size] for idx in range(0, len(values), chunk_size)]
