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
    batch_size: int = 1000,
    asset_chunk_size: int = 50,
    bucket_minutes: int = 60,
    bucket_buffer_seconds: int = 900,
    max_batches_per_chunk: int | None = None,
) -> dict[str, object]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))
    asset_windows = warehouse.recent_5m_asset_windows(
        lookback_hours=lookback_hours,
        bucket_minutes=bucket_minutes,
    )
    fetched = 0
    inserted = 0
    total_batches = 0
    chunks_processed = 0
    asset_count = 0
    for asset_window in asset_windows:
        bucket_start = asset_window["bucket_start"]
        bucket_end = asset_window["bucket_end"]
        window_asset_ids = asset_window["asset_ids"]
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
    registry_rows = warehouse.rebuild_market_5m_registry()
    enriched_rows = warehouse.rebuild_fills_enriched()
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "recent_5m_fills",
        "lookback_hours": int(lookback_hours),
        "since_timestamp": int(cutoff.timestamp()) - 1,
        "asset_window_count": len(asset_windows),
        "asset_count": asset_count,
        "asset_chunk_size": int(asset_chunk_size),
        "bucket_minutes": int(bucket_minutes),
        "bucket_buffer_seconds": int(bucket_buffer_seconds),
        "chunks_processed": chunks_processed,
        "batches": total_batches,
        "fetched": fetched,
        "inserted": inserted,
        "market_5m_registry_rows": registry_rows,
        "fills_enriched_rows": enriched_rows,
        "parquet": parquet_paths,
    }


def _chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [values[idx:idx + chunk_size] for idx in range(0, len(values), chunk_size)]
