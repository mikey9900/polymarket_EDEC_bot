"""Incremental sync runners for the research warehouse."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable

from .sources import FillCursor, FillSource, GammaMarketSource, GoldskyFillSource, normalize_gamma_market, normalize_goldsky_fill

if TYPE_CHECKING:
    from .warehouse import ResearchWarehouse


ProgressCallback = Callable[[str], None]


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
    target_coins: list[str] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, object]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(lookback_days))
    normalized_target_coins = _normalize_target_coins(target_coins)
    _emit_progress(progress_callback, f"Gamma recent markets: cutoff {cutoff.isoformat()}.")
    open_stats = _sync_recent_market_feed(
        warehouse,
        source,
        cutoff=cutoff,
        batch_size=batch_size,
        max_batches=max_batches,
        target_coins=normalized_target_coins,
        closed=None,
        order="createdAt",
        progress_callback=progress_callback,
    )
    closed_stats = _sync_recent_market_feed(
        warehouse,
        source,
        cutoff=cutoff,
        batch_size=batch_size,
        max_batches=max_batches,
        target_coins=normalized_target_coins,
        closed=True,
        order="closedTime",
        progress_callback=progress_callback,
    )
    _emit_progress(progress_callback, "Gamma feeds complete. Rebuilding 5m registry.")
    registry_rows = warehouse.rebuild_market_5m_registry()
    _emit_progress(progress_callback, "Gamma registry rebuilt. Rebuilding enriched fills.")
    enriched_rows = warehouse.rebuild_fills_enriched()
    _emit_progress(progress_callback, "Gamma enrich complete. Exporting parquet snapshots.")
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "recent_markets",
        "lookback_days": int(lookback_days),
        "cutoff": cutoff.isoformat(),
        "target_coins": normalized_target_coins,
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
    target_coins: list[str] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(hours=int(lookback_hours))
    history_cutoff = now - timedelta(days=int(history_lookback_days))
    normalized_target_coins = _normalize_target_coins(target_coins)
    recent_windows = warehouse.asset_windows_between(
        since=recent_cutoff,
        until=now,
        bucket_minutes=bucket_minutes,
    )
    recent_windows = _filter_asset_windows_by_coin(recent_windows, normalized_target_coins)
    _emit_progress(
        progress_callback,
        f"Goldsky recent windows: {len(recent_windows)} windows | {_count_asset_ids(recent_windows)} assets.",
    )
    history_windows: list[dict[str, object]] = []
    if int(history_lookback_days) > 0 and history_cutoff < recent_cutoff:
        history_windows = warehouse.asset_windows_between(
            since=history_cutoff,
            until=recent_cutoff,
            bucket_minutes=history_bucket_minutes,
        )
        history_windows = _filter_asset_windows_by_coin(history_windows, normalized_target_coins)
    _emit_progress(
        progress_callback,
        f"Goldsky history windows: {len(history_windows)} windows | {_count_asset_ids(history_windows)} assets.",
    )
    recent_stats = _sync_asset_windows(
        warehouse,
        source,
        asset_windows=recent_windows,
        batch_size=batch_size,
        asset_chunk_size=asset_chunk_size,
        bucket_buffer_seconds=bucket_buffer_seconds,
        max_batches_per_chunk=max_batches_per_chunk,
        progress_callback=progress_callback,
        label="recent",
    )
    history_stats = _sync_asset_windows(
        warehouse,
        source,
        asset_windows=history_windows,
        batch_size=batch_size,
        asset_chunk_size=asset_chunk_size,
        bucket_buffer_seconds=bucket_buffer_seconds,
        max_batches_per_chunk=max_history_batches_per_chunk,
        progress_callback=progress_callback,
        label="history",
    )
    _emit_progress(progress_callback, "Goldsky scans complete. Rebuilding 5m registry.")
    registry_rows = warehouse.rebuild_market_5m_registry()
    _emit_progress(progress_callback, "Goldsky registry rebuilt. Rebuilding enriched fills.")
    enriched_rows = warehouse.rebuild_fills_enriched()
    _emit_progress(progress_callback, "Goldsky enrich complete. Exporting parquet snapshots.")
    parquet_paths = warehouse.export_parquet()
    return {
        "dataset": "recent_5m_fills",
        "window_end": now.isoformat(),
        "lookback_hours": int(lookback_hours),
        "history_lookback_days": int(history_lookback_days),
        "target_coins": normalized_target_coins,
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
    target_coins: list[str] | None = None,
) -> dict[str, object]:
    market_result = sync_recent_markets(
        warehouse,
        market_source,
        lookback_days=market_lookback_days,
        batch_size=market_batch_size,
        max_batches=market_max_batches,
        target_coins=target_coins,
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
        target_coins=target_coins,
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
    progress_callback: ProgressCallback | None,
    label: str,
) -> dict[str, int]:
    fetched = 0
    inserted = 0
    total_batches = 0
    chunks_processed = 0
    asset_count = 0
    total_chunks = _count_chunks(asset_windows, asset_chunk_size)
    if not asset_windows:
        _emit_progress(progress_callback, f"Goldsky {label}: no asset windows to scan.")
    for asset_window in asset_windows:
        bucket_start = asset_window["bucket_start"]
        bucket_end = asset_window["bucket_end"]
        window_asset_ids = list(asset_window["asset_ids"])
        asset_count += len(window_asset_ids)
        since_timestamp = int((bucket_start - timedelta(seconds=bucket_buffer_seconds)).timestamp()) - 1
        until_timestamp = int((bucket_end + timedelta(seconds=bucket_buffer_seconds)).timestamp())
        for asset_chunk in _chunked(window_asset_ids, asset_chunk_size):
            next_chunk_number = chunks_processed + 1
            _emit_progress(
                progress_callback,
                f"Goldsky {label} chunk {next_chunk_number}/{total_chunks}: "
                f"{len(asset_chunk)} assets | fetched {fetched} fills so far.",
            )
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
                _emit_progress(
                    progress_callback,
                    f"Goldsky {label} chunk {next_chunk_number}/{total_chunks}: "
                    f"batch {chunk_batches} fetched {len(rows)} fills | total {fetched}.",
                )
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
    target_coins: list[str] | None,
    closed: bool | None,
    order: str,
    progress_callback: ProgressCallback | None,
) -> dict[str, int | bool]:
    normalized_target_coins = _normalize_target_coins(target_coins)
    fetched = 0
    inserted = 0
    batches = 0
    offset = 0
    reached_cutoff = False
    matched_coins: set[str] = set()
    feed_label = "closed" if closed else "open"
    effective_max_batches = max_batches
    if normalized_target_coins and max_batches is not None:
        effective_max_batches = max(int(max_batches), min(10, max(2, len(normalized_target_coins) * 2)))
    while True:
        _emit_progress(
            progress_callback,
            f"Gamma {feed_label} feed: batch {batches + 1} offset {offset} order {order}.",
        )
        rows = source.fetch_markets(
            offset=offset,
            limit=batch_size,
            ascending=False,
            closed=closed,
            order=order,
        )
        if not rows:
            break
        normalized: list[dict[str, object]] = []
        for row in rows:
            market = normalize_gamma_market(row)
            created_at = _parse_iso_ts(market.get("created_at"))
            closed_at = _parse_iso_ts(market.get("closed_time"))
            end_at = _parse_iso_ts(market.get("end_time"))
            comparison_ts = closed_at or end_at or created_at
            if comparison_ts is not None and comparison_ts < cutoff:
                reached_cutoff = True
                break
            market_coin = _market_coin(market)
            if normalized_target_coins and market_coin not in normalized_target_coins:
                continue
            if market.get("market_id"):
                normalized.append(market)
                if market_coin:
                    matched_coins.add(market_coin)
        inserted += warehouse.insert_markets(normalized)
        fetched += len(rows)
        offset += len(rows)
        batches += 1
        _emit_progress(
            progress_callback,
            f"Gamma {feed_label} feed: fetched {fetched} markets across {batches} batches.",
        )
        if reached_cutoff or len(rows) < batch_size:
            break
        if effective_max_batches is not None and batches >= effective_max_batches:
            break
    return {
        "closed": bool(closed),
        "order": str(order or "createdAt"),
        "target_coins": normalized_target_coins,
        "matched_coins": sorted(matched_coins),
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


def _market_coin(market: dict[str, object]) -> str:
    ticker = str(market.get("ticker") or "").strip().lower()
    if ticker:
        return ticker
    slug = str(market.get("market_slug") or "").strip().lower()
    if "-updown-5m-" in slug:
        return slug.split("-updown-5m-", 1)[0]
    return slug.split("-", 1)[0] if slug else ""


def _normalize_target_coins(target_coins: list[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for coin in list(target_coins or []):
        value = str(coin or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _filter_asset_windows_by_coin(
    asset_windows: list[dict[str, object]],
    target_coins: list[str] | None,
) -> list[dict[str, object]]:
    normalized_target_coins = _normalize_target_coins(target_coins)
    if not normalized_target_coins:
        return list(asset_windows)
    return [
        dict(window)
        for window in asset_windows
        if str(window.get("coin") or "").strip().lower() in normalized_target_coins
    ]


def _emit_progress(callback: ProgressCallback | None, message: str) -> None:
    if callback is None:
        return
    callback(str(message))


def _count_asset_ids(asset_windows: list[dict[str, object]]) -> int:
    return sum(len(list(window.get("asset_ids") or [])) for window in asset_windows)


def _count_chunks(asset_windows: list[dict[str, object]], chunk_size: int) -> int:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    total = 0
    for window in asset_windows:
        asset_count = len(list(window.get("asset_ids") or []))
        total += (asset_count + chunk_size - 1) // chunk_size
    return total
