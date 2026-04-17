"""Shared Telegram export/archive workflows.

This module keeps export/archive orchestration out of telegram_bot.py so the
Telegram UI layer can stay focused on chat interactions and state.
"""

from __future__ import annotations

import json
import os
from typing import Any, Awaitable, Callable


_OPTIONAL_FILE_KEYS = {"latest_signals", "latest_signals_csv_gz"}

RunBlocking = Callable[[Callable[[], Any]], Awaitable[Any]]
SendFilePath = Callable[[str, str], Awaitable[tuple[bool, str | None]]]


async def send_spreadsheet_export(
    export_fn,
    *,
    today_only: bool,
    caption: str,
    send_file_path: SendFilePath,
    run_blocking: RunBlocking,
) -> dict[str, Any]:
    if not export_fn:
        return {"sent": False, "error": "Export not available", "path": None}

    path = await run_blocking(lambda: export_fn(today_only=today_only))
    ok, error = await send_file_path(path, caption)
    return {"sent": ok, "error": error, "path": path}


async def send_recent_export_files(
    export_recent_fn,
    *,
    archive_fn=None,
    archive_latest_fn=None,
    repo_sync_fn=None,
    send_file_path: SendFilePath,
    run_blocking: RunBlocking,
) -> dict[str, Any]:
    trades_path = None
    signals_path = None
    archive_error = None
    repo_sync_error = None

    if archive_fn:
        try:
            archive_result = await run_blocking(archive_fn)
            latest_trades = archive_result.get("latest_trades")
            latest_signals = archive_result.get("latest_signals")
            if latest_trades and os.path.exists(latest_trades):
                trades_path = latest_trades
            if latest_signals and os.path.exists(latest_signals):
                signals_path = latest_signals
        except Exception as exc:
            archive_error = str(exc)

    if repo_sync_fn:
        try:
            sync_result = await run_blocking(repo_sync_fn)
            synced_trades_csv = sync_result.get("expanded_trades_csv")
            synced_signals_csv = sync_result.get("expanded_signals_csv")
            if synced_trades_csv and os.path.exists(synced_trades_csv):
                trades_path = synced_trades_csv
            if synced_signals_csv and os.path.exists(synced_signals_csv):
                signals_path = synced_signals_csv
        except Exception as exc:
            repo_sync_error = str(exc)

    files: dict[str, Any] = {}

    if not trades_path:
        if not export_recent_fn:
            files["trades"] = {"sent": False, "error": "Recent export not available", "path": None}
            return {
                "files": files,
                "archive_error": archive_error,
                "repo_sync_error": repo_sync_error,
            }
        trades_path = await run_blocking(export_recent_fn)

    if trades_path and os.path.exists(trades_path):
        ok, error = await send_file_path(
            trades_path,
            "📊 Last 100 Trades CSV — compact export for AI analysis",
        )
        files["trades"] = {"sent": ok, "error": error, "path": trades_path}
    else:
        files["trades"] = {"sent": False, "error": "Trades export file not found", "path": trades_path}

    if not signals_path and archive_latest_fn:
        latest_paths = archive_latest_fn() or {}
        latest_signals = latest_paths.get("latest_signals")
        if latest_signals and os.path.exists(latest_signals):
            signals_path = latest_signals

    if signals_path and os.path.exists(signals_path):
        caption = (
            "🧠 Last 100 Signals CSV — companion dataset for filter/skip analysis"
            if signals_path.lower().endswith(".csv")
            else "🧠 Latest Signals CSV.GZ — companion dataset for filter/skip analysis"
        )
        ok, error = await send_file_path(signals_path, caption)
        files["signals"] = {"sent": ok, "error": error, "path": signals_path}
    else:
        files["signals"] = {
            "sent": False,
            "skipped": True,
            "optional_missing": True,
            "error": None,
            "path": signals_path,
        }

    return {
        "files": files,
        "archive_error": archive_error,
        "repo_sync_error": repo_sync_error,
    }


async def send_repo_sync_files(
    sync_result: dict,
    send_file_path: SendFilePath,
    *,
    include_index: bool = True,
) -> dict[str, Any]:
    downloads = (sync_result or {}).get("downloads", {})
    results: dict[str, Any] = {}
    file_specs = [
        ("latest_last24h_xlsx", "Dropbox latest 24h Excel export"),
        ("latest_trades_csv_gz", "Dropbox latest compressed trades export"),
        ("latest_signals_csv_gz", "Dropbox latest compressed signals export"),
    ]
    if include_index:
        file_specs.append(("latest_index_json", "Dropbox latest index pointer"))

    for key, caption in file_specs:
        item = downloads.get(key, {})
        path = item.get("path")
        if item.get("ok") and path and os.path.exists(path):
            ok, error = await send_file_path(path, caption)
            results[key] = {"sent": ok, "error": error, "path": path}
        elif key in _OPTIONAL_FILE_KEYS and item.get("optional_missing"):
            results[key] = {
                "sent": False,
                "skipped": True,
                "optional_missing": True,
                "error": None,
                "path": path,
            }
        else:
            results[key] = {"sent": False, "error": "File not available after Dropbox sync", "path": path}
    return results


def repo_sync_message_lines(result: dict, heading_ok: str, heading_fail: str) -> list[str]:
    ok = bool((result or {}).get("ok"))
    downloads = (result or {}).get("downloads", {})
    lines = [
        heading_ok if ok else heading_fail,
        f"Output dir: `{result.get('output_dir', 'unknown')}`",
        f"Expanded CSV: `{result.get('expanded_trades_csv') or 'none'}`",
        f"Expanded Signals CSV: `{result.get('expanded_signals_csv') or 'none'}`",
    ]
    for key in ("latest_last24h_xlsx", "latest_trades_csv_gz", "latest_signals_csv_gz", "latest_index_json"):
        item = downloads.get(key, {})
        if key in _OPTIONAL_FILE_KEYS and item.get("optional_missing"):
            lines.append(f"`{key}`: optional-missing")
            continue
        status_txt = f"`{key}`: {'ok' if item.get('ok') else 'error'} (status={item.get('status')})"
        if not item.get("ok") and item.get("remote_path"):
            status_txt += f"\n  path: `{item.get('remote_path')}`"
        friendly = ((item.get("error_details") or {}).get("friendly") or "").strip()
        if not item.get("ok") and friendly:
            status_txt += f"\n  fix: `{friendly}`"
        err = item.get("error")
        if not item.get("ok") and err:
            err_compact = " ".join(str(err).split())
            if len(err_compact) > 180:
                err_compact = f"{err_compact[:177]}..."
            status_txt += f"\n  error: `{err_compact}`"
        lines.append(status_txt)
    return lines


def file_send_summary(send_result: dict[str, Any], label_map: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for key, label in label_map.items():
        info = send_result.get(key, {}) if send_result else {}
        if info.get("sent") or info.get("optional_missing"):
            continue
        error = " ".join(str(info.get("error") or "unknown error").split())
        if len(error) > 180:
            error = f"{error[:177]}..."
        lines.append(f"{label}: `{error}`")
    return lines


async def send_latest_archive_files(
    archive_latest_fn,
    archive_fn,
    *,
    send_file_path: SendFilePath,
    run_blocking: RunBlocking,
    include_index: bool = True,
) -> dict[str, Any]:
    if not archive_latest_fn:
        return {"available": False, "files": {}, "sent_any": False}
    paths = archive_latest_fn() or {}
    files: dict[str, Any] = {}

    def _path_set():
        return (
            paths.get("latest_excel"),
            paths.get("latest_trades"),
            paths.get("latest_signals"),
            paths.get("latest_index"),
        )

    excel, trades, signals, index = _path_set()
    if not ((excel and os.path.exists(excel)) or (trades and os.path.exists(trades)) or (signals and os.path.exists(signals))):
        if archive_fn:
            try:
                await run_blocking(archive_fn)
            except Exception:
                pass
            paths = archive_latest_fn() or {}
            excel, trades, signals, index = _path_set()

    if excel and os.path.exists(excel):
        ok, error = await send_file_path(excel, "EDEC latest 24h Excel export")
        files["latest_excel"] = {"sent": ok, "error": error, "path": excel}
    else:
        files["latest_excel"] = {"sent": False, "error": "Latest Excel file not found", "path": excel}

    if trades and os.path.exists(trades):
        ok, error = await send_file_path(trades, "EDEC latest compressed trades export (100)")
        files["latest_trades"] = {"sent": ok, "error": error, "path": trades}
    else:
        files["latest_trades"] = {"sent": False, "error": "Latest trades file not found", "path": trades}

    if signals and os.path.exists(signals):
        ok, error = await send_file_path(signals, "EDEC latest compressed signals export (100)")
        files["latest_signals"] = {"sent": ok, "error": error, "path": signals}
    else:
        files["latest_signals"] = {
            "sent": False,
            "skipped": True,
            "optional_missing": True,
            "error": None,
            "path": signals,
        }

    if include_index and index and os.path.exists(index):
        ok, error = await send_file_path(index, "EDEC latest index pointer (most-recent metadata)")
        files["latest_index"] = {"sent": ok, "error": error, "path": index}
    elif include_index:
        files["latest_index"] = {"sent": False, "error": "Latest index file not found", "path": index}

    sent_any = any(bool(info.get("sent")) for info in files.values())
    available = any((info.get("path") and os.path.exists(info["path"])) for info in files.values() if info.get("path"))
    return {"available": available, "files": files, "sent_any": sent_any}


async def build_archive_health_text(
    archive_latest_fn,
    archive_health_fn,
    *,
    run_blocking: RunBlocking,
) -> str:
    if archive_health_fn:
        try:
            health = await run_blocking(archive_health_fn)
        except Exception as exc:
            return f"Archive health check failed: {exc}"
    else:
        if not archive_latest_fn:
            return "Archive health unavailable (archive not configured)."
        paths = archive_latest_fn() or {}
        index_path = paths.get("latest_index")
        if not index_path or not os.path.exists(index_path):
            return "No archive index found yet. Run /latest_export or wait for the daily archive run."
        try:
            with open(index_path, "r", encoding="utf-8") as fh:
                idx = json.load(fh)
        except Exception as exc:
            return f"Archive index unreadable: {exc}"
        health = {
            "label": idx.get("label", "EDEC-BOT"),
            "checked_at_utc": "unknown",
            "index": idx,
            "dropbox_live": None,
        }

    idx = health.get("index") or {}
    rows = idx.get("row_counts", {})
    label = health.get("label", idx.get("label", "EDEC-BOT"))
    exported = idx.get("exported_at_utc", "unknown")
    checked_at = health.get("checked_at_utc", "unknown")
    upload_results = idx.get("dropbox_uploads") or {}

    live = health.get("dropbox_live")
    if live is None:
        dropbox_line = "Dropbox live check: disabled"
    else:
        ok = bool(live.get("ok"))
        files = live.get("files", {})
        missing = [key for key, value in files.items() if not value.get("exists")]
        auth_failed = [
            key for key, value in files.items()
            if ((value.get("error_details") or {}).get("reason") in ("expired_access_token", "invalid_access_token"))
        ]
        if ok:
            dropbox_line = "Dropbox live check: ok"
        elif auth_failed:
            dropbox_line = "Dropbox live check: token expired/invalid"
        else:
            miss = ", ".join(missing) if missing else "unknown"
            dropbox_line = f"Dropbox live check: missing ({miss})"

    upload_failures = [key for key, value in upload_results.items() if not value.get("ok")]
    upload_line = (
        f"Last upload result: failed ({', '.join(upload_failures)})"
        if upload_failures
        else "Last upload result: ok/unknown"
    )

    return (
        f"Archive Health ({label})\n"
        f"Last export (UTC): {exported}\n"
        f"Live check (UTC): {checked_at}\n"
        f"{dropbox_line}\n"
        f"{upload_line}\n"
        f"24h rows paper/live/decisions/signals: "
        f"{rows.get('paper_trades_24h', 0)}/{rows.get('live_trades_24h', 0)}/"
        f"{rows.get('decisions_24h', 0)}/{rows.get('signals_24h', 0)}\n"
        f"Recent rows trades/signals: {rows.get('recent_trades_rows', 0)}/{rows.get('recent_signals_rows', 0)}"
    )
