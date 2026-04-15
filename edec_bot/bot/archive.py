"""Daily archive export: 24h Excel + compressed recent trades + optional Dropbox sync."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import request
from urllib import error as urlerror

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from bot.export import _auto_width, _freeze, _style_header

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db_iso(ts: datetime) -> str:
    """SQLite tables in this project store naive ISO timestamps."""
    return ts.replace(tzinfo=None).isoformat()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _select_with_missing(conn: sqlite3.Connection, table: str, desired: list[str]) -> str:
    cols = _table_columns(conn, table)
    pieces: list[str] = []
    for col in desired:
        if col in cols:
            pieces.append(col)
        else:
            pieces.append(f"NULL AS {col}")
    return ", ".join(pieces)


def _select_all(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...],
) -> tuple[list[str], list[tuple[Any, ...]]]:
    cur = conn.execute(query, params)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return columns, rows


def _sheet_from_rows(wb: Workbook, sheet_name: str, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    ws = wb.create_sheet(sheet_name)
    if not columns:
        ws.append(["no_data"])
        _style_header(ws, 1)
        _freeze(ws)
        _auto_width(ws)
        return
    ws.append(columns)
    _style_header(ws, len(columns))
    _freeze(ws)
    for row in rows:
        ws.append(list(row))
    ws.auto_filter.ref = f"A1:{get_column_letter(len(columns))}1"
    _auto_width(ws)


def export_last_24h_excel(
    db_path: str,
    output_dir: str,
    label: str,
    now_utc: datetime | None = None,
) -> tuple[str, dict[str, int]]:
    now_utc = now_utc or _utc_now()
    since_utc = now_utc - timedelta(hours=24)
    since_iso = _db_iso(since_utc)

    conn = sqlite3.connect(db_path)
    try:
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]

        pt_select = _select_with_missing(
            conn,
            "paper_trades",
            [
                "id",
                "timestamp",
                "market_slug",
                "coin",
                "strategy_type",
                "side",
                "entry_price",
                "target_price",
                "shares",
                "cost",
                "fee_total",
                "status",
                "exit_price",
                "pnl",
                "exit_reason",
                "exit_timestamp",
                "time_remaining_s",
                "bid_at_exit",
                "market_end_time",
            ],
        )
        pt_cols, pt_rows = _select_all(
            conn,
            f"""
            SELECT {pt_select}
            FROM paper_trades
            WHERE timestamp >= ?
            ORDER BY id DESC
            """,
            (since_iso,),
        )
        _sheet_from_rows(wb, "Paper Trades 24h", pt_cols, pt_rows)

        t_select = _select_with_missing(
            conn,
            "trades",
            [
                "id",
                "decision_id",
                "timestamp",
                "market_slug",
                "coin",
                "strategy_type",
                "side",
                "up_price",
                "down_price",
                "entry_price",
                "target_price",
                "combined_cost",
                "fee_total",
                "shares",
                "status",
                "abort_cost",
                "error",
            ],
        )
        lt_cols, lt_rows = _select_all(
            conn,
            f"""
            SELECT {t_select}
            FROM trades
            WHERE timestamp >= ?
            ORDER BY id DESC
            """,
            (since_iso,),
        )
        _sheet_from_rows(wb, "Live Trades 24h", lt_cols, lt_rows)

        d_select = _select_with_missing(
            conn,
            "decisions",
            [
                "id",
                "timestamp",
                "market_slug",
                "coin",
                "strategy_type",
                "market_end_time",
                "up_best_ask",
                "down_best_ask",
                "combined_cost",
                "btc_price",
                "coin_velocity_30s",
                "coin_velocity_60s",
                "up_depth_usd",
                "down_depth_usd",
                "time_remaining_s",
                "feed_count",
                "filter_passed",
                "filter_failed",
                "action",
                "reason",
            ],
        )
        d_cols, d_rows = _select_all(
            conn,
            f"""
            SELECT {d_select}
            FROM decisions
            WHERE timestamp >= ?
            ORDER BY id DESC
            """,
            (since_iso,),
        )
        _sheet_from_rows(wb, "Decisions 24h", d_cols, d_rows)

        summary = wb.create_sheet("Summary")
        summary_headers = ["Metric", "Value"]
        summary.append(summary_headers)
        _style_header(summary, len(summary_headers))
        summary_rows = [
            ("Label", label),
            ("Exported At (UTC)", now_utc.isoformat()),
            ("Window Start (UTC)", since_utc.isoformat()),
            ("Paper Trades (24h)", len(pt_rows)),
            ("Live Trades (24h)", len(lt_rows)),
            ("Decisions (24h)", len(d_rows)),
        ]
        for r in summary_rows:
            summary.append(list(r))
        _freeze(summary)
        _auto_width(summary)

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        date_stamp = now_utc.strftime("%Y-%m-%d")
        path = str(Path(output_dir) / f"{date_stamp}_{label}_last24h.xlsx")
        wb.save(path)
        return path, {
            "paper_trades_24h": len(pt_rows),
            "live_trades_24h": len(lt_rows),
            "decisions_24h": len(d_rows),
        }
    finally:
        conn.close()


def export_recent_trades_csv_gz(
    db_path: str,
    output_dir: str,
    label: str,
    limit: int,
    now_utc: datetime | None = None,
) -> tuple[str, int, int | None, int | None]:
    now_utc = now_utc or _utc_now()
    conn = sqlite3.connect(db_path)
    try:
        pt_cols = _table_columns(conn, "paper_trades")
        d_cols = _table_columns(conn, "decisions")

        pt_coin = "pt.coin" if "coin" in pt_cols else "'btc' AS coin"
        pt_strategy = "pt.strategy_type" if "strategy_type" in pt_cols else "'dual_leg' AS strategy_type"
        pt_market_end = "pt.market_end_time" if "market_end_time" in pt_cols else "NULL AS market_end_time"
        pt_time_remaining = "pt.time_remaining_s" if "time_remaining_s" in pt_cols else "NULL AS time_remaining_s"
        pt_bid_exit = "pt.bid_at_exit" if "bid_at_exit" in pt_cols else "NULL AS bid_at_exit"
        pt_exit_reason = "pt.exit_reason" if "exit_reason" in pt_cols else "NULL AS exit_reason"
        pt_exit_timestamp = "pt.exit_timestamp" if "exit_timestamp" in pt_cols else "NULL AS exit_timestamp"

        d_filter_passed = "d.filter_passed" if "filter_passed" in d_cols else "NULL AS filter_passed"
        d_filter_failed = "d.filter_failed" if "filter_failed" in d_cols else "NULL AS filter_failed"
        d_reason = "d.reason AS decision_reason" if "reason" in d_cols else "NULL AS decision_reason"
        d_vel_30 = "d.coin_velocity_30s" if "coin_velocity_30s" in d_cols else "NULL AS coin_velocity_30s"
        d_vel_60 = "d.coin_velocity_60s" if "coin_velocity_60s" in d_cols else "NULL AS coin_velocity_60s"
        d_up_depth = "d.up_depth_usd" if "up_depth_usd" in d_cols else "NULL AS up_depth_usd"
        d_down_depth = "d.down_depth_usd" if "down_depth_usd" in d_cols else "NULL AS down_depth_usd"
        d_time_remaining = (
            "d.time_remaining_s AS decision_time_remaining_s"
            if "time_remaining_s" in d_cols
            else "NULL AS decision_time_remaining_s"
        )

        has_pt_strategy = "strategy_type" in pt_cols
        has_d_strategy = "strategy_type" in d_cols
        if has_pt_strategy and has_d_strategy:
            join_sql = """
            LEFT JOIN (
                SELECT market_slug, strategy_type, MAX(id) AS best_id
                FROM decisions
                GROUP BY market_slug, strategy_type
            ) top_d ON top_d.market_slug = pt.market_slug
                   AND top_d.strategy_type = pt.strategy_type
            """
        else:
            join_sql = """
            LEFT JOIN (
                SELECT market_slug, MAX(id) AS best_id
                FROM decisions
                GROUP BY market_slug
            ) top_d ON top_d.market_slug = pt.market_slug
            """

        columns, rows = _select_all(
            conn,
            f"""
            SELECT
                pt.id AS trade_id,
                pt.timestamp,
                pt.market_slug,
                {pt_coin},
                {pt_strategy},
                pt.side,
                pt.entry_price,
                pt.target_price,
                pt.shares,
                pt.cost,
                pt.fee_total,
                pt.status,
                pt.exit_price,
                pt.pnl,
                {pt_exit_reason},
                {pt_exit_timestamp},
                {pt_time_remaining},
                {pt_bid_exit},
                {pt_market_end},
                {d_filter_passed},
                {d_filter_failed},
                {d_reason},
                {d_vel_30},
                {d_vel_60},
                {d_up_depth},
                {d_down_depth},
                {d_time_remaining}
            FROM paper_trades pt
            {join_sql}
            LEFT JOIN decisions d ON d.id = top_d.best_id
            ORDER BY pt.id DESC
            LIMIT ?
            """,
            (limit,),
        )

        ids = [int(r[0]) for r in rows if r and r[0] is not None]
        newest = max(ids) if ids else None
        oldest = min(ids) if ids else None
        id_start = f"{oldest:06d}" if oldest is not None else "000000"
        id_end = f"{newest:06d}" if newest is not None else "000000"

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        date_stamp = now_utc.strftime("%Y-%m-%d")
        time_stamp = now_utc.strftime("%H%M%S")
        out_path = Path(output_dir) / f"{date_stamp}_{time_stamp}_{label}_trades_{id_start}-{id_end}.csv.gz"

        with gzip.open(out_path, "wt", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        return str(out_path), len(rows), oldest, newest
    finally:
        conn.close()


def _dropbox_upload_file(local_path: str, dropbox_path: str, token: str) -> None:
    with open(local_path, "rb") as fh:
        body = fh.read()

    req = request.Request(
        url="https://content.dropboxapi.com/2/files/upload",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": json.dumps(
                {"path": dropbox_path, "mode": "overwrite", "autorename": False, "mute": True}
            ),
        },
    )
    with request.urlopen(req, timeout=30) as resp:
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"Dropbox upload failed for {dropbox_path} with status {resp.status}")


def _dropbox_get_metadata(dropbox_path: str, token: str) -> dict[str, Any]:
    req = request.Request(
        url="https://api.dropboxapi.com/2/files/get_metadata",
        data=json.dumps({"path": dropbox_path}).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
            return {"exists": True, "status": resp.status, "payload": payload}
    except urlerror.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = str(e)
        # Dropbox uses 409 for not_found
        if e.code == 409:
            return {"exists": False, "status": e.code, "error": body}
        return {"exists": False, "status": e.code, "error": body}
    except Exception as e:
        return {"exists": False, "status": None, "error": str(e)}


def _safe_label(label: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in label).strip("-_") or "EDEC-BOT"


def run_daily_archive(
    db_path: str = "data/decisions.db",
    output_dir: str = "data/exports",
    label: str = "EDEC-BOT",
    recent_limit: int = 500,
    dropbox_token: str | None = None,
    dropbox_root: str = "/EDEC-BOT",
) -> dict[str, Any]:
    now_utc = _utc_now()
    label = _safe_label(label)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    excel_path, counts = export_last_24h_excel(
        db_path=db_path,
        output_dir=output_dir,
        label=label,
        now_utc=now_utc,
    )
    recent_path, recent_count, oldest_id, newest_id = export_recent_trades_csv_gz(
        db_path=db_path,
        output_dir=output_dir,
        label=label,
        limit=recent_limit,
        now_utc=now_utc,
    )

    latest_excel = str(output_path / f"{label}_latest_last24h.xlsx")
    latest_trades = str(output_path / f"{label}_latest_trades.csv.gz")
    shutil.copy2(excel_path, latest_excel)
    shutil.copy2(recent_path, latest_trades)

    index_path = output_path / f"{label}_latest_index.json"
    index = {
        "label": label,
        "exported_at_utc": now_utc.isoformat(),
        "window_hours": 24,
        "recent_trades_limit": recent_limit,
        "row_counts": {
            **counts,
            "recent_trades_rows": recent_count,
        },
        "trade_id_range": {
            "oldest": oldest_id,
            "newest": newest_id,
        },
        "local_files": {
            "daily_last24h_xlsx": Path(excel_path).name,
            "daily_recent_trades_csv_gz": Path(recent_path).name,
            "latest_last24h_xlsx": Path(latest_excel).name,
            "latest_trades_csv_gz": Path(latest_trades).name,
            "latest_index_json": index_path.name,
        },
        "dropbox_files": None,
    }
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

    if dropbox_token:
        root = dropbox_root.rstrip("/")
        dbx_paths = {
            "daily_last24h_xlsx": f"{root}/daily-reports/{Path(excel_path).name}",
            "daily_recent_trades_csv_gz": f"{root}/daily-archives/{Path(recent_path).name}",
            "latest_last24h_xlsx": f"{root}/latest/{Path(latest_excel).name}",
            "latest_trades_csv_gz": f"{root}/latest/{Path(latest_trades).name}",
            "latest_index_json": f"{root}/latest/{index_path.name}",
        }
        _dropbox_upload_file(excel_path, dbx_paths["daily_last24h_xlsx"], dropbox_token)
        _dropbox_upload_file(recent_path, dbx_paths["daily_recent_trades_csv_gz"], dropbox_token)
        _dropbox_upload_file(latest_excel, dbx_paths["latest_last24h_xlsx"], dropbox_token)
        _dropbox_upload_file(latest_trades, dbx_paths["latest_trades_csv_gz"], dropbox_token)
        _dropbox_upload_file(str(index_path), dbx_paths["latest_index_json"], dropbox_token)
        index["dropbox_files"] = dbx_paths
        index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

    logger.info("Daily archive export complete: %s", json.dumps(index["local_files"]))
    return {
        "excel_path": excel_path,
        "recent_path": recent_path,
        "latest_excel": latest_excel,
        "latest_trades": latest_trades,
        "index_path": str(index_path),
        "row_counts": index["row_counts"],
        "trade_id_range": index["trade_id_range"],
        "dropbox_files": index["dropbox_files"],
    }


def latest_archive_paths(output_dir: str = "data/exports", label: str = "EDEC-BOT") -> dict[str, str]:
    label = _safe_label(label)
    base = Path(output_dir)
    return {
        "latest_excel": str(base / f"{label}_latest_last24h.xlsx"),
        "latest_trades": str(base / f"{label}_latest_trades.csv.gz"),
        "latest_index": str(base / f"{label}_latest_index.json"),
    }


def read_latest_index(output_dir: str = "data/exports", label: str = "EDEC-BOT") -> dict[str, Any] | None:
    paths = latest_archive_paths(output_dir=output_dir, label=label)
    p = Path(paths["latest_index"])
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def archive_health_snapshot(
    output_dir: str = "data/exports",
    label: str = "EDEC-BOT",
    dropbox_token: str | None = None,
    dropbox_root: str = "/EDEC-BOT",
) -> dict[str, Any]:
    label = _safe_label(label)
    local_paths = latest_archive_paths(output_dir=output_dir, label=label)
    index = read_latest_index(output_dir=output_dir, label=label)

    health: dict[str, Any] = {
        "label": label,
        "checked_at_utc": _utc_now().isoformat(),
        "index": index,
        "local": {
            "latest_excel_exists": Path(local_paths["latest_excel"]).exists(),
            "latest_trades_exists": Path(local_paths["latest_trades"]).exists(),
            "latest_index_exists": Path(local_paths["latest_index"]).exists(),
        },
        "dropbox_live": None,
    }

    if dropbox_token:
        root = dropbox_root.rstrip("/")
        latest_remote = {
            "latest_last24h_xlsx": f"{root}/latest/{label}_latest_last24h.xlsx",
            "latest_trades_csv_gz": f"{root}/latest/{label}_latest_trades.csv.gz",
            "latest_index_json": f"{root}/latest/{label}_latest_index.json",
        }
        files: dict[str, Any] = {}
        for key, p in latest_remote.items():
            files[key] = {"path": p, **_dropbox_get_metadata(p, dropbox_token)}
        live_ok = all(bool(v.get("exists")) for v in files.values())
        health["dropbox_live"] = {
            "enabled": True,
            "ok": live_ok,
            "files": files,
        }

    return health


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate 24h EDEC exports and optional Dropbox sync."
    )
    parser.add_argument("--db-path", default="data/decisions.db")
    parser.add_argument("--output-dir", default="data/exports")
    parser.add_argument("--label", default="EDEC-BOT")
    parser.add_argument("--recent-limit", type=int, default=500)
    parser.add_argument("--dropbox-token", default=os.getenv("EDEC_DROPBOX_TOKEN"))
    parser.add_argument("--dropbox-root", default=os.getenv("EDEC_DROPBOX_ROOT", "/EDEC-BOT"))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_daily_archive(
        db_path=args.db_path,
        output_dir=args.output_dir,
        label=args.label,
        recent_limit=args.recent_limit,
        dropbox_token=args.dropbox_token,
        dropbox_root=args.dropbox_root,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
