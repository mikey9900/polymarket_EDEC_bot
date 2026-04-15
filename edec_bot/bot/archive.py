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


def _latest_run_metadata(db_path: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(db_path)
    try:
        cols = _table_columns(conn, "runs")
        if not cols:
            return None
        row = conn.execute(
            """SELECT run_id, started_at, app_version, strategy_version,
                      config_path, config_hash, dry_run, initial_mode,
                      default_order_size_usd, initial_paper_capital
               FROM runs ORDER BY started_at DESC LIMIT 1"""
        ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "started_at": row[1],
            "app_version": row[2],
            "strategy_version": row[3],
            "config_path": row[4],
            "config_hash": row[5],
            "dry_run": bool(row[6]),
            "mode": row[7],
            "order_size_usd": row[8],
            "paper_capital_total": row[9],
        }
    finally:
        conn.close()


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
                "run_id",
                "app_version",
                "strategy_version",
                "config_hash",
                "mode",
                "dry_run",
                "order_size_usd",
                "paper_capital_total",
                "market_slug",
                "window_id",
                "coin",
                "strategy_type",
                "signal_context",
                "signal_overlap_count",
                "side",
                "entry_price",
                "entry_bid",
                "entry_ask",
                "entry_spread",
                "target_price",
                "shares",
                "shares_requested",
                "shares_filled",
                "blocked_min_5_shares",
                "cost",
                "fee_total",
                "status",
                "exit_price",
                "pnl",
                "exit_reason",
                "exit_timestamp",
                "time_remaining_s",
                "bid_at_exit",
                "ask_at_exit",
                "exit_spread",
                "market_start_time",
                "market_end_time",
                "entry_depth_side_usd",
                "opposite_depth_usd",
                "depth_ratio",
                "max_bid_seen",
                "min_bid_seen",
                "time_to_max_bid_s",
                "time_to_min_bid_s",
                "first_profit_time_s",
                "scalp_hit",
                "high_confidence_hit",
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
                "run_id",
                "app_version",
                "strategy_version",
                "config_hash",
                "mode",
                "dry_run",
                "order_size_usd",
                "paper_capital_total",
                "market_slug",
                "window_id",
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
                "shares_requested",
                "shares_filled",
                "blocked_min_5_shares",
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
                "run_id",
                "app_version",
                "strategy_version",
                "config_hash",
                "mode",
                "dry_run",
                "order_size_usd",
                "paper_capital_total",
                "market_slug",
                "window_id",
                "coin",
                "strategy_type",
                "signal_context",
                "signal_overlap_count",
                "suppressed_reason",
                "market_end_time",
                "market_start_time",
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
        pt_market_start = "pt.market_start_time" if "market_start_time" in pt_cols else "NULL AS market_start_time"
        pt_time_remaining = "pt.time_remaining_s" if "time_remaining_s" in pt_cols else "NULL AS time_remaining_s"
        pt_bid_exit = "pt.bid_at_exit" if "bid_at_exit" in pt_cols else "NULL AS bid_at_exit"
        pt_ask_exit = "pt.ask_at_exit" if "ask_at_exit" in pt_cols else "NULL AS ask_at_exit"
        pt_exit_spread = "pt.exit_spread" if "exit_spread" in pt_cols else "NULL AS exit_spread"
        pt_exit_reason = "pt.exit_reason" if "exit_reason" in pt_cols else "NULL AS exit_reason"
        pt_exit_timestamp = "pt.exit_timestamp" if "exit_timestamp" in pt_cols else "NULL AS exit_timestamp"
        pt_run_id = "pt.run_id" if "run_id" in pt_cols else "NULL AS run_id"
        pt_app_version = "pt.app_version" if "app_version" in pt_cols else "NULL AS app_version"
        pt_strategy_version = "pt.strategy_version" if "strategy_version" in pt_cols else "NULL AS strategy_version"
        pt_config_hash = "pt.config_hash" if "config_hash" in pt_cols else "NULL AS config_hash"
        pt_mode = "pt.mode" if "mode" in pt_cols else "NULL AS mode"
        pt_dry_run = "pt.dry_run" if "dry_run" in pt_cols else "NULL AS dry_run"
        pt_order_size = "pt.order_size_usd" if "order_size_usd" in pt_cols else "NULL AS order_size_usd"
        pt_paper_capital = "pt.paper_capital_total" if "paper_capital_total" in pt_cols else "NULL AS paper_capital_total"
        pt_window_id = "pt.window_id" if "window_id" in pt_cols else "NULL AS window_id"
        pt_signal_context = "pt.signal_context" if "signal_context" in pt_cols else "NULL AS signal_context"
        pt_signal_overlap = "pt.signal_overlap_count" if "signal_overlap_count" in pt_cols else "NULL AS signal_overlap_count"
        pt_shares_requested = "pt.shares_requested" if "shares_requested" in pt_cols else "NULL AS shares_requested"
        pt_shares_filled = "pt.shares_filled" if "shares_filled" in pt_cols else "NULL AS shares_filled"
        pt_blocked_min5 = "pt.blocked_min_5_shares" if "blocked_min_5_shares" in pt_cols else "NULL AS blocked_min_5_shares"
        pt_entry_bid = "pt.entry_bid" if "entry_bid" in pt_cols else "NULL AS entry_bid"
        pt_entry_ask = "pt.entry_ask" if "entry_ask" in pt_cols else "NULL AS entry_ask"
        pt_entry_spread = "pt.entry_spread" if "entry_spread" in pt_cols else "NULL AS entry_spread"
        pt_entry_depth = "pt.entry_depth_side_usd" if "entry_depth_side_usd" in pt_cols else "NULL AS entry_depth_side_usd"
        pt_opp_depth = "pt.opposite_depth_usd" if "opposite_depth_usd" in pt_cols else "NULL AS opposite_depth_usd"
        pt_depth_ratio = "pt.depth_ratio" if "depth_ratio" in pt_cols else "NULL AS depth_ratio"
        pt_max_bid = "pt.max_bid_seen" if "max_bid_seen" in pt_cols else "NULL AS max_bid_seen"
        pt_min_bid = "pt.min_bid_seen" if "min_bid_seen" in pt_cols else "NULL AS min_bid_seen"
        pt_tmax = "pt.time_to_max_bid_s" if "time_to_max_bid_s" in pt_cols else "NULL AS time_to_max_bid_s"
        pt_tmin = "pt.time_to_min_bid_s" if "time_to_min_bid_s" in pt_cols else "NULL AS time_to_min_bid_s"
        pt_tprofit = "pt.first_profit_time_s" if "first_profit_time_s" in pt_cols else "NULL AS first_profit_time_s"
        pt_scalp_hit = "pt.scalp_hit" if "scalp_hit" in pt_cols else "NULL AS scalp_hit"
        pt_high_conf_hit = "pt.high_confidence_hit" if "high_confidence_hit" in pt_cols else "NULL AS high_confidence_hit"

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
                {pt_run_id},
                {pt_app_version},
                {pt_strategy_version},
                {pt_config_hash},
                {pt_mode},
                {pt_dry_run},
                {pt_order_size},
                {pt_paper_capital},
                pt.market_slug,
                {pt_window_id},
                {pt_coin},
                {pt_strategy},
                {pt_signal_context},
                {pt_signal_overlap},
                pt.side,
                pt.entry_price,
                {pt_entry_bid},
                {pt_entry_ask},
                {pt_entry_spread},
                pt.target_price,
                pt.shares,
                {pt_shares_requested},
                {pt_shares_filled},
                {pt_blocked_min5},
                pt.cost,
                pt.fee_total,
                pt.status,
                pt.exit_price,
                pt.pnl,
                {pt_exit_reason},
                {pt_exit_timestamp},
                {pt_time_remaining},
                {pt_bid_exit},
                {pt_ask_exit},
                {pt_exit_spread},
                {pt_market_start},
                {pt_market_end},
                {pt_entry_depth},
                {pt_opp_depth},
                {pt_depth_ratio},
                {pt_max_bid},
                {pt_min_bid},
                {pt_tmax},
                {pt_tmin},
                {pt_tprofit},
                {pt_scalp_hit},
                {pt_high_conf_hit},
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
            compact_names = {
                "trade_id": "id", "timestamp": "ts", "run_id": "rid", "app_version": "av",
                "strategy_version": "sv", "config_hash": "ch", "mode": "md", "dry_run": "dr",
                "order_size_usd": "os", "paper_capital_total": "cap", "market_slug": "mkt",
                "window_id": "wid", "coin": "c", "strategy_type": "st", "signal_context": "ctx",
                "signal_overlap_count": "ov", "side": "sd", "entry_price": "ep", "entry_bid": "eb",
                "entry_ask": "ea", "entry_spread": "es", "target_price": "tp", "shares": "sh",
                "shares_requested": "srq", "shares_filled": "sfl", "blocked_min_5_shares": "b5",
                "cost": "cs", "fee_total": "fee", "status": "status", "exit_price": "xp",
                "pnl": "pnl", "exit_reason": "er", "exit_timestamp": "xt", "time_remaining_s": "tx",
                "bid_at_exit": "xb", "ask_at_exit": "xa", "exit_spread": "xs", "market_start_time": "ms",
                "market_end_time": "me", "entry_depth_side_usd": "eds", "opposite_depth_usd": "ods",
                "depth_ratio": "drt", "max_bid_seen": "maxb", "min_bid_seen": "minb",
                "time_to_max_bid_s": "ttmax", "time_to_min_bid_s": "ttmin", "first_profit_time_s": "tfp",
                "scalp_hit": "sc", "high_confidence_hit": "hc", "filter_passed": "fp",
                "filter_failed": "ff", "decision_reason": "why", "coin_velocity_30s": "v30",
                "coin_velocity_60s": "v60", "up_depth_usd": "du", "down_depth_usd": "dd",
                "decision_time_remaining_s": "te",
            }
            writer.writerow([compact_names.get(col, col) for col in columns])
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


def _dropbox_download_file(dropbox_path: str, token: str, local_path: str) -> dict[str, Any]:
    req = request.Request(
        url="https://content.dropboxapi.com/2/files/download",
        data=b"",
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/plain; charset=utf-8",
            "Dropbox-API-Arg": json.dumps({"path": dropbox_path}),
        },
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            p = Path(local_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(body)
            return {"ok": True, "status": resp.status, "bytes": len(body), "path": str(p)}
    except urlerror.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = str(e)
        return {"ok": False, "status": e.code, "error": body, "path": str(local_path)}
    except Exception as e:
        return {"ok": False, "status": None, "error": str(e), "path": str(local_path)}


def sync_dropbox_latest_to_local(
    dropbox_token: str,
    dropbox_root: str = "/EDEC-BOT",
    output_dir: str = "data/dropbox_sync",
    label: str = "EDEC-BOT",
    expand_trades_csv: bool = True,
) -> dict[str, Any]:
    """Pull stable latest archive files from Dropbox into a local folder."""
    label = _safe_label(label)
    root = _normalize_dropbox_root(dropbox_root)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    latest_filenames = {
        "latest_last24h_xlsx": f"{label}_latest_last24h.xlsx",
        "latest_trades_csv_gz": f"{label}_latest_trades.csv.gz",
        "latest_index_json": f"{label}_latest_index.json",
    }
    remote_candidates = _dropbox_latest_remote_candidates(root, latest_filenames)
    local = {
        "latest_last24h_xlsx": str(out / f"{label}_latest_last24h.xlsx"),
        "latest_trades_csv_gz": str(out / f"{label}_latest_trades.csv.gz"),
        "latest_index_json": str(out / f"{label}_latest_index.json"),
    }

    downloads: dict[str, Any] = {}
    for key, candidates in remote_candidates.items():
        attempts: list[dict[str, Any]] = []
        chosen: dict[str, Any] | None = None
        for remote_path in candidates:
            res = _dropbox_download_file(
                dropbox_path=remote_path,
                token=dropbox_token,
                local_path=local[key],
            )
            attempts.append(
                {
                    "remote_path": remote_path,
                    "ok": bool(res.get("ok")),
                    "status": res.get("status"),
                    "error": res.get("error"),
                }
            )
            if res.get("ok"):
                chosen = res
                chosen["remote_path"] = remote_path
                break
        if chosen is None:
            chosen = {
                "ok": False,
                "status": attempts[-1].get("status") if attempts else None,
                "error": attempts[-1].get("error") if attempts else "No Dropbox path candidates built",
                "path": local[key],
                "remote_path": candidates[0] if candidates else None,
            }
        chosen["attempts"] = attempts
        downloads[key] = chosen

    expanded_csv = None
    if expand_trades_csv and downloads["latest_trades_csv_gz"].get("ok"):
        gz_path = Path(local["latest_trades_csv_gz"])
        csv_path = gz_path.with_suffix("")  # .csv.gz -> .csv
        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        expanded_csv = str(csv_path)

    ok = all(bool(v.get("ok")) for v in downloads.values())
    return {
        "ok": ok,
        "checked_at_utc": _utc_now().isoformat(),
        "output_dir": str(out),
        "downloads": downloads,
        "expanded_trades_csv": expanded_csv,
    }


def _safe_label(label: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in label).strip("-_") or "EDEC-BOT"


def _normalize_dropbox_root(dropbox_root: str | None) -> str:
    """Normalize Dropbox root so generated file paths are always valid."""
    root = (dropbox_root or "/EDEC-BOT").strip()
    if len(root) >= 2 and root[0] == root[-1] and root[0] in ("'", '"'):
        root = root[1:-1].strip()
    root = root.replace("\\", "/")
    if not root:
        root = "/EDEC-BOT"
    if "/home/" in root and "dropbox.com" in root:
        marker = "/home/"
        root = root[root.index(marker) + len(marker):]
    if root.startswith("https://") or root.startswith("http://"):
        root = "/EDEC-BOT"
    if not root.startswith("/"):
        root = f"/{root}"
    while "//" in root:
        root = root.replace("//", "/")
    return root.rstrip("/")


def _dropbox_latest_remote_candidates(
    root: str,
    latest_filenames: dict[str, str],
) -> dict[str, list[str]]:
    base = root.rstrip("/")
    candidate_dirs: list[str] = []
    if base.endswith("/latest"):
        candidate_dirs.extend([base, base[: -len("/latest")] or "/"])
    else:
        candidate_dirs.extend([f"{base}/latest", base])

    seen_dirs: set[str] = set()
    unique_dirs: list[str] = []
    for d in candidate_dirs:
        d = d.rstrip("/") or "/"
        if d not in seen_dirs:
            seen_dirs.add(d)
            unique_dirs.append(d)

    result: dict[str, list[str]] = {}
    for key, filename in latest_filenames.items():
        result[key] = [f"{d}/{filename}" if d != "/" else f"/{filename}" for d in unique_dirs]
    return result


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
    run_meta = _latest_run_metadata(db_path)

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
        "latest_run": run_meta,
        "dropbox_files": None,
    }
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

    if dropbox_token:
        root = _normalize_dropbox_root(dropbox_root)
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
        root = _normalize_dropbox_root(dropbox_root)
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
