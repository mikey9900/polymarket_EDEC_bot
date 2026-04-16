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
from urllib.parse import urlencode

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from bot.export import _auto_width, _freeze, _style_header

logger = logging.getLogger(__name__)

_OPTIONAL_LATEST_KEYS = {"latest_signals_csv_gz"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _build_dropbox_auth(
    dropbox_token: str | None = None,
    dropbox_refresh_token: str | None = None,
    dropbox_app_key: str | None = None,
    dropbox_app_secret: str | None = None,
) -> dict[str, Any] | None:
    auth = {
        "access_token": (dropbox_token or "").strip(),
        "refresh_token": (dropbox_refresh_token or "").strip(),
        "app_key": (dropbox_app_key or "").strip(),
        "app_secret": (dropbox_app_secret or "").strip(),
        "_cached_access_token": None,
        "_cached_expires_at": None,
    }
    if auth["refresh_token"]:
        if not auth["app_key"] or not auth["app_secret"]:
            raise RuntimeError(
                "Dropbox refresh token requires both dropbox_app_key and dropbox_app_secret."
            )
        return auth
    if auth["access_token"]:
        return auth
    return None


def _resolve_dropbox_access_token(dropbox_auth: dict[str, Any]) -> str:
    cached_token = dropbox_auth.get("_cached_access_token")
    cached_expires_at = dropbox_auth.get("_cached_expires_at")
    now_ts = _utc_now().timestamp()
    if cached_token and isinstance(cached_expires_at, (int, float)) and cached_expires_at > now_ts + 60:
        return str(cached_token)

    refresh_token = str(dropbox_auth.get("refresh_token") or "").strip()
    if refresh_token:
        payload = urlencode(
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": str(dropbox_auth.get("app_key") or ""),
                "client_secret": str(dropbox_auth.get("app_secret") or ""),
            }
        ).encode("utf-8")
        req = request.Request(
            url="https://api.dropboxapi.com/oauth2/token",
            data=payload,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            token_payload = json.loads(raw) if raw else {}
            access_token = str(token_payload.get("access_token") or "").strip()
            expires_in = int(token_payload.get("expires_in") or 0)
            if not access_token:
                raise RuntimeError("Dropbox OAuth refresh succeeded but returned no access_token.")
            dropbox_auth["_cached_access_token"] = access_token
            dropbox_auth["_cached_expires_at"] = now_ts + max(0, expires_in)
            return access_token

    access_token = str(dropbox_auth.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Dropbox authentication is not configured.")
    return access_token


def _dropbox_error_details(raw_error: str) -> dict[str, Any]:
    text = (raw_error or "").strip()
    details: dict[str, Any] = {"raw": text}
    if not text:
        return details
    try:
        payload = json.loads(text)
    except Exception:
        if "required scope '" in text:
            scope = text.split("required scope '", 1)[1].split("'", 1)[0]
            details["reason"] = "missing_scope"
            details["friendly"] = f"Dropbox app is missing required scope {scope}. Enable it in the app console and mint a new token."
        return details

    summary = str(payload.get("error_summary") or "")
    error_obj = payload.get("error") or {}
    tag = error_obj.get(".tag") if isinstance(error_obj, dict) else None
    details.update({"payload": payload, "summary": summary, "tag": tag})

    if tag == "expired_access_token":
        details["reason"] = "expired_access_token"
        details["friendly"] = "Dropbox access token expired. Update the configured Dropbox token."
    elif tag == "invalid_access_token":
        details["reason"] = "invalid_access_token"
        details["friendly"] = "Dropbox access token is invalid. Update the configured Dropbox token."
    elif summary.startswith("path/not_found/"):
        details["reason"] = "path_not_found"
        details["friendly"] = "Dropbox file not found at the configured path."

    return details


def _is_optional_latest_missing(item: dict[str, Any] | None, key: str) -> bool:
    if key not in _OPTIONAL_LATEST_KEYS:
        return False
    info = item or {}
    if info.get("ok"):
        return False
    details = info.get("error_details") or {}
    if details.get("reason") == "path_not_found":
        return True
    err_text = str(info.get("error") or "").lower()
    return "path/not_found" in err_text


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


def _aliased_select(table_alias: str, available_cols: set[str], mapping: list[tuple[str, str]]) -> str:
    pieces: list[str] = []
    for source, alias in mapping:
        if source in available_cols:
            pieces.append(f"{table_alias}.{source} AS {alias}")
        else:
            pieces.append(f"NULL AS {alias}")
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
                "signal_score",
                "score_velocity",
                "score_entry",
                "score_depth",
                "score_spread",
                "score_time",
                "score_balance",
                "target_delta",
                "hard_stop_delta",
                "mfe",
                "mae",
                "peak_net_pnl",
                "trough_net_pnl",
                "stall_exit_triggered",
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

        sig_select = _select_with_missing(
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
                "action",
                "suppressed_reason",
                "reason",
                "signal_context",
                "signal_overlap_count",
                "entry_price",
                "target_price",
                "expected_profit_per_share",
                "signal_score",
                "score_velocity",
                "score_entry",
                "score_depth",
                "score_spread",
                "score_time",
                "score_balance",
                "time_remaining_s",
                "coin_velocity_30s",
                "coin_velocity_60s",
                "entry_bid",
                "entry_ask",
                "entry_spread",
                "entry_depth_side_usd",
                "opposite_depth_usd",
                "depth_ratio",
                "resignal_cooldown_s",
                "min_price_improvement",
                "last_signal_age_s",
                "filter_passed",
                "filter_failed",
            ],
        )
        sig_cols, sig_rows = _select_all(
            conn,
            f"""
            SELECT {sig_select}
            FROM decisions
            WHERE timestamp >= ?
              AND action IN ('DRY_RUN_SIGNAL', 'TRADE', 'SUPPRESSED')
            ORDER BY id DESC
            """,
            (since_iso,),
        )
        _sheet_from_rows(wb, "Signals 24h", sig_cols, sig_rows)

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
            ("Signals (24h)", len(sig_rows)),
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
            "signals_24h": len(sig_rows),
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

        pt_select = _aliased_select(
            "pt",
            pt_cols,
            [
                ("id", "trade_id"),
                ("timestamp", "timestamp"),
                ("run_id", "run_id"),
                ("app_version", "app_version"),
                ("strategy_version", "strategy_version"),
                ("config_hash", "config_hash"),
                ("mode", "mode"),
                ("dry_run", "dry_run"),
                ("order_size_usd", "order_size_usd"),
                ("paper_capital_total", "paper_capital_total"),
                ("market_slug", "market_slug"),
                ("window_id", "window_id"),
                ("coin", "coin"),
                ("strategy_type", "strategy_type"),
                ("signal_context", "signal_context"),
                ("signal_overlap_count", "signal_overlap_count"),
                ("side", "side"),
                ("entry_price", "entry_price"),
                ("entry_bid", "entry_bid"),
                ("entry_ask", "entry_ask"),
                ("entry_spread", "entry_spread"),
                ("target_price", "target_price"),
                ("shares", "shares"),
                ("shares_requested", "shares_requested"),
                ("shares_filled", "shares_filled"),
                ("blocked_min_5_shares", "blocked_min_5_shares"),
                ("cost", "cost"),
                ("fee_total", "fee_total"),
                ("status", "status"),
                ("exit_price", "exit_price"),
                ("pnl", "pnl"),
                ("exit_reason", "exit_reason"),
                ("exit_timestamp", "exit_timestamp"),
                ("time_remaining_s", "time_remaining_s"),
                ("bid_at_exit", "bid_at_exit"),
                ("ask_at_exit", "ask_at_exit"),
                ("exit_spread", "exit_spread"),
                ("market_start_time", "market_start_time"),
                ("market_end_time", "market_end_time"),
                ("entry_depth_side_usd", "entry_depth_side_usd"),
                ("opposite_depth_usd", "opposite_depth_usd"),
                ("depth_ratio", "depth_ratio"),
                ("signal_score", "signal_score"),
                ("score_velocity", "score_velocity"),
                ("score_entry", "score_entry"),
                ("score_depth", "score_depth"),
                ("score_spread", "score_spread"),
                ("score_time", "score_time"),
                ("score_balance", "score_balance"),
                ("target_delta", "target_delta"),
                ("hard_stop_delta", "hard_stop_delta"),
                ("max_bid_seen", "max_bid_seen"),
                ("min_bid_seen", "min_bid_seen"),
                ("time_to_max_bid_s", "time_to_max_bid_s"),
                ("time_to_min_bid_s", "time_to_min_bid_s"),
                ("first_profit_time_s", "first_profit_time_s"),
                ("scalp_hit", "scalp_hit"),
                ("high_confidence_hit", "high_confidence_hit"),
                ("mfe", "mfe"),
                ("mae", "mae"),
                ("peak_net_pnl", "peak_net_pnl"),
                ("trough_net_pnl", "trough_net_pnl"),
                ("stall_exit_triggered", "stall_exit_triggered"),
            ],
        )
        d_select = _aliased_select(
            "d",
            d_cols,
            [
                ("filter_passed", "filter_passed"),
                ("filter_failed", "filter_failed"),
                ("reason", "decision_reason"),
                ("coin_velocity_30s", "coin_velocity_30s"),
                ("coin_velocity_60s", "coin_velocity_60s"),
                ("up_depth_usd", "up_depth_usd"),
                ("down_depth_usd", "down_depth_usd"),
                ("time_remaining_s", "decision_time_remaining_s"),
            ],
        )
        decision_join_id = "COALESCE(pt.decision_id, top_d.best_id)" if "decision_id" in pt_cols else "top_d.best_id"

        has_pt_strategy = "strategy_type" in pt_cols
        has_d_strategy = "strategy_type" in d_cols
        if has_pt_strategy and has_d_strategy:
            join_sql = """
            LEFT JOIN (
                SELECT market_slug, strategy_type, MAX(id) AS best_id
                FROM decisions
                WHERE action != 'SKIP'
                GROUP BY market_slug, strategy_type
            ) top_d ON top_d.market_slug = pt.market_slug
                   AND top_d.strategy_type = pt.strategy_type
            """
        else:
            join_sql = """
            LEFT JOIN (
                SELECT market_slug, MAX(id) AS best_id
                FROM decisions
                WHERE action != 'SKIP'
                GROUP BY market_slug
            ) top_d ON top_d.market_slug = pt.market_slug
            """

        columns, rows = _select_all(
            conn,
            f"""
            SELECT
                {pt_select},
                {d_select}
            FROM paper_trades pt
            {join_sql}
            LEFT JOIN decisions d ON d.id = {decision_join_id}
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
                "signal_score": "sg", "score_velocity": "sgv", "score_entry": "sge",
                "score_depth": "sgd", "score_spread": "sgs", "score_time": "sgt",
                "score_balance": "sgb", "target_delta": "td", "hard_stop_delta": "hsd",
                "scalp_hit": "sc", "high_confidence_hit": "hc", "filter_passed": "fp",
                "filter_failed": "ff", "decision_reason": "why", "coin_velocity_30s": "v30",
                "coin_velocity_60s": "v60", "up_depth_usd": "du", "down_depth_usd": "dd",
                "decision_time_remaining_s": "te", "mfe": "mfe", "mae": "mae",
                "peak_net_pnl": "pnp", "trough_net_pnl": "tnp", "stall_exit_triggered": "sx",
            }
            writer.writerow([compact_names.get(col, col) for col in columns])
            writer.writerows(rows)

        return str(out_path), len(rows), oldest, newest
    finally:
        conn.close()


def export_recent_signals_csv_gz(
    db_path: str,
    output_dir: str,
    label: str,
    limit: int,
    now_utc: datetime | None = None,
) -> tuple[str, int, int | None, int | None]:
    now_utc = now_utc or _utc_now()
    conn = sqlite3.connect(db_path)
    try:
        d_cols = _table_columns(conn, "decisions")
        d_select = _aliased_select(
            "d",
            d_cols,
            [
                ("id", "decision_id"),
                ("timestamp", "timestamp"),
                ("run_id", "run_id"),
                ("app_version", "app_version"),
                ("strategy_version", "strategy_version"),
                ("config_hash", "config_hash"),
                ("mode", "mode"),
                ("dry_run", "dry_run"),
                ("order_size_usd", "order_size_usd"),
                ("paper_capital_total", "paper_capital_total"),
                ("market_slug", "market_slug"),
                ("window_id", "window_id"),
                ("coin", "coin"),
                ("strategy_type", "strategy_type"),
                ("action", "action"),
                ("suppressed_reason", "suppressed_reason"),
                ("reason", "reason"),
                ("signal_context", "signal_context"),
                ("signal_overlap_count", "signal_overlap_count"),
                ("entry_price", "entry_price"),
                ("target_price", "target_price"),
                ("expected_profit_per_share", "expected_profit_per_share"),
                ("signal_score", "signal_score"),
                ("score_velocity", "score_velocity"),
                ("score_entry", "score_entry"),
                ("score_depth", "score_depth"),
                ("score_spread", "score_spread"),
                ("score_time", "score_time"),
                ("score_balance", "score_balance"),
                ("time_remaining_s", "time_remaining_s"),
                ("coin_velocity_30s", "coin_velocity_30s"),
                ("coin_velocity_60s", "coin_velocity_60s"),
                ("entry_bid", "entry_bid"),
                ("entry_ask", "entry_ask"),
                ("entry_spread", "entry_spread"),
                ("entry_depth_side_usd", "entry_depth_side_usd"),
                ("opposite_depth_usd", "opposite_depth_usd"),
                ("depth_ratio", "depth_ratio"),
                ("resignal_cooldown_s", "resignal_cooldown_s"),
                ("min_price_improvement", "min_price_improvement"),
                ("last_signal_age_s", "last_signal_age_s"),
                ("filter_passed", "filter_passed"),
                ("filter_failed", "filter_failed"),
            ],
        )
        columns, rows = _select_all(
            conn,
            f"""
            SELECT {d_select}
            FROM decisions d
            WHERE d.action IN ('DRY_RUN_SIGNAL', 'TRADE', 'SUPPRESSED')
            ORDER BY d.id DESC
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
        out_path = Path(output_dir) / f"{date_stamp}_{time_stamp}_{label}_signals_{id_start}-{id_end}.csv.gz"

        with gzip.open(out_path, "wt", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            compact_names = {
                "decision_id": "id", "timestamp": "ts", "run_id": "rid", "app_version": "av",
                "strategy_version": "sv", "config_hash": "ch", "mode": "md", "dry_run": "dr",
                "order_size_usd": "os", "paper_capital_total": "cap", "market_slug": "mkt",
                "window_id": "wid", "coin": "c", "strategy_type": "st", "action": "act",
                "suppressed_reason": "sup", "reason": "why", "signal_context": "ctx",
                "signal_overlap_count": "ov", "entry_price": "ep", "target_price": "tp",
                "expected_profit_per_share": "eps", "signal_score": "sg", "score_velocity": "sgv",
                "score_entry": "sge", "score_depth": "sgd", "score_spread": "sgs",
                "score_time": "sgt", "score_balance": "sgb", "time_remaining_s": "te",
                "coin_velocity_30s": "v30", "coin_velocity_60s": "v60", "entry_bid": "eb",
                "entry_ask": "ea", "entry_spread": "es", "entry_depth_side_usd": "eds",
                "opposite_depth_usd": "ods", "depth_ratio": "drt", "resignal_cooldown_s": "rcd",
                "min_price_improvement": "mpi", "last_signal_age_s": "lsa",
                "filter_passed": "fp", "filter_failed": "ff",
            }
            writer.writerow([compact_names.get(col, col) for col in columns])
            writer.writerows(rows)

        return str(out_path), len(rows), oldest, newest
    finally:
        conn.close()


def _dropbox_upload_file(local_path: str, dropbox_path: str, dropbox_auth: dict[str, Any]) -> dict[str, Any]:
    with open(local_path, "rb") as fh:
        body = fh.read()

    try:
        token = _resolve_dropbox_access_token(dropbox_auth)
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
            raw = resp.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
            if resp.status < 200 or resp.status >= 300:
                return {
                    "ok": False,
                    "status": resp.status,
                    "error": f"Unexpected Dropbox upload status {resp.status}",
                    "path": dropbox_path,
                }
            return {
                "ok": True,
                "status": resp.status,
                "bytes": len(body),
                "path": dropbox_path,
                "payload": payload,
            }
    except urlerror.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = str(e)
        return {
            "ok": False,
            "status": e.code,
            "error": err_body,
            "error_details": _dropbox_error_details(err_body),
            "path": dropbox_path,
        }
    except Exception as e:
        return {
            "ok": False,
            "status": None,
            "error": str(e),
            "error_details": _dropbox_error_details(str(e)),
            "path": dropbox_path,
        }


def _dropbox_get_metadata(dropbox_path: str, dropbox_auth: dict[str, Any]) -> dict[str, Any]:
    try:
        token = _resolve_dropbox_access_token(dropbox_auth)
        req = request.Request(
            url="https://api.dropboxapi.com/2/files/get_metadata",
            data=json.dumps({"path": dropbox_path}).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
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
            return {
                "exists": False,
                "status": e.code,
                "error": body,
                "error_details": _dropbox_error_details(body),
            }
        return {
            "exists": False,
            "status": e.code,
            "error": body,
            "error_details": _dropbox_error_details(body),
        }
    except Exception as e:
        return {
            "exists": False,
            "status": None,
            "error": str(e),
            "error_details": _dropbox_error_details(str(e)),
        }


def _dropbox_download_file(dropbox_path: str, dropbox_auth: dict[str, Any], local_path: str) -> dict[str, Any]:
    try:
        token = _resolve_dropbox_access_token(dropbox_auth)
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
        return {
            "ok": False,
            "status": e.code,
            "error": body,
            "error_details": _dropbox_error_details(body),
            "path": str(local_path),
        }
    except Exception as e:
        return {
            "ok": False,
            "status": None,
            "error": str(e),
            "error_details": _dropbox_error_details(str(e)),
            "path": str(local_path),
        }


def sync_dropbox_latest_to_local(
    dropbox_token: str | None = None,
    dropbox_refresh_token: str | None = None,
    dropbox_app_key: str | None = None,
    dropbox_app_secret: str | None = None,
    dropbox_root: str = "/",
    output_dir: str = "data/dropbox_sync",
    label: str = "EDEC-BOT",
    expand_trades_csv: bool = True,
    expand_signals_csv: bool = True,
) -> dict[str, Any]:
    """Pull stable latest archive files from Dropbox into a local folder."""
    dropbox_auth = _build_dropbox_auth(
        dropbox_token=dropbox_token,
        dropbox_refresh_token=dropbox_refresh_token,
        dropbox_app_key=dropbox_app_key,
        dropbox_app_secret=dropbox_app_secret,
    )
    if not dropbox_auth:
        raise RuntimeError("Dropbox authentication is not configured.")

    label = _safe_label(label)
    root = _normalize_dropbox_root(dropbox_root)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    latest_filenames = {
        "latest_last24h_xlsx": f"{label}_latest_last24h.xlsx",
        "latest_trades_csv_gz": f"{label}_latest_trades.csv.gz",
        "latest_signals_csv_gz": f"{label}_latest_signals.csv.gz",
        "latest_index_json": f"{label}_latest_index.json",
    }
    remote_candidates = _dropbox_latest_remote_candidates(root, latest_filenames)
    local = {
        "latest_last24h_xlsx": str(out / f"{label}_latest_last24h.xlsx"),
        "latest_trades_csv_gz": str(out / f"{label}_latest_trades.csv.gz"),
        "latest_signals_csv_gz": str(out / f"{label}_latest_signals.csv.gz"),
        "latest_index_json": str(out / f"{label}_latest_index.json"),
    }

    downloads: dict[str, Any] = {}
    for key, candidates in remote_candidates.items():
        attempts: list[dict[str, Any]] = []
        chosen: dict[str, Any] | None = None
        for remote_path in candidates:
            res = _dropbox_download_file(
                dropbox_path=remote_path,
                dropbox_auth=dropbox_auth,
                local_path=local[key],
            )
            attempts.append(
                {
                    "remote_path": remote_path,
                    "ok": bool(res.get("ok")),
                    "status": res.get("status"),
                    "error": res.get("error"),
                    "error_details": res.get("error_details"),
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
                "error_details": attempts[-1].get("error_details") if attempts else None,
                "path": local[key],
                "remote_path": candidates[0] if candidates else None,
            }
        if _is_optional_latest_missing(chosen, key):
            chosen["optional_missing"] = True
            details = dict(chosen.get("error_details") or {})
            details.setdefault("friendly", "Optional latest signals file is not in Dropbox yet.")
            chosen["error_details"] = details
        chosen["attempts"] = attempts
        downloads[key] = chosen

    expanded_csv = None
    if expand_trades_csv and downloads["latest_trades_csv_gz"].get("ok"):
        gz_path = Path(local["latest_trades_csv_gz"])
        csv_path = gz_path.with_suffix("")  # .csv.gz -> .csv
        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        expanded_csv = str(csv_path)
    expanded_signals_csv = None
    if expand_signals_csv and downloads["latest_signals_csv_gz"].get("ok"):
        gz_path = Path(local["latest_signals_csv_gz"])
        csv_path = gz_path.with_suffix("")
        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        expanded_signals_csv = str(csv_path)

    required_keys = tuple(key for key in downloads.keys() if key not in _OPTIONAL_LATEST_KEYS)
    ok = all(bool(downloads[key].get("ok")) for key in required_keys)
    return {
        "ok": ok,
        "checked_at_utc": _utc_now().isoformat(),
        "output_dir": str(out),
        "downloads": downloads,
        "expanded_trades_csv": expanded_csv,
        "expanded_signals_csv": expanded_signals_csv,
    }


def _safe_label(label: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in label).strip("-_") or "EDEC-BOT"


def _normalize_dropbox_root(dropbox_root: str | None) -> str:
    """Normalize Dropbox root so generated file paths are always valid."""
    root = (dropbox_root or "/").strip()
    if len(root) >= 2 and root[0] == root[-1] and root[0] in ("'", '"'):
        root = root[1:-1].strip()
    root = root.replace("\\", "/")
    if not root:
        root = "/"
    if "/home/" in root and "dropbox.com" in root:
        root = root.split("/home/", 1)[1].split("?", 1)[0].split("#", 1)[0].strip("/")
        if root.startswith("Apps/"):
            parts = root.split("/", 2)
            root = "/" if len(parts) <= 2 else f"/{parts[2]}"
        else:
            root = f"/{root}" if root else "/"
    if root.startswith("https://") or root.startswith("http://"):
        root = "/"
    if not root.startswith("/"):
        root = f"/{root}"
    while "//" in root:
        root = root.replace("//", "/")
    normalized = root.rstrip("/")
    return normalized or "/"


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
    dropbox_refresh_token: str | None = None,
    dropbox_app_key: str | None = None,
    dropbox_app_secret: str | None = None,
    dropbox_root: str = "/",
) -> dict[str, Any]:
    now_utc = _utc_now()
    dropbox_auth = _build_dropbox_auth(
        dropbox_token=dropbox_token,
        dropbox_refresh_token=dropbox_refresh_token,
        dropbox_app_key=dropbox_app_key,
        dropbox_app_secret=dropbox_app_secret,
    )
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
    recent_signals_path, recent_signals_count, oldest_signal_id, newest_signal_id = export_recent_signals_csv_gz(
        db_path=db_path,
        output_dir=output_dir,
        label=label,
        limit=recent_limit,
        now_utc=now_utc,
    )

    latest_excel = str(output_path / f"{label}_latest_last24h.xlsx")
    latest_trades = str(output_path / f"{label}_latest_trades.csv.gz")
    latest_signals = str(output_path / f"{label}_latest_signals.csv.gz")
    shutil.copy2(excel_path, latest_excel)
    shutil.copy2(recent_path, latest_trades)
    shutil.copy2(recent_signals_path, latest_signals)
    run_meta = _latest_run_metadata(db_path)

    index_path = output_path / f"{label}_latest_index.json"
    index = {
        "label": label,
        "exported_at_utc": now_utc.isoformat(),
        "window_hours": 24,
        "recent_trades_limit": recent_limit,
        "recent_signals_limit": recent_limit,
        "row_counts": {
            **counts,
            "recent_trades_rows": recent_count,
            "recent_signals_rows": recent_signals_count,
        },
        "trade_id_range": {
            "oldest": oldest_id,
            "newest": newest_id,
        },
        "signal_id_range": {
            "oldest": oldest_signal_id,
            "newest": newest_signal_id,
        },
        "local_files": {
            "daily_last24h_xlsx": Path(excel_path).name,
            "daily_recent_trades_csv_gz": Path(recent_path).name,
            "daily_recent_signals_csv_gz": Path(recent_signals_path).name,
            "latest_last24h_xlsx": Path(latest_excel).name,
            "latest_trades_csv_gz": Path(latest_trades).name,
            "latest_signals_csv_gz": Path(latest_signals).name,
            "latest_index_json": index_path.name,
        },
        "latest_run": run_meta,
        "dropbox_files": None,
        "dropbox_uploads": None,
    }
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

    if dropbox_auth:
        root = _normalize_dropbox_root(dropbox_root)
        dbx_paths = {
            "daily_last24h_xlsx": f"{root}/daily-reports/{Path(excel_path).name}",
            "daily_recent_trades_csv_gz": f"{root}/daily-archives/{Path(recent_path).name}",
            "daily_recent_signals_csv_gz": f"{root}/daily-archives/{Path(recent_signals_path).name}",
            "latest_last24h_xlsx": f"{root}/latest/{Path(latest_excel).name}",
            "latest_trades_csv_gz": f"{root}/latest/{Path(latest_trades).name}",
            "latest_signals_csv_gz": f"{root}/latest/{Path(latest_signals).name}",
            "latest_index_json": f"{root}/latest/{index_path.name}",
        }
        upload_results = {
            "daily_last24h_xlsx": _dropbox_upload_file(excel_path, dbx_paths["daily_last24h_xlsx"], dropbox_auth),
            "daily_recent_trades_csv_gz": _dropbox_upload_file(
                recent_path, dbx_paths["daily_recent_trades_csv_gz"], dropbox_auth
            ),
            "daily_recent_signals_csv_gz": _dropbox_upload_file(
                recent_signals_path, dbx_paths["daily_recent_signals_csv_gz"], dropbox_auth
            ),
            "latest_last24h_xlsx": _dropbox_upload_file(
                latest_excel, dbx_paths["latest_last24h_xlsx"], dropbox_auth
            ),
            "latest_trades_csv_gz": _dropbox_upload_file(
                latest_trades, dbx_paths["latest_trades_csv_gz"], dropbox_auth
            ),
            "latest_signals_csv_gz": _dropbox_upload_file(
                latest_signals, dbx_paths["latest_signals_csv_gz"], dropbox_auth
            ),
            "latest_index_json": _dropbox_upload_file(
                str(index_path), dbx_paths["latest_index_json"], dropbox_auth
            ),
        }
        index["dropbox_files"] = dbx_paths
        index["dropbox_uploads"] = upload_results
        index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

    logger.info("Daily archive export complete: %s", json.dumps(index["local_files"]))
    return {
        "excel_path": excel_path,
        "recent_path": recent_path,
        "recent_signals_path": recent_signals_path,
        "latest_excel": latest_excel,
        "latest_trades": latest_trades,
        "latest_signals": latest_signals,
        "index_path": str(index_path),
        "row_counts": index["row_counts"],
        "trade_id_range": index["trade_id_range"],
        "signal_id_range": index["signal_id_range"],
        "dropbox_files": index["dropbox_files"],
        "dropbox_uploads": index["dropbox_uploads"],
    }


def latest_archive_paths(output_dir: str = "data/exports", label: str = "EDEC-BOT") -> dict[str, str]:
    label = _safe_label(label)
    base = Path(output_dir)
    return {
        "latest_excel": str(base / f"{label}_latest_last24h.xlsx"),
        "latest_trades": str(base / f"{label}_latest_trades.csv.gz"),
        "latest_signals": str(base / f"{label}_latest_signals.csv.gz"),
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
    dropbox_refresh_token: str | None = None,
    dropbox_app_key: str | None = None,
    dropbox_app_secret: str | None = None,
    dropbox_root: str = "/",
) -> dict[str, Any]:
    label = _safe_label(label)
    dropbox_auth = _build_dropbox_auth(
        dropbox_token=dropbox_token,
        dropbox_refresh_token=dropbox_refresh_token,
        dropbox_app_key=dropbox_app_key,
        dropbox_app_secret=dropbox_app_secret,
    )
    local_paths = latest_archive_paths(output_dir=output_dir, label=label)
    index = read_latest_index(output_dir=output_dir, label=label)

    health: dict[str, Any] = {
        "label": label,
        "checked_at_utc": _utc_now().isoformat(),
        "index": index,
        "local": {
            "latest_excel_exists": Path(local_paths["latest_excel"]).exists(),
            "latest_trades_exists": Path(local_paths["latest_trades"]).exists(),
            "latest_signals_exists": Path(local_paths["latest_signals"]).exists(),
            "latest_index_exists": Path(local_paths["latest_index"]).exists(),
        },
        "dropbox_live": None,
    }

    if dropbox_auth:
        root = _normalize_dropbox_root(dropbox_root)
        latest_remote = {
            "latest_last24h_xlsx": f"{root}/latest/{label}_latest_last24h.xlsx",
            "latest_trades_csv_gz": f"{root}/latest/{label}_latest_trades.csv.gz",
            "latest_signals_csv_gz": f"{root}/latest/{label}_latest_signals.csv.gz",
            "latest_index_json": f"{root}/latest/{label}_latest_index.json",
        }
        files: dict[str, Any] = {}
        for key, p in latest_remote.items():
            files[key] = {"path": p, **_dropbox_get_metadata(p, dropbox_auth)}
            if _is_optional_latest_missing(files[key], key):
                files[key]["optional_missing"] = True
        required_keys = tuple(key for key in files.keys() if key not in _OPTIONAL_LATEST_KEYS)
        live_ok = all(bool(files[key].get("exists")) for key in required_keys)
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
    parser.add_argument("--dropbox-refresh-token", default=os.getenv("EDEC_DROPBOX_REFRESH_TOKEN"))
    parser.add_argument("--dropbox-app-key", default=os.getenv("EDEC_DROPBOX_APP_KEY"))
    parser.add_argument("--dropbox-app-secret", default=os.getenv("EDEC_DROPBOX_APP_SECRET"))
    parser.add_argument("--dropbox-root", default=os.getenv("EDEC_DROPBOX_ROOT", "/"))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_daily_archive(
        db_path=args.db_path,
        output_dir=args.output_dir,
        label=args.label,
        recent_limit=args.recent_limit,
        dropbox_token=args.dropbox_token,
        dropbox_refresh_token=args.dropbox_refresh_token,
        dropbox_app_key=args.dropbox_app_key,
        dropbox_app_secret=args.dropbox_app_secret,
        dropbox_root=args.dropbox_root,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

