"""Artifact builders for runtime research policy and daily reporting."""

from __future__ import annotations

import csv
import gzip
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import duckdb

from .buckets import cluster_payload
from .paths import (
    DEFAULT_POLICY_PATH,
    DEFAULT_REPORT_JSON_PATH,
    DEFAULT_REPORT_MD_PATH,
    LOCAL_TRACKER_DB,
    WAREHOUSE_PATH,
    discover_session_export_files,
    ensure_research_dirs,
    resolve_repo_path,
)


@dataclass(frozen=True)
class OutcomeRow:
    dedupe_key: tuple
    trade_id: str
    run_id: str
    coin: str
    strategy_type: str
    entry_price: float
    velocity_30s: float
    time_remaining_s: float
    pnl: float
    timestamp: datetime


def build_artifacts(
    *,
    warehouse_path: str | Path = WAREHOUSE_PATH,
    tracker_db: str | Path = LOCAL_TRACKER_DB,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    report_json_path: str | Path = DEFAULT_REPORT_JSON_PATH,
    report_md_path: str | Path = DEFAULT_REPORT_MD_PATH,
    lookback_days: int = 30,
) -> dict[str, object]:
    ensure_research_dirs()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=int(lookback_days))
    outcomes = list(_deduped_outcomes(_load_local_outcomes(resolve_repo_path(tracker_db), cutoff), _load_export_outcomes(cutoff)))
    policy = _build_policy_payload(outcomes, generated_at=now, lookback_days=lookback_days)
    fill_summary = _build_fill_summary(resolve_repo_path(warehouse_path), cutoff=now - timedelta(days=1))
    coin_features = _build_coin_features(
        fill_summary["fill_flow_5m_1d"],
        fill_summary["trader_concentration_5m_1d"],
    )
    policy["coin_features"] = coin_features
    report_payload = {
        "generated_at": now.isoformat(),
        "lookback_days": int(lookback_days),
        "policy": {
            "cluster_count": policy["cluster_count"],
            "outcome_count": policy["outcome_count"],
        },
        "cluster_winners": _top_clusters(policy["clusters"], reverse=True),
        "cluster_losers": _top_clusters(policy["clusters"], reverse=False),
        "by_coin": _group_cluster_rollup(policy["clusters"], "coin"),
        "by_strategy": _group_cluster_rollup(policy["clusters"], "strategy_type"),
        "fill_flow_5m_1d": fill_summary["fill_flow_5m_1d"],
        "trader_concentration_5m_1d": fill_summary["trader_concentration_5m_1d"],
        "coin_features_5m_1d": sorted(
            coin_features.values(),
            key=lambda item: (
                float(item.get("signal_score_adjustment") or 0.0),
                float(item.get("usd_volume_1d") or 0.0),
            ),
            reverse=True,
        ),
    }
    policy_output = resolve_repo_path(policy_path)
    report_json_output = resolve_repo_path(report_json_path)
    report_md_output = resolve_repo_path(report_md_path)
    policy_output.parent.mkdir(parents=True, exist_ok=True)
    report_json_output.parent.mkdir(parents=True, exist_ok=True)
    report_md_output.parent.mkdir(parents=True, exist_ok=True)
    policy_output.write_text(json.dumps(policy, indent=2, sort_keys=True), encoding="utf-8")
    report_json_output.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")
    report_md_output.write_text(_render_markdown_report(report_payload), encoding="utf-8")
    return {
        "policy_path": str(policy_output),
        "report_json_path": str(report_json_output),
        "report_md_path": str(report_md_output),
        "cluster_count": policy["cluster_count"],
        "outcome_count": policy["outcome_count"],
        "fill_flow_rows": len(report_payload["fill_flow_5m_1d"]),
    }


def _load_local_outcomes(db_path: Path, cutoff: datetime) -> list[OutcomeRow]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        pt_cols = _sqlite_columns(conn, "paper_trades")
        d_cols = _sqlite_columns(conn, "decisions")
        has_decision_join = "decision_id" in pt_cols and "id" in d_cols
        join_clause = "LEFT JOIN decisions d ON d.id = pt.decision_id" if has_decision_join else ""
        pt_run = "pt.run_id" if "run_id" in pt_cols else "''"
        d_run = "d.run_id" if has_decision_join and "run_id" in d_cols else "''"
        pt_coin = "pt.coin" if "coin" in pt_cols else ("d.coin" if has_decision_join and "coin" in d_cols else "''")
        pt_strategy = "pt.strategy_type" if "strategy_type" in pt_cols else ("d.strategy_type" if has_decision_join and "strategy_type" in d_cols else "''")
        pt_entry = "pt.entry_price" if "entry_price" in pt_cols else ("COALESCE(d.entry_price, 0.0)" if has_decision_join and "entry_price" in d_cols else "0.0")
        d_velocity = "d.coin_velocity_30s" if has_decision_join and "coin_velocity_30s" in d_cols else "0.0"
        remaining_parts = []
        if has_decision_join and "time_remaining_s" in d_cols:
            remaining_parts.append("d.time_remaining_s")
        if "time_remaining_s" in pt_cols:
            remaining_parts.append("pt.time_remaining_s")
        remaining_expr = f"COALESCE({', '.join(remaining_parts)}, 0.0)" if remaining_parts else "0.0"
        pnl_expr = "COALESCE(pt.pnl, 0.0)" if "pnl" in pt_cols else "0.0"
        exit_ts_expr = "pt.exit_timestamp" if "exit_timestamp" in pt_cols else "NULL"
        ts_expr = "pt.timestamp" if "timestamp" in pt_cols else "NULL"
        status_expr = "COALESCE(pt.status, '')" if "status" in pt_cols else "''"
        rows = conn.execute(
            f"""
            SELECT
                pt.id AS trade_id,
                COALESCE({pt_run}, {d_run}, '') AS run_id,
                {pt_coin} AS coin,
                {pt_strategy} AS strategy_type,
                {pt_entry} AS entry_price,
                COALESCE({d_velocity}, 0.0) AS velocity_30s,
                {remaining_expr} AS time_remaining_s,
                {pnl_expr} AS pnl,
                COALESCE({exit_ts_expr}, {ts_expr}) AS outcome_timestamp
            FROM paper_trades pt
            {join_clause}
            WHERE COALESCE({exit_ts_expr}, {ts_expr}) >= ?
              AND ({pnl_expr} IS NOT NULL OR {status_expr} NOT IN ('open', 'pending'))
            """,
            (cutoff.isoformat(),),
        ).fetchall()
    finally:
        conn.close()
    outcomes: list[OutcomeRow] = []
    for row in rows:
        timestamp = _parse_dt(row["outcome_timestamp"])
        if not timestamp:
            continue
        trade_id = str(row["trade_id"] or "")
        run_id = str(row["run_id"] or "")
        dedupe_key = ("run_trade", run_id, trade_id) if run_id and trade_id else ("local", trade_id)
        outcomes.append(
            OutcomeRow(
                dedupe_key=dedupe_key,
                trade_id=trade_id,
                run_id=run_id,
                coin=str(row["coin"] or "").lower(),
                strategy_type=str(row["strategy_type"] or "").lower(),
                entry_price=float(row["entry_price"] or 0.0),
                velocity_30s=float(row["velocity_30s"] or 0.0),
                time_remaining_s=float(row["time_remaining_s"] or 0.0),
                pnl=float(row["pnl"] or 0.0),
                timestamp=timestamp,
            )
        )
    return outcomes


def _load_export_outcomes(cutoff: datetime) -> list[OutcomeRow]:
    outcomes: list[OutcomeRow] = []
    for path in discover_session_export_files():
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                timestamp = _parse_dt(_field(row, "xt", "exit_timestamp", "ts", "timestamp"))
                if not timestamp or timestamp < cutoff:
                    continue
                trade_id = _field(row, "id", "trade_id")
                run_id = _field(row, "rid", "run_id")
                if run_id and trade_id:
                    dedupe_key = ("run_trade", run_id, trade_id)
                else:
                    dedupe_key = ("export_path", str(path), trade_id)
                outcomes.append(
                    OutcomeRow(
                        dedupe_key=dedupe_key,
                        trade_id=trade_id,
                        run_id=run_id,
                        coin=_field(row, "c", "coin").lower(),
                        strategy_type=_field(row, "st", "strategy_type").lower(),
                        entry_price=_float_field(row, "ep", "entry_price"),
                        velocity_30s=_float_field(row, "v30", "coin_velocity_30s"),
                        time_remaining_s=_float_field(row, "te", "decision_time_remaining_s", "time_remaining_s"),
                        pnl=_float_field(row, "pnl"),
                        timestamp=timestamp,
                    )
                )
    return outcomes


def _deduped_outcomes(*sources: Iterable[OutcomeRow]) -> Iterable[OutcomeRow]:
    deduped: dict[tuple, OutcomeRow] = {}
    for source in sources:
        for row in source:
            current = deduped.get(row.dedupe_key)
            if current is None or row.timestamp >= current.timestamp:
                deduped[row.dedupe_key] = row
    return sorted(deduped.values(), key=lambda item: item.timestamp)


def _build_policy_payload(outcomes: list[OutcomeRow], *, generated_at: datetime, lookback_days: int) -> dict[str, object]:
    clusters: dict[str, dict[str, object]] = {}
    for outcome in outcomes:
        payload = cluster_payload(
            strategy_type=outcome.strategy_type,
            coin=outcome.coin,
            entry_price=outcome.entry_price,
            velocity_30s=outcome.velocity_30s,
            time_remaining_s=outcome.time_remaining_s,
        )
        cluster = clusters.setdefault(
            payload["cluster_id"],
            {
                **payload,
                "sample_size": 0,
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_pct": 0.0,
                "policy_action": "advisory",
            },
        )
        cluster["sample_size"] += 1
        cluster["wins"] += int(outcome.pnl > 0)
        cluster["losses"] += int(outcome.pnl < 0)
        cluster["total_pnl"] += float(outcome.pnl)

    for cluster in clusters.values():
        sample_size = int(cluster["sample_size"] or 0)
        total_pnl = float(cluster["total_pnl"] or 0.0)
        wins = int(cluster["wins"] or 0)
        avg_pnl = total_pnl / sample_size if sample_size else 0.0
        win_pct = (wins / sample_size * 100.0) if sample_size else 0.0
        cluster["avg_pnl"] = round(avg_pnl, 6)
        cluster["total_pnl"] = round(total_pnl, 6)
        cluster["win_pct"] = round(win_pct, 2)
        if sample_size >= 30 and avg_pnl < 0 and win_pct < 45.0:
            cluster["policy_action"] = "paper_blocked"

    return {
        "generated_at": generated_at.isoformat(),
        "lookback_days": int(lookback_days),
        "outcome_count": len(outcomes),
        "cluster_count": len(clusters),
        "clusters": dict(sorted(clusters.items())),
    }


def _top_clusters(clusters: dict[str, dict[str, object]], *, reverse: bool) -> list[dict[str, object]]:
    values = [dict(value) for value in clusters.values() if int(value.get("sample_size") or 0) > 0]
    values.sort(key=lambda item: (float(item.get("avg_pnl") or 0.0), int(item.get("sample_size") or 0)), reverse=reverse)
    return values[:5]


def _group_cluster_rollup(clusters: dict[str, dict[str, object]], key: str) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for cluster in clusters.values():
        name = str(cluster.get(key) or "unknown")
        bucket = grouped.setdefault(
            name,
            {"name": name, "sample_size": 0, "wins": 0, "total_pnl": 0.0, "paper_blocked_clusters": 0},
        )
        bucket["sample_size"] += int(cluster.get("sample_size") or 0)
        bucket["wins"] += int(cluster.get("wins") or 0)
        bucket["total_pnl"] += float(cluster.get("total_pnl") or 0.0)
        bucket["paper_blocked_clusters"] += int(cluster.get("policy_action") == "paper_blocked")
    rows = []
    for bucket in grouped.values():
        sample_size = int(bucket["sample_size"] or 0)
        win_pct = (int(bucket["wins"] or 0) / sample_size * 100.0) if sample_size else 0.0
        rows.append(
            {
                "name": bucket["name"],
                "sample_size": sample_size,
                "win_pct": round(win_pct, 2),
                "total_pnl": round(float(bucket["total_pnl"] or 0.0), 6),
                "paper_blocked_clusters": int(bucket["paper_blocked_clusters"] or 0),
            }
        )
    rows.sort(key=lambda item: (item["total_pnl"], item["sample_size"]), reverse=True)
    return rows


def _build_fill_summary(warehouse_path: Path, *, cutoff: datetime) -> dict[str, list[dict[str, object]]]:
    if not warehouse_path.exists():
        return {"fill_flow_5m_1d": [], "trader_concentration_5m_1d": []}
    conn = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        fill_rows = conn.execute(
            """
            SELECT coin, COUNT(*) AS fill_count, SUM(usd_amount) AS usd_volume, AVG(price) AS avg_price
            FROM fills_enriched
            WHERE is_5m_updown = TRUE AND event_time >= ?
            GROUP BY coin
            ORDER BY usd_volume DESC NULLS LAST
            """,
            [cutoff],
        ).fetchall()
        trader_rows = conn.execute(
            """
            WITH activity AS (
                SELECT coin, maker AS trader, usd_amount
                FROM fills_enriched
                WHERE is_5m_updown = TRUE AND event_time >= ?
                UNION ALL
                SELECT coin, taker AS trader, usd_amount
                FROM fills_enriched
                WHERE is_5m_updown = TRUE AND event_time >= ?
            )
            SELECT coin, trader, SUM(usd_amount) AS usd_volume
            FROM activity
            WHERE trader IS NOT NULL AND trader <> ''
            GROUP BY coin, trader
            ORDER BY coin ASC, usd_volume DESC
            """,
            [cutoff, cutoff],
        ).fetchall()
    finally:
        conn.close()

    fill_flow = [
        {
            "coin": str(row[0] or ""),
            "fill_count": int(row[1] or 0),
            "usd_volume": round(float(row[2] or 0.0), 6),
            "avg_price": round(float(row[3] or 0.0), 6),
        }
        for row in fill_rows
    ]

    by_coin: dict[str, list[tuple[str, float]]] = {}
    for coin, trader, usd_volume in trader_rows:
        by_coin.setdefault(str(coin or ""), []).append((str(trader or ""), float(usd_volume or 0.0)))
    concentration = []
    for coin, entries in by_coin.items():
        total = sum(value for _, value in entries) or 1.0
        top_trader, top_volume = entries[0]
        concentration.append(
            {
                "coin": coin,
                "top_trader": top_trader,
                "top_trader_share_pct": round(top_volume / total * 100.0, 2),
                "top_3_share_pct": round(sum(value for _, value in entries[:3]) / total * 100.0, 2),
                "unique_trader_count": len(entries),
            }
        )
    concentration.sort(key=lambda item: item["top_trader_share_pct"], reverse=True)
    return {
        "fill_flow_5m_1d": fill_flow,
        "trader_concentration_5m_1d": concentration,
    }


def _build_coin_features(
    fill_flow_rows: list[dict[str, object]],
    concentration_rows: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    if not fill_flow_rows and not concentration_rows:
        return {}

    features: dict[str, dict[str, object]] = {}
    total_volume = sum(float(row.get("usd_volume") or 0.0) for row in fill_flow_rows)
    total_fills = sum(int(row.get("fill_count") or 0) for row in fill_flow_rows)

    for row in fill_flow_rows:
        coin = str(row.get("coin") or "").lower()
        if not coin:
            continue
        features[coin] = {
            "coin": coin,
            "fill_count_1d": int(row.get("fill_count") or 0),
            "usd_volume_1d": round(float(row.get("usd_volume") or 0.0), 6),
            "avg_price_1d": round(float(row.get("avg_price") or 0.0), 6),
            "volume_share_pct_1d": round(_safe_share(float(row.get("usd_volume") or 0.0), total_volume), 2),
            "fill_share_pct_1d": round(_safe_share(float(row.get("fill_count") or 0.0), float(total_fills)), 2),
            "top_trader": "",
            "top_trader_share_pct_1d": 0.0,
            "top_3_share_pct_1d": 0.0,
            "unique_trader_count_1d": 0,
        }

    for row in concentration_rows:
        coin = str(row.get("coin") or "").lower()
        if not coin:
            continue
        bucket = features.setdefault(
            coin,
            {
                "coin": coin,
                "fill_count_1d": 0,
                "usd_volume_1d": 0.0,
                "avg_price_1d": 0.0,
                "volume_share_pct_1d": 0.0,
                "fill_share_pct_1d": 0.0,
            },
        )
        bucket["top_trader"] = str(row.get("top_trader") or "")
        bucket["top_trader_share_pct_1d"] = round(float(row.get("top_trader_share_pct") or 0.0), 2)
        bucket["top_3_share_pct_1d"] = round(float(row.get("top_3_share_pct") or 0.0), 2)
        bucket["unique_trader_count_1d"] = int(row.get("unique_trader_count") or 0)

    volume_ranks = _normalized_ranks({coin: float(row.get("usd_volume_1d") or 0.0) for coin, row in features.items()})
    fill_ranks = _normalized_ranks({coin: float(row.get("fill_count_1d") or 0.0) for coin, row in features.items()})
    unique_ranks = _normalized_ranks(
        {coin: float(row.get("unique_trader_count_1d") or 0.0) for coin, row in features.items()}
    )

    for coin, row in features.items():
        liquidity_score = (
            0.45 * volume_ranks.get(coin, 0.0)
            + 0.35 * fill_ranks.get(coin, 0.0)
            + 0.20 * unique_ranks.get(coin, 0.0)
        ) * 100.0
        top_trader_share = float(row.get("top_trader_share_pct_1d") or 0.0)
        top_3_share = float(row.get("top_3_share_pct_1d") or 0.0)
        unique_penalty = 1.0 - unique_ranks.get(coin, 0.0)
        crowding_score = (
            0.50 * _clamp((top_trader_share - 18.0) / 22.0, 0.0, 1.0)
            + 0.30 * _clamp((top_3_share - 30.0) / 30.0, 0.0, 1.0)
            + 0.20 * unique_penalty
        ) * 100.0
        score_flow = ((liquidity_score - 50.0) / 50.0) * 5.0
        score_crowding = (crowding_score / 100.0) * 6.0
        signal_score_adjustment = score_flow - score_crowding
        row.update(
            {
                "liquidity_score_1d": round(liquidity_score, 2),
                "crowding_score_1d": round(crowding_score, 2),
                "score_flow_1d": round(score_flow, 2),
                "score_crowding_1d": round(score_crowding, 2),
                "signal_score_adjustment": round(signal_score_adjustment, 2),
                "market_regime_1d": _market_regime(liquidity_score, crowding_score),
            }
        )

    return dict(sorted(features.items()))


def _render_markdown_report(report: dict[str, object]) -> str:
    lines = [
        "# Research Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Lookback: {report['lookback_days']} days",
        "",
        "## Cluster Winners",
    ]
    lines.extend(_render_cluster_table(report.get("cluster_winners") or []))
    lines.extend(["", "## Cluster Losers"])
    lines.extend(_render_cluster_table(report.get("cluster_losers") or []))
    lines.extend(["", "## By Coin"])
    lines.extend(_render_rollup_table(report.get("by_coin") or []))
    lines.extend(["", "## By Strategy"])
    lines.extend(_render_rollup_table(report.get("by_strategy") or []))
    lines.extend(["", "## Fill Flow (5m, 1d)"])
    lines.extend(_render_fill_table(report.get("fill_flow_5m_1d") or []))
    lines.extend(["", "## Trader Concentration (5m, 1d)"])
    lines.extend(_render_concentration_table(report.get("trader_concentration_5m_1d") or []))
    lines.extend(["", "## Runtime Coin Features (5m, 1d)"])
    lines.extend(_render_coin_feature_table(report.get("coin_features_5m_1d") or []))
    return "\n".join(lines).strip() + "\n"


def _render_cluster_table(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["No data."]
    lines = ["| Cluster | N | Win % | Avg PnL | Policy |", "| --- | ---: | ---: | ---: | --- |"]
    for row in rows:
        lines.append(
            f"| {row['cluster_id']} | {row['sample_size']} | {row['win_pct']:.2f} | {row['avg_pnl']:.4f} | {row['policy_action']} |"
        )
    return lines


def _render_rollup_table(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["No data."]
    lines = ["| Name | N | Win % | Total PnL | Paper-Blocked Clusters |", "| --- | ---: | ---: | ---: | ---: |"]
    for row in rows:
        lines.append(
            f"| {row['name']} | {row['sample_size']} | {row['win_pct']:.2f} | {row['total_pnl']:.4f} | {row['paper_blocked_clusters']} |"
        )
    return lines


def _render_fill_table(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["No data."]
    lines = ["| Coin | Fills | USD Volume | Avg Price |", "| --- | ---: | ---: | ---: |"]
    for row in rows:
        lines.append(f"| {row['coin']} | {row['fill_count']} | {row['usd_volume']:.2f} | {row['avg_price']:.4f} |")
    return lines


def _render_concentration_table(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["No data."]
    lines = ["| Coin | Top Trader | Top Share % | Top 3 Share % | Unique Traders |", "| --- | --- | ---: | ---: | ---: |"]
    for row in rows:
        lines.append(
            f"| {row['coin']} | {row['top_trader']} | {row['top_trader_share_pct']:.2f} | {row['top_3_share_pct']:.2f} | {row['unique_trader_count']} |"
        )
    return lines


def _render_coin_feature_table(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["No data."]
    lines = [
        "| Coin | Regime | Liquidity | Crowding | Score Flow | Score Crowd | Net Adj |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['coin']} | {row['market_regime_1d']} | {row['liquidity_score_1d']:.2f} | "
            f"{row['crowding_score_1d']:.2f} | {row['score_flow_1d']:.2f} | "
            f"{row['score_crowding_1d']:.2f} | {row['signal_score_adjustment']:.2f} |"
        )
    return lines


def _field(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return str(value)
    return ""


def _float_field(row: dict[str, str], *names: str) -> float:
    value = _field(row, *names)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _sqlite_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(row[1]) for row in rows}


def _safe_share(value: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return value / total * 100.0


def _normalized_ranks(values: dict[str, float]) -> dict[str, float]:
    items = sorted(values.items(), key=lambda item: (float(item[1]), item[0]))
    if not items:
        return {}
    if len(items) == 1:
        coin, _ = items[0]
        return {coin: 1.0}
    return {
        coin: idx / max(len(items) - 1, 1)
        for idx, (coin, _) in enumerate(items)
    }


def _market_regime(liquidity_score: float, crowding_score: float) -> str:
    if liquidity_score >= 70.0 and crowding_score <= 35.0:
        return "liquid_balanced"
    if liquidity_score >= 70.0:
        return "liquid_crowded"
    if liquidity_score < 40.0 and crowding_score >= 55.0:
        return "thin_crowded"
    if liquidity_score < 40.0:
        return "thin"
    return "mixed"


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
