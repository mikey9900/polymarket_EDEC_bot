"""Runtime-facing loader for summarized research policy artifacts."""

from __future__ import annotations

import json
from pathlib import Path

from .buckets import cluster_payload
from .paths import DEFAULT_POLICY_PATH, resolve_repo_path


class ResearchSnapshotProvider:
    """Cheap runtime loader for advisory cluster metadata."""

    def __init__(self, artifact_path: str | Path = DEFAULT_POLICY_PATH):
        self.path = resolve_repo_path(artifact_path)
        self._mtime_ns: int | None = None
        self._snapshot: dict = {"clusters": {}, "coin_features": {}}

    def lookup(
        self,
        *,
        strategy_type: str,
        coin: str,
        entry_price: float,
        velocity_30s: float,
        time_remaining_s: float,
    ) -> dict[str, object]:
        self._reload_if_needed()
        payload = cluster_payload(strategy_type, coin, entry_price, velocity_30s, time_remaining_s)
        cluster = (self._snapshot.get("clusters") or {}).get(payload["cluster_id"]) or {}
        coin_features = (self._snapshot.get("coin_features") or {}).get(str(coin or "").lower()) or {}
        policy_action = str(cluster.get("policy_action") or "unclassified")
        return {
            "research_cluster_id": payload["cluster_id"],
            "research_cluster_n": int(cluster.get("sample_size") or 0),
            "research_cluster_win_pct": float(cluster.get("win_pct") or 0.0),
            "research_cluster_avg_pnl": float(cluster.get("avg_pnl") or 0.0),
            "research_policy_action": policy_action,
            "research_market_regime_1d": str(coin_features.get("market_regime_1d") or ""),
            "research_liquidity_score_1d": float(coin_features.get("liquidity_score_1d") or 0.0),
            "research_crowding_score_1d": float(coin_features.get("crowding_score_1d") or 0.0),
            "research_score_flow_1d": float(coin_features.get("score_flow_1d") or 0.0),
            "research_score_crowding_1d": float(coin_features.get("score_crowding_1d") or 0.0),
            "research_signal_score_adjustment": float(coin_features.get("signal_score_adjustment") or 0.0),
        }

    def _reload_if_needed(self) -> None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self._snapshot = {"clusters": {}, "coin_features": {}}
            self._mtime_ns = None
            return
        if self._mtime_ns == stat.st_mtime_ns:
            return
        with self.path.open("r", encoding="utf-8") as fh:
            self._snapshot = json.load(fh)
        self._mtime_ns = stat.st_mtime_ns
