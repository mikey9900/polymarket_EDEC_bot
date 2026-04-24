"""Runtime-facing loader for summarized research policy artifacts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .buckets import cluster_payload
from .paths import DEFAULT_POLICY_PATH, resolve_repo_path


class ResearchSnapshotProvider:
    """Cheap runtime loader for advisory cluster metadata."""

    def __init__(self, artifact_path: str | Path = DEFAULT_POLICY_PATH):
        self.path = resolve_repo_path(artifact_path)
        self._mtime_ns: int | None = None
        self._snapshot: dict = {"clusters": {}, "coin_features": {}, "live_filter_overrides": {}}
        self._last_loaded_at: str | None = None
        self._last_source_modified_at: str | None = None
        self._reload_count = 0
        self._last_error: str | None = None

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

    def filter_overrides(self, *, strategy_type: str, coin: str) -> dict[str, object]:
        self._reload_if_needed()
        override_bundle = (self._snapshot.get("live_filter_overrides") or {})
        if not isinstance(override_bundle, dict):
            return {}
        strategies = override_bundle.get("strategies") or {}
        if not isinstance(strategies, dict):
            return {}
        payload = strategies.get(str(strategy_type or "").strip()) or {}
        if not isinstance(payload, dict):
            return {}
        return dict(payload)

    def status(self) -> dict[str, object]:
        self._reload_if_needed()
        clusters = self._snapshot.get("clusters") or {}
        coin_features = self._snapshot.get("coin_features") or {}
        live_filter_overrides = (self._snapshot.get("live_filter_overrides") or {}).get("strategies") or {}
        override_count = 0
        if isinstance(live_filter_overrides, dict):
            override_count = sum(len(payload or {}) for payload in live_filter_overrides.values() if isinstance(payload, dict))
        return {
            "artifact_path": str(self.path),
            "artifact_exists": self.path.exists(),
            "last_loaded_at": self._last_loaded_at,
            "last_source_modified_at": self._last_source_modified_at,
            "reload_count": int(self._reload_count),
            "cluster_count": len(clusters) if isinstance(clusters, dict) else 0,
            "coin_feature_count": len(coin_features) if isinstance(coin_features, dict) else 0,
            "live_filter_override_count": int(override_count),
            "last_error": self._last_error,
        }

    def _reload_if_needed(self) -> None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self._snapshot = {"clusters": {}, "coin_features": {}, "live_filter_overrides": {}}
            self._mtime_ns = None
            self._last_source_modified_at = None
            self._last_error = None
            return
        if self._mtime_ns == stat.st_mtime_ns:
            return
        try:
            with self.path.open("r", encoding="utf-8") as fh:
                self._snapshot = json.load(fh)
        except json.JSONDecodeError as exc:
            self._last_error = f"Policy artifact is not valid JSON: {exc}"
            return
        self._mtime_ns = stat.st_mtime_ns
        self._last_source_modified_at = datetime.fromtimestamp(stat.st_mtime_ns / 1_000_000_000, tz=timezone.utc).isoformat()
        self._last_loaded_at = datetime.now(timezone.utc).isoformat()
        self._reload_count += 1
        self._last_error = None
