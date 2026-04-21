"""Stable bucket definitions for runtime research clusters."""

from __future__ import annotations

import math


VELOCITY_BUCKETS = (
    (0.04, "<0.04"),
    (0.06, "0.04-0.06"),
    (0.08, "0.06-0.08"),
    (0.10, "0.08-0.10"),
    (0.12, "0.10-0.12"),
    (0.15, "0.12-0.15"),
    (0.20, "0.15-0.20"),
)

TIME_BUCKETS = (
    (30.0, "0-30"),
    (60.0, "30-60"),
    (90.0, "60-90"),
    (120.0, "90-120"),
    (180.0, "120-180"),
)


def entry_bucket_label(entry_price: float, width: float = 0.02) -> str:
    safe = min(max(float(entry_price or 0.0), 0.0), 1.0)
    if safe >= 1.0:
        lower = 1.0 - width
        upper = 1.0
    else:
        lower = math.floor(safe / width) * width
        upper = min(1.0, lower + width)
    return f"{lower:.2f}-{upper:.2f}"


def velocity_bucket_label(velocity_30s: float) -> str:
    velocity = abs(float(velocity_30s or 0.0))
    for upper, label in VELOCITY_BUCKETS:
        if velocity < upper:
            return label
    return "0.20+"


def time_remaining_bucket_label(time_remaining_s: float) -> str:
    remaining = max(float(time_remaining_s or 0.0), 0.0)
    for upper, label in TIME_BUCKETS:
        if remaining < upper:
            return label
    return "180+"


def cluster_payload(
    strategy_type: str,
    coin: str,
    entry_price: float,
    velocity_30s: float,
    time_remaining_s: float,
) -> dict[str, str]:
    strategy = (strategy_type or "unknown").strip().lower()
    coin_key = (coin or "unknown").strip().lower()
    entry_bucket = entry_bucket_label(entry_price)
    velocity_bucket = velocity_bucket_label(velocity_30s)
    time_bucket = time_remaining_bucket_label(time_remaining_s)
    cluster_id = "|".join([strategy, coin_key, entry_bucket, velocity_bucket, time_bucket])
    return {
        "cluster_id": cluster_id,
        "strategy_type": strategy,
        "coin": coin_key,
        "entry_bucket": entry_bucket,
        "abs_velocity_30s_bucket": velocity_bucket,
        "time_remaining_bucket": time_bucket,
    }
