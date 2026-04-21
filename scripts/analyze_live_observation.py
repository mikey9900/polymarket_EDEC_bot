from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


def _load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _pct(num: int, den: int) -> float:
    return (100.0 * num / den) if den else 0.0


def _iter_signals(snapshot: dict) -> Iterable[tuple[str, dict, dict]]:
    for coin, data in (snapshot.get("coins") or {}).items():
        market = data.get("market") or {}
        for signal in data.get("bot_signals") or []:
            yield coin, data, {"market": market, "signal": signal}


def _source_distribution(rows: list[dict]) -> dict[str, dict]:
    source_dist: dict[str, Counter] = defaultdict(Counter)
    source_ge2: Counter = Counter()
    source_total: Counter = Counter()
    live_price_missing: Counter = Counter()
    signal_action: dict[str, Counter] = defaultdict(Counter)
    open_trade_counts: Counter = Counter()

    for snapshot in rows:
        total_open = 0
        for coin, data in (snapshot.get("coins") or {}).items():
            sources = data.get("sources") or {}
            count = sources.get("count")
            total_open += len(data.get("open_trades") or [])
            if count is not None:
                source_dist[coin][int(count)] += 1
                source_total[coin] += 1
                if int(count) >= 2:
                    source_ge2[coin] += 1
            if data.get("live_price") in (None, ""):
                live_price_missing[coin] += 1
            for signal in data.get("bot_signals") or []:
                signal_action[coin][(signal.get("strategy"), signal.get("action"))] += 1
        open_trade_counts[total_open] += 1

    return {
        "source_dist": source_dist,
        "source_ge2": source_ge2,
        "source_total": source_total,
        "live_price_missing": live_price_missing,
        "signal_action": signal_action,
        "open_trade_counts": open_trade_counts,
    }


def _gating_stats(
    rows: list[dict],
    *,
    single_min: float,
    single_max: float,
    lead_min: float,
    lead_max: float,
    max_spread: float,
) -> dict[str, dict[str, int]]:
    stats: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for snapshot in rows:
        for coin, data, payload in _iter_signals(snapshot):
            signal = payload["signal"]
            market = payload["market"]
            strategy = signal.get("strategy")
            side = str(signal.get("side") or "").lower()
            entry = signal.get("entry_price")
            if strategy not in ("single_leg", "lead_lag") or side not in ("up", "down") or entry is None:
                continue

            if strategy == "single_leg":
                in_window = single_min <= float(entry) <= single_max
            else:
                in_window = lead_min <= float(entry) <= lead_max

            bid = market.get(f"{side}_bid")
            ask = market.get(f"{side}_ask")
            spread = (ask - bid) if bid is not None and ask is not None else None
            source_count = int(((data.get("sources") or {}).get("count")) or 0)

            key = f"{coin}:{strategy}"
            stats[key]["signals"] += 1
            if source_count >= 2:
                stats[key]["source_ge2"] += 1
            if in_window:
                stats[key]["entry_in_window"] += 1
            if spread is not None and spread <= max_spread:
                stats[key]["spread_ok"] += 1
            if source_count >= 2 and in_window:
                stats[key]["source_and_window"] += 1
            if source_count >= 2 and spread is not None and spread <= max_spread:
                stats[key]["source_and_spread"] += 1
            if in_window and spread is not None and spread <= max_spread:
                stats[key]["window_and_spread"] += 1
            if source_count >= 2 and in_window and spread is not None and spread <= max_spread:
                stats[key]["all_three"] += 1

    return stats


def _signal_live_drift(rows: list[dict]) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"n": 0, "abs_diff_sum": 0.0, "max_abs_diff": 0.0, "gt_0_05": 0, "gt_0_10": 0, "gt_0_25": 0}
    )

    for snapshot in rows:
        for coin, _data, payload in _iter_signals(snapshot):
            signal = payload["signal"]
            market = payload["market"]
            strategy = signal.get("strategy")
            side = str(signal.get("side") or "").lower()
            entry = signal.get("entry_price")
            live_ask = market.get(f"{side}_ask") if side in ("up", "down") else None
            if strategy not in ("single_leg", "lead_lag") or live_ask is None or entry is None:
                continue
            diff = abs(float(entry) - float(live_ask))
            key = f"{coin}:{strategy}"
            stats[key]["n"] += 1
            stats[key]["abs_diff_sum"] += diff
            stats[key]["max_abs_diff"] = max(stats[key]["max_abs_diff"], diff)
            stats[key]["gt_0_05"] += int(diff > 0.05)
            stats[key]["gt_0_10"] += int(diff > 0.10)
            stats[key]["gt_0_25"] += int(diff > 0.25)

    return stats


def _directional_micro_scalp(
    rows: list[dict],
    *,
    fee_rate: float,
    order_size: float,
    horizons: list[int],
) -> dict[tuple[str, str, str], dict[int, dict[str, float]]]:
    summary: dict[tuple[str, str, str], dict[int, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"n": 0, "wins": 0, "gross": 0.0, "net": 0.0})
    )

    for index, snapshot in enumerate(rows):
        for coin, data, payload in _iter_signals(snapshot):
            signal = payload["signal"]
            market = payload["market"]
            strategy = signal.get("strategy")
            side = str(signal.get("side") or "").lower()
            if strategy not in ("single_leg", "lead_lag") or side not in ("up", "down"):
                continue

            entry = market.get(f"{side}_ask")
            if entry is None or float(entry) <= 0:
                continue
            entry = float(entry)
            shares = math.floor(order_size / entry)
            if shares <= 0:
                continue

            slug = market.get("slug")
            source_count = int(((data.get("sources") or {}).get("count")) or 0)
            bucket = "source_ge2" if source_count >= 2 else "all"

            for horizon in horizons:
                if index + horizon >= len(rows):
                    continue
                future = rows[index + horizon]
                future_coin = (future.get("coins") or {}).get(coin) or {}
                future_market = future_coin.get("market") or {}
                if future_market.get("slug") != slug:
                    continue
                exit_bid = future_market.get(f"{side}_bid")
                if exit_bid is None or float(exit_bid) <= 0:
                    continue
                exit_bid = float(exit_bid)
                fee_buy = fee_rate * entry * (1.0 - entry)
                fee_sell = fee_rate * exit_bid * (1.0 - exit_bid)
                gross = (exit_bid - entry) * shares
                net = (exit_bid - entry - fee_buy - fee_sell) * shares
                cell = summary[(coin, strategy, bucket)][horizon]
                cell["n"] += 1
                cell["gross"] += gross
                cell["net"] += net
                if net > 0:
                    cell["wins"] += 1

    return summary


def _best_exit_within(
    rows: list[dict],
    *,
    fee_rate: float,
    order_size: float,
    max_horizon: int,
) -> dict[tuple[str, str, str], dict[str, float]]:
    summary: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {"n": 0, "wins": 0, "gross": 0.0, "net": 0.0}
    )

    for index, snapshot in enumerate(rows):
        for coin, data, payload in _iter_signals(snapshot):
            signal = payload["signal"]
            market = payload["market"]
            strategy = signal.get("strategy")
            side = str(signal.get("side") or "").lower()
            if strategy not in ("single_leg", "lead_lag") or side not in ("up", "down"):
                continue

            entry = market.get(f"{side}_ask")
            if entry is None or float(entry) <= 0:
                continue
            entry = float(entry)
            shares = math.floor(order_size / entry)
            if shares <= 0:
                continue

            slug = market.get("slug")
            future_bids: list[float] = []
            for horizon in range(1, max_horizon + 1):
                if index + horizon >= len(rows):
                    break
                future = rows[index + horizon]
                future_coin = (future.get("coins") or {}).get(coin) or {}
                future_market = future_coin.get("market") or {}
                if future_market.get("slug") != slug:
                    break
                bid = future_market.get(f"{side}_bid")
                if bid is not None and float(bid) > 0:
                    future_bids.append(float(bid))

            if not future_bids:
                continue

            exit_bid = max(future_bids)
            fee_buy = fee_rate * entry * (1.0 - entry)
            fee_sell = fee_rate * exit_bid * (1.0 - exit_bid)
            gross = (exit_bid - entry) * shares
            net = (exit_bid - entry - fee_buy - fee_sell) * shares
            source_count = int(((data.get("sources") or {}).get("count")) or 0)
            bucket = "source_ge2" if source_count >= 2 else "all"
            cell = summary[(coin, strategy, bucket)]
            cell["n"] += 1
            cell["gross"] += gross
            cell["net"] += net
            if net > 0:
                cell["wins"] += 1

    return summary


def _latest_observation_dir(root: Path) -> Path:
    dirs = [item for item in root.iterdir() if item.is_dir()]
    if not dirs:
        raise FileNotFoundError(f"No observation directories found in {root}")
    return sorted(dirs)[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize a live HA observation directory.")
    parser.add_argument("--dir", type=Path, help="Observation directory. Defaults to the latest under data/live_observation.")
    parser.add_argument("--root", type=Path, default=Path("data/live_observation"))
    parser.add_argument("--single-min", type=float, default=0.5)
    parser.add_argument("--single-max", type=float, default=0.56)
    parser.add_argument("--lead-min", type=float, default=0.5)
    parser.add_argument("--lead-max", type=float, default=0.60)
    parser.add_argument("--max-spread", type=float, default=0.04)
    parser.add_argument("--fee-rate", type=float, default=0.072)
    parser.add_argument("--order-size", type=float, default=10.0)
    parser.add_argument("--horizons", default="1,2,3,5")
    parser.add_argument("--best-exit-horizon", type=int, default=5)
    args = parser.parse_args()

    observation_dir = args.dir or _latest_observation_dir(args.root)
    state_rows = _load_jsonl(observation_dir / "state_snapshots.jsonl")
    event_rows = _load_jsonl(observation_dir / "events.jsonl")
    history_rows = _load_jsonl(observation_dir / "history_snapshots.jsonl")
    horizons = [int(part) for part in args.horizons.split(",") if part.strip()]

    print(f"OBSERVATION_DIR {observation_dir}")
    print(f"STATE_ROWS {len(state_rows)}")
    print(f"EVENT_ROWS {len(event_rows)}")
    print(f"HISTORY_ROWS {len(history_rows)}")
    print()

    source = _source_distribution(state_rows)
    print("OPEN_TRADE_COUNTS")
    for open_count, total in source["open_trade_counts"].most_common():
        print(f"  {open_count}: {total}")
    print()

    print("SOURCE_DISTRIBUTION")
    for coin in sorted(source["source_total"]):
        total = source["source_total"][coin]
        ge2 = source["source_ge2"][coin]
        missing = source["live_price_missing"][coin]
        dist = ", ".join(f"{count}={seen}" for count, seen in sorted(source["source_dist"][coin].items()))
        print(
            f"  {coin}: ge2={ge2}/{total} ({_pct(ge2, total):.1f}%), "
            f"missing_live_price={missing}, dist[{dist}]"
        )
    print()

    print("SIGNAL_ACTION_COUNTS")
    for coin in sorted(source["signal_action"]):
        parts = ", ".join(
            f"{strategy}/{action}={count}"
            for (strategy, action), count in source["signal_action"][coin].most_common()
        )
        print(f"  {coin}: {parts}")
    print()

    gating = _gating_stats(
        state_rows,
        single_min=args.single_min,
        single_max=args.single_max,
        lead_min=args.lead_min,
        lead_max=args.lead_max,
        max_spread=args.max_spread,
    )
    print("GATING_STATS")
    gating_fields = (
        "signals",
        "source_ge2",
        "entry_in_window",
        "spread_ok",
        "source_and_window",
        "source_and_spread",
        "window_and_spread",
        "all_three",
    )
    for key in sorted(gating):
        total = gating[key]["signals"]
        print(f"  {key}")
        for field in gating_fields:
            value = gating[key][field]
            if field == "signals":
                print(f"    {field}: {value}")
            else:
                print(f"    {field}: {value} ({_pct(value, total):.1f}%)")
    print()

    drift = _signal_live_drift(state_rows)
    print("SIGNAL_LIVE_DRIFT")
    for key in sorted(drift):
        row = drift[key]
        total = int(row["n"])
        avg_abs_diff = row["abs_diff_sum"] / total if total else 0.0
        print(
            f"  {key}: n={total}, avg_abs_diff={avg_abs_diff:.3f}, "
            f"max_abs_diff={row['max_abs_diff']:.3f}, "
            f">0.05={_pct(int(row['gt_0_05']), total):.1f}%, "
            f">0.10={_pct(int(row['gt_0_10']), total):.1f}%, "
            f">0.25={_pct(int(row['gt_0_25']), total):.1f}%"
        )
    print()

    micro = _directional_micro_scalp(
        state_rows,
        fee_rate=args.fee_rate,
        order_size=args.order_size,
        horizons=horizons,
    )
    print("LIVE_BOOK_DIRECTIONAL_MICROSCALP")
    for key in sorted(micro):
        coin, strategy, bucket = key
        print(f"  {coin} {strategy} {bucket}")
        for horizon in horizons:
            cell = micro[key].get(horizon)
            if not cell or not cell["n"]:
                continue
            total = int(cell["n"])
            print(
                f"    {horizon}s: n={total}, win_rate={_pct(int(cell['wins']), total):.1f}%, "
                f"avg_gross={cell['gross'] / total:+.4f}, avg_net={cell['net'] / total:+.4f}"
            )
    print()

    best = _best_exit_within(
        state_rows,
        fee_rate=args.fee_rate,
        order_size=args.order_size,
        max_horizon=args.best_exit_horizon,
    )
    print(f"BEST_EXIT_WITHIN_{args.best_exit_horizon}S")
    for key in sorted(best):
        coin, strategy, bucket = key
        row = best[key]
        total = int(row["n"])
        print(
            f"  {coin} {strategy} {bucket}: n={total}, "
            f"win_rate={_pct(int(row['wins']), total):.1f}%, "
            f"avg_gross={row['gross'] / total:+.4f}, avg_net={row['net'] / total:+.4f}"
        )


if __name__ == "__main__":
    main()
