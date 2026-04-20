# Project Instructions

## Git

- Always push to the `main` branch.
- Never push to any other branch unless the user explicitly asks.

## Versioning

- When bumping the patch version, skip any number ending in `0`.
- Examples: `x.x.9` → `x.x.11` (skip 10), `x.x.19` → `x.x.21` (skip 20).
- Always bump both `version.py` and `config.json` together.

---

## Tuning & Analysis

- To analyse bot performance and tune parameters, read `TUNING_CLAUDE.md` and follow the steps there.
- For other AI agents (Codex, GPT, etc.) use `TUNING.md` instead.
- **`STRATEGY.md` is stale** — last updated at v5.0.22 with old entry logic. Do not use it as a reference for current parameters. Read the active YAML instead.

### Where to find session exports (priority order)

1. **Dropbox (local sync)** — `C:/Users/micha/Dropbox/EDEC-bot-archive/session-exports/` — check here first, no auth needed.
2. **Data repo** — `data/github_exports/` after fetching via GitHub.
3. GitHub API / Dropbox API — only fall back to these if the local paths are missing.

The most recent export is the folder with the latest timestamp in its name (`YYYY-MM-DD_HHMMSS`).

### Reading a session export

Each session folder contains one or more CSVs. Rows are individual trades with 90+ columns.

**Timing**
- `ts` — absolute UTC timestamp of the trade entry.
- `xt` — exit timestamp. Trade duration = `xt - ts` in seconds. There is no pre-computed duration column.
- `td` — **target delta** (price distance from entry to profit target at entry time). Not trade duration.

**Entry / book state**
- `ep` — entry price (ask paid)
- `eb` — entry bid at time of entry
- `es` — entry spread (`ep - eb`); higher = wider book, more slippage risk
- `ea` — entry ask
- `b5` — best 5-level book depth USD at entry side

**Coin price feed quality** (logged at entry — key for diagnosing bad fills)
- `sdp` — source_dispersion_pct: % spread between Binance/Coinbase/CoinGecko prices. High values mean feeds disagree; entry is unreliable.
- `ssx` — source_staleness_max_s: age of the stalest feed in seconds. High = stale data.
- `ssa` — source_staleness_avg_s: average feed age. Use alongside `ssx`.
- `v30` — coin velocity over 30s (% move)
- `v60` — coin velocity over 60s (% move)

**Signal score components** (all informational, logged at entry)
- `sg` — composite signal score (0–100)
- `sgv` — velocity score component
- `sge` — entry price score component
- `sgd` — depth score component
- `sgs` — spread score component (higher = tighter spread = better)
- `sgt` — time remaining score component
- `sgb` — book balance score component

**Exit / outcome**
- `xp` — exit price
- `pnl` — realised P&L in USD
- `mfe` — max favourable excursion (highest bid seen while in position)
- `mae` — max adverse excursion (lowest bid seen while in position)
- `why` — exit reason string (e.g. `scalp_take_profit`, `loss_cut`, `near_close`, `stall_exit`)
- `xt` — exit timestamp

**Resolution / counterfactual** (only populated after markets resolve — blank on mid-session exports)
- `hr` — hold-to-resolution outcome
- `rpn` — resolution P&L if held to end
- `whw` — would-have-won flag
- `wbe` — would-have-broken-even flag
- `lct` — loss cut threshold pct. **Known bug: only written for `single_leg`, not `lead_lag`.**
- `lpx`, `fex`, `evp` — other resolution learning fields

**Filter rejection**
- `ff` — filter that caused rejection (e.g. `source_staleness`, `entry_spread`). **Check this first after a config change** to see which filter is firing most. Empty on trades that passed all filters.
- `sv` — strategy version. **Known bug: always "unknown"** — not populated from STRATEGY.md parser.

---

## Config Architecture

### Active config file

`edec_bot/config_phase_a_single.yaml` is the active Phase A config (single_leg + lead_lag enabled, dual_leg + swing_leg disabled). The root `config.yaml` is the reference/default and is not the one loaded by HA.

### Adding new YAML fields — required steps

Frozen dataclasses are used (`@dataclass(frozen=True)`). Adding a key to the YAML without adding the field to the dataclass causes a `TypeError` on startup.

**Always do these in order:**
1. Add the field with a default to the dataclass in `edec_bot/bot/config.py`
2. Add filter logic in the relevant strategy file (`bot/strategies/single_leg.py`, `lead_lag.py`, etc.)
3. Add the key to the YAML config
4. Bump the version

### `LeadLagConfig` — extra gotcha

`resolve_lead_lag_params()` in `config.py` builds a dict of effective params after applying per-coin overrides. If you add a new field to `LeadLagConfig` that you want accessible via coin overrides, add it to `resolve_lead_lag_params()` too. If the field has no coin-level override, read from `cfg` directly in `lead_lag.py` — no need to add it to the params dict.

### Current non-default fields (added 5.1.7) on `SingleLegConfig` and `LeadLagConfig`

| Field | Default | Phase A value | Purpose |
|---|---|---|---|
| `max_entry_spread` | 0.06 | 0.04 | Reject if bid-ask spread at entry exceeds this |
| `max_source_dispersion_pct` | 0.50 | 0.25 | Reject if price feeds disagree beyond this % |
| `max_source_staleness_s` | 4.0 | 1.5 | Reject if any feed is older than this (seconds) |

### `disabled_coins` pattern

In YAML, `disabled_coins` is a list. `load_config()` converts it to a tuple via `tuple(raw[...].get("disabled_coins", []))`. Always use the list form in YAML.

---

## Telemetry Notes (as of 5.1.7)

| Field | Note |
|---|---|
| `td` | **target_delta** — price gap to profit target at entry. Not trade duration. Compute duration from `xt - ts`. |
| `sv` | Fixed in 5.1.7 — now populated from `__version__`. Previously always "unknown". |
| `lct` | Fixed in 5.1.7 — now written for `lead_lag` exits (uses `hard_stop_loss_pct`). Previously only written for `single_leg`. |
| `hr`, `rpn`, `whw`, `wbe` | Only populated post-resolution. Mid-session exports will always show these blank. Wait for post-resolution export before counterfactual analysis. |
