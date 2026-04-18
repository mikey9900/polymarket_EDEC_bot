# EDEC Bot Codex Playbook

Practical tuning script for Codex.
This is a working playbook, not a rigid checklist. Use judgment, but keep the data lineage strict.

## Purpose

Use recent EDEC session exports to:

- verify the data is trustworthy
- identify what is actually hurting performance
- propose small, evidence-backed config changes
- separate tuning changes from telemetry fixes

## Core stance

- Prefer evidence over inherited assumptions.
- Do not widen stops just because a prior script suggested it.
- Do not tune from exports with weak attribution.
- Treat telemetry integrity issues as blockers, not footnotes.
- Present config changes, advisories, and data-quality issues as separate outputs.

## Primary data source

Preferred source is the EDEC GitHub data repo.

Credential lookup priority:

- CLI args
- environment variables
- project `.env`
- Home Assistant add-on options at `/data/options.json`

Expected keys:

- `EDEC_GITHUB_TOKEN` or `github_token`
- `EDEC_GITHUB_REPO` or `github_repo`

Optional keys:

- `EDEC_GITHUB_BRANCH` default `main`
- `EDEC_GITHUB_EXPORT_PATH` default `session_exports`

## Pull data

From repo root:

```bash
python edec_bot/fetch_github_data.py --limit 3
```

Useful variants:

```bash
python edec_bot/fetch_github_data.py --limit 5
python edec_bot/fetch_github_data.py --output-dir data/github_exports
python edec_bot/fetch_github_data.py --github-repo owner/edec-bot-data --github-branch main
```

Use the newest timestamped export folder and prefer:

- `*_session_trades.csv`
- `*_session_signals.csv`
- `index.json`

## Required telemetry checks

Before tuning, confirm the export is safe to learn from.

Minimum checks:

- `session_trades` rows are linked by real `decision_id`, not inferred by market fallback
- `session_trades` includes path fields such as `mfe`, `mae`, `peak_net_pnl`, `trough_net_pnl`
- `session_trades` includes hold/exit-learning fields such as `hold_to_resolution`, `loss_cut_threshold_pct`, `loss_pct_at_exit`, `favorable_excursion`, `ever_profitable`
- `session_signals` includes feed-quality fields such as `source_prices_json`, `source_ages_json`, `source_dispersion_pct`, `source_staleness_max_s`, `source_staleness_avg_s`
- config hash and strategy/app version are present in the export metadata

If these checks fail, fix telemetry first or clearly downgrade confidence in the recommendations.

## Resolve tuning target

Default config target:

```bash
python -c "import os; print(os.getenv('EDEC_CONFIG_PATH', 'edec_bot/config_phase_a_single.yaml'))"
```

Use the resolved file unless the user explicitly wants a different profile.

## Analysis workflow

1. Read the latest `index.json` and identify the session window, version context, and row counts.
2. Load `session_trades` and `session_signals`.
3. Validate attribution and field completeness before making any recommendation.
4. Compute headline performance:
   - total closed trades
   - win/loss counts
   - total and average P&L
   - breakdown by strategy
5. Slice the data by:
   - coin
   - strategy
   - velocity bucket
   - entry-price bucket
   - exit reason
   - depth ratio / liquidity bucket
6. Use path metrics to answer:
   - were winners stopped too early?
   - were losers allowed to get too large?
   - did trades ever go meaningfully positive before failing?
   - did feed disagreement or stale pricing correlate with bad entries?
7. Compare paper and live behavior if live telemetry is present.

## High-value tuning targets

Prioritize these first:

- `single_leg.min_velocity_30s`
- `single_leg.entry_min`
- `single_leg.entry_max`
- `single_leg.loss_cut_pct`
- `single_leg.disabled_coins`
- `lead_lag.min_velocity_30s`
- `lead_lag.min_entry`
- `lead_lag.max_entry`
- `lead_lag.hard_stop_loss_pct`
- `lead_lag.disabled_coins`

Advisory-only unless strongly supported:

- high-confidence thresholds
- stall-window behavior
- broad order-size changes

## Decision rules

- Favor incremental moves over large jumps.
- Require adequate sample size before changing a threshold.
- Prefer removing obviously bad trade clusters over broad loosening.
- If average losses are much larger than average wins, focus on entry quality and exit containment first.
- If feed disagreement is elevated on bad trades, flag data quality before over-tuning strategy logic.

## Expected output

Produce three blocks:

1. Findings
   - what the data says
2. Proposed config deltas
   - current value
   - recommended value
   - short justification
3. Advisories
   - telemetry concerns
   - weak-sample warnings
   - items to watch next run

## Safe apply flow

Before editing configs:

1. Show proposed changes.
2. Wait for confirmation.
3. If approved, update the config file.
4. Bump patch version in:
   - `edec_bot/version.py`
   - `edec_bot/config.json`

## Quick run

```bash
python edec_bot/fetch_github_data.py --limit 3
python -c "import os; print(os.getenv('EDEC_CONFIG_PATH', 'edec_bot/config_phase_a_single.yaml'))"
```

Then:

- inspect the newest export folder
- verify telemetry integrity
- analyze trades and signals
- recommend deltas
- apply only after confirmation
