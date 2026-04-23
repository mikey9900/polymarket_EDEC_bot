# EDEC Bot Codex Playbook

Practical tuning script for Codex.
This is a working playbook, not a rigid checklist. Use judgment, but keep the data lineage strict.

## Git

- Always push to the `main` branch.
- Never push to any other branch unless the user explicitly asks.

## Versioning

- When bumping the patch version, skip any number ending in `0`.
- Examples: `x.x.9` → `x.x.11` (skip 10), `x.x.19` → `x.x.21` (skip 20).
- Always bump both `version.py` and `config.json` together.

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

## Local command rule

For local runs on this Windows workstation, execute Python through the repo venv helper:

```powershell
.\scripts\venv_python.cmd ...
```

Do not use bare `python` for local repo commands. Home Assistant and CI keep using their own environments.

## Primary data source

Preferred source order is:

1. repo-local latest pointers and synced files
2. repo-local session mirror
3. Dropbox repo sync
4. GitHub fetch as a fallback, not the default

Do not start with remote fetch if a fresh local export is already present.

Fastest places to check first:

- `edec_bot/data/exports/EDEC-BOT_latest_index.json`
- `edec_bot/data/exports/EDEC-BOT_latest_trades.csv.gz`
- `edec_bot/data/exports/EDEC-BOT_latest_signals.csv.gz`
- `.tmp_edec_data_repo/session_exports/<newest timestamp>/`

Treat the newest local `index.json` or latest-pointer file as the authority for "most recent".
Prefer `.csv.gz` directly if present. Do not require a decompressed `.csv` if the gzip file is readable.

If local latest files are stale or missing:

1. try repo-local Dropbox sync
2. only then try GitHub fetch

If GitHub credentials are missing, do not stop there. Fall back to local mirror or Dropbox sync.

### Repo-local Dropbox sync

From repo root:

```powershell
.\scripts\venv_python.cmd edec_bot/sync_dropbox_to_repo_latest.py
```

Expected synced outputs:

- `dropbox_sync/EDEC-BOT_latest_index.json`
- `dropbox_sync/EDEC-BOT_latest_trades.csv.gz`
- `dropbox_sync/EDEC-BOT_latest_trades.csv`
- `dropbox_sync/EDEC-BOT_latest_last24h.xlsx`

### Repo-local session mirror

If `.tmp_edec_data_repo/session_exports/` exists, inspect the newest timestamped folder first.
Prefer:

- `*_session_index.json`
- `*_session_trades.csv.gz`
- `*_session_signals.csv.gz`

This path is usually faster and more reliable than fetching again.

### GitHub fetch fallback

Only use this if the local latest files and repo-local mirror are missing or stale.

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

```powershell
.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --limit 3
```

Useful variants:

```powershell
.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --limit 5
.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --output-dir data/github_exports
.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --github-repo owner/edec-bot-data --github-branch main
```

Default download location:

- `data/github_exports/`

Use the newest timestamped export folder and prefer:

- `*_session_trades.csv.gz`
- `*_session_signals.csv.gz`
- `*_session_index.json`

Do not assume the fetch step is required every time.
If `edec_bot/data/exports/`, `dropbox_sync/`, or `.tmp_edec_data_repo/session_exports/` already has a newer export, use that instead.

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

```powershell
.\scripts\venv_python.cmd -c "import os; print(os.getenv('EDEC_CONFIG_PATH', 'edec_bot/config_phase_a_single.yaml'))"
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

Preferred quick path:

1. check `edec_bot/data/exports/EDEC-BOT_latest_index.json`
2. if stale/missing, check `.tmp_edec_data_repo/session_exports/`
3. if stale/missing, run Dropbox sync:

```powershell
.\scripts\venv_python.cmd edec_bot/sync_dropbox_to_repo_latest.py
```

4. if still missing, use GitHub fetch:

```powershell
.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --limit 3
.\scripts\venv_python.cmd -c "import os; print(os.getenv('EDEC_CONFIG_PATH', 'edec_bot/config_phase_a_single.yaml'))"
```

Then:

- inspect the newest local export pointer or export folder
- verify telemetry integrity
- analyze trades and signals
- recommend deltas
- apply only after confirmation
