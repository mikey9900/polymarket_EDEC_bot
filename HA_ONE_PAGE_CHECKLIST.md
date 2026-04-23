# HA One-Page Checklist

Use this when installing the bot and Codex runner on Home Assistant OS.

## Before You Start

- [ ] Home Assistant OS is running
- [ ] This repo is cloned on the HA machine at `/share/polymarket_EDEC_bot`
- [ ] You have bot secrets ready for the main `EDEC Polymarket Bot` add-on

## Add-on Repository

- [ ] In HA, open `Settings -> Add-ons -> Add-on Store`
- [ ] Open the repository menu
- [ ] Add this GitHub repo as a custom repository
- [ ] Confirm both add-ons appear:
  - [ ] `EDEC Polymarket Bot`
  - [ ] `EDEC Codex Runner`

## Install Codex Runner

- [ ] Install `EDEC Codex Runner`
- [ ] Set options:
  - [ ] `workspace_path=/share/polymarket_EDEC_bot`
  - [ ] `config_path=edec_bot/config_phase_a_single.yaml`
  - [ ] `poll_seconds=15`
  - [ ] `timezone=America/Edmonton` or your local timezone
  - [ ] `codex_home=/data/codex`
- [ ] Start the add-on
- [ ] Confirm the add-on stays healthy after first boot

## Verify Shared State

- [ ] Confirm `/share/edec/codex/state.json` exists
- [ ] Confirm `/share/edec/codex/queue/` exists
- [ ] Confirm `/share/edec/codex/runs/` exists

## Install Bot

- [ ] Install or update `EDEC Polymarket Bot`
- [ ] Enter the normal bot secrets and GitHub/archive settings
- [ ] Start the bot add-on
- [ ] Open the dashboard

## Dashboard Checks

- [ ] `RUN RESEARCH` queues a daily refresh
- [ ] `RUN NOW` under `TUNER` queues a weekly desktop review bundle
- [ ] `PAUSE` and `RESUME` change weekly tuning state
- [ ] `WEEKLY` and `MANUAL` change cadence
- [ ] `SKIP NEXT` sets the next weekly run to skip
- [ ] `PROMOTE` only works when a candidate is ready
- [ ] `REJECT` marks the latest candidate rejected

## Final Success Check

- [ ] Daily research runs appear under `/share/edec/codex/runs/`
- [ ] Weekly review bundles write compact bundle artifacts for desktop Codex review
- [ ] The dashboard shows live `codex` queue depth and tuner status
