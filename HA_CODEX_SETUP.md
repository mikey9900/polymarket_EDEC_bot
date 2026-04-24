# HA Codex Setup

This repo now contains two Home Assistant add-ons:

- [`edec_bot/config.json`](C:/Users/micha/polymarket_EDEC_bot/edec_bot/config.json)
- [`ha_codex/config.json`](C:/Users/micha/polymarket_EDEC_bot/ha_codex/config.json)

## Recommended install flow

1. Clone the repo onto the HA machine at `/share/polymarket_EDEC_bot`.
2. In Home Assistant, add this GitHub repo as an add-on repository.
3. Install `EDEC Polymarket Bot`.
4. Install `EDEC Codex Runner`.
5. Configure `EDEC Codex Runner`:
   - `workspace_path`: `/share/polymarket_EDEC_bot`
   - `config_path`: `/share/edec/config/active_config.yaml`
   - `poll_seconds`: `15`
   - `timezone`: your local timezone
   - `codex_home`: `/data/codex`
   - optional GitHub mirroring:
     - `github_token`: a GitHub token with repo contents write access
     - `github_repo`: a data repo such as `owner/edec-research-data`
     - `github_branch`: usually `main`
     - `github_research_path`: folder root like `research_exports`
6. Start `EDEC Codex Runner`.
7. Verify `/share/edec/codex/state.json` appears.
8. Start `EDEC Polymarket Bot`.
9. Use the HA dashboard controls to:
   - queue a daily research refresh
   - queue a weekly desktop review bundle
   - pause/resume weekly tuning
   - switch weekly/manual cadence
   - skip next weekly run
   - promote or reject the latest candidate

## Shared storage layout

- `/share/edec/codex/state.json`
- `/share/edec/codex/latest.json`
- `/share/edec/codex/queue/*.json`
- `/share/edec/codex/runs/<run_id>/`

## Important behavior

- The Codex runner becomes the primary automation engine for research/tuning.
- Daily and weekly timing are computed locally by the runner.
- Daily tuning proposals are deterministic repo code.
- Weekly runs prepare a compact desktop review bundle instead of calling an API from HA.
- Promotion remains manual.
- If GitHub mirroring is configured on the runner, it publishes the latest research bundle and active config without touching the code repo checkout.
