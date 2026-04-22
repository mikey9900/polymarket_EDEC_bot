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
   - `config_path`: `edec_bot/config_phase_a_single.yaml`
   - `poll_seconds`: `15`
   - `timezone`: your local timezone
   - `codex_home`: `/data/codex`
   - `openai_api_key`: your OpenAI API key
6. Start `EDEC Codex Runner`.
7. Verify `/data/edec/codex/state.json` appears.
8. Start `EDEC Polymarket Bot`.
9. Use the HA dashboard controls to:
   - queue a daily research refresh
   - queue a tuning proposal
   - pause/resume weekly tuning
   - switch weekly/manual cadence
   - skip next weekly run
   - promote or reject the latest candidate

## Shared storage layout

- `/data/edec/codex/state.json`
- `/data/edec/codex/latest.json`
- `/data/edec/codex/queue/*.json`
- `/data/edec/codex/runs/<run_id>/`

## Important behavior

- The Codex runner becomes the primary automation engine for research/tuning.
- Daily and weekly timing are computed locally by the runner.
- Tuning proposals are deterministic repo code.
- Promotion remains manual.
