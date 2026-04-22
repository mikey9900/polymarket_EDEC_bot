# EDEC Codex Runner

Home Assistant sidecar/add-on for the HA-local Codex workflow runner.

## What it runs

It mounts your repo workspace, points shared orchestration state at `/data/edec`, and runs:

```bash
python -m edec_bot.research codex-runner
```

The runner owns:
- daily research refresh at 6:15 AM local time
- weekly tuning proposal at Monday 6:30 AM local time
- queue-backed manual jobs from the HA dashboard

## Required setup on the HA machine

1. Clone this repo onto the HA machine under `/share/polymarket_EDEC_bot`.
2. Add this GitHub repo as a Home Assistant add-on repository.
3. Install both add-ons:
   - `EDEC Polymarket Bot`
   - `EDEC Codex Runner`
4. Configure the Codex Runner options:
   - `workspace_path`: `/share/polymarket_EDEC_bot`
   - `config_path`: `edec_bot/config_phase_a_single.yaml`
   - `poll_seconds`: `15`
   - `timezone`: your local zone, for example `America/Edmonton`
   - `codex_home`: `/data/codex`
   - `openai_api_key`: your API key
5. Start the Codex Runner add-on.
6. Confirm it creates `/data/edec/codex/state.json`.
7. Start the bot add-on and use the dashboard controls to queue research/tuner jobs.

## Notes

- The sidecar caches a Python venv under `/data/codex/venv`, so it does not reinstall dependencies on every boot.
- Shared queue/state/results live under `/data/edec/codex`.
- The runner does not rewrite live config automatically. Promotion still requires the explicit HA control action.
