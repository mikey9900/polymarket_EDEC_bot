# Project Instructions

## Tuning

- To analyse bot performance and tune parameters, read `TUNING_CLAUDE.md` and follow the steps there.
- For other AI agents (Codex, GPT, etc.) use `TUNING.md` instead.
- Data repo exports land in `data/github_exports/` after fetching.
- Dropbox session exports are locally synced at `C:/Users/micha/Dropbox/EDEC-bot-archive/session-exports/` — check here first before attempting API fetch.
- The active Phase A config is `edec_bot/config_phase_a_single.yaml` (not `config.yaml`).

## Session Export CSV Fields

Trade rows are timestamped — use `ts` for absolute time, `td` for duration (note: `td` scale may be broken, use `ts` diff instead).

Key feed quality fields logged at entry:
- `sdp` — source_dispersion_pct: % spread between price feeds (high = stale/divergent feeds)
- `ssx` — source_staleness_max_s: max age of any feed in seconds
- `ssa` — source_staleness_avg_s: average feed age

Signal score fields: `sg` (composite), `sgv` (velocity), `sge` (entry), `sgd` (depth), `sgs` (spread), `sgt` (time), `sgb` (book balance).

Resolution/outcome fields: `hr`, `lct`, `lpx`, `fex`, `evp`, `rpn`, `whw`, `wbe` — these are only populated after markets resolve, so exports taken mid-session will show them empty.

## Config Dataclass Fields

When adding new YAML keys, the corresponding field **must** be added to the frozen dataclass in `edec_bot/bot/config.py` first or the bot will crash on load.

Current non-obvious fields on `SingleLegConfig` and `LeadLagConfig` (added 5.1.7):
- `max_entry_spread` — rejects entries where bid-ask spread exceeds threshold
- `max_source_dispersion_pct` — rejects when price feeds disagree beyond threshold
- `max_source_staleness_s` — rejects when freshest feed is older than threshold

## Git

- Always push to the `main` branch.
- Never push to any other branch unless the user explicitly asks.

## Versioning

- When bumping the patch version, skip any number ending in `0`.
- Examples: `x.x.9` → `x.x.11` (skip 10), `x.x.19` → `x.x.21` (skip 20).
- Always bump both `version.py` and `config.json` together.
