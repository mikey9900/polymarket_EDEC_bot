# Project Instructions

## Tuning

- To analyse bot performance and tune parameters, read `TUNING.md` and follow the steps there.
- Data repo exports land in `data/github_exports/` after running `python edec_bot/fetch_github_data.py`.

## Git

- Always push to the `main` branch.
- Never push to any other branch unless the user explicitly asks.

## Versioning

- When bumping the patch version, skip any number ending in `0`.
- Examples: `x.x.9` → `x.x.11` (skip 10), `x.x.19` → `x.x.21` (skip 20).
- Always bump both `version.py` and `config.json` together.
