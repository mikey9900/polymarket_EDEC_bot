# EDEC Data Archive Setup (Home Assistant + Dropbox)

This adds a daily export pipeline with AI-friendly naming and a deterministic latest pointer.

## What Gets Produced Every Run

Local output folder: `edec_bot/data/exports/`

1. Daily 24h Excel report  
   `YYYY-MM-DD_EDEC-BOT_last24h.xlsx`
2. Daily compressed recent-trades snapshot  
   `YYYY-MM-DD_HHMMSS_EDEC-BOT_trades_000123-000622.csv.gz`
3. Stable latest Excel copy  
   `EDEC-BOT_latest_last24h.xlsx`
4. Stable latest trades copy  
   `EDEC-BOT_latest_trades.csv.gz`
5. Stable latest metadata pointer  
   `EDEC-BOT_latest_index.json`

`EDEC-BOT_latest_index.json` is the key AI pointer for "most recent" resolution.

## Dropbox Layout

If `EDEC_DROPBOX_TOKEN` is set:

1. `/EDEC-BOT/daily-reports/` -> daily Excel
2. `/EDEC-BOT/daily-archives/` -> daily compressed trades files
3. `/EDEC-BOT/latest/` -> latest copies + `EDEC-BOT_latest_index.json`

## Manual Run

From `edec_bot/`:

```powershell
python archive_daily.py --recent-limit 500 --label EDEC-BOT
```

Optional args:

1. `--db-path data/decisions.db`
2. `--output-dir data/exports`
3. `--dropbox-token <token>` (or env var)
4. `--dropbox-root /EDEC-BOT` (or env var)

## Home Assistant Add-on Settings (recommended for HA OS)

This repository now supports archive scheduling directly inside the add-on.
Set these in the add-on UI (`Configuration` tab):

1. `archive_enabled: true`
2. `archive_time: "00:05"` (local add-on time)
3. `archive_recent_limit: 500`
4. `archive_label: "EDEC-BOT"`
5. `archive_output_dir: "data/exports"`
6. `archive_telegram_files: true`
7. `dropbox_token: "<token>"`
8. `dropbox_root: "/EDEC-BOT"`

No separate HA `shell_command` is required when using add-on options.

## Telegram Integration

1. Daily run sends archive status summary.
2. Daily run can auto-send latest archive files (`archive_telegram_files: true`).
3. On-demand command: `/latest_export` sends:
   - latest 24h Excel
   - latest compressed trades file
   - latest index JSON

## Repo-Local Dropbox Sync (No Drag/Drop)

Use this when you want local workspace access to the latest files for AI analysis.

From `edec_bot/`:

```powershell
python sync_dropbox_to_repo_latest.py --output-dir data/dropbox_sync
```

This pulls from Dropbox `/EDEC-BOT/latest/` into:

1. `data/dropbox_sync/EDEC-BOT_latest_last24h.xlsx`
2. `data/dropbox_sync/EDEC-BOT_latest_trades.csv.gz`
3. `data/dropbox_sync/EDEC-BOT_latest_trades.csv` (decompressed helper)
4. `data/dropbox_sync/EDEC-BOT_latest_index.json`

Required env or arg:

1. `EDEC_DROPBOX_TOKEN` (or `--dropbox-token`)
