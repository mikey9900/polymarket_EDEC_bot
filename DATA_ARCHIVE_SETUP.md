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

If Dropbox auth is set (`EDEC_DROPBOX_TOKEN` or refresh-token credentials):

1. `/daily-reports/` -> daily Excel
2. `/daily-archives/` -> daily compressed trades files
3. `/latest/` -> latest copies + `EDEC-BOT_latest_index.json`

For app-folder Dropbox apps, `/` maps to `Apps/<your-app-name>/`.

## Manual Run

From `edec_bot/`:

```powershell
python archive_daily.py --recent-limit 500 --label EDEC-BOT
```

Optional args:

1. `--db-path data/decisions.db`
2. `--output-dir data/exports`
3. `--dropbox-token <token>` (or env var)
4. `--dropbox-refresh-token <token>` + `--dropbox-app-key` + `--dropbox-app-secret`
5. `--dropbox-root /` (or env var)

## Home Assistant Add-on Settings (recommended for HA OS)

This repository now supports archive scheduling directly inside the add-on.
Set these in the add-on UI (`Configuration` tab):

1. `archive_enabled: true`
2. `archive_time: "00:05"` (local add-on time)
3. `archive_recent_limit: 500`
4. `archive_label: "EDEC-BOT"`
5. `archive_output_dir: "data/exports"`
6. `archive_telegram_files: true`
7. `dropbox_token: "<token>"` for quick/manual testing
8. `dropbox_refresh_token: "<refresh-token>"` for automatic renewal
9. `dropbox_app_key: "<app-key>"`
10. `dropbox_app_secret: "<app-secret>"`
11. `dropbox_root: "/"`

No separate HA `shell_command` is required when using add-on options.
The bot will prefer refresh-token auth when those fields are present, and fall back to `dropbox_token` otherwise.

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
python sync_dropbox_to_repo_latest.py
```

This pulls from Dropbox `/latest/` into:

1. `dropbox_sync/EDEC-BOT_latest_last24h.xlsx`
2. `dropbox_sync/EDEC-BOT_latest_trades.csv.gz`
3. `dropbox_sync/EDEC-BOT_latest_trades.csv` (decompressed helper)
4. `dropbox_sync/EDEC-BOT_latest_index.json`

The Telegram `Sync Dropbox` button/command now also sends the synced files back into chat after a successful pull, so you can confirm the Excel arrived without separately pressing `Latest Archive`.

Required env or arg:

1. `EDEC_DROPBOX_TOKEN` for direct token auth
2. Or `EDEC_DROPBOX_REFRESH_TOKEN` + `EDEC_DROPBOX_APP_KEY` + `EDEC_DROPBOX_APP_SECRET` for auto-renewed auth
