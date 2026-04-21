## Repo Handoff

Repo:
`https://github.com/mikey9900/polymarket_EDEC_bot`

Branch:
`codex-polymarket-cli-ops-layer`

Latest branch commit:
`fae0abc` - `Add Polymarket CLI operator tooling`

## What Is Done

- Polymarket CLI ops layer is implemented.
- Telegram commands added:
  - `/pmaccount`
  - `/pmorders`
  - `/pmtrades`
  - `/pmcancelall`
- Dockerfile updated to build and include the pinned `polymarket` CLI binary.
- Python tests added for the CLI adapter and Telegram handlers.
- Local Python test suite passed:
  - `12 passed`

## What Is Not Done Yet

- Docker Desktop / Linux container smoke test has not been completed.
- Real runtime verification is still needed for:
  - container build
  - CLI startup health check
  - live Telegram command behavior
  - guarded cancel-all flow

## Easiest Setup On The Other PC

### 1. Get the code

If the repo is not cloned yet:

```powershell
git clone https://github.com/mikey9900/polymarket_EDEC_bot.git
cd polymarket_EDEC_bot
git switch --track origin/codex-polymarket-cli-ops-layer
```

If the repo is already cloned:

```powershell
git fetch origin
git switch codex-polymarket-cli-ops-layer
git pull
```

### 2. Run the Python tests

Install Python 3.11 if needed, then:

```powershell
python -m pip install -r edec_bot/requirements.txt
python -m pytest -q -p no:cacheprovider
```

Expected result:

```text
12 passed
```

### 3. Install Docker Desktop

- Install Docker Desktop manually
- Use the WSL 2 backend
- Launch Docker Desktop and wait until it reports that Docker is running

### 4. Build the container

From repo root:

```powershell
docker build -t edec-bot-cli-test -f edec_bot/Dockerfile edec_bot
```

### 5. Run the bot container

Use real secrets:

```powershell
docker run --rm -it ^
  -e PRIVATE_KEY="YOUR_PRIVATE_KEY" ^
  -e TELEGRAM_BOT_TOKEN="YOUR_TELEGRAM_BOT_TOKEN" ^
  -e TELEGRAM_CHAT_ID="YOUR_TELEGRAM_CHAT_ID" ^
  edec-bot-cli-test
```

### 6. Smoke test in Telegram

Run these commands:

- `/pmaccount`
- `/pmorders`
- `/pmtrades`
- `/pmcancelall`

Expected checks:

- bot starts cleanly
- CLI health check logs correctly
- read-only CLI commands return data or friendly availability/auth errors
- `pmcancelall` is blocked while `cli.allow_mutating_commands: false`

## Important Notes

- Production target is the Linux container, not Windows-native execution.
- The trading hot path was intentionally left unchanged.
- Local untracked folders on the original machine were not part of the branch:
  - `edec-bot-data/`
  - `edec-bot-data-1/`
  - `desktop.ini`
