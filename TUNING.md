# EDEC Bot — Tuning Session

**Claude: when asked to read this file and tune the bot, execute every numbered step below in order without waiting for further input. Use your Bash, Read, and Edit tools directly.**

---

## Step 1 — Fetch the latest session data from GitHub

Run the fetch script. It reads credentials from environment variables.

```bash
cd /path/to/repo && python edec_bot/fetch_github_data.py --limit 3
```

Substitute the actual repo root. If the working directory is already the repo root, use:

```bash
python edec_bot/fetch_github_data.py --limit 3
```

If this fails with a credentials error, check for a `.env` file in the `edec_bot/` folder containing `EDEC_GITHUB_TOKEN` and `EDEC_GITHUB_REPO`. If those vars are missing, tell the user and stop.

The script prints the exact paths of downloaded CSV files. Note them — you will read them in Step 3.

---

## Step 2 — Identify the active config file

Read `edec_bot/main.py` lines 239–240 to find the active config path:

```python
config_path = os.getenv("EDEC_CONFIG_PATH", "config_phase_a_single.yaml")
```

The default is `edec_bot/config_phase_a_single.yaml`. Read that file now.

---

## Step 3 — Read the CSV files

After Step 1 the CSV files live under `data/github_exports/{YYYY-MM-DD_HHMMSS}/`.

There are two files per folder:
- `*_session_trades.csv` — one row per trade (primary analysis file)
- `*_session_signals.csv` — one row per signal decision that fired

Read both. If multiple folders were fetched, use the most recent one (highest timestamp in folder name) for primary analysis, and the others for trend context.

### Trades CSV column glossary

| Col | Meaning | Notes |
|-----|---------|-------|
| `id` | trade ID | |
| `ts` | timestamp (UTC) | |
| `c` | coin | btc / eth / sol / xrp / bnb / doge / hype |
| `st` | strategy type | single_leg / lead_lag / dual_leg / swing_leg |
| `sd` | side | up / down |
| `ep` | entry price | 0–1 range (Polymarket) |
| `tp` | target sell price | |
| `eb` / `ea` | entry bid / ask | |
| `es` | entry spread | ask − bid |
| `cs` | cost (USD) | |
| `fee` | fees paid (USD) | |
| `status` | trade outcome | closed_win / closed_loss / open |
| `xp` | exit price | |
| `xb` | bid at exit | |
| `er` | exit reason | see below |
| `tx` | time remaining at exit (s) | seconds left on the market |
| `pnl` | P&L (USD) | negative = loss |
| `v30` | 30s velocity (%) | coin price velocity at signal time |
| `v60` | 60s velocity (%) | |
| `eds` | entry-side book depth (USD) | liquidity in trade direction |
| `ods` | opposite-side book depth (USD) | |
| `drt` | depth ratio | eds / ods |
| `maxb` | max bid seen during trade | highest point the bid reached |
| `minb` | min bid seen during trade | lowest point (drawdown) |
| `mfe` | max favourable excursion (USD) | best unrealised P&L during trade |
| `mae` | max adverse excursion (USD) | worst unrealised P&L during trade |
| `tfp` | first profit time (s) | seconds until trade first went positive |
| `sc` | scalp exit hit (0/1) | 1 = scalp_take_profit_bid was reached |
| `hc` | high-confidence exit hit (0/1) | 1 = high_confidence_bid was reached |
| `sx` | stall exit triggered (0/1) | |
| `fp` | filters passed (CSV list) | |
| `ff` | filters failed (CSV list) | what blocked other trades |
| `te` | time remaining at entry (s) | |
| `sg` | signal score | composite quality score |
| `sgv`/`sge`/`sgd`/`sgs`/`sgt`/`sgb` | score sub-components | velocity/entry/depth/spread/time/balance |
| `pnp` / `tnp` | peak / trough net P&L | |

**Exit reason values:**
- `profit_target` — bid hit target_price → win
- `scalp` — bid hit scalp_take_profit_bid → win
- `high_confidence` — bid hit high_confidence_bid early → win
- `loss_cut` — bid fell below entry × (1 − loss_cut_pct) → loss
- `stall_exit` — bid stalled with no progress → loss
- `near_close` — forced out near market expiry
- `resolution` — market resolved (win at $1.00 payout, loss at $0.00)
- `dead_leg` — one leg of arb collapsed (dual_leg only)

### Signals CSV column glossary

| Col | Meaning |
|-----|---------|
| `id` | decision ID |
| `ts` | timestamp |
| `c` | coin |
| `st` | strategy type |
| `act` | action: DRY_RUN_SIGNAL / TRADE / SUPPRESSED / SKIP |
| `sup` | suppression reason (if act=SUPPRESSED) |
| `ep` | entry price |
| `v30` / `v60` | velocity at signal time |
| `te` | time remaining on market |
| `eds` / `ods` | depth |
| `sg` | signal score |
| `fp` / `ff` | filters passed / failed |
| `why` | reason string |

---

## Step 4 — Analyse the trade data

Compute the following. Use pandas-style reasoning or write a quick Python script if the file is large, otherwise reason from the raw CSV.

### 4a. Overall summary
- Total trades, wins, losses, open
- Win rate %, total P&L, avg P&L per trade

### 4b. Win rate by exit reason
Group by `er`. For each: trade count, win count, win rate %, total P&L.
Flag any exit reason with > 5 trades and win rate < 40% — that exit logic is leaking.

### 4c. Win rate by entry price bucket
Group `ep` into: <0.50, 0.50–0.55, 0.55–0.60, 0.60–0.65, 0.65+.
Find which bucket has the best win rate. Tighten `entry_min`/`entry_max` toward it.

### 4d. Velocity analysis
Group `v30` into: <0.05, 0.05–0.10, 0.10–0.15, 0.15–0.20, 0.20+.
Find the threshold below which win rate drops below 50%. That is the new `min_velocity_30s`.

### 4e. Stop loss calibration
Compute: `mae` distribution (min, median, 75th percentile, 90th percentile).
The 75th–80th percentile of `|mae|` as a fraction of `ep` is a reasonable `loss_cut_pct`.
Current value is in config — compare and flag if mae p75 is significantly different.

### 4f. Exit timing
- Trades where `er = near_close` and `pnl < 0`: these exited too late. Check `tx` median.
- Trades where `sc = 1` or `hc = 1`: check if those are more profitable than trades that didn't hit the threshold. If scalp/HC trades have better win rates, consider tightening those bid levels.
- `maxb` distribution: if median `maxb` is significantly above `high_confidence_bid`, we are leaving money on the table — lower the HC level.

### 4g. Per-coin breakdown
Group by `c`. For each coin: trades, win rate, avg P&L.
Coins with < 40% win rate and ≥ 5 trades should be considered for `disabled_coins`.

### 4h. Per-strategy breakdown
Group by `st`. Same analysis as 4g.

### 4i. Skipped trade analysis (signals CSV)
From the signals file, look at rows where `ff` is non-empty (filters failed).
What are the most commonly failing filters? If a filter is rejecting > 30% of signals, it may be too aggressive.

### 4j. Depth ratio check
For losing trades, what is the median `drt`? If `drt` is high (> 2.0) for losses, it suggests unbalanced books — consider stricter depth filters.

---

## Step 5 — Read the current config

The active config file is `edec_bot/config_phase_a_single.yaml` (confirmed in Step 2).

Key parameters and what they control:

**single_leg section:**
| Parameter | Effect |
|-----------|--------|
| `entry_min` / `entry_max` | Price range filter for entries |
| `min_velocity_30s` | Minimum momentum required to enter |
| `loss_cut_pct` | Stop loss as % of entry price |
| `loss_cut_max_factor` | Max loss cut in absolute terms |
| `high_confidence_bid` | Bid level that triggers early profit exit |
| `scalp_take_profit_bid` | Lower bid level for scalp exits |
| `scalp_min_profit_usd` | Minimum USD profit to take scalp |
| `time_pressure_s` | Hold-to-resolution window (s) |
| `min_time_remaining_s` | Minimum market time required to enter |
| `max_time_remaining_s` | Maximum market time allowed to enter |
| `min_book_depth_usd` | Minimum liquidity on entry side |
| `resignal_cooldown_s` | Seconds before re-signalling same market |

**lead_lag section:**
| Parameter | Effect |
|-----------|--------|
| `min_velocity_30s` | Minimum momentum |
| `min_entry` / `max_entry` | Entry price range |
| `profit_take_delta` | Rise above entry required to take profit |
| `profit_take_cap` | Max bid level at which to take profit |
| `hard_stop_loss_pct` | Stop loss % |
| `stall_window_s` | Window to detect bid stall |
| `min_progress_delta` | Minimum bid progress within stall window |

---

## Step 6 — Produce a structured recommendation

Output the following:

```
## Tuning Recommendations — [date]

### Summary
[2–3 sentences: what the data shows overall]

### Proposed changes to config_phase_a_single.yaml

| Section | Parameter | Current | Recommended | Reason |
|---------|-----------|---------|-------------|--------|
| single_leg | entry_min | 0.50 | 0.52 | Trades below 0.52 had 35% win rate (n=18) |
| ... | ... | ... | ... | ... |

### Coins to disable
[list any coins with < 40% win rate over ≥ 5 trades, if not already disabled]

### Filters to loosen
[list any filter in ff that is firing too often relative to its benefit]

### No change needed
[list parameters that look well-calibrated with reason]
```

---

## Step 7 — Apply the changes (only if user confirms)

Ask the user: "Apply these changes to config_phase_a_single.yaml?"

If yes, use Edit tool to make the changes. Then bump the version in `edec_bot/version.py` and `edec_bot/config.json` (skip patch numbers ending in 0, per CLAUDE.md).
