# EDEC Bot — Tuning Protocol

> **Instructions for any AI agent.** Execute every numbered step in order. No user input is required between steps 1–6. Do not apply config edits until the user confirms in step 7.

---

## Required capabilities
- Execute shell commands
- Read files
- Edit files

All local commands below assume repo root on this Windows machine and run through `.\scripts\venv_python.cmd` so the repo `.venv` is always used.

---

## STEP 1 — Verify credentials

```powershell
@'
import os, sys
t = os.getenv('EDEC_GITHUB_TOKEN','').strip()
r = os.getenv('EDEC_GITHUB_REPO','').strip()
if not t: sys.exit('STOP: EDEC_GITHUB_TOKEN is not set')
if not r: sys.exit('STOP: EDEC_GITHUB_REPO is not set')
print('OK token=' + t[:4] + '... repo=' + r)
'@ | .\scripts\venv_python.cmd -
```

**If output starts with STOP** → report the exact message to the user and halt.  
Credentials are read from environment variables. They are also accepted from a `.env` file in `edec_bot/` (KEY=VALUE format) or from `/data/options.json` (HA add-on).

---

## STEP 2 — Fetch session export data

```powershell
.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --limit 3
```

**On success:** script prints `Ready for analysis. CSV files are at:` followed by absolute paths.  
**On failure (exit code ≠ 0):** report the full error output to the user and halt.

From the output, record:
- `TRADES_CSV` = path ending in `_session_trades.csv` (most recent folder)
- `SIGNALS_CSV` = path ending in `_session_signals.csv` (same folder)
- `FOLDER_TS` = timestamp portion of the folder name (`YYYY-MM-DD_HHMMSS`)

If multiple folders were fetched, the most recent folder (highest `FOLDER_TS`) is primary.

---

## STEP 3 — Identify active config file

```powershell
.\scripts\venv_python.cmd -c "import os; print(os.getenv('EDEC_CONFIG_PATH','edec_bot/config_phase_a_single.yaml'))"
```

Read the printed path. That file is `CONFIG` for all subsequent steps.

---

## STEP 4 — Run the analysis script

Copy the script below verbatim and execute it, substituting `{TRADES_CSV}` and `{SIGNALS_CSV}` with the paths from STEP 2. The script uses only the Python standard library.

```powershell
@'
import csv, json, sys, statistics
from collections import defaultdict
from pathlib import Path

TRADES_CSV  = "{TRADES_CSV}"
SIGNALS_CSV = "{SIGNALS_CSV}"

# ── helpers ──────────────────────────────────────────────────────────────────

def flt(v, default=None):
    try: return float(v)
    except: return default

def ivl(v, default=None):
    try: return int(float(v))
    except: return default

def pct(wins, total):
    return round(wins / total * 100, 1) if total > 0 else None

def ptile(data, p):
    s = sorted(x for x in data if x is not None)
    if not s: return None
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 4)

def ep_bucket(ep):
    if ep is None: return "null"
    if ep < 0.50: return "<0.50"
    if ep < 0.52: return "0.50-0.52"
    if ep < 0.54: return "0.52-0.54"
    if ep < 0.56: return "0.54-0.56"
    if ep < 0.58: return "0.56-0.58"
    if ep < 0.60: return "0.58-0.60"
    if ep < 0.63: return "0.60-0.63"
    if ep < 0.66: return "0.63-0.66"
    return "0.66+"

def v30_bucket(v):
    if v is None: return "null"
    av = abs(v)
    if av < 0.04: return "<0.04"
    if av < 0.06: return "0.04-0.06"
    if av < 0.08: return "0.06-0.08"
    if av < 0.10: return "0.08-0.10"
    if av < 0.12: return "0.10-0.12"
    if av < 0.15: return "0.12-0.15"
    if av < 0.20: return "0.15-0.20"
    return "0.20+"

def group_stats(rows, key_fn):
    g = defaultdict(lambda: {"n":0,"wins":0,"losses":0,"pnl":[]})
    for r in rows:
        k = key_fn(r)
        g[k]["n"] += 1
        if r["status"] == "closed_win":   g[k]["wins"]   += 1
        if r["status"] == "closed_loss":  g[k]["losses"] += 1
        p = flt(r.get("pnl"))
        if p is not None: g[k]["pnl"].append(p)
    out = {}
    for k, d in g.items():
        out[k] = {
            "n": d["n"],
            "wins": d["wins"],
            "losses": d["losses"],
            "win_pct": pct(d["wins"], d["wins"]+d["losses"]),
            "total_pnl": round(sum(d["pnl"]), 4),
            "avg_pnl": round(statistics.mean(d["pnl"]), 4) if d["pnl"] else None,
        }
    return out

# ── load trades ───────────────────────────────────────────────────────────────

trades = []
with open(TRADES_CSV, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        trades.append(row)

closed = [r for r in trades if r.get("status") in ("closed_win","closed_loss")]
wins   = [r for r in closed if r["status"] == "closed_win"]
losses = [r for r in closed if r["status"] == "closed_loss"]

# ── overall ───────────────────────────────────────────────────────────────────

pnls = [flt(r["pnl"]) for r in closed if flt(r.get("pnl")) is not None]
overall = {
    "total": len(trades),
    "closed": len(closed),
    "wins": len(wins),
    "losses": len(losses),
    "open": len([r for r in trades if r.get("status") == "open"]),
    "win_pct": pct(len(wins), len(closed)),
    "total_pnl": round(sum(pnls), 4),
    "avg_pnl": round(statistics.mean(pnls), 4) if pnls else None,
}

# ── by grouping ───────────────────────────────────────────────────────────────

by_exit   = group_stats(closed, lambda r: r.get("er","null"))
by_coin   = group_stats(closed, lambda r: r.get("c","null"))
by_strat  = group_stats(closed, lambda r: r.get("st","null"))
by_ep     = group_stats(closed, lambda r: ep_bucket(flt(r.get("ep"))))
by_v30    = group_stats(closed, lambda r: v30_bucket(flt(r.get("v30"))))

# ── distributions ─────────────────────────────────────────────────────────────

mae_vals  = [abs(flt(r["mae"])) for r in closed if flt(r.get("mae")) is not None]
mfe_vals  = [flt(r["mfe"])      for r in closed if flt(r.get("mfe")) is not None]
maxb_vals = [flt(r["maxb"])     for r in closed if flt(r.get("maxb")) is not None]
ep_vals   = [flt(r["ep"])       for r in closed if flt(r.get("ep"))  is not None]

mae_as_pct = []
for r in closed:
    m, e = flt(r.get("mae")), flt(r.get("ep"))
    if m is not None and e and e > 0:
        mae_as_pct.append(abs(m) / e)

tx_near_close = [flt(r["tx"]) for r in losses if r.get("er") == "near_close" and flt(r.get("tx")) is not None]

distributions = {
    "mae_usd":         {"p50": ptile(mae_vals,50), "p75": ptile(mae_vals,75), "p90": ptile(mae_vals,90)},
    "mae_pct_of_ep":   {"p50": ptile(mae_as_pct,50), "p75": ptile(mae_as_pct,75), "p90": ptile(mae_as_pct,90)},
    "mfe_usd":         {"p50": ptile(mfe_vals,50), "p75": ptile(mfe_vals,75)},
    "maxb":            {"p25": ptile(maxb_vals,25), "p50": ptile(maxb_vals,50), "p75": ptile(maxb_vals,75)},
    "tx_near_close_losses": {"n": len(tx_near_close), "median": ptile(tx_near_close,50)},
}

sc_rows = [r for r in closed if ivl(r.get("sc")) == 1]
hc_rows = [r for r in closed if ivl(r.get("hc")) == 1]
sx_rows = [r for r in closed if ivl(r.get("sx")) == 1]
exit_flags = {
    "scalp_hit":  {"n": len(sc_rows), "win_pct": pct(sum(1 for r in sc_rows if r["status"]=="closed_win"), len(sc_rows))},
    "hc_hit":     {"n": len(hc_rows), "win_pct": pct(sum(1 for r in hc_rows if r["status"]=="closed_win"), len(hc_rows))},
    "stall_exit": {"n": len(sx_rows), "win_pct": pct(sum(1 for r in sx_rows if r["status"]=="closed_win"), len(sx_rows))},
}

# ── filter analysis (signals CSV) ─────────────────────────────────────────────

filter_fail_counts = defaultdict(int)
total_signals = 0
try:
    with open(SIGNALS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total_signals += 1
            for fname in (row.get("ff") or "").split(","):
                fname = fname.strip()
                if fname:
                    filter_fail_counts[fname] += 1
except Exception as e:
    filter_fail_counts["_error"] = str(e)

filter_analysis = {
    "total_signals": total_signals,
    "fail_counts": {k: {"n": v, "pct_of_signals": round(v/total_signals*100,1) if total_signals else None}
                    for k, v in sorted(filter_fail_counts.items(), key=lambda x: -x[1]) if k != "_error"},
}

# ── depth ratio for losses ────────────────────────────────────────────────────

loss_drt = [flt(r["drt"]) for r in losses if flt(r.get("drt")) is not None]

# ── output ────────────────────────────────────────────────────────────────────

result = {
    "folder_ts": Path(TRADES_CSV).parent.name,
    "overall": overall,
    "by_exit_reason": by_exit,
    "by_coin": by_coin,
    "by_strategy": by_strat,
    "by_ep_bucket": by_ep,
    "by_v30_bucket": by_v30,
    "distributions": distributions,
    "exit_flags": exit_flags,
    "filter_analysis": filter_analysis,
    "loss_depth_ratio": {"median_drt": ptile(loss_drt, 50), "p75_drt": ptile(loss_drt, 75)},
}
print(json.dumps(result, indent=2))
'@ | .\scripts\venv_python.cmd -
```

Save the printed JSON as `ANALYSIS_JSON`. If the script errors, report the traceback to the user and halt.

---

## STEP 5 — Read the config values

From `CONFIG` (identified in STEP 3), extract the current values of these parameters and label them `CURRENT`:

```
single_leg.entry_min
single_leg.entry_max
single_leg.min_velocity_30s
single_leg.loss_cut_pct
single_leg.high_confidence_bid
single_leg.scalp_take_profit_bid
single_leg.scalp_min_profit_usd
single_leg.min_time_remaining_s
single_leg.max_time_remaining_s
single_leg.min_book_depth_usd
single_leg.disabled_coins

lead_lag.min_velocity_30s
lead_lag.min_entry
lead_lag.max_entry
lead_lag.profit_take_delta
lead_lag.profit_take_cap
lead_lag.hard_stop_loss_pct
lead_lag.stall_window_s
lead_lag.min_progress_delta
lead_lag.disabled_coins
```

---

## STEP 6 — Apply decision rules to ANALYSIS_JSON

Work through each rule below. For each, compute `RECOMMENDED`. If `RECOMMENDED ≠ CURRENT`, add a row to the output table. Use `n` (sample size) from `ANALYSIS_JSON` to gate each rule — rules with insufficient data are labelled `NO_DATA`.

### RULE: min_velocity_30s (single_leg and lead_lag)

```
For each bucket in by_v30_bucket where n >= 5:
  find the lowest bucket whose win_pct >= 45
  new_min = lower bound of that bucket (e.g. "0.08-0.10" → 0.08)
  if new_min > CURRENT.min_velocity_30s + 0.01:
    RECOMMENDED = new_min
    REASON = f"Buckets below {new_min} had win_pct={win_pct}% (n={n})"
```

### RULE: entry_min / entry_max (single_leg)

```
For each bucket in by_ep_bucket where n >= 5:
  find the lowest bucket with win_pct >= 45  → new entry_min
  find the highest bucket with win_pct >= 45 → new entry_max
  if new entry_min > CURRENT.entry_min + 0.01: RECOMMENDED entry_min = new entry_min
  if new entry_max < CURRENT.entry_max - 0.01: RECOMMENDED entry_max = new entry_max
```

### RULE: loss_cut_pct (single_leg and lead_lag.hard_stop_loss_pct)

```
mae_p75 = distributions.mae_pct_of_ep.p75
if mae_p75 is not None and n(closed) >= 10:
  calibrated = round(mae_p75 + 0.02, 2)   # p75 + 2% buffer
  if abs(calibrated - CURRENT.loss_cut_pct) >= 0.02:
    RECOMMENDED = calibrated
    REASON = f"MAE p75 = {mae_p75:.1%} of entry price (n={n})"
```

### RULE: high_confidence_bid (single_leg)

```
maxb_p50 = distributions.maxb.p50
if maxb_p50 is not None and n(closed) >= 10:
  if maxb_p50 > CURRENT.high_confidence_bid + 0.04:
    RECOMMENDED = round(CURRENT.high_confidence_bid + 0.02, 2)
    REASON = f"Median max bid {maxb_p50:.3f} is well above HC level {CURRENT.high_confidence_bid}"
  if maxb_p50 < CURRENT.high_confidence_bid - 0.03:
    RECOMMENDED = round(CURRENT.high_confidence_bid - 0.02, 2)
    REASON = f"Median max bid {maxb_p50:.3f} is below HC level — HC rarely triggered"
```

### RULE: disabled_coins

```
For each coin in by_coin where n >= 5 AND win_pct < 40:
  if coin not already in CURRENT.disabled_coins:
    RECOMMENDED = add coin to disabled_coins
    REASON = f"{coin}: win_pct={win_pct}% (n={n})"
```

### RULE: exit reason leaks

```
For each exit_reason in by_exit_reason where n >= 5 AND win_pct < 35:
  flag: "Exit reason '{er}' is losing at {win_pct}% (n={n}) — review exit logic"
  (this is advisory only, no config param maps to it directly)
```

### RULE: filter aggression (from filter_analysis)

```
For each filter in filter_analysis.fail_counts where pct_of_signals > 30:
  flag: "Filter '{filter}' is rejecting {pct}% of signals — verify it is intentional"
```

### RULE: depth ratio on losses

```
loss_drt_p75 = loss_depth_ratio.p75_drt
if loss_drt_p75 is not None and loss_drt_p75 > 2.0 and n(losses) >= 10:
  flag: "Losses have p75 depth ratio {loss_drt_p75:.2f} — consider tightening min_book_depth_usd"
```

---

## STEP 7 — Output recommendations

Print the following. Fill every `{placeholder}` from your computed values.

```
## EDEC Tuning Recommendations — {FOLDER_TS}

### Data
- Trades analysed : {overall.closed} closed ({overall.wins}W / {overall.losses}L)
- Session win rate: {overall.win_pct}%
- Session P&L     : ${overall.total_pnl}
- Config file     : {CONFIG}

### Proposed config changes

| Section | Parameter | Current | Recommended | Evidence |
|---------|-----------|---------|-------------|----------|
[one row per triggered rule; omit if no changes recommended]

### Advisory flags
[one bullet per triggered exit-reason / filter / depth-ratio flag; "None" if none triggered]

### Parameters with no change recommended
[list params that were evaluated and passed; one line each with brief reason]
```

---

## STEP 8 — Apply changes (user confirmation required)

Present the output from STEP 7 to the user and ask:

> "Apply these changes to {CONFIG}? (yes / no / edit first)"

**If yes:**
1. Use file-edit tool to apply every row from the Proposed config changes table to `{CONFIG}`.
2. Bump `edec_bot/version.py` and `edec_bot/config.json` to the next patch version (skip numbers ending in 0; e.g. 5.0.11 → 5.0.12, 5.0.19 → 5.0.21).

**If no or edit first:** make no file changes.

---

## Reference: trades CSV column schema

All columns are strings in the CSV. Cast as noted.

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | trade ID |
| `ts` | str | entry timestamp UTC |
| `c` | str | coin: btc / eth / sol / xrp / bnb / doge / hype |
| `st` | str | strategy: single_leg / lead_lag / dual_leg / swing_leg |
| `sd` | str | side: up / down |
| `ep` | float | entry price (0–1) |
| `tp` | float | target sell price (0–1) |
| `eb` / `ea` | float | entry bid / ask |
| `es` | float | entry spread (ask − bid) |
| `cs` | float | cost USD |
| `fee` | float | fees USD |
| `status` | str | closed_win / closed_loss / open |
| `xp` | float | exit price |
| `xb` | float | bid at exit |
| `er` | str | exit reason (see below) |
| `tx` | float | seconds remaining at exit |
| `pnl` | float | P&L USD (negative = loss) |
| `v30` | float | 30s coin velocity % at signal time |
| `v60` | float | 60s coin velocity % |
| `eds` | float | entry-side book depth USD |
| `ods` | float | opposite-side book depth USD |
| `drt` | float | depth ratio (eds / ods) |
| `maxb` | float | highest bid seen during trade |
| `minb` | float | lowest bid seen during trade |
| `mfe` | float | max favourable excursion USD |
| `mae` | float | max adverse excursion USD (negative value) |
| `tfp` | float | seconds until trade first went positive |
| `sc` | 0/1 | scalp_take_profit_bid was reached |
| `hc` | 0/1 | high_confidence_bid was reached |
| `sx` | 0/1 | stall exit triggered |
| `fp` | str | comma-separated list of passed filters |
| `ff` | str | comma-separated list of failed filters |
| `te` | float | seconds remaining at entry |
| `sg` | float | composite signal score |

**Exit reason values:**
- `profit_target` — bid hit target_price
- `scalp` — bid hit scalp_take_profit_bid
- `high_confidence` — bid hit high_confidence_bid early
- `loss_cut` — bid dropped below entry × (1 − loss_cut_pct)
- `stall_exit` — bid stalled with no progress in stall window
- `near_close` — forced exit near market expiry
- `resolution` — market resolved at $1.00 (win) or $0.00 (loss)
- `dead_leg` — one arb leg collapsed (dual_leg only)

## Reference: signals CSV column schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | decision ID |
| `ts` | str | timestamp UTC |
| `c` | str | coin |
| `st` | str | strategy |
| `act` | str | DRY_RUN_SIGNAL / TRADE / SUPPRESSED |
| `sup` | str | suppression reason (if act=SUPPRESSED) |
| `ep` | float | entry price |
| `v30` / `v60` | float | velocity at signal time |
| `te` | float | market time remaining at signal |
| `eds` / `ods` | float | book depth |
| `sg` | float | signal score |
| `fp` | str | comma-separated passed filters |
| `ff` | str | comma-separated failed filters |
| `why` | str | decision reason string |
