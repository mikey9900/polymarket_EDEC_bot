# EDEC Bot — Claude Tuning Session

## On reading this file

**In your very first response**, do all three of the following in one message (parallel tool calls):
1. `TodoWrite` — create the six tasks listed at the bottom of this section
2. `Bash` — run `.\scripts\venv_python.cmd edec_bot/fetch_github_data.py --limit 3`
3. `Bash` — run `.\scripts\venv_python.cmd -c "import os; print(os.getenv('EDEC_CONFIG_PATH','edec_bot/config_phase_a_single.yaml'))"`

Then work through each task in order, marking it done immediately when complete.

**TodoWrite tasks:**
- [ ] Fetch data + identify config
- [ ] Read config + run metrics script
- [ ] Cross-session trend (if multiple folders)
- [ ] Apply decision rules
- [ ] Present recommendations
- [ ] Apply changes (on user confirmation)

If the fetch script exits with an error containing `EDEC_GITHUB_TOKEN` or `EDEC_GITHUB_REPO`, stop and tell the user which variable is missing.

All local commands below assume repo root on this Windows machine and should run through `.\scripts\venv_python.cmd`.

---

## TASK 1 — Fetch data + identify config

From the fetch script output:
- `TRADES_CSV` = most recent `*_session_trades.csv` path printed
- `SIGNALS_CSV` = most recent `*_session_signals.csv` path printed
- `FOLDER_TS` = timestamp in the folder name (`YYYY-MM-DD_HHMMSS`)
- `ALL_FOLDERS` = all fetched folder paths (for trend analysis)

From the config path output, `Read` that file. That is `CONFIG`.

---

## TASK 2 — Read config values + run metrics script

**In one message**, do both:
1. From `CONFIG`, extract and record `CURRENT` values for every parameter listed in the Decision Rules section
2. Run the metrics script below, substituting the real paths

```powershell
@'
import csv, json, sys, statistics
from collections import defaultdict
from pathlib import Path

TRADES_CSV  = "SUBSTITUTE_TRADES_CSV"
SIGNALS_CSV = "SUBSTITUTE_SIGNALS_CSV"

def flt(v):
    try: return float(v)
    except: return None

def ivl(v):
    try: return int(float(v))
    except: return None

def pct(w, t): return round(w/t*100, 1) if t > 0 else None

def ptile(data, p):
    s = sorted(x for x in data if x is not None)
    if not s: return None
    k = (len(s)-1)*p/100; lo, hi = int(k), min(int(k)+1, len(s)-1)
    return round(s[lo] + (s[hi]-s[lo])*(k-lo), 4)

def grp(rows, keyfn):
    g = defaultdict(lambda: {"n":0,"w":0,"l":0,"pnl":[]})
    for r in rows:
        k = keyfn(r); g[k]["n"] += 1
        if r.get("status")=="closed_win":  g[k]["w"] += 1
        if r.get("status")=="closed_loss": g[k]["l"] += 1
        p = flt(r.get("pnl"))
        if p is not None: g[k]["pnl"].append(p)
    return {k: {"n":d["n"],"wins":d["w"],"losses":d["l"],
                "win_pct":pct(d["w"],d["w"]+d["l"]),
                "total_pnl":round(sum(d["pnl"]),4)} for k,d in g.items()}

def ep_b(v):
    if v is None: return "null"
    b = [.50,.52,.54,.56,.58,.60,.63,.66]
    for i,hi in enumerate(b):
        if v < hi: return f"{'<.50' if i==0 else f'{b[i-1]:.2f}-{hi:.2f}'}"
    return ".66+"

def v30_b(v):
    if v is None: return "null"
    av = abs(v); b = [.04,.06,.08,.10,.12,.15,.20]
    for i,hi in enumerate(b):
        if av < hi: return f"{'<.04' if i==0 else f'{b[i-1]:.2f}-{hi:.2f}'}"
    return ".20+"

trades = list(csv.DictReader(open(TRADES_CSV, encoding="utf-8")))
closed = [r for r in trades if r.get("status") in ("closed_win","closed_loss")]
wins   = [r for r in closed if r["status"]=="closed_win"]
losses = [r for r in closed if r["status"]=="closed_loss"]
pnls   = [flt(r["pnl"]) for r in closed if flt(r.get("pnl")) is not None]

mae_pct = [abs(flt(r["mae"]))/flt(r["ep"]) for r in closed
           if flt(r.get("mae")) is not None and flt(r.get("ep"))]
maxb    = [flt(r["maxb"]) for r in closed if flt(r.get("maxb")) is not None]
loss_drt= [flt(r["drt"])  for r in losses if flt(r.get("drt"))  is not None]
tx_nc   = [flt(r["tx"])   for r in losses if r.get("er")=="near_close" and flt(r.get("tx")) is not None]

ff_counts = defaultdict(int); total_sig = 0
for row in csv.DictReader(open(SIGNALS_CSV, encoding="utf-8")):
    total_sig += 1
    for f in (row.get("ff") or "").split(","):
        f = f.strip()
        if f: ff_counts[f] += 1

sc_r = [r for r in closed if ivl(r.get("sc"))==1]
hc_r = [r for r in closed if ivl(r.get("hc"))==1]

result = {
    "folder_ts": Path(TRADES_CSV).parent.name,
    "overall": {
        "total":len(trades), "closed":len(closed),
        "wins":len(wins), "losses":len(losses),
        "open":len(trades)-len(closed),
        "win_pct":pct(len(wins),len(closed)),
        "total_pnl":round(sum(pnls),4) if pnls else 0,
        "avg_pnl":round(statistics.mean(pnls),4) if pnls else None,
    },
    "by_exit":     grp(closed, lambda r: r.get("er","null")),
    "by_coin":     grp(closed, lambda r: r.get("c","null")),
    "by_strategy": grp(closed, lambda r: r.get("st","null")),
    "by_ep":       grp(closed, lambda r: ep_b(flt(r.get("ep")))),
    "by_v30":      grp(closed, lambda r: v30_b(flt(r.get("v30")))),
    "dist": {
        "mae_pct_ep": {"p50":ptile(mae_pct,50),"p75":ptile(mae_pct,75),"p90":ptile(mae_pct,90)},
        "maxb":       {"p25":ptile(maxb,25),"p50":ptile(maxb,50),"p75":ptile(maxb,75)},
        "loss_drt":   {"p50":ptile(loss_drt,50),"p75":ptile(loss_drt,75)},
        "tx_near_close_losses": {"n":len(tx_nc),"median":ptile(tx_nc,50)},
    },
    "exit_flags": {
        "scalp_hit": {"n":len(sc_r),"win_pct":pct(sum(1 for r in sc_r if r["status"]=="closed_win"),len(sc_r))},
        "hc_hit":    {"n":len(hc_r),"win_pct":pct(sum(1 for r in hc_r if r["status"]=="closed_win"),len(hc_r))},
    },
    "filters": {
        "total_signals": total_sig,
        "top_failures":  sorted(
            [{"filter":k,"n":v,"pct":round(v/total_sig*100,1) if total_sig else None}
             for k,v in ff_counts.items()],
            key=lambda x: -x["n"]
        )[:10],
    },
}
print(json.dumps(result, indent=2))
'@ | .\scripts\venv_python.cmd -
```

This is `METRICS`. If it errors, show the traceback and halt.

---

## TASK 3 — Cross-session trend

Only if `ALL_FOLDERS` contains more than one folder:

Run the same metrics script for each older folder (substitute paths), capture only `overall` from each. Then report:

```
Session trend (oldest → newest):
  YYYY-MM-DD_HHMMSS : win_pct=X% | total_pnl=$Y | n=Z
  ...
Trajectory: [improving / declining / flat] — based on win_pct direction
```

If win_pct is declining across sessions, flag it prominently in the final output.

---

## TASK 4 — Apply decision rules

Use `METRICS` JSON and `CURRENT` config values. Assign a confidence level to each triggered rule based on sample size:

| n (closed trades) | Confidence |
|-------------------|------------|
| < 15 | LOW — treat as directional signal only |
| 15–49 | MEDIUM |
| ≥ 50 | HIGH |

Apply every rule below. If a rule's required group has n < 5, mark it `NO_DATA`.

---

### RULE — min_velocity_30s (single_leg + lead_lag)

```
For each v30 bucket in by_v30 (ascending order) where n >= 5:
  find the lowest bucket where win_pct >= 45
  new_min = lower bound of that bucket
  if new_min > CURRENT.min_velocity_30s + 0.01 → RECOMMEND new_min
  if new_min < CURRENT.min_velocity_30s - 0.01 → RECOMMEND new_min
```

### RULE — entry_min / entry_max (single_leg) + min_entry / max_entry (lead_lag)

```
For ep buckets with n >= 5:
  find lowest bucket with win_pct >= 45 → new entry_min
  find highest bucket with win_pct >= 45 → new entry_max
  recommend if delta from current >= 0.02
```

### RULE — loss_cut_pct / hard_stop_loss_pct

```
calibrated = round(dist.mae_pct_ep.p75 + 0.02, 2)
if |calibrated - CURRENT.loss_cut_pct| >= 0.02 → RECOMMEND calibrated
same comparison for lead_lag.hard_stop_loss_pct
```

### RULE — high_confidence_bid

```
if dist.maxb.p50 > CURRENT.high_confidence_bid + 0.04 → RECOMMEND current + 0.02 (bid is routinely exceeded — lower it)
if dist.maxb.p50 < CURRENT.high_confidence_bid - 0.03 → RECOMMEND current - 0.02 (HC almost never hit — lower it to match reality)
```

### RULE — disabled_coins

```
For each coin in by_coin where n >= 5 AND win_pct < 40:
  if not already in CURRENT.disabled_coins → RECOMMEND adding it
```

### RULE — exit reason flags (advisory, no config param)

```
For each exit reason in by_exit where n >= 5 AND win_pct < 35:
  flag: "{er} losing at {win_pct}% (n={n})"
```

### RULE — filter aggression (advisory)

```
For each filter in filters.top_failures where pct > 30:
  flag: "{filter} rejecting {pct}% of signals"
```

### RULE — depth ratio (advisory)

```
if dist.loss_drt.p75 > 2.0 AND len(losses) >= 10:
  flag: "Loss trades have p75 depth ratio {p75:.2f} — books were unbalanced at entry"
```

---

## TASK 5 — Present recommendations

Output in this order:

**1. Narrative** (3–5 sentences): what the data shows, the dominant pattern, any trajectory concern from trend analysis, overall confidence level.

**2. Proposed changes table:**

| Section | Parameter | Current | Recommended | Confidence | Evidence |
|---------|-----------|---------|-------------|------------|----------|

**3. Advisory flags** (bullets): exit reason leaks, filter aggression, depth ratio, trend warning. Write "None" if nothing triggered.

**4. Parameters checked, no change needed** (one line each).

Then ask: `"Apply these changes to {CONFIG}? (yes / no / edit first)"`

---

## TASK 6 — Apply changes

Only on explicit user confirmation.

For each row in the table:
1. Use the `Edit` tool on `CONFIG` — match the exact current YAML value as `old_string`, replace with the new value as `new_string`. Edit one parameter at a time.
2. After all edits, bump `edec_bot/version.py` and `edec_bot/config.json` to the next patch version (skip numbers ending in 0, per CLAUDE.md versioning rules).

Do not commit or push unless the user asks.

---

## Column reference

**Trades CSV** — key columns:

| col | type | meaning |
|-----|------|---------|
| `c` | str | coin |
| `st` | str | strategy: single_leg / lead_lag / dual_leg / swing_leg |
| `sd` | str | side: up / down |
| `ep` | float | entry price 0–1 |
| `status` | str | closed_win / closed_loss / open |
| `er` | str | exit reason (see below) |
| `pnl` | float | P&L USD |
| `v30` | float | 30s coin velocity % |
| `mae` | float | max adverse excursion USD |
| `mfe` | float | max favourable excursion USD |
| `maxb` | float | highest bid seen during trade |
| `drt` | float | book depth ratio (entry side / opposite) |
| `tx` | float | seconds remaining at exit |
| `sc` | 0/1 | scalp_take_profit_bid was reached |
| `hc` | 0/1 | high_confidence_bid was reached |
| `sx` | 0/1 | stall exit triggered |
| `fp` / `ff` | str | comma-separated passed / failed filters |

**Exit reasons:** `profit_target` · `scalp` · `high_confidence` · `loss_cut` · `stall_exit` · `near_close` · `resolution` · `dead_leg`

**Signals CSV** — key columns: `c` `st` `act` `ep` `v30` `te` `sg` `fp` `ff` `why`
(`act` values: `DRY_RUN_SIGNAL` · `TRADE` · `SUPPRESSED`)
