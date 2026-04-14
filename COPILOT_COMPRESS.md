# Copilot: Compress Last 25 Trades to CSV

Paste this entire prompt into Copilot, then immediately attach or paste the spreadsheet data. Copilot will output the CSV with zero clarifying questions.

---

## PROMPT (copy everything below this line)

I have a spreadsheet of the last 25 trades from a Polymarket trading bot. Convert it to a compressed CSV using the exact rules below. Do not ask any clarifying questions — all decisions are made for you here.

---

### COLUMN RULES

Use these columns in this exact order:

```
id, status, date, time, coin, strategy, side, entry, target, shares, cost,
time_remaining_s, vel_30s, vel_60s, filters_passed, filters_failed,
entry_reason, exit_reason, exit_price, bid_at_exit, hold_s, pnl, pnl_pct, fees
```

**Mapping from spreadsheet headers:**
- `id` ← ID
- `status` ← Status
- `date` ← Date
- `time` ← Time
- `coin` ← Coin
- `strategy` ← Strategy
- `side` ← Side
- `entry` ← Entry $
- `target` ← Target $
- `shares` ← Shares
- `cost` ← Cost $
- `time_remaining_s` ← **Time @ Entry (s) only** — ignore "Time @ Exit (s)"
- `vel_30s` ← Vel 30s %
- `vel_60s` ← Vel 60s %
- `filters_passed` ← Filters Passed
- `filters_failed` ← Filters Failed
- `entry_reason` ← Entry Reason
- `exit_reason` ← Exit Reason
- `exit_price` ← Exit $
- `bid_at_exit` ← Bid @ Exit $
- `hold_s` ← Hold (s)
- `pnl` ← P&L $
- `pnl_pct` ← P&L %
- `fees` ← Fees $

**Columns to ignore entirely:**
- Depth UP $
- Depth DOWN $
- Time @ Exit (s)

---

### OPEN TRADE RULES

Trades with status = "open" are included. For open trades:
- `exit_reason`, `exit_price`, `bid_at_exit`, `hold_s`, `pnl`, `pnl_pct`, `fees` → leave blank (empty string, not "N/A" or "0")
- All entry fields fill normally

---

### FORMATTING RULES

- Numbers: round to 4 decimal places max, strip trailing zeros (0.5000 → 0.5)
- Percentages: keep as decimal (e.g. -96.96, not -0.9696)
- Filters Passed / Filters Failed: keep as-is (comma-separated list inside quotes if needed)
- Empty cells in source → empty string in CSV
- Dates: keep as-is (YYYY-MM-DD)
- Times: keep as-is (HH:MM:SS)
- No currency symbols — numbers only
- Wrap any field containing commas in double quotes

---

### OUTPUT FORMAT

1. **First: the raw CSV** — header row + one row per trade, nothing else before it
2. **Then: a pattern summary** after the CSV using this exact structure:

```
=== PATTERN SUMMARY ===
Trades: [total] | Closed: [n] | Open: [n]
Win rate: [x]% ([wins]W / [losses]L)
Total P&L: $[x] | Avg win: $[x] | Avg loss: $[x]

By strategy:
  single_leg:  [n] trades, [n]W/[n]L, avg pnl $[x]
  swing_leg:   [n] trades, [n]W/[n]L, avg pnl $[x]
  lead_lag:    [n] trades, [n]W/[n]L, avg pnl $[x]
  dual_leg:    [n] trades, [n]W/[n]L, avg pnl $[x]

By exit reason:
  profit_target: [n] trades, avg pnl $[x]
  loss_cut:      [n] trades, avg pnl $[x]
  near_close:    [n] trades, avg pnl $[x]
  resolution:    [n] trades, avg pnl $[x]
  dead_leg:      [n] trades, avg pnl $[x]

Loss patterns (top observations):
  L1: [observation]
  L2: [observation]
  L3: [observation]

Win patterns:
  W1: [observation]
  W2: [observation]
```

---

Output everything in one message. Start the CSV immediately on the first line — no preamble.
