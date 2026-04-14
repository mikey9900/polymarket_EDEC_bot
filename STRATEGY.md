# EDEC Bot — Strategy & Logic Summary
> Paste this file + a compressed trade CSV into any AI for analysis.
> Current version: 3.2.24

---

## What the bot does

Trades **Polymarket 5-minute binary markets** on 7 coins: BTC, ETH, SOL, XRP, BNB, DOGE, HYPE.

Each market asks: *"Will [coin] be higher or lower in 5 minutes?"*
- UP token pays $1 if price goes up, $0 if it goes down
- DOWN token pays $1 if price goes down, $0 if it goes up
- Tokens trade between $0.01–$0.99 reflecting live probability

The bot runs **4 strategies** simultaneously in dry-run (paper money) mode.

---

## Strategy 1: Single-Leg Momentum

**Idea:** One side is deeply discounted (≤0.32 ask) while the other is expensive (≥0.62 ask).
This confirms a real directional move. Buy the cheap side; sell when it reprices upward.

**Entry filters (ALL must pass):**
| Filter | Rule | Current value |
|---|---|---|
| market_active | Market accepting orders | — |
| time_remaining | Enough time left | >90s |
| entry_window | Not too early (direction not yet established) | ≤200s |
| books_available | Both UP and DOWN books exist | — |
| coin_velocity | Coin actually moving (30s) | ≥0.08% |
| entry_threshold | One side cheap, other side expensive | ask ≤0.32, opposite ≥0.62 |
| entry_floor | Entry ask not already near zero | ≥0.15 |
| vel_divergence | 60s trend not opposing trade direction | 60s within 0.03% of 30s direction |
| liquidity_depth | Enough USD at entry price | ≥$5 |
| feed_count | At least 2 price feeds live | ≥2 |
| risk_limits | Daily loss / position / hourly limits OK | — |

**Exit logic (priority order, checked every ~1s):**
1. **High-confidence** — bid ≥0.82 → cancel sell order, hold to $1 resolution
2. **Net profit** — any fee-adjusted positive P&L → sell now
3. **Progressive loss cut** — loss ≥ `loss_cut_pct × (remaining/time_pressure_s)` → exit
   - At 90s+: full 25% loss cut
   - At 45s: 12.5% loss cut (tighter because less time to recover)
   - At 0s: 0% (forces exit)
4. **Near-close** — ≤30s remaining → exit regardless of P&L
5. **Resolution** — market ends → outcome tracker settles at $0 or $1

**Key parameters:**
```
entry_max:          0.32    target_sell:        0.52
opposite_min:       0.62    order_size_usd:     $3
loss_cut_pct:       0.25    time_pressure_s:    90s
high_confidence:    0.82    min_velocity_30s:   0.08%
max_time_remain:    200s    max_vel_divergence: 0.03%
entry_min:          0.15    min_time_remain:    90s
```

---

## Strategy 2: Swing Leg

**Idea:** Buy one side when it's cheap (≤0.33), wait for the OTHER side to also dip cheap.
If both sides fill below $0.50 combined, it's guaranteed profit regardless of outcome.
If second leg never comes, sell first leg at a small profit (or loss-cut if it moves against us).
BTC is excluded — its momentum profile is hostile to mean-reversion swing setups.

**Entry filters (ALL must pass):**
| Filter | Rule | Current value |
|---|---|---|
| market_active | Market accepting orders | — |
| time_remaining | Enough runway to wait for second leg | >140s |
| entry_window | Not too early | ≤200s |
| coin_allowed | Coin not in disabled list | BTC excluded |
| books_available | Both books exist | — |
| first_leg_price | At least one side cheap enough | ask ≤0.33 |
| first_leg_floor | First leg not already near zero | ≥0.25 |
| directional_neutrality | Velocity opposes entry side (mean-reversion setup, not momentum fade) | UP: vel_30s ≤0, DOWN: vel_30s ≥0 |
| not_already_arb | Combined cost still above arb threshold | combined >0.99 |
| coin_velocity | Coin NOT trending too hard (want mean reversion) | 30s ≤0.12% |
| vel_divergence | 60s trend not strongly opposing 30s | 60s within 0.03% of 30s direction |
| liquidity_symmetry | Books not heavily one-sided (second leg needs to fill) | ratio ≤2.5× |
| liquidity_depth | Enough USD at entry price | ≥$5 |
| feed_count | At least 2 feeds live | ≥2 |
| risk_limits | Risk limits OK | — |

**Exit logic — Phase 1 (first leg only):**
1. **Second leg dips** → buy it (arb complete, hold both to resolution)
2. **High-confidence bid** ≥0.82 → hold first leg to resolution
3. **Progressive loss cut** same formula as single_leg (0.25 × time_factor)
4. **Target hit** → any net-positive exit after fees → sell
5. **Near-close** ≤30s → exit regardless

**Exit logic — Phase 2 (both legs held):**
- If either leg's bid drops below 0.05 ("dead leg") → sell that leg for whatever it's worth, hold the other to resolution

**Key parameters:**
```
first_leg_max:      0.33    second_leg_max:     0.45
first_leg_exit:     0.55    first_leg_min:      0.25
order_size_usd:     $3      loss_cut_pct:       0.25
high_confidence:    0.82    time_pressure_s:    90s
max_time_remain:    200s    dead_leg_threshold: 0.05
max_velocity_30s:   0.12    min_time_remain:    140s
max_vel_divergence: 0.03    max_depth_ratio:    2.5×
disabled_coins:     [btc]
```

---

## Strategy 3: Lead-Lag

**Idea:** Coin moves fast on Binance/Coinbase but Polymarket books haven't repriced yet (5-30s lag).
Buy the WINNING side while it's still cheap; sell when Polymarket catches up.

**Entry filters:**
| Filter | Rule | Current value |
|---|---|---|
| market_active | — | — |
| time_remaining | >60s | >60s |
| books_available | — | — |
| coin_velocity | Fast move required | ≥0.12% in 30s |
| entry_range | Winning side partially repriced but still cheap | 0.48–0.62 ask |
| liquidity_depth | ≥$5 | ≥$5 |
| feed_count | ≥2 feeds | ≥2 |
| risk_limits | — | — |

**Exit:** Single sell order at target_sell (0.67). No progressive loss cut — held to fill or resolution.

**Key parameters:**
```
min_velocity_30s:   0.12%   min_entry:          0.48
max_entry:          0.62    target_sell:        0.67
order_size_usd:     $3      min_time_remain:    60s
```

---

## Strategy 4: Dual-Leg Arb

**Idea:** Both sides are mispriced simultaneously — combined ask < $1.00.
Buy both UP and DOWN; guaranteed profit regardless of outcome (true arb).

**Entry filters:**
| Filter | Rule | Current value |
|---|---|---|
| market_active | — | — |
| time_remaining | >60s | >60s |
| books_available | — | — |
| combined_cost | UP ask + DOWN ask below threshold | ≤0.99 |
| per_side_price | Each side individually cheap | ≤0.45 each |
| edge_after_fees | Net profit after both-side fees | ≥0.05 |
| velocity_check | Market not trending hard against arb | 30s ≤0.15%, 60s ≤0.25% |
| liquidity_depth | ≥$10 each side | ≥$10 |
| risk_limits | — | — |

**Exit:** Holds both positions to market resolution (one always pays $1).

**Key parameters:**
```
max_combined_cost:  0.99    price_threshold:    0.45
min_edge_after_fees:0.05    order_size_usd:     $3
max_velocity_30s:   0.15%   max_velocity_60s:   0.25%
```

---

## Risk Management

| Limit | Value |
|---|---|
| Max daily loss (kill switch) | $20 |
| Max open positions | 5 |
| Max trades per hour | 30 |
| Session profit target | disabled (0) |

Kill switch: auto-activates if daily P&L hits -$20. Deactivated manually via Telegram Start button or Reset Stats (which also clears the in-memory P&L counter).

---

## Exit Reason Codes (in trade data)

| Code | Meaning |
|---|---|
| `profit_target` | Sold at target price (fee-adjusted positive P&L) |
| `loss_cut` | Progressive stop-loss triggered |
| `high_confidence` | Bid surged ≥0.82, held to $1 resolution |
| `near_close` | Forced exit at ≤30s remaining |
| `dead_leg` | One side of swing position collapsed (bid <0.05) |
| `resolution` | Market ended, settled at $0 or $1 by outcome |
| `manual` | Manually closed via Telegram |

---

## Filter Change History (recent)

| Version | Change | Reason |
|---|---|---|
| 3.2.16 | Added `entry_window` (max 200s) | Entries >200s reversed before exit |
| 3.2.16 | Added `vel_divergence` (max 0.03%) | 60s trend opposing 30s = counter-trend entry |
| 3.2.16 | `loss_cut_pct` 0.40 → 0.25 | 40% cut too wide; bids gap through it |
| 3.2.18 | Added `entry_floor` 0.15 (single), 0.20 (swing) | Sub-floor bids collapse instantly, unrecoverable |
| 3.2.22 | `entry_min` 0.15 → 0.12 | Paper run — testing if 0.12-0.15 range is viable |
| 3.2.22 | `order_size_usd` $2 → $3 | Larger size makes P&L signal cleaner in analysis |
| 3.2.23 | `entry_min` 0.12 → 0.15 | 0.12 test confirmed: -97% on BTC, unrecoverable |
| 3.2.23 | `swing first_leg_max` 0.40 → 0.33 | Sim showed 0.30–0.33 blocks 82% of swing losses |
| 3.2.23 | `swing first_leg_min` 0.20 → 0.25 | 0.20–0.24 range is bid-evaporation zone |
| 3.2.23 | `swing max_velocity_30s` 0.20% → 0.12% | Chop regime kills swing; need lower threshold |
| 3.2.23 | `swing min_time_remaining` 120s → 140s | More runway for second leg to fill |
| 3.2.23 | `swing first_leg_exit` 0.52 → 0.55 | Avg win was only $0.43; raise target to improve win size |
| 3.2.23 | Added `directional_neutrality` filter to swing | Buying UP while coin moves UP = fading momentum, not swing |
| 3.2.23 | Added `vel_divergence` filter to swing | 60s opposing 30s = trending market, wrong regime for swing |
| 3.2.23 | Added `liquidity_symmetry` filter to swing | Asymmetric books → second leg structurally won't fill |
| 3.2.23 | Added `disabled_coins: [btc]` to swing | BTC sim confirmed worst performer; momentum profile hostile |
| 3.2.23 | `lead_lag min_velocity_30s` 0.15% → 0.12% | Zero fires in 25 trades; loosen to get signal |
| 3.2.23 | `lead_lag max_entry` 0.60 → 0.62 | Widen lag window slightly |

---

## Suggested prompts for AI analysis

**Pattern analysis:**
> "Given the strategy rules above and this trade CSV, identify which filters are blocking the most profitable trades, which are failing to block losing trades, and what parameter changes would improve EV."

**Parameter tuning:**
> "Focus on single_leg trades only. For each loss, identify which filter(s) *should* have blocked it but didn't. For each win, confirm no filter was close to blocking it. Suggest threshold changes."

**Exit analysis:**
> "Analyse the exit_reason distribution. For loss_cut exits, what was the average time_remaining_s? Were these entries that just needed more time, or were they fundamentally bad entries?"
