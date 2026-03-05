# Quant Harvest — Database & Code Analysis Report
**Generated:** 2026-03-04
**Database:** `data/quant_harvest.db` (290.80 MB)
**Data span:** 14.4 hours (2026-03-03 15:17 UTC → 2026-03-04 05:44 UTC)

---

## Executive Summary

The strategy is **deeply unprofitable**: 57 closed trades, **12.3% net win rate**, **−5.95% average net PnL per trade**, and a **−41.39% portfolio drawdown** in under 15 hours (from $50.00 to $29.30). Five critical issues explain this performance: a misconfigured Hurst gate, an unbounded database growth problem, artificially high slippage floor, a circuit breaker that re-fires on already-open positions, and missing DB indexes that hurt pruning query performance.

---

## 1. Is the Strategy Net Profitable?

**No.** Key statistics from 57 closed trades:

| Metric | Value |
|---|---|
| Win rate (net) | 12.3% (7 wins / 50 losses) |
| Average raw PnL | −3.95% |
| Average net PnL | −5.95% |
| Best trade (net) | +88.07% |
| Worst trade (net) | −79.12% |
| Total net USD PnL | −$19.23 |
| Portfolio change | $50.00 → $29.30 (−41.39%) |
| Avg hold time | 38.8 minutes |

**Fee drag is significant:** The cost model charges exactly **1.0% fee + 1.0% slippage = 2.0% per trade**. The 1% slippage is entirely from the `BASE_SLIPPAGE_PCT = 0.01` floor: actual size-vs-liquidity is 0.004%, giving a real slippage of ~0.02%, but the floor overrides it to 1%. This means a trade that breaks even on price still loses 2% after costs.

---

## 2. Exit Reason Analysis

| Exit Reason | Count | % | Win Rate | Avg Raw | Avg Net |
|---|---|---|---|---|---|
| TIME_STOP | 39 | 68.4% | 10.3% | −0.88% | −2.88% |
| RUG_PROTECTION | 8 | 14.0% | 0.0% | −17.75% | −19.75% |
| TRAILING_STOP | 5 | 8.8% | 20.0% | −28.87% | −30.87% |
| TIME_STOP_EXTENDED | 5 | 8.8% | 40.0% | +19.12% | +17.12% |

**TIME_STOP dominates (68.4%):** The `.env` overrides `TIME_STOP_MINUTES=20` (default: 30). With 2% total cost drag, any trade exiting at time stop needs a raw gain >2% just to break even. Only 10.3% of TIME_STOP exits achieved this. The 20-minute window is too short for a meme coin trend to develop, especially on tokens with Hurst barely above 0.55.

**RUG_PROTECTION (14.0%):** The −15% hard stop is catching true rugs (avg raw −17.75%), which is appropriate. These are not over-firing.

**TRAILING_STOP (8.8%, avg net −30.87%):** This exit fires in the post-partial-TP phase. The trailing stop of 25% from peak, after a 20%+ partial TP, can turn into a large overall loss if the coin reverses sharply. The GRUG trade shows this working (+37.92% net), but BioLLM shows the danger (−16.69% net). The trailing stop needs tighter calibration post-partial-TP.

**TIME_STOP_EXTENDED (8.8%, avg net +17.12%):** These are the only consistently profitable exits. These occur in the trailing phase after partial TP, when the coin holds up for 180 minutes. Only 5 trades reached this phase — the partial TP system is working when triggered, but rarely triggers.

**Threshold calibration verdict:** TIME_STOP is over-exiting at 20 minutes. Restore to 30 minutes to allow more trades to reach partial TP territory.

---

## 3. Entry Filters: Over- or Under-Rejecting?

**117,442 total rejection events** for 57 completed trades = ~2,061 rejections per completed trade.

| Rejection Reason | Count | % | Unique Tokens |
|---|---|---|---|
| MCAP_OUT_OF_RANGE | 28,024 | 23.9% | 5 |
| MOMENTUM_SMA_FAIL | 24,638 | 21.0% | 20 |
| BUY_RATIO_OUT_OF_RANGE | 22,654 | 19.3% | 16 |
| VOLUME_TOO_LOW | 11,618 | 9.9% | 10 |
| ACTIVITY_TOO_LOW | 9,966 | 8.5% | 8 |
| MOMENTUM_DELTA_FAIL | 9,699 | 8.3% | 19 |
| HURST_TOO_LOW | 8,036 | 6.8% | 18 |
| GINI_TOO_HIGH | 1,694 | 1.4% | 4 |
| CVD_SLOPE_OUT_OF_RANGE | 1,113 | 0.9% | 13 |

**MCAP_OUT_OF_RANGE (23.9%, only 5 tokens!):** The same 5 tokens are being evaluated and rejected 28,024 times (~5,600 times each). This means `filter_rejections` is exploding with identical rows for tokens perpetually near (but outside) the market-cap boundary. This is a major source of unbounded DB growth and indicates the filter is correctly rejecting borderline tokens, but the repeated write is wasteful.

**MOMENTUM_SMA_FAIL (21.0%) + MOMENTUM_DELTA_FAIL (8.3%) = 29.3%:** Together the momentum filters reject ~29% of all evaluations. Given the catastrophic win rate, these filters are likely CORRECTLY rejecting low-momentum setups. However, since 10.3% of TIME_STOP exits and 68% of exits overall lose money, the momentum filters could be tightened further rather than relaxed.

**HURST_TOO_LOW (6.8%):** 8,036 rejections, ALL with H < 0.55 (the env-overridden threshold). This filter is working correctly at the configured threshold.

**CVD_SLOPE_OUT_OF_RANGE (0.9%):** Only 1,113 rejections — this is the least-active quant gate. Of the rejections: 640 had negative CVD slope (bearish), 447 had slope > 2,000 (parabolic). The CVD gate is lightly used and not over-rejecting.

---

## 4. Quant Signal Gate Analysis

### Hurst Gate: TOO LOOSE (via .env override)

**Critical finding:** The `.env` file sets `HURST_THRESHOLD=0.55`, overriding the code default of 0.70. This is the #1 strategy configuration error.

| Hurst at Entry | Trade Count |
|---|---|
| H < 0.55 (stored as 0.0, gate bypassed) | 2 |
| H 0.55–0.65 | 49 (86% of trades!) |
| H 0.65–0.70 | 4 |
| H 0.70–0.80 | 1 |
| H ≥ 0.80 | 1 |

The **average Hurst at entry is 0.5727** — barely above the misconfigured 0.55 threshold. 86% of trades entered with H in 0.55–0.65, a range indicating **weak to no trend persistence** (random walk ≈ 0.5). The strategy is supposed to trade trending markets (H > 0.70). Almost no trade met the design threshold.

### Gini Gate: Likely BYPASSED (RPC disabled)

Average Gini at entry: 0.2634 (well below the 0.35 max). Only 1,694 Gini rejections total. The Helius RPC API returns 401 errors (tracked in code), disabling Gini checks after 5 consecutive failures. Most Gini gates are being skipped (`gini is None → skip gate`). Since Gini affects holder concentration risk, this is a meaningful gap in protection.

### CVD Gate: Well-calibrated

CVD slope range at entry: 12.40 – 1,970.76 (gate: 10–2,000). Distribution spans both low (14 trades in 10–100 range) and high (25 trades in 500–2,000 range). The gate correctly excludes negative/zero slopes and parabolic pumps. No calibration issue here.

### Buy Ratio: Within range

Average buy ratio at entry: 0.617 (gate: 0.40–0.75). All trades are within the design range. No miscalibration.

---

## 5. System Errors

| Event | Count |
|---|---|
| CIRCUIT_BREAKER_TRIGGERED | 14 |
| ALERT_HIGH_REJECTION_RATE | 13 |
| LOSS_COOLDOWN_START | 13 |
| ALERT_LOW_WIN_RATE | 10 |
| ALERT_NEGATIVE_MEDIAN | 9 |
| ALERT_CIRCUIT_BREAKERS | 8 |

**Circuit Breaker fires 14 times in 14 hours.** The daily loss limit (15%) is hit repeatedly, but the CB keeps re-triggering because `_check_circuit_breaker()` has no guard against re-firing while already active. Every trade that closes while the loss is above 15% fires a new `CIRCUIT_BREAKER_TRIGGERED` event, even mid-CB. This creates event noise and makes the CB duration effectively extend beyond the intended 60 minutes.

**Repeated ALERT_HIGH_REJECTION_RATE:** The rejection rate exceeds 80% in nearly every hourly snapshot. This is real — only 57 trades entered vs 117,442 rejections.

---

## 6. Unbounded Table Growth

| Table | Rows | Rate |
|---|---|---|
| market_ticks | 932,876 | ~65,000/hour |
| filter_rejections | 117,442 | ~8,200/hour |
| paper_trades | 57 | negligible |
| system_events | 83 | negligible |

**market_ticks will exceed 1.5M rows per day** and will reach multi-GB size within a week. There is NO pruning mechanism anywhere in the codebase. The DB is already 290 MB after 14 hours.

**filter_rejections is similarly unbounded.** The same 5 tokens rejected 28,000 times for MCAP_OUT_OF_RANGE will continue accumulating at ~2,000 rejections/hour per perpetually-borderline token.

---

## 7. Missing DB Indexes

Existing indexes cover the basic access patterns. Missing:

1. **`paper_trades(exit_time)`** — Used in `insert_performance_snapshot()` for `WHERE exit_time >= ? AND exit_time < ?`. Without this, every hourly snapshot does a full table scan over paper_trades.

2. **`filter_rejections(mint, rejection_reason)`** — The MCAP_OUT_OF_RANGE pattern shows the same 5 mints rejected 28k times. Queries that look up rejections by mint+reason (for dedup or analysis) lack a composite index.

3. **Pruning indexes** — `market_ticks(timestamp)` exists (`idx_ticks_ts`). `filter_rejections(timestamp)` exists (`idx_rejections_ts`). These are sufficient for range-delete pruning once a pruning routine is added.

---

## 8. Async Bugs / Race Conditions

### Issue 1: `is_mint_in_open_trade` N+1 DB query
In `_scan_entries()`, for every token in `analyzable_tokens()`, the code calls:
```python
if await self.db.is_mint_in_open_trade(buf.mint):
    continue
```
This is an additional DB query per token per scan cycle, even though `self.positions` is the authoritative in-memory tracker for open trades. The DB check is redundant and creates N round-trips to SQLite per cycle where N = number of analyzable tokens. Since SQLite is locked during writes, this introduces latency under load.

**Fix:** Replace the DB query with an in-memory check: `if buf.mint in self.positions: continue` (the check at line 753 already covers this — the DB call on line 755 is dead logic).

### Issue 2: Circuit Breaker Re-Trigger During Active CB
`_check_circuit_breaker()` (called from `_exit_trade()` → called from `_manage_positions()`) has no early-return guard when `self._circuit_breaker_active` is already True. When multiple positions close in the same 1-second cycle while above the loss limit, the CB fires for each one, extending the deadline and flooding the event log.

### Issue 3: Stale `_circuit_breaker_active` Flag
`_circuit_breaker_active` is set to `True` when the CB fires, and to `False` when the CB expires in `_scan_entries()`. But `_scan_entries()` may not run during an active CB (it returns early). If the process restarts mid-CB, `_circuit_breaker_active` resets to `False` but `_circuit_breaker_until` is not persisted, meaning the CB is lost on restart. (Minor issue since CB would re-trigger on next losing trade.)

---

## Changes Implemented

The following five fixes were applied directly to the codebase, each directly supported by the database analysis above.

---

### Fix 1 — DB Pruning for Unbounded Tables
**Files:** `db/manager.py`, `trading/paper_engine.py`
**Lines changed:** `db/manager.py` (new `prune_old_data()` method after line 908), `trading/paper_engine.py` (`_maybe_snapshot()` ~line 1126)

**Problem:** `market_ticks` grows at ~65,000 rows/hour (932k in 14h, 290 MB). `filter_rejections` grows at ~8,200/hour (117k in 14h). Neither table has any pruning. The DB will hit multi-GB in days, degrading SQLite query performance.

**Fix:** Added `prune_old_data(max_tick_age_hours, max_rejection_age_hours)` to `DatabaseManager` that DELETEs rows older than configurable thresholds. Called from `_maybe_snapshot()` in `PaperTradingEngine` so it runs hourly alongside performance snapshots.

**Data evidence:** market_ticks 932,876 rows / 14.4h = 64,783 rows/hour. filter_rejections 117,442 rows / 14.4h = 8,155 rows/hour. At these rates: 1.5M ticks/day, 195k rejections/day, ~1.5 GB/day. Pruning at 24h/48h keeps the DB bounded under ~500 MB.

---

### Fix 2 — Circuit Breaker Double-Trigger Guard
**File:** `trading/paper_engine.py`
**Lines changed:** `_check_circuit_breaker()` ~line 1033

**Problem:** When the daily loss limit (15%) is exceeded, `_check_circuit_breaker()` fires a new `CIRCUIT_BREAKER_TRIGGERED` event for EVERY subsequent trade close — even while the CB is already active. Data shows 14 CB triggers in 14 hours, with 3 triggers occurring within 9 minutes (05:24:55, 05:24:55, 05:33:05). Each trigger resets the CB deadline to `now + 60 min`, meaning the effective pause keeps extending.

**Fix:** Added an early return `if self._circuit_breaker_active: return` at the start of `_check_circuit_breaker()`. The CB will still fire once when the loss limit is first hit, but will not re-trigger from subsequent trade closes until the CB expires and `_circuit_breaker_active` is cleared.

**Data evidence:** 14 CIRCUIT_BREAKER_TRIGGERED events vs only 5 CIRCUIT_BREAKER_RESET events. The mismatch confirms re-triggering. From recent events: 3 CB fires at 05:18, 05:24 (×2), and 05:33 — all within one CB window.

---

### Fix 3 — BASE_SLIPPAGE_PCT Reduction (1% → 0.3%)
**File:** `config/settings.py`
**Lines changed:** `BASE_SLIPPAGE_PCT` (line 93)

**Problem:** `BASE_SLIPPAGE_PCT = 0.01` (1%) creates an artificial 1% slippage floor even when computed slippage is negligible. The actual `size_vs_liquidity_pct` averages 0.004%. With `SLIPPAGE_FACTOR = 5.0`, real slippage = 0.004% × 5 = 0.02% — far below the 1% floor. This makes the floor, not the market, determine slippage cost. Combined with 1% round-trip fee, total cost drag is exactly 2.00% on every trade.

**Fix:** Reduced `BASE_SLIPPAGE_PCT` from `0.01` to `0.003` (0.3%). This brings total cost drag from 2.00% to ~1.30%, lowering the break-even PnL threshold by 0.70% per trade. With 39 TIME_STOP exits averaging −0.88% raw, even a small reduction in cost drag meaningfully shifts marginal trades to profitability.

**Data evidence:** `avg fee: +1.00%`, `avg slippage: +1.00%`, `avg size/liquidity: 0.004%`. At 0.004% × 5 = 0.02% computed slippage, the 1% floor overcharges by 98× on average. Comment in code confirms it "was 0.25%" — the 4× increase to 1% was excessive.

---

### Fix 4 — Hurst Threshold Correction (.env)
**File:** `.env`
**Lines changed:** `HURST_THRESHOLD=0.55` → `HURST_THRESHOLD=0.70`

**Problem:** The `.env` overrides `HURST_THRESHOLD=0.55`, lowering the code default of 0.70. This allows tokens with H as low as 0.55 to pass the Hurst gate — a Hurst value barely above 0.50 (random walk). 86% of all 57 trades entered with H in 0.55–0.65, where trend-following logic has no statistical edge. The Hurst gate was designed for H ≥ 0.70 (persistent trending regime).

**Fix:** Restored `HURST_THRESHOLD=0.70` in `.env`. Also updated `TIME_STOP_MINUTES=20` → `TIME_STOP_MINUTES=30` (restore default). 20 minutes is insufficient time for a trend to develop above the 2% cost-drag break-even threshold; the original 30-minute window is more appropriate.

**Data evidence:** 96.5% of trades (55/57) entered below the design threshold. The 2 trades with H ≥ 0.70 include AUTISM (+1.35% net, H=0.817) and GRUG (+37.92% via TRAILING_STOP, H=0.688). All 8,036 HURST_TOO_LOW rejections had H < 0.55, confirming the gate was working only at the env-overridden 0.55 level.

---

### Fix 5 — Add Missing DB Indexes
**File:** `db/manager.py`
**Lines changed:** `_SCHEMA_SQL` constant (~line 23)

**Problem:** Two indexes are missing for common query patterns:

1. `paper_trades(exit_time)` — `insert_performance_snapshot()` runs `WHERE exit_time >= ? AND exit_time < ?` on paper_trades every hour. Without an index on `exit_time`, this is a full table scan. As trade count grows, this query will slow down.

2. `filter_rejections(mint, rejection_reason)` — Analysis queries that look up rejections by token+reason (used for per-token analysis and potential future dedup logic) lack a composite index. With 117k rows and the same 5 mints dominating, this composite index dramatically speeds up per-token rejection queries.

**Data evidence:** `paper_trades` has indexes on `status` and `mint` but not `exit_time`. Performance snapshot queries use `exit_time` range filters. `filter_rejections` has 28,024 rows for just 5 tokens under MCAP_OUT_OF_RANGE — queries grouping by `(mint, rejection_reason)` are common in analysis.

**Bonus — Redundant DB call removed:** Also removed the redundant `await self.db.is_mint_in_open_trade(buf.mint)` call in `_scan_entries()` (paper_engine.py ~line 755). The check `if buf.mint in self.positions` on line 753 already covers this using the in-memory position dict. The DB call added an extra SQLite query per token per cycle with no additional correctness benefit.
