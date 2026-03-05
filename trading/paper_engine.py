"""
Paper Trading Simulation Engine.
v4.1 — Comprehensive Upgrade: Scaled Exits, Realistic Costs, Momentum Filter
────────────────────────────────────────────────────────
Changes from v3.0:
- Scaled exit system: partial TP + trailing stop on remainder
- Realistic cost model: base slippage floor + priority fees
- Per-token re-entry cooldown
- Momentum confirmation filter (SMA + price delta)
- Tighter default stop loss (-15%) + partial stop loss tier
- Extended rolling window (80) and Hurst tuning (min 15)
- Enhanced performance reporting (payoff ratio, profit factor, etc.)
"""
import asyncio
import datetime
import json
import time
import traceback
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from config.settings import Settings
from db.manager import DatabaseManager
from ingestion.harvester import DataHarvester, TokenBuffer
from quant.math_engine import (
    hurst_exponent, micro_cvd, gini_coefficient, calculate_optimal_tp,
    compute_atr, price_efficiency_ratio, volume_acceleration,
)
from utils.logger import get_logger

log = get_logger("PaperEngine")

# ── Optimizer scheduling constants ───────────────────────────
OPTIMIZER_TRADE_INTERVAL = 20   # Re-optimize every N new closed trades
OPTIMIZER_TIME_INTERVAL = 3600.0  # Re-optimize at least every hour
OPTIMIZER_MIN_TRADES = 5        # Minimum trades before first optimization
OPTIMIZER_LOOKBACK = 100        # How many recent trades to feed optimizer
DEFAULT_TAKE_PROFIT = 0.20      # 20% default before optimizer has data

# Ticks older than this are not re-persisted in tiered ingest (avoids duplicate DB writes)
FRESH_TICK_WINDOW_SECONDS = 2.0


@dataclass
class LivePaperPosition:
    """Runtime tracking struct (mirrors the DB row but kept in memory)."""
    trade_id: str
    mint: str
    symbol: str
    entry_time: float
    entry_price: float
    peak_high: float
    peak_low: float
    usd_size: float = 0.0
    entry_liquidity: float = 0.0
    was_size_capped: bool = False
    last_tick_time: float = 0.0  # Track data freshness
    data_stale_warned: bool = False
    data_stale_error: bool = False
    # ── Scaled exit state (v4.1) ─────────────────────────
    partial_tp_taken: bool = False
    original_usd_size: float = 0.0
    trail_active: bool = False
    trail_peak: float = 0.0
    partial_stop_taken: bool = False
    # ── Per-trade dynamic exits (v5.0) ───────────────────
    entry_atr: float = 0.0             # ATR at entry (price fraction)
    per_trade_tp: float = 0.0          # Per-token TP computed at entry
    per_trade_sl: float = 0.0          # Per-token SL (negative value)
    breakeven_activated: bool = False  # Whether break-even stop is active
    speed_bonus_applied: bool = False  # Whether TP speed bonus was applied
    entry_timestamp: float = 0.0       # Entry time (for speed bonus calc)


@dataclass
class OptimizerState:
    """Tracks the state of the dynamic TP optimizer."""
    current_tp: float = DEFAULT_TAKE_PROFIT
    last_run_time: float = 0.0
    last_run_trade_count: int = 0
    best_ev: float = 0.0
    hit_rate: float = 0.0
    sample_size: int = 0
    confidence: str = "UNINITIALIZED"
    ev_curve: list[tuple[float, float, float]] = None
    run_count: int = 0

    def __post_init__(self):
        if self.ev_curve is None:
            self.ev_curve = []


class PaperTradingEngine:
    """
    Orchestrates the full simulation loop each cycle:
    1. Monitor & exit existing paper positions.
    2. Scan tokens for new entry signals (with volume/liquidity/mcap/buy-ratio filters).
    3. Compute Hurst/CVD/Gini for logging (not gating).
    4. Periodically re-optimize take-profit level from MFE data.
    5. Hourly performance snapshots.
    """

    def __init__(self, db: DatabaseManager, harvester: DataHarvester):
        self.db = db
        self.harvester = harvester
        self.positions: dict[str, LivePaperPosition] = {}  # mint -> pos
        self._cycle_count = 0
        self._last_report_time = time.time()
        self._report_interval = 120.0  # seconds
        self.optimizer = OptimizerState()
        self._closed_trade_count_at_last_check: int = 0

        # ── Risk management state ────────────────────────────
        self._circuit_breaker_until: float = 0.0  # timestamp when CB expires
        self._circuit_breaker_active: bool = False  # True while CB is active
        self._loss_cooldown_until: float = 0.0    # timestamp when cooldown expires
        self._loss_cooldown_active: bool = False   # True while cooldown is active
        self._day_start_ts: float = self._utc_day_start()
        self._day_start_balance: float = 0.0

        # ── Data staleness: tokens blocked after DATA_TIMEOUT ─
        self._data_blocked_tokens: dict[str, float] = {}  # mint -> resume_after_ts

        # ── Trade cooldown: tokens blocked after trade exit (v4.1) ─
        self._trade_cooldown_tokens: dict[str, float] = {}  # mint -> resume_after_ts

        # ── Portfolio heat pause (v5.0) ──────────────────────
        self._portfolio_heat_until: float = 0.0

        # ── Hourly snapshots ─────────────────────────────────
        self._last_snapshot_time: float = time.time()
        self._snapshot_interval: float = 3600.0  # 1 hour

    # ══════════════════════════════════════════════════════════
    # Initialization
    # ══════════════════════════════════════════════════════════

    async def initialize(self):
        """Load open trades from previous session and run initial optimization."""
        # Load day_start_balance from DB: most recent balance_after at or before midnight UTC
        self._day_start_balance = await self.db.get_balance_at(self._day_start_ts)
        log.info(
            f"📅 Day start balance loaded: ${self._day_start_balance:,.2f} "
            f"(midnight UTC: {time.ctime(self._day_start_ts)})"
        )

        open_trades = await self.db.get_open_trades()
        for row in open_trades:
            pos = LivePaperPosition(
                trade_id=row["trade_id"],
                mint=row["mint"],
                symbol=row["symbol"],
                entry_time=row["entry_time"],
                entry_price=row["entry_price"],
                peak_high=row["peak_high"],
                peak_low=row["peak_low"],
                usd_size=row["usd_size"] or 0.0,
                entry_liquidity=row["entry_liquidity"] or 0.0,
                last_tick_time=time.time(),
            )
            self.positions[pos.mint] = pos

            # Re-register zombie trades with harvester
            if pos.mint not in self.harvester.tokens:
                self.harvester.tokens[pos.mint] = TokenBuffer(
                    mint=pos.mint,
                    symbol=pos.symbol,
                    name=pos.symbol,
                    pair_address="",
                    dex_id="",
                )

            # Also register in the tiered poller as Tier 1
            from ingestion.tiered_poller import TrackedToken, TokenTier
            if pos.mint not in self.harvester.poller.tokens:
                self.harvester.poller.tokens[pos.mint] = TrackedToken(
                    mint=pos.mint,
                    symbol=pos.symbol,
                    pair_address="",
                    tier=TokenTier.OPEN_POSITION,
                )
            else:
                self.harvester.poller.promote_to_tier1(pos.mint)

            log.info(
                f"♻️ Recovered open trade: {pos.trade_id} │ "
                f"{pos.symbol} @ ${pos.entry_price:.10f}"
            )

        if self.positions:
            log.info(f"♻️ Recovered {len(self.positions)} open paper trades")
        await self._maybe_reoptimize_tp(force=True)

        # ── Wait for initial harvester poll ──────────────────
        # On cold start the poller has no tokens yet. Trigger discovery
        # immediately so _scan_entries() doesn't log "Scanning 0 tokens"
        # for the first polling cycle.
        if not self.harvester.poller.tokens:
            log.info("⏳ Waiting for initial harvester poll...")
            wait_start = time.time()
            timeout = 15.0
            open_mints = set(self.positions.keys())
            while not self.harvester.poller.tokens:
                if time.time() - wait_start > timeout:
                    log.warning("⚠️ Timeout waiting for initial harvester poll — continuing")
                    break
                await self.harvester.poll_tiered(open_mints)
                if not self.harvester.poller.tokens:
                    await asyncio.sleep(1.0)
            if self.harvester.poller.tokens:
                log.info(
                    f"✅ Initial poll complete — "
                    f"{len(self.harvester.poller.tokens)} tokens loaded"
                )

    # ══════════════════════════════════════════════════════════
    # Dynamic TP Optimizer (PRESERVED from v2.0)
    # ══════════════════════════════════════════════════════════

    async def _maybe_reoptimize_tp(self, force: bool = False):
        """Check if re-optimization is due and run if needed."""
        now = time.time()
        current_closed_count = await self.db.get_closed_trade_count()
        trades_since_last = current_closed_count - self.optimizer.last_run_trade_count
        time_since_last = now - self.optimizer.last_run_time

        should_run = force or (
            current_closed_count >= OPTIMIZER_MIN_TRADES
            and (
                trades_since_last >= OPTIMIZER_TRADE_INTERVAL
                or time_since_last >= OPTIMIZER_TIME_INTERVAL
            )
        )
        if not should_run:
            return

        log.info(
            f"🧮 Running MFE Take-Profit Optimizer "
            f"(trades_since_last={trades_since_last}, total={current_closed_count})"
        )
        recent_trades = await self.db.get_recent_closed_trades(limit=OPTIMIZER_LOOKBACK)

        if len(recent_trades) < OPTIMIZER_MIN_TRADES:
            log.info(
                f" Optimizer: only {len(recent_trades)} trades available, "
                f"keeping TP={self.optimizer.current_tp:.0%}"
            )
            self.optimizer.last_run_time = now
            self.optimizer.last_run_trade_count = current_closed_count
            return

        ep = np.array([t["entry_price"] for t in recent_trades], dtype=np.float64)
        ph = np.array([t["peak_high"] for t in recent_trades], dtype=np.float64)
        # Use raw_pnl_pct if available, fall back to pnl_pct
        ap = np.array(
            [t["raw_pnl_pct"] if t["raw_pnl_pct"] is not None else (t["pnl_pct"] or 0.0)
             for t in recent_trades],
            dtype=np.float64,
        )

        old_tp = self.optimizer.current_tp
        optimal_tp, diagnostics = calculate_optimal_tp(
            entry_prices=ep, peak_highs=ph, actual_pnls=ap,
            default_tp=DEFAULT_TAKE_PROFIT,
        )

        if self.optimizer.run_count > 0:
            smoothed_tp = round(0.7 * optimal_tp + 0.3 * old_tp, 2)
            smoothed_tp = max(0.05, min(1.00, smoothed_tp))
        else:
            smoothed_tp = optimal_tp

        self.optimizer.current_tp = smoothed_tp
        self.optimizer.last_run_time = now
        self.optimizer.last_run_trade_count = current_closed_count
        self.optimizer.best_ev = diagnostics["best_ev"]
        self.optimizer.hit_rate = diagnostics["hit_rate"]
        self.optimizer.sample_size = diagnostics["sample_size"]
        self.optimizer.confidence = diagnostics["confidence"]
        self.optimizer.ev_curve = diagnostics.get("ev_curve", [])
        self.optimizer.run_count += 1

        log.info(
            f"🎯 TP UPDATED: {old_tp:.0%} → {smoothed_tp:.0%} "
            f"(raw={optimal_tp:.0%}) │ EV={diagnostics['best_ev']:+.2%} │ "
            f"Hit={diagnostics['hit_rate']:.0%} │ "
            f"Confidence={diagnostics['confidence']}"
        )

    # ══════════════════════════════════════════════════════════
    # Main Cycle
    # ══════════════════════════════════════════════════════════

    async def run_cycle(self):
        """One complete paper-trading evaluation cycle (tiered, ~1s cadence)."""
        self._cycle_count += 1

        # Log full header only every 30 cycles to reduce noise at 1s intervals
        if self._cycle_count % 30 == 1:
            log.info(f"{'═' * 60}")
            log.info(
                f"🔄 PAPER CYCLE #{self._cycle_count} │ "
                f"TP={self.optimizer.current_tp:.0%} ({self.optimizer.confidence}) │ "
                f"Bal=${self.db.balance:,.2f}"
            )
            log.info(f"{'═' * 60}")

        # Reset day tracking if new UTC day
        await self._check_day_reset()

        await self._maybe_reoptimize_tp()
        await self._ingest_tiered()
        await self._manage_positions()
        await self._scan_entries()

        # Hourly performance snapshots
        await self._maybe_snapshot()

        if time.time() - self._last_report_time >= self._report_interval:
            await self._print_report()
            self._last_report_time = time.time()

        # Periodic tier-distribution summary (every 30 seconds)
        now = time.time()
        if int(now) % 30 == 0 and int(now) != getattr(self, '_last_tier_log', -1):
            self._last_tier_log = int(now)
            open_mints = set(self.positions.keys())
            tier_counts = self.harvester.poller.tier_counts()
            budget = self.harvester.poller._requests_in_window()
            log.info(
                f"📊 Status │ Tracked: {len(self.harvester.tokens)} │ "
                f"T1:{tier_counts.get('OPEN_POSITION', 0)} "
                f"T2:{tier_counts.get('HOT_WATCHLIST', 0)} "
                f"T3:{tier_counts.get('WARM_SCANNER', 0)} │ "
                f"API: {budget}/min │ "
                f"Open trades: {len(open_mints)}"
            )

    async def _ingest_tiered(self) -> int:
        """
        Run one tiered polling task and persist new ticks to SQLite.
        Uses poll_tiered() with open-position awareness.
        """
        open_mints = set(self.positions.keys())
        ticks_processed = await self.harvester.poll_tiered(open_mints)

        # Persist only the ticks that were freshly updated this cycle
        for mint, buf in self.harvester.tokens.items():
            if buf.ticks:
                t = buf.ticks[-1]
                # Only persist if tick is recent (updated within freshness window)
                if time.time() - t.timestamp < FRESH_TICK_WINDOW_SECONDS:
                    await self.db.insert_tick(
                        mint=buf.mint, symbol=buf.symbol, price_usd=t.price_usd,
                        liquidity_usd=t.liquidity_usd, volume_5m=t.volume_5m,
                        buys_5m=t.buys_5m, sells_5m=t.sells_5m,
                        market_cap=t.market_cap, pair_address=buf.pair_address,
                    )
        return ticks_processed

    async def _ingest(self) -> int:
        """Poll DexScreener and persist every tick to SQLite (legacy path)."""
        ticks_processed = await self.harvester.poll()
        for mint, buf in self.harvester.tokens.items():
            if buf.ticks:
                t = buf.ticks[-1]
                await self.db.insert_tick(
                    mint=buf.mint, symbol=buf.symbol, price_usd=t.price_usd,
                    liquidity_usd=t.liquidity_usd, volume_5m=t.volume_5m,
                    buys_5m=t.buys_5m, sells_5m=t.sells_5m,
                    market_cap=t.market_cap, pair_address=buf.pair_address,
                )
        return ticks_processed

    # ══════════════════════════════════════════════════════════
    # Position Management & Exit Logic
    # ══════════════════════════════════════════════════════════

    async def _manage_positions(self):
        if not self.positions:
            return

        # ── SOL crash check (v5.0) ───────────────────────────
        sol_pct_change = self.harvester.get_sol_pct_change(Settings.SOL_CRASH_WINDOW_SECONDS)
        if sol_pct_change is not None and sol_pct_change <= Settings.SOL_CRASH_PCT:
            log.critical(
                f"💥 SOL CRASH DETECTED: {sol_pct_change:+.2%} in "
                f"{Settings.SOL_CRASH_WINDOW_SECONDS/60:.0f}min — force-closing ALL positions"
            )
            for mint, pos in list(self.positions.items()):
                buf = self.harvester.get(mint)
                exit_price = (buf.latest_price if buf and buf.latest_price > 0
                              else pos.entry_price)
                await self._exit_trade(pos, exit_price, "SOL_CRASH_EXIT")
            # Activate circuit breaker
            self._circuit_breaker_until = time.time() + Settings.CIRCUIT_BREAKER_MINUTES * 60
            self._circuit_breaker_active = True
            await self.db.insert_system_event(
                event_type="SOL_CRASH_EXIT", severity="CRITICAL",
                description=f"SOL dropped {sol_pct_change:+.2%} — all positions closed",
                metadata={"sol_pct_change": sol_pct_change},
            )
            return

        for mint, pos in list(self.positions.items()):
            try:
                buf = self.harvester.get(mint)

                # ── Data staleness handling ───────────────────
                if buf is None or buf.count == 0:
                    await self._handle_data_staleness(pos, no_buf=True)
                    continue

                current_price = buf.latest_price
                if current_price <= 0:
                    await self._handle_data_staleness(pos, no_buf=True)
                    continue

                # We have fresh data — update staleness tracker
                pos.last_tick_time = time.time()
                pos.data_stale_warned = False
                pos.data_stale_error = False

                # Update extremes
                if current_price > pos.peak_high:
                    pos.peak_high = current_price
                if current_price < pos.peak_low or pos.peak_low <= 0:
                    pos.peak_low = current_price

                await self.db.update_paper_trade_extremes(
                    pos.trade_id, pos.peak_high, pos.peak_low
                )

                exit_reason = self._check_exits(pos, current_price)
                if exit_reason:
                    await self._exit_trade(pos, current_price, exit_reason)
                else:
                    pnl = (
                        (current_price - pos.entry_price) / pos.entry_price
                        if pos.entry_price else 0
                    )
                    log.debug(
                        f"  📊 {pos.symbol}: ${current_price:.10f} │ "
                        f"PnL={pnl:+.2%} │ TP@{pos.per_trade_tp if pos.per_trade_tp > 0 else self.optimizer.current_tp:.0%}"
                    )
            except Exception as e:
                log.error(f"Error managing position {pos.symbol}: {e}")

    async def _handle_data_staleness(self, pos: LivePaperPosition, no_buf: bool = False):
        """Staged data staleness handling: 60s warn, 120s error, 180s exit."""
        now = time.time()
        if pos.last_tick_time <= 0:
            pos.last_tick_time = pos.entry_time

        seconds_stale = now - pos.last_tick_time

        if seconds_stale >= Settings.DATA_TIMEOUT_EXIT_SECONDS:
            # 180s — force exit
            log.error(
                f"DATA_TIMEOUT: No data for {pos.symbol} in {seconds_stale:.0f}s — "
                f"closing trade {pos.trade_id}"
            )
            await self.db.insert_system_event(
                event_type="DATA_TIMEOUT", severity="ERROR",
                description=f"No tick for {pos.symbol} in {seconds_stale:.0f}s",
                metadata={"trade_id": pos.trade_id, "mint": pos.mint},
            )
            await self._exit_trade(
                pos, pos.entry_price,
                f"DATA_TIMEOUT (no data for {seconds_stale:.0f}s)",
            )
            # Block new trades on this token until data resumes
            self._data_blocked_tokens[pos.mint] = now + Settings.DATA_RESUME_WAIT_SECONDS
        elif seconds_stale >= Settings.DATA_STALE_ERROR_SECONDS and not pos.data_stale_error:
            pos.data_stale_error = True
            log.error(
                f"DATA_STALE_ERROR: No tick for {pos.symbol} in {seconds_stale:.0f}s — "
                f"attempting reconnection"
            )
            await self.db.insert_system_event(
                event_type="DATA_STALE", severity="ERROR",
                description=f"No tick for {pos.symbol} in {seconds_stale:.0f}s",
                metadata={"trade_id": pos.trade_id, "mint": pos.mint},
            )
        elif seconds_stale >= Settings.DATA_STALE_WARNING_SECONDS and not pos.data_stale_warned:
            pos.data_stale_warned = True
            log.warning(
                f"DATA_STALE: No tick for {pos.symbol} in {seconds_stale:.0f}s"
            )
            await self.db.insert_system_event(
                event_type="DATA_STALE", severity="WARNING",
                description=f"No tick for {pos.symbol} in {seconds_stale:.0f}s",
                metadata={"trade_id": pos.trade_id, "mint": pos.mint},
            )

    def _check_exits(self, pos: LivePaperPosition, price: float) -> Optional[str]:
        """
        Evaluate exit conditions (v5.0 — Per-Trade Dynamic Exits).

        Before partial TP:
          1. Speed bonus TP adjustment (first 60s)
          2. Per-trade TP → partial sell
          3. Break-even stop (after 4% gain, stop at entry+buffer)
          4. Per-trade SL (liquidity-scaled, triggers full exit)
          5. RUG_PROTECTION at -25% (catastrophic only)
          6. TIME_STOP at TIME_STOP_MINUTES
        After partial TP (trailing phase):
          1. MAX_GAIN → force full exit (anti-greed)
          2. TRAILING_STOP with time-decay tightening
          3. RUG_PROTECTION → still active
          4. HARD_TIME_STOP at 25min
        """
        ep = pos.entry_price
        if ep <= 0:
            return None

        gain = (price - ep) / ep
        now = time.time()
        pos_age = now - pos.entry_time

        if not pos.partial_tp_taken:
            # ── Phase 1: Pre-partial-TP ──────────────────────

            # Determine effective TP (per-trade or optimizer fallback)
            effective_tp = pos.per_trade_tp if pos.per_trade_tp > 0 else self.optimizer.current_tp

            # 1. Speed bonus check (first SPEED_BONUS_WINDOW_SECONDS)
            if (not pos.speed_bonus_applied
                    and pos_age <= Settings.SPEED_BONUS_WINDOW_SECONDS
                    and gain >= Settings.SPEED_BONUS_THRESHOLD):
                new_tp = min(
                    effective_tp * Settings.SPEED_BONUS_MULTIPLIER,
                    Settings.ATR_TP_MAX,
                )
                pos.per_trade_tp = new_tp
                pos.speed_bonus_applied = True
                effective_tp = new_tp
                log.info(
                    f"⚡ SPEED_BONUS for {pos.symbol}: TP raised to {new_tp:.1%} "
                    f"(gain={gain:+.2%} in {pos_age:.0f}s)"
                )

            # 2. Dynamic Take Profit
            if gain >= effective_tp:
                reason_tag = "DYNAMIC_TP" if Settings.PARTIAL_TP_SELL_PCT >= 1.0 else "PARTIAL_TP"
                return f"{reason_tag} (+{gain * 100:.1f}% gain)"

            # 3. Break-even stop
            if not pos.breakeven_activated and gain >= Settings.BREAKEVEN_ACTIVATION_PCT:
                pos.breakeven_activated = True
                log.info(
                    f"🔒 BREAKEVEN activated for {pos.symbol}: "
                    f"gain={gain:+.2%}, SL locked at +{Settings.BREAKEVEN_BUFFER_PCT:.1%}"
                )
            if pos.breakeven_activated and gain <= Settings.BREAKEVEN_BUFFER_PCT:
                return f"BREAKEVEN_STOP (gain={gain:+.2%})"

            # 4. Per-trade SL (liquidity-scaled, full exit)
            effective_sl = pos.per_trade_sl if pos.per_trade_sl != 0 else Settings.PARTIAL_STOP_LOSS_PCT
            if gain <= effective_sl:
                return f"STOP_LOSS ({gain * 100:.1f}% loss)"

            # 5. Rug Protection (catastrophic circuit breaker)
            if gain <= Settings.RUG_PROTECTION_PCT:
                log.warning(
                    f"⚠️ RUG_PROTECTION triggered for {pos.symbol}: "
                    f"{gain * 100:.1f}% loss — entry filters may have failed"
                )
                return f"RUG_PROTECTION (-{abs(gain) * 100:.1f}% from entry)"

            # 6. Time stop
            if pos_age >= Settings.TIME_STOP_SECONDS:
                return f"TIME_STOP ({pos_age / 60:.1f} min)"
        else:
            # ── Phase 2: Post-partial-TP (trailing stop phase) ──

            # 1. Max gain cap (anti-greed)
            if gain >= Settings.MAX_GAIN_PCT:
                return f"MAX_GAIN_CAP (+{gain * 100:.1f}% gain)"

            # 2. Trailing stop with time-decay tightening
            trail_dist = Settings.TRAILING_STOP_PCT
            if pos_age > Settings.TRAIL_TIGHTEN_2_MINUTES * 60:
                trail_dist *= Settings.TRAIL_TIGHTEN_2_FACTOR
            elif pos_age > Settings.TRAIL_TIGHTEN_1_MINUTES * 60:
                trail_dist *= Settings.TRAIL_TIGHTEN_1_FACTOR

            if pos.trail_active:
                trail_drop = (pos.trail_peak - price) / pos.trail_peak if pos.trail_peak > 0 else 0
                if trail_drop >= trail_dist:
                    return f"TRAILING_STOP (-{trail_drop * 100:.1f}% from peak ${pos.trail_peak:.10f})"
            else:
                if gain >= Settings.TRAILING_ACTIVATION_PCT:
                    pos.trail_active = True
                    pos.trail_peak = price
                    log.info(
                        f"📈 TRAIL ACTIVATED for {pos.symbol}: "
                        f"gain={gain:+.1%}, peak=${price:.10f}"
                    )

            # Update trail peak if price is making new highs
            if pos.trail_active and price > pos.trail_peak:
                pos.trail_peak = price

            # 3. Rug protection still applies
            if gain <= Settings.RUG_PROTECTION_PCT:
                log.warning(
                    f"⚠️ RUG_PROTECTION triggered for {pos.symbol}: "
                    f"{gain * 100:.1f}% loss — trailing phase"
                )
                return f"RUG_PROTECTION (-{abs(gain) * 100:.1f}% from entry)"

            # 4. Hard time stop (25 min — replaces old 180 min extended stop)
            if pos_age >= Settings.HARD_TIME_STOP_SECONDS:
                return f"TIME_STOP_EXTENDED ({pos_age / 60:.1f} min)"

        return None

    async def _exit_trade(self, pos: LivePaperPosition, exit_price: float, reason: str):
        """Close trade (full or partial) with cost model applied."""
        is_partial = reason.startswith("PARTIAL_TP") or reason.startswith("PARTIAL_STOP_LOSS")

        if is_partial and reason.startswith("PARTIAL_TP"):
            sell_pct = Settings.PARTIAL_TP_SELL_PCT
        elif is_partial and reason.startswith("PARTIAL_STOP_LOSS"):
            sell_pct = Settings.PARTIAL_STOP_SELL_PCT
        else:
            sell_pct = 1.0

        sell_usd = pos.usd_size * sell_pct
        remaining_usd = pos.usd_size - sell_usd

        # Calculate cost model on the portion being sold
        cost_info = self._calculate_costs(pos, exit_price, usd_size_override=sell_usd)

        if is_partial and remaining_usd > 0:
            # ── Partial exit: credit PnL, update position, keep tracking ──
            raw_pnl_pct = (exit_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0.0
            fee_pct = cost_info.get("fee_pct", 0.0)
            slippage_pct = cost_info.get("slippage_pct", 0.0)
            priority_fee_usd = cost_info.get("priority_fee_usd", 0.0)
            net_pnl_pct = raw_pnl_pct - fee_pct - slippage_pct
            net_usd_pnl = sell_usd * net_pnl_pct - priority_fee_usd

            # Credit balance
            balance_before = self.db.balance
            self.db.adjust_balance(net_usd_pnl)

            # Record partial exit in portfolio_state
            desc = (
                f"Partial exit ({sell_pct:.0%}): {reason} | "
                f"{raw_pnl_pct:+.2%} raw, {net_pnl_pct:+.2%} net "
                f"(${net_usd_pnl:+.2f}) "
                f"[fee={fee_pct:.2%}, slip={slippage_pct:.2%}]"
            )
            await self.db.insert_portfolio_event(
                event_type=reason.split(" ")[0],
                trade_id=pos.trade_id,
                balance_before=balance_before,
                balance_after=self.db.balance,
                usd_change=net_usd_pnl,
                description=desc,
            )
            log.info(
                f"📘 Partial exit {pos.trade_id} ({pos.symbol}): {reason} | {desc}"
            )

            # Update position state
            if not pos.original_usd_size:
                pos.original_usd_size = pos.usd_size
            pos.usd_size = remaining_usd

            if reason.startswith("PARTIAL_TP"):
                pos.partial_tp_taken = True
                pos.trail_peak = exit_price
            elif reason.startswith("PARTIAL_STOP_LOSS"):
                pos.partial_stop_taken = True

            # Do NOT remove from self.positions or demote from Tier 1
        else:
            # ── Full exit ──
            result = await self.db.close_paper_trade(
                pos.trade_id, exit_price, reason, cost_info=cost_info,
                final_usd_size=pos.usd_size
            )
            del self.positions[pos.mint]

            # Add trade cooldown
            self._trade_cooldown_tokens[pos.mint] = (
                time.time() + Settings.TRADE_COOLDOWN_MINUTES * 60
            )

            # Check for large loss cooldown
            if result and result.get("net_pnl_pct", 0) <= Settings.LARGE_LOSS_THRESHOLD_PCT:
                self._loss_cooldown_until = time.time() + Settings.LOSS_COOLDOWN_MINUTES * 60
                self._loss_cooldown_active = True
                log.warning(
                    f"LOSS_COOLDOWN: Trade {pos.trade_id} lost "
                    f"{result['net_pnl_pct']:.2%}. "
                    f"Cooling down for {Settings.LOSS_COOLDOWN_MINUTES:.0f} minutes."
                )
                await self.db.insert_system_event(
                    event_type="LOSS_COOLDOWN_START", severity="WARNING",
                    description=(
                        f"Trade {pos.trade_id} lost {result['net_pnl_pct']:.2%}. "
                        f"Cooldown for {Settings.LOSS_COOLDOWN_MINUTES:.0f}min."
                    ),
                    metadata={
                        "trade_id": pos.trade_id,
                        "net_pnl_pct": result["net_pnl_pct"],
                    },
                )

            # Check daily loss circuit breaker
            await self._check_circuit_breaker()

    def _calculate_costs(self, pos: LivePaperPosition, exit_price: float,
                         usd_size_override: float = None) -> dict:
        """Apply fee + slippage cost model to paper trade."""
        entry_price = pos.entry_price
        usd_size = usd_size_override if usd_size_override is not None else pos.usd_size
        liquidity = pos.entry_liquidity

        # Fee: per side (entry + exit)
        fee_pct = Settings.FEE_PER_SIDE_PCT * 2  # round trip

        # Slippage: proportional to size relative to liquidity
        if liquidity > 0:
            size_ratio = usd_size / liquidity
        else:
            size_ratio = 0.01
        slippage_pct = size_ratio * Settings.SLIPPAGE_FACTOR
        slippage_pct = min(slippage_pct, Settings.MAX_SLIPPAGE_PCT)
        # Apply base slippage floor (realistic minimum DEX swap cost)
        slippage_pct = max(slippage_pct, Settings.BASE_SLIPPAGE_PCT)

        # Priority fee: round trip (entry + exit)
        priority_fee_usd = Settings.PRIORITY_FEE_USD * 2

        return {
            "fee_pct": fee_pct,
            "slippage_pct": slippage_pct,
            "was_size_capped": pos.was_size_capped,
            "priority_fee_usd": priority_fee_usd,
        }

    # ══════════════════════════════════════════════════════════
    # Position Sizing
    # ══════════════════════════════════════════════════════════

    def _calculate_position_size(self, liquidity_usd: float) -> tuple[float, bool]:
        """
        Liquidity-aware position sizing.
        Returns (actual_size, was_capped).
        """
        balance = self.db.balance

        # Step 1: Desired size as percentage of balance
        desired_size = balance * Settings.POSITION_PCT

        # Step 2: Hard cap at percentage of pool liquidity
        max_executable = liquidity_usd * Settings.MAX_LIQUIDITY_PCT

        # Step 3: Take the smaller of the two
        was_capped = desired_size > max_executable
        actual_size = min(desired_size, max_executable)

        # Step 4: Minimum viable trade size
        if actual_size < Settings.MIN_TRADE_SIZE_USD:
            return 0.0, False

        if was_capped:
            log.info(
                f"SIZE_CAPPED: desired=${desired_size:.2f}, "
                f"max_liq=${max_executable:.2f}, "
                f"actual=${actual_size:.2f}, liquidity=${liquidity_usd:,.0f}"
            )

        return actual_size, was_capped

    # ══════════════════════════════════════════════════════════
    # Entry Signal Evaluation
    # ══════════════════════════════════════════════════════════

    async def _scan_entries(self):
        now = time.time()

        # ── UTC time blackout check (v5.0) ───────────────────
        current_utc_hour = datetime.datetime.utcnow().hour
        start_h = Settings.TRADING_BLACKOUT_START_UTC
        end_h = Settings.TRADING_BLACKOUT_END_UTC
        if start_h < end_h:
            in_blackout = start_h <= current_utc_hour < end_h
        else:
            # Wraps midnight (e.g., 22:00 – 06:00)
            in_blackout = current_utc_hour >= start_h or current_utc_hour < end_h
        if in_blackout:
            log.info(f"  🌙 UTC blackout ({start_h:02d}:00–{end_h:02d}:00) — skipping scan")
            return

        # ── Portfolio heat pause (v5.0) ──────────────────────
        if now < self._portfolio_heat_until:
            remaining = (self._portfolio_heat_until - now) / 60
            log.info(f"  🔥 Portfolio heat pause — {remaining:.1f}min remaining")
            return

        # Check circuit breaker
        if now < self._circuit_breaker_until:
            remaining = (self._circuit_breaker_until - now) / 60
            log.info(f"  🔴 Circuit breaker active — {remaining:.1f}min remaining")
            return
        elif self._circuit_breaker_active:
            # Circuit breaker just expired — emit reset event once
            self._circuit_breaker_active = False
            await self.db.insert_system_event(
                event_type="CIRCUIT_BREAKER_RESET", severity="INFO",
                description="Circuit breaker cooldown expired, resuming trading",
            )

        # Check loss cooldown
        if now < self._loss_cooldown_until:
            remaining = (self._loss_cooldown_until - now) / 60
            log.info(f"  🟡 Loss cooldown active — {remaining:.1f}min remaining")
            return
        elif self._loss_cooldown_active:
            # Loss cooldown just expired — emit end event once
            self._loss_cooldown_active = False
            await self.db.insert_system_event(
                event_type="LOSS_COOLDOWN_END", severity="INFO",
                description="Loss cooldown expired, resuming trading",
            )

        analyzable = self.harvester.analyzable_tokens()
        log.info(f"🔬 Scanning {len(analyzable)} tokens for entry signals…")

        # Check max positions BEFORE iterating tokens
        if len(self.positions) >= Settings.MAX_OPEN_TRADES:
            log.info(
                f"  ⛔ Max open trades ({Settings.MAX_OPEN_TRADES}) — skipping scan"
            )
            return

        for buf in analyzable:
            if buf.mint in self.positions:
                continue
            if await self.db.is_mint_in_open_trade(buf.mint):
                continue

            # Check data block
            if buf.mint in self._data_blocked_tokens:
                if now < self._data_blocked_tokens[buf.mint]:
                    continue
                else:
                    del self._data_blocked_tokens[buf.mint]
                    await self.db.insert_system_event(
                        event_type="DATA_RESUMED", severity="INFO",
                        description=f"Data resumed for {buf.symbol}, allowing trades",
                    )

            # Check trade cooldown (v4.1)
            if buf.mint in self._trade_cooldown_tokens:
                if now < self._trade_cooldown_tokens[buf.mint]:
                    log.debug(f"  ⏳ {buf.symbol}: trade cooldown active")
                    continue
                else:
                    del self._trade_cooldown_tokens[buf.mint]

            await self._evaluate_entry(buf)

    async def _evaluate_entry(self, buf: TokenBuffer):
        """
        Entry filter chain (v5.0 — fastest rejections first):
        1. Activity check (MIN_ACTIVITY_TXNS + MIN_BUY_TXNS_5M)
        2. Liquidity check
        3. Volume check
        4. Market cap check
        5. Buy ratio check
        6. Token age check (NEW)
        7. UTC time blackout (per-token secondary check)
        8. SOL regime check (NEW)
        9. Momentum SMA + delta
        10. Volume acceleration (NEW)
        11. Hurst gate (or PER for young tokens)
        12. Gini gate — BOTH bounds (CRITICAL: MIN and MAX)
        13. CVD slope gate
        14. Portfolio heat check (NEW)
        15. Position sizing
        16. Compute per-trade TP/SL
        17. ENTER TRADE
        """
        latest_tick = buf.ticks[-1] if buf.ticks else None
        if latest_tick is None:
            return

        price = buf.latest_price
        liquidity = latest_tick.liquidity_usd
        volume = latest_tick.volume_5m
        buys = latest_tick.buys_5m
        sells = latest_tick.sells_5m
        mcap = latest_tick.market_cap

        # ── 1. Activity check ────────────────────────────────
        total_txns = buys + sells
        if total_txns < Settings.MIN_ACTIVITY_TXNS:
            reason = f"ACTIVITY_TOO_LOW (total_txns={total_txns}, min={Settings.MIN_ACTIVITY_TXNS})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
            )
            return

        # Also check MIN_BUY_TXNS_5M
        if buys < Settings.MIN_BUY_TXNS_5M:
            reason = f"BUY_TXNS_5M_TOO_LOW (buys={buys}, min={Settings.MIN_BUY_TXNS_5M})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
            )
            return

        # ── 2. Liquidity check ───────────────────────────────
        if liquidity is None or liquidity < Settings.MIN_LIQUIDITY:
            reason = f"LIQUIDITY_TOO_LOW (liq={liquidity}, min={Settings.MIN_LIQUIDITY:.0f})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity or 0, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
            )
            return

        # ── 3. Volume check ──────────────────────────────────
        if volume is None or volume < Settings.MIN_VOLUME_5M:
            reason = f"VOLUME_TOO_LOW (vol={volume}, min={Settings.MIN_VOLUME_5M:.0f})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume or 0,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
            )
            return

        # ── 4. Market cap check ──────────────────────────────
        if mcap is None or mcap <= 0 or not (Settings.MIN_MARKET_CAP <= mcap <= Settings.MAX_MARKET_CAP):
            reason = f"MCAP_OUT_OF_RANGE (mcap={mcap}, range={Settings.MIN_MARKET_CAP:.0f}-{Settings.MAX_MARKET_CAP:.0f})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap or 0,
            )
            return

        # ── 5. Buy/sell ratio check ──────────────────────────
        buy_ratio = buys / total_txns
        if not (Settings.MIN_BUY_RATIO <= buy_ratio <= Settings.MAX_BUY_RATIO):
            reason = f"BUY_RATIO_OUT_OF_RANGE (ratio={buy_ratio:.2f}, range={Settings.MIN_BUY_RATIO:.2f}-{Settings.MAX_BUY_RATIO:.2f})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio,
            )
            return

        # ── 5a. RugCheck score gate (v5.2) ─────────────────────
        if (buf.rugcheck_score is not None
                and buf.rugcheck_score > Settings.RUGCHECK_MAX_SCORE):
            reason = f"RUGCHECK_SCORE_TOO_HIGH (score={buf.rugcheck_score}, max={Settings.RUGCHECK_MAX_SCORE})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio,
            )
            return

        # ── 5b. LP lock gate (v5.2, optional) ─────────────────
        if (Settings.RUGCHECK_REQUIRE_LP_LOCKED
                and buf.lp_locked is not None
                and buf.lp_locked is False):
            reason = "LP_NOT_LOCKED"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio,
            )
            return

        # ── 6. Token age check (v5.0) ────────────────────────
        # Pump.fun migration bonus: bypass age-too-young gate (v5.2)
        pumpfun_age_bypass = (
            Settings.PUMPFUN_MIGRATION_BONUS
            and buf.is_pumpfun_migration is True
        )
        token_age_minutes: Optional[float] = None
        if buf.pair_created_at is not None:
            token_age_seconds = time.time() - buf.pair_created_at
            token_age_minutes = token_age_seconds / 60.0
            min_age_s = Settings.TOKEN_MIN_AGE_MINUTES * 60
            max_age_s = Settings.TOKEN_MAX_AGE_HOURS * 3600
            if token_age_seconds < min_age_s:
                if pumpfun_age_bypass:
                    log.info(
                        f"  ⚡ {buf.symbol}: pump.fun migration — bypassing age gate"
                    )
                else:
                    reason = f"TOKEN_TOO_YOUNG (age={token_age_minutes:.1f}min, min={Settings.TOKEN_MIN_AGE_MINUTES:.0f}min)"
                    log.debug(f"  ❌ {buf.symbol}: {reason}")
                    await self.db.insert_filter_rejection(
                        mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                        price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                        buys_5m=buys, sells_5m=sells, market_cap=mcap,
                        buy_ratio=buy_ratio, token_age_minutes=token_age_minutes,
                    )
                    return
            if token_age_seconds > max_age_s:
                reason = f"TOKEN_TOO_OLD (age={token_age_minutes:.1f}min, max={Settings.TOKEN_MAX_AGE_HOURS:.0f}h)"
                log.debug(f"  ❌ {buf.symbol}: {reason}")
                await self.db.insert_filter_rejection(
                    mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                    price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                    buys_5m=buys, sells_5m=sells, market_cap=mcap,
                    buy_ratio=buy_ratio, token_age_minutes=token_age_minutes,
                )
                return
        else:
            log.debug(f"  ℹ️ {buf.symbol}: pair_created_at unavailable — skipping age gate")

        # ── 7. SOL regime check (v5.0) ───────────────────────
        sol_current_price: Optional[float] = None
        if Settings.SOL_REGIME_ENABLED:
            sol_sma = self.harvester.get_sol_sma(Settings.SOL_SMA_LOOKBACK)
            sol_current_price = self.harvester.get_sol_price()
            if sol_sma is not None and sol_current_price is not None:
                if sol_current_price < sol_sma:
                    reason = (f"SOL_DOWNTREND (sol={sol_current_price:.2f}, "
                              f"sma{Settings.SOL_SMA_LOOKBACK}={sol_sma:.2f})")
                    log.debug(f"  ❌ {buf.symbol}: {reason}")
                    await self.db.insert_filter_rejection(
                        mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                        price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                        buys_5m=buys, sells_5m=sells, market_cap=mcap,
                        buy_ratio=buy_ratio, token_age_minutes=token_age_minutes,
                        sol_price=sol_current_price,
                    )
                    return
            else:
                log.debug(f"  ℹ️ {buf.symbol}: SOL data unavailable — skipping regime gate")

        # ── 8. Momentum SMA + delta (v4.1) ───────────────────
        if Settings.MOMENTUM_CHECK_ENABLED:
            prices_arr = buf.prices
            sma_n = Settings.MOMENTUM_SMA_LOOKBACK
            if len(prices_arr) >= sma_n:
                sma = float(np.mean(prices_arr[-sma_n:]))
                if price <= sma:
                    reason = f"MOMENTUM_SMA_FAIL (price={price:.10f}, sma{sma_n}={sma:.10f})"
                    log.debug(f"  ❌ {buf.symbol}: {reason}")
                    await self.db.insert_filter_rejection(
                        mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                        price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                        buys_5m=buys, sells_5m=sells, market_cap=mcap,
                        buy_ratio=buy_ratio,
                    )
                    return
            else:
                log.debug(f"  ℹ️ {buf.symbol}: Momentum SMA data insufficient ({len(prices_arr)}/{sma_n}) — skipping gate")

            delta_n = Settings.MOMENTUM_DELTA_LOOKBACK
            if len(prices_arr) > delta_n:
                price_delta = price - float(prices_arr[-delta_n])
                if price_delta <= 0:
                    reason = f"MOMENTUM_DELTA_FAIL (delta={price_delta:.10f}, lookback={delta_n})"
                    log.debug(f"  ❌ {buf.symbol}: {reason}")
                    await self.db.insert_filter_rejection(
                        mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                        price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                        buys_5m=buys, sells_5m=sells, market_cap=mcap,
                        buy_ratio=buy_ratio,
                    )
                    return
            else:
                log.debug(f"  ℹ️ {buf.symbol}: Momentum delta data insufficient ({len(prices_arr)}/{delta_n}) — skipping gate")

        # ── 9. Volume acceleration check (v5.0) ──────────────
        vol_accel = volume_acceleration(buf.buys, buf.sells)
        if vol_accel.accel_ratio is not None:
            if vol_accel.accel_ratio < Settings.VOLUME_ACCEL_MULTIPLIER:
                reason = (f"VOLUME_ACCEL_LOW (accel={vol_accel.accel_ratio:.2f}, "
                          f"min={Settings.VOLUME_ACCEL_MULTIPLIER}x)")
                log.debug(f"  ❌ {buf.symbol}: {reason}")
                await self.db.insert_filter_rejection(
                    mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                    price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                    buys_5m=buys, sells_5m=sells, market_cap=mcap,
                    buy_ratio=buy_ratio, token_age_minutes=token_age_minutes,
                    volume_accel_ratio=vol_accel.accel_ratio,
                )
                return
        else:
            log.debug(f"  ℹ️ {buf.symbol}: Volume acceleration data insufficient — skipping gate")

        # ── 10. Calculate Hurst/CVD/Gini and apply entry gates ──
        H = hurst_exponent(buf.prices)
        cvd_val, cvd_slope, is_bullish = micro_cvd(
            buf.buys, buf.sells, buf.volumes, buf.prices,
            lookback=Settings.CVD_LOOKBACK,
        )
        gini = await self._get_gini(buf)

        # ── 10a. Hurst gate ──────────────────────────────────
        # For tokens with insufficient snapshot data, fall back to Price Efficiency Ratio
        prices_arr = buf.prices
        insufficient_hurst_data = buf.count < Settings.MIN_SNAPSHOTS_HURST
        if H is None or insufficient_hurst_data:
            per_val = price_efficiency_ratio(prices_arr)
            if per_val is not None:
                if per_val < 0.40:
                    reason = f"PER_TOO_LOW (PER={per_val:.4f}, min=0.40)"
                    log.debug(f"  ❌ {buf.symbol}: {reason}")
                    await self.db.insert_filter_rejection(
                        mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                        price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                        buys_5m=buys, sells_5m=sells, market_cap=mcap,
                        buy_ratio=buy_ratio, hurst_value=H or 0.0,
                        cvd_slope=cvd_slope or 0.0, gini_coeff=gini or 0.0,
                        price_efficiency_ratio=per_val,
                    )
                    return
            else:
                log.debug(f"  ℹ️ {buf.symbol}: PER data insufficient — skipping Hurst/PER gate")
        elif H < Settings.HURST_THRESHOLD:
            reason = f"HURST_TOO_LOW (H={H:.4f}, min={Settings.HURST_THRESHOLD})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio, hurst_value=H,
                cvd_slope=cvd_slope or 0.0, gini_coeff=gini or 0.0,
            )
            return

        # ── 10b. Gini gate — BOTH bounds (CRITICAL v5.0 change) ──
        if gini is None:
            log.debug(f"  ℹ️ {buf.symbol}: Gini data unavailable — skipping gate")
        elif gini < Settings.MIN_GINI:
            reason = f"GINI_TOO_LOW (G={gini:.4f}, min={Settings.MIN_GINI})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio, hurst_value=H or 0.0,
                cvd_slope=cvd_slope or 0.0, gini_coeff=gini,
            )
            return
        elif gini > Settings.MAX_GINI:
            reason = f"GINI_TOO_HIGH (G={gini:.4f}, max={Settings.MAX_GINI})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio, hurst_value=H or 0.0,
                cvd_slope=cvd_slope or 0.0, gini_coeff=gini,
            )
            return

        # ── 10c. CVD slope gate ──────────────────────────────
        if cvd_slope is None:
            log.debug(f"  ℹ️ {buf.symbol}: CVD slope data unavailable — skipping gate")
        elif cvd_slope < Settings.MIN_CVD_SLOPE or cvd_slope > Settings.MAX_CVD_SLOPE:
            reason = f"CVD_SLOPE_OUT_OF_RANGE (slope={cvd_slope:.4f}, range={Settings.MIN_CVD_SLOPE}-{Settings.MAX_CVD_SLOPE})"
            log.debug(f"  ❌ {buf.symbol}: {reason}")
            await self.db.insert_filter_rejection(
                mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                buys_5m=buys, sells_5m=sells, market_cap=mcap,
                buy_ratio=buy_ratio, hurst_value=H or 0.0,
                cvd_slope=cvd_slope, gini_coeff=gini or 0.0,
            )
            return

        # ── 11. Portfolio heat check (v5.0) ──────────────────
        if self.positions:
            total_unrealized = 0.0
            for p in self.positions.values():
                p_buf = self.harvester.get(p.mint)
                if p_buf and p_buf.latest_price > 0 and p.entry_price > 0:
                    p_gain = (p_buf.latest_price - p.entry_price) / p.entry_price
                    total_unrealized += (
                        p_gain * p.usd_size / self.db.balance
                        if self.db.balance > 0 else 0.0
                    )
            if total_unrealized <= Settings.PORTFOLIO_HEAT_LIMIT_PCT:
                self._portfolio_heat_until = (
                    time.time() + Settings.PORTFOLIO_HEAT_PAUSE_MINUTES * 60
                )
                reason = f"PORTFOLIO_HEAT_EXCEEDED (unrealized={total_unrealized:+.2%}, limit={Settings.PORTFOLIO_HEAT_LIMIT_PCT:.0%})"
                log.warning(f"  🔥 {buf.symbol}: {reason} — pausing new entries")
                await self.db.insert_filter_rejection(
                    mint=buf.mint, symbol=buf.symbol, rejection_reason=reason,
                    price_usd=price, liquidity_usd=liquidity, volume_5m=volume,
                    buys_5m=buys, sells_5m=sells, market_cap=mcap,
                    buy_ratio=buy_ratio,
                )
                return

        log.info(
            f"  📈 {buf.symbol}: All filters PASSED │ "
            f"Vol=${volume:,.0f} Liq=${liquidity:,.0f} MCap=${mcap:,.0f} "
            f"BuyR={buy_ratio:.2f} │ "
            f"H={H or 0:.4f} CVD={cvd_slope or 0:.4f} G={gini or 0:.4f} │ "
            f"Accel={vol_accel.accel_ratio or 0:.2f}x"
        )

        # ── 12. Position sizing ──────────────────────────────
        usd_size, was_capped = self._calculate_position_size(liquidity)
        if usd_size <= 0:
            log.info(
                f"  ⏭️ {buf.symbol}: Position too small "
                f"(min=${Settings.MIN_TRADE_SIZE_USD:.2f})"
            )
            return

        if was_capped:
            await self.db.insert_system_event(
                event_type="SIZE_CAPPED", severity="INFO",
                description=(
                    f"Position size capped for {buf.symbol}: "
                    f"desired=${self.db.balance * Settings.POSITION_PCT:.2f}, "
                    f"max_liq=${liquidity * Settings.MAX_LIQUIDITY_PCT:.2f}, "
                    f"actual=${usd_size:.2f}"
                ),
            )

        # ── 13. Compute per-trade TP and SL ─────────────────
        atr_val = compute_atr(buf.prices, Settings.ATR_LOOKBACK_PERIODS)
        if atr_val is not None:
            per_trade_tp = Settings.ATR_TP_MULTIPLIER * atr_val + Settings.ATR_TP_COST_BUFFER
            per_trade_tp = max(Settings.ATR_TP_MIN, min(Settings.ATR_TP_MAX, per_trade_tp))
            tp_source = "ATR"
        else:
            per_trade_tp = self.optimizer.current_tp
            tp_source = "optimizer_fallback"

        # Liquidity-scaled stop loss
        if liquidity < Settings.SL_THIN_POOL_THRESHOLD:
            per_trade_sl = Settings.SL_THIN_POOL_PCT
        elif liquidity < Settings.SL_DEEP_POOL_THRESHOLD:
            per_trade_sl = Settings.SL_MEDIUM_POOL_PCT
        else:
            per_trade_sl = Settings.SL_DEEP_POOL_PCT

        # ── 14. ENTER TRADE ──────────────────────────────────
        entry_price = buf.latest_price
        log.info(
            f" {'━' * 50}\n"
            f"  🎯 PAPER BUY: {buf.symbol} @ ${entry_price:.10f} │ "
            f"Size=${usd_size:.2f} │ "
            f"H={H or 0:.4f} │ CVD_slope={cvd_slope or 0:.4f} │ "
            f"G={gini or 0:.4f} │ TP={per_trade_tp:.0%} ({tp_source}) │ "
            f"SL={per_trade_sl:.0%}\n"
            f" {'━' * 50}"
        )

        trade_id = await self.db.open_paper_trade(
            mint=buf.mint, symbol=buf.symbol, entry_price=entry_price,
            usd_size=usd_size, entry_liquidity=liquidity,
            entry_volume_5m=volume, entry_market_cap=mcap,
            entry_buy_ratio=buy_ratio,
            entry_atr=atr_val or 0.0,
            per_trade_tp=per_trade_tp,
            per_trade_sl=per_trade_sl,
            pair_created_at=buf.pair_created_at,
        )
        await self.db.insert_quant_signal(
            trade_id=trade_id,
            hurst_value=H or 0.0, cvd_value=cvd_val or 0.0,
            cvd_slope=cvd_slope or 0.0, gini_coeff=gini,
            snapshot_count=buf.count, buy_ratio=buy_ratio,
        )
        now = time.time()
        self.positions[buf.mint] = LivePaperPosition(
            trade_id=trade_id, mint=buf.mint, symbol=buf.symbol,
            entry_time=now, entry_price=entry_price,
            peak_high=entry_price, peak_low=entry_price,
            usd_size=usd_size, entry_liquidity=liquidity,
            was_size_capped=was_capped, last_tick_time=now,
            original_usd_size=usd_size,
            entry_atr=atr_val or 0.0,
            per_trade_tp=per_trade_tp,
            per_trade_sl=per_trade_sl,
            entry_timestamp=now,
        )

        # Promote to Tier 1 so the open position gets fast 2s polling
        self.harvester.poller.promote_to_tier1(buf.mint)
        log.info(f"📡 Promoted {buf.symbol} to TIER 1 (open position)")

    async def _get_gini(self, buf: TokenBuffer) -> Optional[float]:
        cached = self.harvester.get_cached_gini(buf.mint)
        if cached is not None:
            return cached
        log.info(f"  🔍 Fetching top holders for {buf.symbol}…")
        balances = await self.harvester.fetch_top_holder_balances(buf.mint)
        if balances is None:
            return None
        gini_val = gini_coefficient(balances)
        if gini_val is not None:
            self.harvester.cache_gini(buf.mint, gini_val)
        return gini_val

    # ══════════════════════════════════════════════════════════
    # Risk Management
    # ══════════════════════════════════════════════════════════

    async def _check_circuit_breaker(self):
        """Check if daily loss limit has been exceeded."""
        if self._day_start_balance <= 0:
            return

        current_balance = self.db.balance
        daily_loss_amount = self._day_start_balance - current_balance
        loss_pct = daily_loss_amount / self._day_start_balance

        log.debug(
            f"Circuit breaker check: day_start=${self._day_start_balance:,.2f} "
            f"current=${current_balance:,.2f} "
            f"daily_pnl={-daily_loss_amount:+,.2f} ({-loss_pct:+.1%}) "
            f"threshold={Settings.DAILY_LOSS_LIMIT_PCT:.0%}"
        )

        if loss_pct >= Settings.DAILY_LOSS_LIMIT_PCT:
            self._circuit_breaker_until = (
                time.time() + Settings.CIRCUIT_BREAKER_MINUTES * 60
            )
            self._circuit_breaker_active = True

            log.critical(
                f"CIRCUIT_BREAKER: Daily loss {loss_pct:.1%} exceeds "
                f"{Settings.DAILY_LOSS_LIMIT_PCT:.0%} limit. "
                f"Paused for {Settings.CIRCUIT_BREAKER_MINUTES:.0f} minutes."
            )
            await self.db.insert_system_event(
                event_type="CIRCUIT_BREAKER_TRIGGERED", severity="CRITICAL",
                description=(
                    f"Daily loss {loss_pct:.1%} exceeds "
                    f"{Settings.DAILY_LOSS_LIMIT_PCT:.0%}. "
                    f"Paused until {time.ctime(self._circuit_breaker_until)}"
                ),
                metadata={
                    "day_start_balance": self._day_start_balance,
                    "current_balance": current_balance,
                    "daily_loss_amount": daily_loss_amount,
                    "loss_pct": loss_pct,
                    "resume_time": self._circuit_breaker_until,
                },
            )

    async def _check_day_reset(self):
        """Reset daily tracking at UTC midnight."""
        current_day_start = self._utc_day_start()
        if current_day_start > self._day_start_ts:
            self._day_start_ts = current_day_start
            self._day_start_balance = await self.db.get_balance_at(current_day_start)
            log.info(
                f"📅 Day rollover: new day_start_balance=${self._day_start_balance:,.2f} "
                f"(midnight UTC: {time.ctime(current_day_start)})"
            )

    @staticmethod
    def _utc_day_start() -> float:
        """Get timestamp of the start of the current UTC day."""
        import calendar
        import datetime
        now = datetime.datetime.utcnow()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return calendar.timegm(midnight.timetuple())

    # ══════════════════════════════════════════════════════════
    # Graceful Shutdown
    # ══════════════════════════════════════════════════════════

    async def shutdown(self):
        """Close all open positions at current market price on shutdown."""
        if not self.positions:
            return
        log.warning(f"🛑 Shutting down — closing {len(self.positions)} open positions")
        for mint, pos in list(self.positions.items()):
            try:
                buf = self.harvester.get(mint)
                if buf and buf.latest_price > 0:
                    exit_price = buf.latest_price
                else:
                    exit_price = pos.entry_price  # Flat close if no data
                await self._exit_trade(pos, exit_price, "SHUTDOWN")
                log.info(f"  ✅ Closed {pos.trade_id} ({pos.symbol}) at ${exit_price:.10f}")
            except Exception as e:
                log.error(f"  ❌ Failed to close {pos.trade_id}: {e}")

        await self.db.insert_system_event(
            event_type="SHUTDOWN", severity="INFO",
            description="Graceful shutdown complete",
        )

    # ══════════════════════════════════════════════════════════
    # Hourly Performance Snapshots
    # ══════════════════════════════════════════════════════════

    async def _maybe_snapshot(self):
        """Generate hourly performance snapshot if due."""
        now = time.time()
        if now - self._last_snapshot_time >= self._snapshot_interval:
            period_start = self._last_snapshot_time
            period_end = now
            self._last_snapshot_time = now
            await self.db.insert_performance_snapshot(period_start, period_end)

    # ══════════════════════════════════════════════════════════
    # Reporting
    # ══════════════════════════════════════════════════════════

    async def _print_report(self):
        stats = await self.db.get_session_stats()
        log.info(
            f"{'═' * 60}\n📊 PERFORMANCE REPORT (cycle #{self._cycle_count})\n"
            f"{'─' * 60}"
        )
        log.info(
            f" Database ticks:   {stats['total_ticks']:>10,}\n"
            f" Tokens tracked:   {len(self.harvester.tokens):>10}\n"
            f" Total paper trades: {stats['total_trades']:>10}\n"
            f" Currently open:     {stats['open_trades']:>10}\n"
            f" Closed trades:      {stats['closed_trades']:>10}\n"
            f" Balance:            ${stats['balance']:>12,.2f}"
        )
        if stats["closed_trades"] > 0:
            log.info(
                f" Win rate:        {stats['win_rate']:>9.1%}\n"
                f" Avg PnL:         {stats['avg_pnl']:>+9.2%}\n"
                f" Best trade:      {stats['best_pnl']:>+9.2%}\n"
                f" Worst trade:     {stats['worst_pnl']:>+9.2%}"
            )
            # ── Enhanced metrics (v4.1) ──────────────────────
            ext = await self.db.get_extended_stats()
            if ext:
                log.info(
                    f" Payoff ratio:    {ext['payoff_ratio']:>9.2f}\n"
                    f" Profit factor:   {ext['profit_factor']:>9.2f}\n"
                    f" Expectancy:      {ext['expectancy']:>+9.4f}\n"
                    f" Avg hold time:   {ext['avg_hold_minutes']:>8.1f}m"
                )
            # ── Filter rejection breakdown ───────────────────
            rej = await self.db.get_filter_rejection_counts(
                since=self._last_report_time - self._report_interval
            )
            if rej:
                parts = [f"{k}={v}" for k, v in rej.items() if v > 0]
                if parts:
                    log.info(f" Rejections:  {', '.join(parts)}")
        log.info(
            f"{'─' * 60}\n"
            f"  🎯 Dynamic TP:\n"
            f"  Current level: {self.optimizer.current_tp:>9.0%}\n"
            f"  Expected EV: {self.optimizer.best_ev:>+9.2%}\n"
            f"  Hit rate: {self.optimizer.hit_rate:>9.0%}\n"
            f"  Sample size: {self.optimizer.sample_size:>9}\n"
            f"  Confidence: {self.optimizer.confidence:>9}\n"
            f"  Optimizations: {self.optimizer.run_count:>9}"
        )
        log.info(
            f"{'─' * 60}\n"
            f" Rate limiters:\n"
            f"   {self.harvester.dex_limiter.stats}\n"
            f"   {self.harvester.rpc_limiter.stats}\n"
            f"{'═' * 60}"
        )
