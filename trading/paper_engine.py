"""
Paper Trading Simulation Engine.
v3.0 — Complete Refactor: Statistical Edge Optimization
────────────────────────────────────────────────────────
Changes from v2.0:
- Trailing stop REMOVED (6.4% win rate, -$28M losses)
- Hard stop REMOVED → replaced by rug protection at -50%
- Entry filters: volume, liquidity, market cap, buy ratio, activity
- Hurst/CVD/Gini demoted from gates to logging only
- Liquidity-aware position sizing with cost model
- Risk management: circuit breaker, cooldown, position limits
- Data staleness handling (60s/120s/180s staged)
- Hourly performance snapshots
"""
import asyncio
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
)
from utils.logger import get_logger

log = get_logger("PaperEngine")

# ── Optimizer scheduling constants ───────────────────────────
OPTIMIZER_TRADE_INTERVAL = 20   # Re-optimize every N new closed trades
OPTIMIZER_TIME_INTERVAL = 3600.0  # Re-optimize at least every hour
OPTIMIZER_MIN_TRADES = 5        # Minimum trades before first optimization
OPTIMIZER_LOOKBACK = 100        # How many recent trades to feed optimizer
DEFAULT_TAKE_PROFIT = 0.20      # 20% default before optimizer has data


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

        # ── Hourly snapshots ─────────────────────────────────
        self._last_snapshot_time: float = time.time()
        self._snapshot_interval: float = 3600.0  # 1 hour

    # ══════════════════════════════════════════════════════════
    # Initialization
    # ══════════════════════════════════════════════════════════

    async def initialize(self):
        """Load open trades from previous session and run initial optimization."""
        self._day_start_balance = self.db.balance

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
            log.info(
                f"♻️ Recovered open trade: {pos.trade_id} │ "
                f"{pos.symbol} @ ${pos.entry_price:.10f}"
            )

        if self.positions:
            log.info(f"♻️ Recovered {len(self.positions)} open paper trades")
        await self._maybe_reoptimize_tp(force=True)

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
        """One complete paper-trading evaluation cycle."""
        self._cycle_count += 1
        log.info(f"{'═' * 60}")
        log.info(
            f"🔄 PAPER CYCLE #{self._cycle_count} │ "
            f"TP={self.optimizer.current_tp:.0%} ({self.optimizer.confidence}) │ "
            f"Bal=${self.db.balance:,.2f}"
        )
        log.info(f"{'═' * 60}")

        # Reset day tracking if new UTC day
        self._check_day_reset()

        await self._maybe_reoptimize_tp()
        await self._ingest()
        await self._manage_positions()
        await self._scan_entries()

        # Hourly performance snapshots
        await self._maybe_snapshot()

        if time.time() - self._last_report_time >= self._report_interval:
            await self._print_report()
            self._last_report_time = time.time()

    async def _ingest(self) -> int:
        """Poll DexScreener and persist every tick to SQLite."""
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
                        f"PnL={pnl:+.2%} │ TP@{self.optimizer.current_tp:.0%}"
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
        Evaluate exit conditions. Order:
        1. DYNAMIC_TP (MFE-Optimized) — kept exactly as-is
        2. RUG_PROTECTION at -50% — replaces hard stop
        3. TIME_STOP at 30 minutes — kept exactly as-is
        (Trailing stop REMOVED, hard stop REMOVED)
        """
        ep = pos.entry_price
        if ep <= 0:
            return None

        gain = (price - ep) / ep

        # 1. Dynamic Take Profit (MFE-Optimized) — PRESERVED
        if gain >= self.optimizer.current_tp:
            return f"DYNAMIC_TP (+{gain * 100:.1f}% gain)"

        # 2. Rug Protection: price drops 50% from entry
        if gain <= Settings.RUG_PROTECTION_PCT:
            log.warning(
                f"⚠️ RUG_PROTECTION triggered for {pos.symbol}: "
                f"{gain * 100:.1f}% loss — entry filters may have failed"
            )
            return f"RUG_PROTECTION (-{abs(gain) * 100:.1f}% from entry)"

        # 3. Time stop: trade open longer than 30 minutes
        age = time.time() - pos.entry_time
        if age >= Settings.TIME_STOP_SECONDS:
            return f"TIME_STOP ({age / 60:.1f} min)"

        return None

    async def _exit_trade(self, pos: LivePaperPosition, exit_price: float, reason: str):
        """Close trade with cost model applied."""
        # Calculate cost model
        cost_info = self._calculate_costs(pos, exit_price)

        result = await self.db.close_paper_trade(
            pos.trade_id, exit_price, reason, cost_info=cost_info
        )
        del self.positions[pos.mint]

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

    def _calculate_costs(self, pos: LivePaperPosition, exit_price: float) -> dict:
        """Apply fee + slippage cost model to paper trade."""
        entry_price = pos.entry_price
        usd_size = pos.usd_size
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

        return {
            "fee_pct": fee_pct,
            "slippage_pct": slippage_pct,
            "was_size_capped": pos.was_size_capped,
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
        # Check circuit breaker
        now = time.time()
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

        for buf in analyzable:
            if buf.mint in self.positions:
                continue
            if await self.db.is_mint_in_open_trade(buf.mint):
                continue

            # Max concurrent positions
            if len(self.positions) >= Settings.MAX_OPEN_TRADES:
                log.info(
                    f"  ⛔ Max open trades ({Settings.MAX_OPEN_TRADES}) — "
                    f"skipping scan"
                )
                await self.db.insert_filter_rejection(
                    mint=buf.mint, symbol=buf.symbol,
                    rejection_reason=f"MAX_POSITIONS_REACHED (open={len(self.positions)}, max={Settings.MAX_OPEN_TRADES})",
                    price_usd=buf.latest_price,
                )
                await self.db.insert_system_event(
                    event_type="MAX_POSITIONS_BLOCKED", severity="INFO",
                    description=f"Entry rejected for {buf.symbol}: max positions reached",
                )
                return

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

            await self._evaluate_entry(buf)

    async def _evaluate_entry(self, buf: TokenBuffer):
        """
        Entry filter chain (fastest rejections first):
        1. Activity check: buys + sells >= 3
        2. Liquidity check: liquidity >= 20,000
        3. Volume check: volume_5m >= 500
        4. Market cap check: 50K <= mcap <= 2M
        5. Buy ratio check: 0.40 <= ratio <= 0.75
        6. Calculate Hurst/CVD/Gini (for LOGGING only, not gating)
        7. Position sizing check
        8. ENTER TRADE
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
        buy_ratio = buys / total_txns  # total_txns >= 3 guaranteed
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

        # ── 6. Calculate Hurst/CVD/Gini (logging only, NOT gating) ──
        H = hurst_exponent(buf.prices)
        cvd_val, cvd_slope, is_bullish = micro_cvd(
            buf.buys, buf.sells, buf.volumes, buf.prices,
            lookback=Settings.CVD_LOOKBACK,
        )
        gini = await self._get_gini(buf)

        log.info(
            f"  📈 {buf.symbol}: Filters PASSED │ "
            f"Vol=${volume:,.0f} Liq=${liquidity:,.0f} MCap=${mcap:,.0f} "
            f"BuyR={buy_ratio:.2f} │ "
            f"H={H or 0:.4f} CVD={cvd_slope or 0:.4f} G={gini or 0:.4f}"
        )

        # ── 7. Position sizing ───────────────────────────────
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

        # ── 8. ENTER TRADE ───────────────────────────────────
        entry_price = buf.latest_price
        log.info(
            f" {'━' * 50}\n"
            f"  🎯 PAPER BUY: {buf.symbol} @ ${entry_price:.10f} │ "
            f"Size=${usd_size:.2f} │ "
            f"H={H or 0:.4f} │ CVD_slope={cvd_slope or 0:.4f} │ "
            f"G={gini or 0:.4f} │ TP={self.optimizer.current_tp:.0%}\n"
            f" {'━' * 50}"
        )

        trade_id = await self.db.open_paper_trade(
            mint=buf.mint, symbol=buf.symbol, entry_price=entry_price,
            usd_size=usd_size, entry_liquidity=liquidity,
            entry_volume_5m=volume, entry_market_cap=mcap,
            entry_buy_ratio=buy_ratio,
        )
        await self.db.insert_quant_signal(
            trade_id=trade_id,
            hurst_value=H or 0.0, cvd_value=cvd_val or 0.0,
            cvd_slope=cvd_slope or 0.0, gini_coeff=gini or 0.0,
            snapshot_count=buf.count, buy_ratio=buy_ratio,
        )
        self.positions[buf.mint] = LivePaperPosition(
            trade_id=trade_id, mint=buf.mint, symbol=buf.symbol,
            entry_time=time.time(), entry_price=entry_price,
            peak_high=entry_price, peak_low=entry_price,
            usd_size=usd_size, entry_liquidity=liquidity,
            was_size_capped=was_capped, last_tick_time=time.time(),
        )

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
        daily_loss = await self.db.get_daily_realized_loss(self._day_start_ts)
        if self._day_start_balance <= 0:
            return

        loss_pct = abs(daily_loss) / self._day_start_balance
        if loss_pct >= Settings.DAILY_LOSS_LIMIT_PCT:
            self._circuit_breaker_until = (
                time.time() + Settings.CIRCUIT_BREAKER_MINUTES * 60
            )
            self._circuit_breaker_active = True
            # Reset threshold to current balance after CB
            self._day_start_balance = self.db.balance

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
                    "daily_loss": daily_loss,
                    "loss_pct": loss_pct,
                    "resume_time": self._circuit_breaker_until,
                },
            )

    def _check_day_reset(self):
        """Reset daily tracking at UTC midnight."""
        current_day_start = self._utc_day_start()
        if current_day_start > self._day_start_ts:
            self._day_start_ts = current_day_start
            self._day_start_balance = self.db.balance

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
