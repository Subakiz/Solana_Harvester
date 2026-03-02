"""
Paper Trading Simulation Engine.
v2.0 — Dynamic MFE-Optimized Take Profit
─────────────────────────────────────────
The static TAKE_PROFIT_PCT is replaced by a self-optimizing calculator that searches for T* = argmax EV(T) over historical Maximum Favorable Excursion data.
"""
import asyncio
import time
from dataclasses import dataclass
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
OPTIMIZER_TRADE_INTERVAL = 20 # Re-optimize every N new closed trades
OPTIMIZER_TIME_INTERVAL = 3600.0 # Re-optimize at least every hour
OPTIMIZER_MIN_TRADES = 5 # Minimum trades before first optimization
OPTIMIZER_LOOKBACK = 100 # How many recent trades to feed optimizer
DEFAULT_TAKE_PROFIT = 0.20 # 20% default before optimizer has data

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
        if self.ev_curve is None: self.ev_curve = []

class PaperTradingEngine:
    """
    Orchestrates the full simulation loop each cycle:
    1. Monitor & exit existing paper positions.
    2. Scan tokens for new entry signals.
    3. Validate with Gini, then open paper trade.
    4. Periodically re-optimize take-profit level from MFE data.
    """
    def __init__(self, db: DatabaseManager, harvester: DataHarvester):
        self.db = db
        self.harvester = harvester
        self.positions: dict[str, LivePaperPosition] = {} # mint -> pos
        self._cycle_count = 0
        self._last_report_time = time.time()
        self._report_interval = 120.0 # seconds
        self.optimizer = OptimizerState()
        self._closed_trade_count_at_last_check: int = 0

    async def initialize(self):
        """Load open trades from previous session and run initial optimization."""
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
            )
            self.positions[pos.mint] = pos
            
            # RE-REGISTER ZOMBIE TRADES WITH HARVESTER
            if pos.mint not in self.harvester.tokens:
                self.harvester.tokens[pos.mint] = TokenBuffer(
                    mint=pos.mint,
                    symbol=pos.symbol,
                    name=pos.symbol,
                    pair_address="",
                    dex_id=""
                )
            log.info(f"♻️ Recovered open trade: {pos.trade_id} │ {pos.symbol} @ ${pos.entry_price:.10f}")
            
        if self.positions:
            log.info(f"♻️ Recovered {len(self.positions)} open paper trades")
        await self._maybe_reoptimize_tp(force=True)

    async def _maybe_reoptimize_tp(self, force: bool = False):
        """Check if re-optimization is due and run if needed."""
        now = time.time()
        current_closed_count = await self.db.get_closed_trade_count()
        trades_since_last = current_closed_count - self.optimizer.last_run_trade_count
        time_since_last = now - self.optimizer.last_run_time
        
        should_run = force or (
            current_closed_count >= OPTIMIZER_MIN_TRADES and (
                trades_since_last >= OPTIMIZER_TRADE_INTERVAL or 
                time_since_last >= OPTIMIZER_TIME_INTERVAL
            )
        )
        if not should_run: return

        log.info(f"🧮 Running MFE Take-Profit Optimizer (trades_since_last={trades_since_last}, total={current_closed_count})")
        recent_trades = await self.db.get_recent_closed_trades(limit=OPTIMIZER_LOOKBACK)
        
        if len(recent_trades) < OPTIMIZER_MIN_TRADES:
            log.info(f" Optimizer: only {len(recent_trades)} trades available, keeping TP={self.optimizer.current_tp:.0%}")
            self.optimizer.last_run_time = now
            self.optimizer.last_run_trade_count = current_closed_count
            return

        ep = np.array([t["entry_price"] for t in recent_trades], dtype=np.float64)
        ph = np.array([t["peak_high"] for t in recent_trades], dtype=np.float64)
        ap = np.array([t["pnl_pct"] for t in recent_trades], dtype=np.float64)
        
        old_tp = self.optimizer.current_tp
        optimal_tp, diagnostics = calculate_optimal_tp(entry_prices=ep, peak_highs=ph, actual_pnls=ap, default_tp=DEFAULT_TAKE_PROFIT)
        
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
        
        log.info(f"🎯 TP UPDATED: {old_tp:.0%} → {smoothed_tp:.0%} (raw={optimal_tp:.0%}) │ EV={diagnostics['best_ev']:+.2%} │ Hit={diagnostics['hit_rate']:.0%} │ Confidence={diagnostics['confidence']}")

    async def run_cycle(self):
        """One complete paper-trading evaluation cycle."""
        self._cycle_count += 1
        log.info(f"{'═' * 60}")
        log.info(f"🔄 PAPER CYCLE #{self._cycle_count} │ TP={self.optimizer.current_tp:.0%} ({self.optimizer.confidence})")
        log.info(f"{'═' * 60}")
        
        await self._maybe_reoptimize_tp()
        ticks = await self._ingest()
        await self._manage_positions()
        await self._scan_entries()
        
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

    async def _manage_positions(self):
        if not self.positions:
            # log.debug(" No open paper positions.") # Reduced noise
            return
        # log.info(f"📋 Managing {len(self.positions)} open paper positions…")
        for mint, pos in list(self.positions.items()):
            try:
                buf = self.harvester.get(mint)
                # Wait for harvester to fetch fresh tick data
                if buf is None or buf.count == 0:
                    continue
                    
                current_price = buf.latest_price
                if current_price <= 0:
                    continue

                if current_price > pos.peak_high: pos.peak_high = current_price
                if current_price < pos.peak_low or pos.peak_low <= 0: pos.peak_low = current_price
                
                await self.db.update_paper_trade_extremes(
                    pos.trade_id, pos.peak_high, pos.peak_low
                )
                
                exit_reason = self._check_exits(pos, current_price)
                if exit_reason:
                    await self._exit_trade(pos, current_price, exit_reason)
                else:
                    pnl = (current_price - pos.entry_price) / pos.entry_price if pos.entry_price else 0
                    age = time.time() - pos.entry_time
                    log.debug(f"  📊 {pos.symbol}: ${current_price:.10f} │ PnL={pnl:+.2%} │ TP@{self.optimizer.current_tp:.0%}")
            except Exception as e:
                log.error(f"Error managing position {pos.symbol}: {e}")

    def _check_exits(self, pos: LivePaperPosition, price: float) -> Optional[str]:
        """Evaluate dynamic take profit, hard stop, trailing stop, and time stop."""
        ep = pos.entry_price
        if ep <= 0: return None

        gain = (price - ep) / ep
        
        # 0. Dynamic Take Profit (MFE-Optimized)
        if gain >= self.optimizer.current_tp:
            return f"DYNAMIC_TP (+{gain*100:.1f}% gain)"

        # 1. Hard stop: price drops 15% below entry
        drawdown_from_entry = (ep - price) / ep
        if drawdown_from_entry >= Settings.HARD_STOP_PCT:
            return f"HARD_STOP (−{drawdown_from_entry*100:.1f}% from entry)"

        # 2. Trailing stop: price drops 10% from peak_high
        if pos.peak_high > 0:
            drawdown_from_high = (pos.peak_high - price) / pos.peak_high
            if drawdown_from_high >= Settings.TRAILING_STOP_PCT:
                return f"TRAILING_STOP (−{drawdown_from_high*100:.1f}% from peak)"
        
        # 3. Time stop: trade open longer than settings allow
        age = time.time() - pos.entry_time
        if age >= Settings.TIME_STOP_SECONDS:
            return f"TIME_STOP ({age / 60:.1f} min)"
            
        return None

    async def _exit_trade(self, pos: LivePaperPosition, exit_price: float, reason: str):
        await self.db.close_paper_trade(pos.trade_id, exit_price, reason)
        del self.positions[pos.mint]

    async def _scan_entries(self):
        analyzable = self.harvester.analyzable_tokens()
        log.info(f"🔬 Scanning {len(analyzable)} tokens for entry signals…")
        for buf in analyzable:
            if buf.mint in self.positions: continue
            if await self.db.is_mint_in_open_trade(buf.mint): continue
            if len(self.positions) >= Settings.MAX_OPEN_TRADES:
                log.info(f"  ⛔ Max open trades ({Settings.MAX_OPEN_TRADES}) — skipping scan")
                return
            await self._evaluate_entry(buf)

    async def _evaluate_entry(self, buf: TokenBuffer):
        H = hurst_exponent(buf.prices)
        if H is None or H <= Settings.HURST_THRESHOLD: return
        log.info(f"  ✅ {buf.symbol}: H={H:.4f} > {Settings.HURST_THRESHOLD} — TRENDING regime")
        
        cvd_val, cvd_slope, is_bullish = micro_cvd(buf.buys, buf.sells, buf.volumes, buf.prices, lookback=Settings.CVD_LOOKBACK)
        if cvd_val is None or not is_bullish: return
        log.info(f"  ✅ {buf.symbol}: Bullish CVD divergence (CVD={cvd_val:.2f}, slope={cvd_slope:.4f})")
        
        gini = await self._get_gini(buf)
        if gini is None or gini >= Settings.MAX_GINI: return
        log.info(f"  ✅ {buf.symbol}: Gini={gini:.4f} < {Settings.MAX_GINI} — supply distribution OK")
        
        entry_price = buf.latest_price
        log.info(f" {'━' * 50}\n  🎯 PAPER BUY: {buf.symbol} @ ${entry_price:.10f} │ H={H:.4f} │ CVD_slope={cvd_slope:.4f} │ G={gini:.4f} │ TP={self.optimizer.current_tp:.0%}\n {'━' * 50}")
        
        trade_id = await self.db.open_paper_trade(mint=buf.mint, symbol=buf.symbol, entry_price=entry_price)
        await self.db.insert_quant_signal(trade_id=trade_id, hurst_value=H, cvd_value=cvd_val, cvd_slope=cvd_slope, gini_coeff=gini, snapshot_count=buf.count)
        self.positions[buf.mint] = LivePaperPosition(trade_id=trade_id, mint=buf.mint, symbol=buf.symbol, entry_time=time.time(), entry_price=entry_price, peak_high=entry_price, peak_low=entry_price)

    async def _get_gini(self, buf: TokenBuffer) -> Optional[float]:
        cached = self.harvester.get_cached_gini(buf.mint)
        if cached is not None: return cached
        log.info(f"  🔍 Fetching top holders for {buf.symbol}…")
        balances = await self.harvester.fetch_top_holder_balances(buf.mint)
        if balances is None: return None
        gini_val = gini_coefficient(balances)
        if gini_val is not None: self.harvester.cache_gini(buf.mint, gini_val)
        return gini_val

    async def _print_report(self):
        stats = await self.db.get_session_stats()
        log.info(f"{'═' * 60}\n📊 PERFORMANCE REPORT (cycle #{self._cycle_count})\n{'─' * 60}")
        log.info(f" Database ticks:   {stats['total_ticks']:>10,}\n Tokens tracked:   {len(self.harvester.tokens):>10}\n Total paper trades: {stats['total_trades']:>10}\n Currently open:     {stats['open_trades']:>10}\n Closed trades:      {stats['closed_trades']:>10}")
        if stats["closed_trades"] > 0:
            log.info(f" Win rate:        {stats['win_rate']:>9.1%}\n Avg PnL:         {stats['avg_pnl']:>+9.2%}\n Best trade:      {stats['best_pnl']:>+9.2%}\n Worst trade:     {stats['worst_pnl']:>+9.2%}")
        log.info(f"{'─' * 60}\n  🎯 Dynamic TP:\n  Current level: {self.optimizer.current_tp:>9.0%}\n  Expected EV: {self.optimizer.best_ev:>+9.2%}\n  Hit rate: {self.optimizer.hit_rate:>9.0%}\n  Sample size: {self.optimizer.sample_size:>9}\n  Confidence: {self.optimizer.confidence:>9}\n  Optimizations: {self.optimizer.run_count:>9}")
        log.info(f"{'─' * 60}\n Rate limiters:\n   {self.harvester.dex_limiter.stats}\n   {self.harvester.rpc_limiter.stats}\n{'═' * 60}")
