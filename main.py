#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════╗
║     SOLANA MEME COIN DATA HARVESTER & PAPER TRADING ENGINE        ║
║                                                                   ║
║    Quantitative Models:                                           ║
║    • Hurst Exponent (R/S) — Regime detection                      ║
║    • Micro-CVD — Order-flow proxy                                 ║
║    • Gini Coefficient — Rug-pull defense                          ║
║                                                                   ║
║    Output: SQLite database (quant_harvest.db)                     ║
║    Tables: market_ticks, paper_trades, quant_signals              ║
║                                                                   ║
║    NO LIVE TRADES — Paper simulation only.                        ║
╚═══════════════════════════════════════════════════════════════════╝
"""
import asyncio
import signal
import sys
import time
from config.settings import Settings
from db.manager import DatabaseManager
from ingestion.harvester import DataHarvester
from trading.paper_engine import PaperTradingEngine
from utils.logger import get_logger

log = get_logger("Main")

_shutdown = False

def _on_signal(signum, _frame):
    global _shutdown
    sig_name = signal.Signals(signum).name
    log.warning(f"🛑 Received {sig_name} — initiating graceful shutdown…")
    _shutdown = True

async def main():
    global _shutdown
    
    # ── Banner ───────────────────────────────────────────────
    print()
    log.info("╔═══════════════════════════════════════════════════════════╗")
    log.info("║   SOLANA DATA HARVESTER & PAPER TRADING ENGINE v1.0.0     ║")
    log.info("╚═══════════════════════════════════════════════════════════╝")
    print()
    
    # ── Config summary ───────────────────────────────────────
    log.info("Configuration:")
    for k, v in Settings.summary().items():
        log.info(f"  {k:>15s}: {v}")
    print()
    
    # ── Initialize components ────────────────────────────────
    db = DatabaseManager()
    await db.initialize()
    
    harvester = DataHarvester()
    engine = PaperTradingEngine(db, harvester)
    await engine.initialize()
    
    log.info(
        f"🚀 Engine ready. Polling every {Settings.POLL_INTERVAL}s. "
        f"Database: {Settings.DB_PATH}"
    )
    log.info(
        f"🔢 Entry: H>{Settings.HURST_THRESHOLD} + bullish CVD + "
        f"G<{Settings.MAX_GINI}"
    )
    log.info(
        f"🛑 Exits: Hard={Settings.HARD_STOP_PCT:.0%} | "
        f"Trail={Settings.TRAILING_STOP_PCT:.0%} | "
        f"Time={Settings.TIME_STOP_MINUTES}min"
    )
    print()
    
    # ── Signal handlers ──────────────────────────────────────
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    
    # ── Main loop ────────────────────────────────────────────
    consecutive_errors = 0
    max_errors = 15
    
    try:
        while not _shutdown:
            t0 = time.time()
            
            try:
                await engine.run_cycle()
                consecutive_errors = 0
            except KeyboardInterrupt:
                _shutdown = True
                break
            except Exception as exc:
                consecutive_errors += 1
                log.error(
                    f"💥 Cycle error ({consecutive_errors}/{max_errors}): {exc}",
                    exc_info=True,
                )
                if consecutive_errors >= max_errors:
                    log.critical(
                        f"⛔ {max_errors} consecutive errors — "
                        f"shutting down for safety."
                    )
                    _shutdown = True
                    break
                
                # Exponential backoff capped at 60 s
                backoff = min(consecutive_errors * 3, 60)
                log.warning(f"   Backing off {backoff}s before retry…")
                await asyncio.sleep(backoff)
                continue
                
            # Sleep until next poll
            elapsed = time.time() - t0
            sleep = max(0.0, Settings.POLL_INTERVAL - elapsed)
            if sleep > 0:
                await asyncio.sleep(sleep)
                
    except Exception as fatal:
        log.critical(f"Fatal error: {fatal}", exc_info=True)
    finally:
        log.info("🧹 Shutting down…")
        
        # Final report
        try:
            stats = await db.get_session_stats()
            log.info("Final database stats:")
            for k, v in stats.items():
                if isinstance(v, float):
                    log.info(f"  {k}: {v:+.4f}")
                else:
                    log.info(f"  {k}: {v}")
        except Exception:
            pass
            
        # Warn about open positions
        if engine.positions:
            log.warning(
                f"⚠️ {len(engine.positions)} paper positions still open:"
            )
            for mint, pos in engine.positions.items():
                log.warning(f"  • {pos.symbol} ({pos.trade_id})")
                
        await harvester.close()
        await db.close()
        log.info("👋 Shutdown complete. Database saved.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)
