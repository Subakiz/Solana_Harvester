#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════╗
║     SOLANA MEME COIN DATA HARVESTER & PAPER TRADING ENGINE        ║
║                                                                   ║
║    Quantitative Models:                                           ║
║    • Hurst Exponent (R/S) — Regime detection (logged, not gating) ║
║    • Micro-CVD — Order-flow proxy (logged, not gating)            ║
║    • Gini Coefficient — Rug-pull defense (logged, not gating)     ║
║                                                                   ║
║    Entry Filters: Volume, Liquidity, Market Cap, Buy Ratio        ║
║    Exit: DYNAMIC_TP, RUG_PROTECTION (-50%), TIME_STOP (30min)     ║
║                                                                   ║
║    Output: SQLite database (quant_harvest.db)                     ║
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
    log.info("║   SOLANA DATA HARVESTER & PAPER TRADING ENGINE v3.0.0     ║")
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
        f"📊 Entry Filters: Vol≥${Settings.MIN_VOLUME_5M:.0f} | "
        f"Liq≥${Settings.MIN_LIQUIDITY:,.0f} | "
        f"MCap ${Settings.MIN_MARKET_CAP:,.0f}-${Settings.MAX_MARKET_CAP:,.0f} | "
        f"BuyR {Settings.MIN_BUY_RATIO:.0%}-{Settings.MAX_BUY_RATIO:.0%}"
    )
    log.info(
        f"🛑 Exits: RugProtect={Settings.RUG_PROTECTION_PCT:.0%} | "
        f"Time={Settings.TIME_STOP_MINUTES:.0f}min | "
        f"DYNAMIC_TP"
    )
    log.info(
        f"💰 Sizing: {Settings.POSITION_PCT:.0%} bal / "
        f"{Settings.MAX_LIQUIDITY_PCT:.0%} liq | "
        f"Max {Settings.MAX_OPEN_TRADES} positions"
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
                # Log to system_events
                try:
                    await db.insert_system_event(
                        event_type="CYCLE_ERROR", severity="ERROR",
                        description=str(exc),
                    )
                except Exception:
                    pass

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
        
        # Graceful shutdown: close all open positions
        try:
            await engine.shutdown()
        except Exception as e:
            log.error(f"Error during engine shutdown: {e}")

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
                
        await harvester.close()
        await db.close()
        log.info("👋 Shutdown complete. Database saved.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)
