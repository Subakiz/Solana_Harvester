#!/usr/bin/env python3
"""
query_results.py — Quick analysis of harvested data.
Run separately: python query_results.py
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = "data/quant_harvest.db"

def main():
    if not Path(DB_PATH).exists():
        print(f"Error: Database file '{DB_PATH}' not found.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("\n" + "=" * 70)
    print("        QUANT HARVEST DATABASE REPORT")
    print("=" * 70)
    
    # Total ticks
    row = conn.execute("SELECT COUNT(*) AS n FROM market_ticks").fetchone()
    print(f"\n  Total market ticks:   {row['n']:,}")
    
    # Unique tokens tracked
    row = conn.execute(
        "SELECT COUNT(DISTINCT mint) AS n FROM market_ticks"
    ).fetchone()
    print(f"  Unique tokens seen:   {row['n']:,}")
    
    # Trade summary
    print(f"\n{'─' * 70}")
    print("  PAPER TRADES")
    print(f"{'─' * 70}")
    
    trades = conn.execute("""
        SELECT t.trade_id, t.symbol, t.entry_price, t.exit_price, t.peak_high, t.peak_low, t.pnl_pct, t.exit_reason, t.status,
               s.hurst_value, s.cvd_value, s.cvd_slope, s.gini_coeff
        FROM paper_trades t
        LEFT JOIN quant_signals s ON t.trade_id = s.trade_id
        ORDER BY t.entry_time DESC
        LIMIT 50
    """).fetchall()
    
    if not trades:
        print("  No paper trades recorded yet.")
    else:
        for t in trades:
            pnl = t["pnl_pct"]
            pnl_str = f"{pnl:+.2%}" if pnl is not None else "OPEN"
            emoji = "🟢" if (pnl or 0) > 0 else ("🔴" if (pnl or 0) < 0 else "⏳")
            
            mfe = 0
            if t["entry_price"] and t["peak_high"]:
                mfe = (t["peak_high"] - t["entry_price"]) / t["entry_price"]
            
            mae = 0
            if t["entry_price"] and t["peak_low"]:
                mae = (t["entry_price"] - t["peak_low"]) / t["entry_price"]
                
            print(
                f"  {emoji} {t['trade_id']} | {t['symbol']:>10s} | "
                f"PnL={pnl_str:>8s} | MFE={mfe:+.2%} | MAE={mae:+.2%} | "
                f"H={t['hurst_value'] or 0:.3f} | G={t['gini_coeff'] or 0:.3f} | "
                f"{t['status']} {t['exit_reason'] or ''}"
            )
            
    # Aggregate
    agg = conn.execute("""
        SELECT COUNT(*) AS total, SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
               AVG(pnl_pct) AS avg_pnl, MIN(pnl_pct) AS worst, MAX(pnl_pct) AS best
        FROM paper_trades
        WHERE status = 'CLOSED'
    """).fetchone()
    
    if agg["total"]:
        wr = (agg["wins"] or 0) / agg["total"]
        print(f"\n  Closed: {agg['total']} | Win rate: {wr:.1%} | "
              f"Avg PnL: {agg['avg_pnl']:+.2%} | "
              f"Best: {agg['best']:+.2%} | Worst: {agg['worst']:+.2%}")
              
    conn.close()
    print(f"\n{'=' * 70}\n")

if __name__ == "__main__":
    main()
