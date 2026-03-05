#!/usr/bin/env python3
"""
analyze_db.py — Full diagnostic report for quant_harvest.db.
Queries every table and prints a comprehensive analysis report.
"""
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = "data/quant_harvest.db"

SEP  = "=" * 72
SEP2 = "─" * 72

def pct(val):
    if val is None: return "N/A"
    return f"{val:+.2%}"

def fmt(val, fmt_str=".2f"):
    if val is None: return "N/A"
    return format(val, fmt_str)

def ts_to_str(ts):
    if ts is None: return "N/A"
    try:
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)

def main():
    path = Path(DB_PATH)
    if not path.exists():
        print(f"Error: Database not found at '{DB_PATH}'")
        sys.exit(1)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row

    print(f"\n{SEP}")
    print("       QUANT HARVEST — FULL DIAGNOSTIC REPORT")
    print(SEP)
    print(f"  Database: {path.resolve()}")
    print(f"  Size:     {path.stat().st_size / 1024 / 1024:.2f} MB")

    # ──────────────────────────────────────────────────────────
    # 1. MARKET TICKS
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  1. MARKET TICKS")
    print(SEP2)

    r = conn.execute("SELECT COUNT(*) AS n FROM market_ticks").fetchone()
    total_ticks = r["n"]
    print(f"  Total ticks:          {total_ticks:,}")

    r = conn.execute("SELECT COUNT(DISTINCT mint) AS n FROM market_ticks").fetchone()
    unique_tokens = r["n"]
    print(f"  Unique tokens:        {unique_tokens:,}")

    r = conn.execute("SELECT MIN(timestamp) AS mn, MAX(timestamp) AS mx FROM market_ticks").fetchone()
    print(f"  Earliest tick:        {ts_to_str(r['mn'])}")
    print(f"  Latest tick:          {ts_to_str(r['mx'])}")
    if r["mn"] and r["mx"]:
        duration_h = (r["mx"] - r["mn"]) / 3600
        print(f"  Data span:            {duration_h:.1f} hours")

    # Top 10 most-tracked tokens
    top_tokens = conn.execute("""
        SELECT symbol, COUNT(*) AS cnt
        FROM market_ticks
        GROUP BY mint
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    print(f"\n  Top 10 most-tracked tokens:")
    for row in top_tokens:
        print(f"    {row['symbol']:<12s} {row['cnt']:>8,} ticks")

    # ──────────────────────────────────────────────────────────
    # 2. PAPER TRADES — WIN RATE & PNL
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  2. PAPER TRADES — OVERVIEW")
    print(SEP2)

    agg = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_cnt,
            SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) AS closed_cnt
        FROM paper_trades
    """).fetchone()
    print(f"  Total trades:         {agg['total']:,}")
    print(f"  Open trades:          {agg['open_cnt']:,}")
    print(f"  Closed trades:        {agg['closed_cnt']:,}")

    if agg["closed_cnt"] and agg["closed_cnt"] > 0:
        stats = conn.execute("""
            SELECT
                SUM(CASE WHEN net_pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN net_pnl_pct < 0 THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN net_pnl_pct = 0 OR net_pnl_pct IS NULL THEN 1 ELSE 0 END) AS flat,
                AVG(raw_pnl_pct)   AS avg_raw,
                MAX(raw_pnl_pct)   AS best_raw,
                MIN(raw_pnl_pct)   AS worst_raw,
                AVG(net_pnl_pct)   AS avg_net,
                MAX(net_pnl_pct)   AS best_net,
                MIN(net_pnl_pct)   AS worst_net,
                SUM(net_usd_pnl)   AS total_net_usd,
                AVG(fee_pct)       AS avg_fee,
                AVG(slippage_pct)  AS avg_slip
            FROM paper_trades
            WHERE status='CLOSED'
        """).fetchone()

        n = agg["closed_cnt"]
        wins = stats["wins"] or 0
        losses = stats["losses"] or 0
        flat = stats["flat"] or 0
        win_rate = wins / n if n > 0 else 0

        print(f"\n  Win/Loss/Flat:         {wins} / {losses} / {flat}")
        print(f"  Win Rate (net):        {win_rate:.1%}")
        print(f"\n  Raw PnL  — avg: {pct(stats['avg_raw'])}  best: {pct(stats['best_raw'])}  worst: {pct(stats['worst_raw'])}")
        print(f"  Net PnL  — avg: {pct(stats['avg_net'])}  best: {pct(stats['best_net'])}  worst: {pct(stats['worst_net'])}")
        print(f"  Total net USD PnL:    ${stats['total_net_usd'] or 0:+,.2f}")
        print(f"\n  Avg fee (round-trip): {pct(stats['avg_fee'])}")
        print(f"  Avg slippage:         {pct(stats['avg_slip'])}")
        if stats["avg_fee"] and stats["avg_slip"]:
            drag = (stats["avg_fee"] or 0) + (stats["avg_slip"] or 0)
            print(f"  Total cost drag:      {drag:+.2%}  (fee+slip per trade)")

        # Avg hold time
        hold = conn.execute("""
            SELECT AVG((exit_time - entry_time)/60.0) AS avg_min
            FROM paper_trades WHERE status='CLOSED' AND exit_time IS NOT NULL
        """).fetchone()
        print(f"  Avg hold time:        {hold['avg_min'] or 0:.1f} minutes")

    # ──────────────────────────────────────────────────────────
    # 3. EXIT REASON BREAKDOWN
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  3. EXIT REASON BREAKDOWN")
    print(SEP2)

    reasons = conn.execute("""
        SELECT
            SUBSTR(exit_reason, 1, INSTR(exit_reason || ' ', ' ') - 1) AS reason_key,
            COUNT(*) AS cnt,
            AVG(net_pnl_pct) AS avg_net,
            AVG(raw_pnl_pct) AS avg_raw,
            SUM(CASE WHEN net_pnl_pct > 0 THEN 1 ELSE 0 END) AS wins
        FROM paper_trades
        WHERE status='CLOSED' AND exit_reason IS NOT NULL
        GROUP BY reason_key
        ORDER BY cnt DESC
    """).fetchall()

    if reasons:
        print(f"  {'Exit Reason':<30s} {'Count':>6s} {'WinR':>6s} {'AvgRaw':>8s} {'AvgNet':>8s}")
        print(f"  {'-'*30} {'-'*6} {'-'*6} {'-'*8} {'-'*8}")
        for r in reasons:
            wr = (r["wins"] / r["cnt"]) if r["cnt"] else 0
            print(f"  {(r['reason_key'] or 'UNKNOWN'):<30s} {r['cnt']:>6d} {wr:>6.1%} {pct(r['avg_raw']):>8s} {pct(r['avg_net']):>8s}")
    else:
        print("  No closed trades yet.")

    # ──────────────────────────────────────────────────────────
    # 4. FILTER REJECTIONS
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  4. FILTER REJECTIONS")
    print(SEP2)

    r = conn.execute("SELECT COUNT(*) AS n FROM filter_rejections").fetchone()
    total_rejections = r["n"]
    print(f"  Total rejection events: {total_rejections:,}")

    # Count by prefix (first word of rejection_reason)
    rej_by_reason = conn.execute("""
        SELECT
            SUBSTR(rejection_reason, 1,
                CASE WHEN INSTR(rejection_reason, ' ') > 0
                     THEN INSTR(rejection_reason, ' ') - 1
                     ELSE LENGTH(rejection_reason) END
            ) AS reason_key,
            COUNT(*) AS cnt,
            COUNT(DISTINCT mint) AS unique_tokens
        FROM filter_rejections
        GROUP BY reason_key
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()

    print(f"\n  {'Rejection Reason':<35s} {'Count':>8s} {'Unique':>8s} {'%':>6s}")
    print(f"  {'-'*35} {'-'*8} {'-'*8} {'-'*6}")
    for r in rej_by_reason:
        pct_of_total = r["cnt"] / total_rejections * 100 if total_rejections > 0 else 0
        print(f"  {(r['reason_key'] or 'UNKNOWN'):<35s} {r['cnt']:>8,} {r['unique_tokens']:>8,} {pct_of_total:>5.1f}%")

    # ──────────────────────────────────────────────────────────
    # 5. QUANT SIGNALS AT ENTRY
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  5. QUANT SIGNALS AT ENTRY (closed trades only)")
    print(SEP2)

    sigs = conn.execute("""
        SELECT
            AVG(qs.hurst_value)   AS avg_h,
            MIN(qs.hurst_value)   AS min_h,
            MAX(qs.hurst_value)   AS max_h,
            AVG(qs.gini_coeff)    AS avg_g,
            MIN(qs.gini_coeff)    AS min_g,
            MAX(qs.gini_coeff)    AS max_g,
            AVG(qs.cvd_slope)     AS avg_cvd,
            MIN(qs.cvd_slope)     AS min_cvd,
            MAX(qs.cvd_slope)     AS max_cvd,
            AVG(qs.buy_ratio)     AS avg_br,
            MIN(qs.buy_ratio)     AS min_br,
            MAX(qs.buy_ratio)     AS max_br,
            COUNT(*)              AS total
        FROM quant_signals qs
        JOIN paper_trades pt ON qs.trade_id = pt.trade_id
        WHERE pt.status = 'CLOSED'
    """).fetchone()

    if sigs and sigs["total"]:
        print(f"  Trades with signals: {sigs['total']}")
        print(f"\n  Hurst  — avg: {fmt(sigs['avg_h'], '.4f')}  min: {fmt(sigs['min_h'], '.4f')}  max: {fmt(sigs['max_h'], '.4f')}")
        print(f"  Gini   — avg: {fmt(sigs['avg_g'], '.4f')}  min: {fmt(sigs['min_g'], '.4f')}  max: {fmt(sigs['max_g'], '.4f')}")
        print(f"  CVD    — avg: {fmt(sigs['avg_cvd'], '.2f')}  min: {fmt(sigs['min_cvd'], '.2f')}  max: {fmt(sigs['max_cvd'], '.2f')}")
        print(f"  BuyR   — avg: {fmt(sigs['avg_br'], '.3f')}  min: {fmt(sigs['min_br'], '.3f')}  max: {fmt(sigs['max_br'], '.3f')}")
    else:
        print("  No quant signals recorded for closed trades.")

    # Distribution of Hurst at entry
    h_dist = conn.execute("""
        SELECT
            SUM(CASE WHEN qs.hurst_value < 0.55 THEN 1 ELSE 0 END) AS below_55,
            SUM(CASE WHEN qs.hurst_value >= 0.55 AND qs.hurst_value < 0.65 THEN 1 ELSE 0 END) AS h55_65,
            SUM(CASE WHEN qs.hurst_value >= 0.65 AND qs.hurst_value < 0.70 THEN 1 ELSE 0 END) AS h65_70,
            SUM(CASE WHEN qs.hurst_value >= 0.70 AND qs.hurst_value < 0.80 THEN 1 ELSE 0 END) AS h70_80,
            SUM(CASE WHEN qs.hurst_value >= 0.80 THEN 1 ELSE 0 END) AS above_80
        FROM quant_signals qs
        JOIN paper_trades pt ON qs.trade_id = pt.trade_id
        WHERE pt.status = 'CLOSED'
    """).fetchone()
    if h_dist:
        print(f"\n  Hurst distribution at entry:")
        print(f"    < 0.55:     {h_dist['below_55']:>5}")
        print(f"    0.55-0.65:  {h_dist['h55_65']:>5}")
        print(f"    0.65-0.70:  {h_dist['h65_70']:>5}")
        print(f"    0.70-0.80:  {h_dist['h70_80']:>5}  ← (gate=0.70)")
        print(f"    >= 0.80:    {h_dist['above_80']:>5}")

    # CVD distribution
    cvd_dist = conn.execute("""
        SELECT
            SUM(CASE WHEN qs.cvd_slope < 0 THEN 1 ELSE 0 END) AS neg,
            SUM(CASE WHEN qs.cvd_slope >= 0 AND qs.cvd_slope < 10 THEN 1 ELSE 0 END) AS lt10,
            SUM(CASE WHEN qs.cvd_slope >= 10 AND qs.cvd_slope < 100 THEN 1 ELSE 0 END) AS c10_100,
            SUM(CASE WHEN qs.cvd_slope >= 100 AND qs.cvd_slope < 500 THEN 1 ELSE 0 END) AS c100_500,
            SUM(CASE WHEN qs.cvd_slope >= 500 AND qs.cvd_slope < 2000 THEN 1 ELSE 0 END) AS c500_2000,
            SUM(CASE WHEN qs.cvd_slope >= 2000 THEN 1 ELSE 0 END) AS above2000
        FROM quant_signals qs
        JOIN paper_trades pt ON qs.trade_id = pt.trade_id
        WHERE pt.status = 'CLOSED'
    """).fetchone()
    if cvd_dist:
        print(f"\n  CVD slope distribution at entry (gate: 10–2000):")
        print(f"    < 0:             {cvd_dist['neg']:>5}  ← below lower gate")
        print(f"    0–10:            {cvd_dist['lt10']:>5}  ← below lower gate")
        print(f"    10–100:          {cvd_dist['c10_100']:>5}")
        print(f"    100–500:         {cvd_dist['c100_500']:>5}")
        print(f"    500–2000:        {cvd_dist['c500_2000']:>5}")
        print(f"    >= 2000:         {cvd_dist['above2000']:>5}  ← above upper gate")

    # ──────────────────────────────────────────────────────────
    # 6. SYSTEM EVENTS
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  6. SYSTEM EVENTS")
    print(SEP2)

    r = conn.execute("SELECT COUNT(*) AS n FROM system_events").fetchone()
    print(f"  Total events: {r['n']:,}")

    events_by_type = conn.execute("""
        SELECT event_type, severity, COUNT(*) AS cnt
        FROM system_events
        GROUP BY event_type, severity
        ORDER BY cnt DESC
        LIMIT 25
    """).fetchall()

    if events_by_type:
        print(f"\n  {'Event Type':<40s} {'Severity':<10s} {'Count':>6s}")
        print(f"  {'-'*40} {'-'*10} {'-'*6}")
        for ev in events_by_type:
            print(f"  {ev['event_type']:<40s} {ev['severity']:<10s} {ev['cnt']:>6,}")

    # Recent errors/warnings
    recent_errors = conn.execute("""
        SELECT event_type, severity, description, timestamp
        FROM system_events
        WHERE severity IN ('ERROR', 'CRITICAL')
        ORDER BY timestamp DESC
        LIMIT 10
    """).fetchall()
    if recent_errors:
        print(f"\n  Recent ERROR/CRITICAL events:")
        for ev in recent_errors:
            print(f"    [{ts_to_str(ev['timestamp'])}] {ev['event_type']} — {(ev['description'] or '')[:80]}")

    # ──────────────────────────────────────────────────────────
    # 7. PORTFOLIO STATE
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  7. PORTFOLIO STATE")
    print(SEP2)

    first_event = conn.execute("""
        SELECT balance_after, timestamp FROM portfolio_state ORDER BY id ASC LIMIT 1
    """).fetchone()
    last_event = conn.execute("""
        SELECT balance_after, timestamp FROM portfolio_state ORDER BY id DESC LIMIT 1
    """).fetchone()

    if first_event and last_event:
        start_bal = first_event["balance_after"]
        end_bal = last_event["balance_after"]
        pnl_usd = end_bal - start_bal
        pnl_pct_portfolio = pnl_usd / start_bal if start_bal else 0
        print(f"  Starting balance:  ${start_bal:,.2f}  ({ts_to_str(first_event['timestamp'])})")
        print(f"  Current balance:   ${end_bal:,.2f}  ({ts_to_str(last_event['timestamp'])})")
        print(f"  Total P&L (USD):   ${pnl_usd:+,.2f}  ({pnl_pct_portfolio:+.2%})")
    else:
        print("  No portfolio events recorded yet.")

    # ──────────────────────────────────────────────────────────
    # 8. TRADE COSTS
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  8. TRADE COSTS")
    print(SEP2)

    tc = conn.execute("""
        SELECT
            COUNT(*) AS n,
            AVG(fee_pct) AS avg_fee,
            AVG(slippage_pct) AS avg_slip,
            AVG(total_cost_pct) AS avg_total,
            AVG(size_vs_liquidity_pct) AS avg_size_vs_liq,
            SUM(was_size_capped) AS capped_count,
            AVG(raw_pnl_pct) AS avg_raw,
            AVG(net_pnl_pct) AS avg_net
        FROM trade_costs
    """).fetchone()

    if tc and tc["n"]:
        print(f"  Records: {tc['n']:,}")
        print(f"  Avg fee (round trip):   {pct(tc['avg_fee'])}")
        print(f"  Avg slippage:           {pct(tc['avg_slip'])}")
        print(f"  Avg total cost:         {pct(tc['avg_total'])}")
        print(f"  Avg size/liquidity:     {tc['avg_size_vs_liq'] or 0:.3f}%")
        print(f"  Size-capped trades:     {int(tc['capped_count'] or 0)}")
        print(f"  Avg raw PnL:            {pct(tc['avg_raw'])}")
        print(f"  Avg net PnL:            {pct(tc['avg_net'])}")
    else:
        print("  No trade cost records yet.")

    # ──────────────────────────────────────────────────────────
    # 9. PERFORMANCE SNAPSHOTS
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  9. PERFORMANCE SNAPSHOTS")
    print(SEP2)

    r = conn.execute("SELECT COUNT(*) AS n FROM performance_snapshots").fetchone()
    print(f"  Total snapshots: {r['n']:,}")

    latest_snap = conn.execute("""
        SELECT *
        FROM performance_snapshots
        ORDER BY timestamp DESC LIMIT 1
    """).fetchone()

    if latest_snap:
        print(f"  Latest snapshot ({ts_to_str(latest_snap['timestamp'])}):")
        print(f"    Period trades:   {latest_snap['total_trades'] or 0}")
        print(f"    Win rate:        {(latest_snap['win_rate_pct'] or 0):.1f}%")
        print(f"    Mean net PnL:    {pct(latest_snap['mean_net_pnl_pct'])}")
        print(f"    Median net PnL:  {pct(latest_snap['median_net_pnl_pct'])}")
        print(f"    Total net USD:   ${latest_snap['total_net_usd_pnl'] or 0:+,.2f}")
        print(f"    Balance start:   ${latest_snap['balance_start'] or 0:,.2f}")
        print(f"    Balance end:     ${latest_snap['balance_end'] or 0:,.2f}")

    # ──────────────────────────────────────────────────────────
    # 10. TABLE SIZE / UNBOUNDED GROWTH CHECK
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  10. TABLE ROW COUNTS (unbounded growth check)")
    print(SEP2)

    tables = [
        "market_ticks", "paper_trades", "quant_signals",
        "portfolio_state", "filter_rejections", "trade_costs",
        "system_events", "performance_snapshots"
    ]
    for t in tables:
        r = conn.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()
        print(f"  {t:<30s} {r['n']:>10,} rows")

    # ──────────────────────────────────────────────────────────
    # 11. INDEX CHECK
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  11. DATABASE INDEXES")
    print(SEP2)

    indexes = conn.execute("""
        SELECT name, tbl_name, sql
        FROM sqlite_master
        WHERE type='index' AND name NOT LIKE 'sqlite_%'
        ORDER BY tbl_name, name
    """).fetchall()

    if indexes:
        for idx in indexes:
            print(f"  {idx['tbl_name']:<25s} → {idx['name']}")
    else:
        print("  No custom indexes found!")

    # ──────────────────────────────────────────────────────────
    # 12. QUANT SIGNAL HURST GATE CALIBRATION
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  12. HURST GATE CALIBRATION — rejected vs entered PnL")
    print(SEP2)

    # Distribution of Hurst values in rejections
    h_rej = conn.execute("""
        SELECT
            SUM(CASE WHEN hurst_value > 0 AND hurst_value < 0.55 THEN 1 ELSE 0 END) AS below_55,
            SUM(CASE WHEN hurst_value >= 0.55 AND hurst_value < 0.65 THEN 1 ELSE 0 END) AS h55_65,
            SUM(CASE WHEN hurst_value >= 0.65 AND hurst_value < 0.70 THEN 1 ELSE 0 END) AS h65_70,
            SUM(CASE WHEN hurst_value >= 0.70 THEN 1 ELSE 0 END) AS above_70,
            COUNT(*) AS total
        FROM filter_rejections
        WHERE rejection_reason LIKE 'HURST_%'
    """).fetchone()
    if h_rej and h_rej["total"]:
        print(f"  Hurst rejections by bucket (total HURST_TOO_LOW rejections: {h_rej['total']:,}):")
        print(f"    H < 0.55:   {h_rej['below_55']:>6,}")
        print(f"    H 0.55-0.65:{h_rej['h55_65']:>6,}")
        print(f"    H 0.65-0.70:{h_rej['h65_70']:>6,}")
        print(f"    H >= 0.70:  {h_rej['above_70']:>6,}  (these should NOT appear here)")
    else:
        print("  No HURST rejection records found.")

    # CVD slope rejections
    cvd_rej = conn.execute("""
        SELECT
            SUM(CASE WHEN cvd_slope < 0 THEN 1 ELSE 0 END) AS negative,
            SUM(CASE WHEN cvd_slope >= 0 AND cvd_slope < 10 THEN 1 ELSE 0 END) AS zero_to_10,
            SUM(CASE WHEN cvd_slope > 2000 THEN 1 ELSE 0 END) AS above_2000,
            COUNT(*) AS total,
            AVG(cvd_slope) AS avg_cvd,
            MIN(cvd_slope) AS min_cvd,
            MAX(cvd_slope) AS max_cvd
        FROM filter_rejections
        WHERE rejection_reason LIKE 'CVD_%'
    """).fetchone()
    if cvd_rej and cvd_rej["total"]:
        print(f"\n  CVD slope rejections (total: {cvd_rej['total']:,}):")
        print(f"    Negative slope:    {cvd_rej['negative']:>6,}  ← bearish")
        print(f"    Slope 0–10:        {cvd_rej['zero_to_10']:>6,}  ← too weak")
        print(f"    Slope > 2000:      {cvd_rej['above_2000']:>6,}  ← parabolic/risky")
        print(f"    Avg CVD slope:     {cvd_rej['avg_cvd'] or 0:.2f}")
        print(f"    Range (min/max):   {cvd_rej['min_cvd'] or 0:.2f} / {cvd_rej['max_cvd'] or 0:.2f}")

    # ──────────────────────────────────────────────────────────
    # 13. RECENT CLOSED TRADES (last 20)
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  13. RECENT CLOSED TRADES (last 20)")
    print(SEP2)

    recent = conn.execute("""
        SELECT pt.trade_id, pt.symbol, pt.entry_price, pt.exit_price,
               pt.raw_pnl_pct, pt.net_pnl_pct, pt.exit_reason,
               pt.entry_time, pt.exit_time,
               qs.hurst_value, qs.cvd_slope, qs.gini_coeff, qs.buy_ratio
        FROM paper_trades pt
        LEFT JOIN quant_signals qs ON pt.trade_id = qs.trade_id
        WHERE pt.status = 'CLOSED'
        ORDER BY pt.exit_time DESC
        LIMIT 20
    """).fetchall()

    if recent:
        print(f"  {'Symbol':<12s} {'Raw':>7s} {'Net':>7s} {'H':>6s} {'CVD':>8s} {'BR':>5s} {'Exit Reason'}")
        print(f"  {'-'*12} {'-'*7} {'-'*7} {'-'*6} {'-'*8} {'-'*5} {'-'*30}")
        for t in recent:
            marker = "✓" if (t["net_pnl_pct"] or 0) > 0 else "✗"
            h = f"{t['hurst_value']:.3f}" if t["hurst_value"] else " N/A "
            cvd = f"{t['cvd_slope']:.1f}" if t["cvd_slope"] else " N/A "
            br = f"{t['buy_ratio']:.2f}" if t["buy_ratio"] else "N/A"
            raw = pct(t["raw_pnl_pct"])
            net = pct(t["net_pnl_pct"])
            reason = (t["exit_reason"] or "")[:35]
            print(f"  {marker} {t['symbol']:<10s} {raw:>7s} {net:>7s} {h:>6s} {cvd:>8s} {br:>5s} {reason}")
    else:
        print("  No closed trades yet.")

    print(f"\n{SEP}")
    print("  END OF REPORT")
    print(SEP)
    print()

    conn.close()

if __name__ == "__main__":
    main()
