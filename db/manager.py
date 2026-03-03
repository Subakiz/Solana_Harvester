"""
Async SQLite Database Manager for Quant Harvest Bot.
Manages all tables: market_ticks, paper_trades, quant_signals,
portfolio_state, filter_rejections, trade_costs, system_events,
performance_snapshots.
"""
import json
import time
import traceback
import uuid
from pathlib import Path
from typing import Optional

import aiosqlite

from config.settings import Settings
from utils.logger import get_logger

log = get_logger("DatabaseManager")

# ── Schema Definitions ────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS market_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price_usd REAL,
    liquidity_usd REAL,
    volume_5m REAL,
    buys_5m INTEGER,
    sells_5m INTEGER,
    market_cap REAL,
    pair_address TEXT
);
CREATE INDEX IF NOT EXISTS idx_ticks_mint ON market_ticks(mint);
CREATE INDEX IF NOT EXISTS idx_ticks_ts ON market_ticks(timestamp);

CREATE TABLE IF NOT EXISTS paper_trades (
    trade_id TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    entry_time REAL NOT NULL,
    exit_time REAL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    peak_high REAL,
    peak_low REAL,
    exit_reason TEXT,
    pnl_pct REAL,
    usd_size REAL,
    usd_pnl REAL,
    raw_pnl_pct REAL,
    net_pnl_pct REAL,
    fee_pct REAL,
    slippage_pct REAL,
    raw_usd_pnl REAL,
    net_usd_pnl REAL,
    entry_liquidity REAL,
    entry_volume_5m REAL,
    entry_market_cap REAL,
    entry_buy_ratio REAL
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON paper_trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_mint ON paper_trades(mint);

CREATE TABLE IF NOT EXISTS quant_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    hurst_value REAL,
    cvd_value REAL,
    cvd_slope REAL,
    gini_coeff REAL,
    snapshot_count INTEGER,
    buy_ratio REAL,
    FOREIGN KEY (trade_id) REFERENCES paper_trades(trade_id)
);
CREATE INDEX IF NOT EXISTS idx_signals_trade ON quant_signals(trade_id);

CREATE TABLE IF NOT EXISTS portfolio_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    event_type TEXT NOT NULL,
    trade_id TEXT,
    balance_before REAL,
    balance_after REAL,
    usd_change REAL,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_portfolio_ts ON portfolio_state(timestamp);

CREATE TABLE IF NOT EXISTS filter_rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    price_usd REAL,
    liquidity_usd REAL,
    volume_5m REAL,
    buys_5m INTEGER,
    sells_5m INTEGER,
    market_cap REAL,
    buy_ratio REAL,
    hurst_value REAL,
    cvd_slope REAL,
    gini_coeff REAL
);
CREATE INDEX IF NOT EXISTS idx_rejections_reason ON filter_rejections(rejection_reason);
CREATE INDEX IF NOT EXISTS idx_rejections_ts ON filter_rejections(timestamp);

CREATE TABLE IF NOT EXISTS trade_costs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL,
    entry_liquidity REAL,
    exit_liquidity REAL,
    position_size REAL,
    size_vs_liquidity_pct REAL,
    was_size_capped INTEGER DEFAULT 0,
    fee_pct REAL,
    slippage_pct REAL,
    total_cost_pct REAL,
    raw_pnl_pct REAL,
    net_pnl_pct REAL,
    raw_usd_pnl REAL,
    net_usd_pnl REAL,
    FOREIGN KEY (trade_id) REFERENCES paper_trades(trade_id)
);
CREATE INDEX IF NOT EXISTS idx_costs_trade ON trade_costs(trade_id);

CREATE TABLE IF NOT EXISTS system_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    description TEXT,
    metadata TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_type ON system_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_ts ON system_events(timestamp);

CREATE TABLE IF NOT EXISTS performance_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    period_start REAL NOT NULL,
    period_end REAL NOT NULL,
    total_trades INTEGER,
    winning_trades INTEGER,
    losing_trades INTEGER,
    flat_trades INTEGER,
    win_rate_pct REAL,
    mean_raw_pnl_pct REAL,
    median_raw_pnl_pct REAL,
    mean_net_pnl_pct REAL,
    median_net_pnl_pct REAL,
    total_net_usd_pnl REAL,
    max_drawdown_pct REAL,
    trades_rejected_volume INTEGER,
    trades_rejected_liquidity INTEGER,
    trades_rejected_mcap INTEGER,
    trades_rejected_buy_ratio INTEGER,
    trades_rejected_max_positions INTEGER,
    trades_size_capped INTEGER,
    data_timeout_events INTEGER,
    circuit_breaker_events INTEGER,
    balance_start REAL,
    balance_end REAL
);
"""


class DatabaseManager:
    """Async SQLite wrapper for all Quant Harvest data operations."""

    def __init__(self):
        self._db: Optional[aiosqlite.Connection] = None
        self._balance: float = Settings.INITIAL_BALANCE

    # ══════════════════════════════════════════════════════════
    # Lifecycle
    # ══════════════════════════════════════════════════════════

    async def initialize(self):
        """Open database, create tables, load balance."""
        db_path = Path(Settings.DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(str(db_path))
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")

        for statement in _SCHEMA_SQL.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                await self._db.execute(stmt)
        await self._db.commit()

        # Migrate existing tables: add new columns if they don't exist
        await self._migrate()

        # Load balance from most recent portfolio event
        await self._load_balance()
        log.info(f"Database initialized: {Settings.DB_PATH} | Balance: ${self._balance:,.2f}")

    async def _migrate(self):
        """Add new columns to existing tables if they don't already exist."""
        # paper_trades additions
        new_cols = [
            ("raw_pnl_pct", "REAL"),
            ("net_pnl_pct", "REAL"),
            ("fee_pct", "REAL"),
            ("slippage_pct", "REAL"),
            ("raw_usd_pnl", "REAL"),
            ("net_usd_pnl", "REAL"),
            ("entry_liquidity", "REAL"),
            ("entry_volume_5m", "REAL"),
            ("entry_market_cap", "REAL"),
            ("entry_buy_ratio", "REAL"),
            ("partial_tp_taken", "INTEGER DEFAULT 0"),
            ("trail_active", "INTEGER DEFAULT 0"),
        ]
        for col_name, col_type in new_cols:
            try:
                await self._db.execute(
                    f"ALTER TABLE paper_trades ADD COLUMN {col_name} {col_type}"
                )
            except Exception:
                pass  # Column already exists

        # quant_signals: add buy_ratio if missing
        try:
            await self._db.execute(
                "ALTER TABLE quant_signals ADD COLUMN buy_ratio REAL"
            )
        except Exception:
            pass

        await self._db.commit()

    async def _load_balance(self):
        """Load balance from latest portfolio_state event."""
        try:
            cursor = await self._db.execute(
                "SELECT balance_after FROM portfolio_state ORDER BY id DESC LIMIT 1"
            )
            row = await cursor.fetchone()
            if row and row["balance_after"] is not None:
                self._balance = row["balance_after"]
        except Exception:
            pass

    async def close(self):
        """Flush and close the database connection."""
        if self._db:
            await self._db.commit()
            await self._db.close()
            self._db = None

    @property
    def balance(self) -> float:
        return self._balance

    # ══════════════════════════════════════════════════════════
    # Market Ticks
    # ══════════════════════════════════════════════════════════

    async def insert_tick(self, *, mint: str, symbol: str, price_usd: float,
                          liquidity_usd: float, volume_5m: float,
                          buys_5m: int, sells_5m: int, market_cap: float,
                          pair_address: str):
        try:
            await self._db.execute(
                """INSERT INTO market_ticks
                   (timestamp, mint, symbol, price_usd, liquidity_usd,
                    volume_5m, buys_5m, sells_5m, market_cap, pair_address)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), mint, symbol, price_usd, liquidity_usd,
                 volume_5m, buys_5m, sells_5m, market_cap, pair_address),
            )
            await self._db.commit()
        except Exception as exc:
            log.error(f"insert_tick error: {exc}")
            await self._db.rollback()

    # ══════════════════════════════════════════════════════════
    # Paper Trades
    # ══════════════════════════════════════════════════════════

    async def open_paper_trade(self, *, mint: str, symbol: str,
                               entry_price: float, usd_size: float = 0.0,
                               entry_liquidity: float = 0.0,
                               entry_volume_5m: float = 0.0,
                               entry_market_cap: float = 0.0,
                               entry_buy_ratio: float = 0.0) -> str:
        trade_id = f"PT-{uuid.uuid4().hex[:12].upper()}"
        now = time.time()
        try:
            await self._db.execute(
                """INSERT INTO paper_trades
                   (trade_id, mint, symbol, status, entry_time, entry_price,
                    peak_high, peak_low, usd_size,
                    entry_liquidity, entry_volume_5m, entry_market_cap, entry_buy_ratio)
                   VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_id, mint, symbol, now, entry_price,
                 entry_price, entry_price, usd_size,
                 entry_liquidity, entry_volume_5m, entry_market_cap, entry_buy_ratio),
            )
            # Record portfolio event
            await self._db.execute(
                """INSERT INTO portfolio_state
                   (timestamp, event_type, trade_id, balance_before, balance_after,
                    usd_change, description)
                   VALUES (?, 'OPEN', ?, ?, ?, 0, ?)""",
                (now, trade_id, self._balance, self._balance,
                 f"Opened: {symbol} @ ${entry_price:.10f} size=${usd_size:.2f}"),
            )
            await self._db.commit()
            return trade_id
        except Exception as exc:
            log.error(f"open_paper_trade error: {exc}")
            await self._db.rollback()
            raise

    async def close_paper_trade(self, trade_id: str, exit_price: float,
                                reason: str, cost_info: Optional[dict] = None):
        """Close a paper trade with optional cost model data."""
        now = time.time()
        try:
            cursor = await self._db.execute(
                "SELECT * FROM paper_trades WHERE trade_id = ?", (trade_id,)
            )
            row = await cursor.fetchone()
            if not row:
                log.error(f"Trade {trade_id} not found for closing")
                return

            entry_price = row["entry_price"]
            usd_size = row["usd_size"] or 0.0

            # Calculate raw PnL
            raw_pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0

            # Apply cost model if provided
            if cost_info:
                fee_pct = cost_info.get("fee_pct", 0.0)
                slippage_pct = cost_info.get("slippage_pct", 0.0)
                priority_fee_usd = cost_info.get("priority_fee_usd", 0.0)
                net_pnl_pct = raw_pnl_pct - fee_pct - slippage_pct
            else:
                fee_pct = 0.0
                slippage_pct = 0.0
                priority_fee_usd = 0.0
                net_pnl_pct = raw_pnl_pct

            raw_usd_pnl = usd_size * raw_pnl_pct
            net_usd_pnl = usd_size * net_pnl_pct - priority_fee_usd

            # Update trade record
            await self._db.execute(
                """UPDATE paper_trades SET
                       status='CLOSED', exit_time=?, exit_price=?, exit_reason=?,
                       pnl_pct=?, raw_pnl_pct=?, net_pnl_pct=?,
                       fee_pct=?, slippage_pct=?,
                       usd_pnl=?, raw_usd_pnl=?, net_usd_pnl=?
                   WHERE trade_id=?""",
                (now, exit_price, reason,
                 net_pnl_pct, raw_pnl_pct, net_pnl_pct,
                 fee_pct, slippage_pct,
                 net_usd_pnl, raw_usd_pnl, net_usd_pnl,
                 trade_id),
            )

            # Update portfolio balance with NET PnL
            balance_before = self._balance
            self._balance += net_usd_pnl
            balance_after = self._balance

            desc = (
                f"Closed: {raw_pnl_pct:+.2%} raw, {net_pnl_pct:+.2%} net "
                f"(${net_usd_pnl:+.2f}) "
                f"[fee={fee_pct:.2%}, slip={slippage_pct:.2%}]"
            )

            await self._db.execute(
                """INSERT INTO portfolio_state
                   (timestamp, event_type, trade_id, balance_before,
                    balance_after, usd_change, description)
                   VALUES (?, 'CLOSE', ?, ?, ?, ?, ?)""",
                (now, trade_id, balance_before, balance_after,
                 net_usd_pnl, desc),
            )

            # Insert trade_costs record
            entry_liquidity = row["entry_liquidity"] or 0.0
            exit_liquidity = 0.0  # We don't track exit liquidity separately yet
            size_vs_liq = (usd_size / entry_liquidity * 100) if entry_liquidity > 0 else 0.0
            was_capped = 1 if cost_info and cost_info.get("was_size_capped", False) else 0

            await self._db.execute(
                """INSERT INTO trade_costs
                   (trade_id, entry_liquidity, exit_liquidity, position_size,
                    size_vs_liquidity_pct, was_size_capped,
                    fee_pct, slippage_pct, total_cost_pct,
                    raw_pnl_pct, net_pnl_pct, raw_usd_pnl, net_usd_pnl)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_id, entry_liquidity, exit_liquidity, usd_size,
                 size_vs_liq, was_capped,
                 fee_pct, slippage_pct, fee_pct + slippage_pct,
                 raw_pnl_pct, net_pnl_pct, raw_usd_pnl, net_usd_pnl),
            )

            await self._db.commit()
            log.info(f"📕 Closed {trade_id}: {reason} | {desc}")
            return {
                "raw_pnl_pct": raw_pnl_pct,
                "net_pnl_pct": net_pnl_pct,
                "net_usd_pnl": net_usd_pnl,
            }
        except Exception as exc:
            log.error(f"close_paper_trade error: {exc}\n{traceback.format_exc()}")
            await self._db.rollback()

    async def update_paper_trade_extremes(self, trade_id: str,
                                          peak_high: float, peak_low: float):
        try:
            await self._db.execute(
                "UPDATE paper_trades SET peak_high=?, peak_low=? WHERE trade_id=?",
                (peak_high, peak_low, trade_id),
            )
            await self._db.commit()
        except Exception as exc:
            log.error(f"update_paper_trade_extremes error: {exc}")
            await self._db.rollback()

    async def get_open_trades(self) -> list:
        cursor = await self._db.execute(
            "SELECT * FROM paper_trades WHERE status='OPEN'"
        )
        return await cursor.fetchall()

    async def is_mint_in_open_trade(self, mint: str) -> bool:
        cursor = await self._db.execute(
            "SELECT 1 FROM paper_trades WHERE mint=? AND status='OPEN' LIMIT 1",
            (mint,),
        )
        return await cursor.fetchone() is not None

    async def get_open_trade_count(self) -> int:
        cursor = await self._db.execute(
            "SELECT COUNT(*) as cnt FROM paper_trades WHERE status='OPEN'"
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def get_closed_trade_count(self) -> int:
        cursor = await self._db.execute(
            "SELECT COUNT(*) as cnt FROM paper_trades WHERE status='CLOSED'"
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def get_recent_closed_trades(self, limit: int = 100) -> list:
        cursor = await self._db.execute(
            """SELECT * FROM paper_trades
               WHERE status='CLOSED'
               ORDER BY exit_time DESC LIMIT ?""",
            (limit,),
        )
        return await cursor.fetchall()

    # ══════════════════════════════════════════════════════════
    # Quant Signals
    # ══════════════════════════════════════════════════════════

    async def insert_quant_signal(self, *, trade_id: str, hurst_value: float,
                                  cvd_value: float, cvd_slope: float,
                                  gini_coeff: float = None, snapshot_count: int,
                                  buy_ratio: float = 0.0):
        try:
            await self._db.execute(
                """INSERT INTO quant_signals
                   (trade_id, timestamp, hurst_value, cvd_value, cvd_slope,
                    gini_coeff, snapshot_count, buy_ratio)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_id, time.time(), hurst_value, cvd_value, cvd_slope,
                 gini_coeff, snapshot_count, buy_ratio),
            )
            await self._db.commit()
        except Exception as exc:
            log.error(f"insert_quant_signal error: {exc}")
            await self._db.rollback()

    # ══════════════════════════════════════════════════════════
    # Filter Rejections
    # ══════════════════════════════════════════════════════════

    async def insert_filter_rejection(self, *, mint: str, symbol: str,
                                      rejection_reason: str,
                                      price_usd: float = 0.0,
                                      liquidity_usd: float = 0.0,
                                      volume_5m: float = 0.0,
                                      buys_5m: int = 0, sells_5m: int = 0,
                                      market_cap: float = 0.0,
                                      buy_ratio: float = 0.0,
                                      hurst_value: float = 0.0,
                                      cvd_slope: float = 0.0,
                                      gini_coeff: float = 0.0):
        try:
            await self._db.execute(
                """INSERT INTO filter_rejections
                   (timestamp, mint, symbol, rejection_reason,
                    price_usd, liquidity_usd, volume_5m, buys_5m, sells_5m,
                    market_cap, buy_ratio, hurst_value, cvd_slope, gini_coeff)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), mint, symbol, rejection_reason,
                 price_usd, liquidity_usd, volume_5m, buys_5m, sells_5m,
                 market_cap, buy_ratio, hurst_value, cvd_slope, gini_coeff),
            )
            await self._db.commit()
        except Exception as exc:
            log.error(f"insert_filter_rejection error: {exc}")
            await self._db.rollback()

    # ══════════════════════════════════════════════════════════
    # System Events
    # ══════════════════════════════════════════════════════════

    async def insert_system_event(self, *, event_type: str, severity: str,
                                  description: str = "",
                                  metadata: Optional[dict] = None):
        try:
            meta_json = json.dumps(metadata) if metadata else None
            await self._db.execute(
                """INSERT INTO system_events
                   (timestamp, event_type, severity, description, metadata)
                   VALUES (?, ?, ?, ?, ?)""",
                (time.time(), event_type, severity, description, meta_json),
            )
            await self._db.commit()
        except Exception as exc:
            log.error(f"insert_system_event error: {exc}")
            await self._db.rollback()

    # ══════════════════════════════════════════════════════════
    # Performance Snapshots
    # ══════════════════════════════════════════════════════════

    async def insert_performance_snapshot(self, period_start: float,
                                          period_end: float):
        """Generate and store an hourly performance snapshot."""
        try:
            now = time.time()

            # Trades closed in this period
            cursor = await self._db.execute(
                """SELECT raw_pnl_pct, net_pnl_pct, net_usd_pnl
                   FROM paper_trades
                   WHERE status='CLOSED' AND exit_time >= ? AND exit_time < ?""",
                (period_start, period_end),
            )
            trades = await cursor.fetchall()
            total = len(trades)

            raw_pnls = [t["raw_pnl_pct"] or 0.0 for t in trades]
            net_pnls = [t["net_pnl_pct"] or 0.0 for t in trades]
            net_usds = [t["net_usd_pnl"] or 0.0 for t in trades]

            winning = sum(1 for p in net_pnls if p > 0)
            losing = sum(1 for p in net_pnls if p < 0)
            flat = total - winning - losing
            win_rate = (winning / total * 100) if total > 0 else 0.0

            import statistics
            mean_raw = statistics.mean(raw_pnls) if raw_pnls else 0.0
            median_raw = statistics.median(raw_pnls) if raw_pnls else 0.0
            mean_net = statistics.mean(net_pnls) if net_pnls else 0.0
            median_net = statistics.median(net_pnls) if net_pnls else 0.0
            total_net = sum(net_usds)

            # Rejection counts in this period
            rej_counts = {}
            for reason_prefix in ["VOLUME_TOO_LOW", "LIQUIDITY_TOO_LOW",
                                  "MCAP_OUT_OF_RANGE", "BUY_RATIO_OUT_OF_RANGE",
                                  "MAX_POSITIONS_REACHED"]:
                cursor = await self._db.execute(
                    """SELECT COUNT(*) as cnt FROM filter_rejections
                       WHERE timestamp >= ? AND timestamp < ?
                       AND rejection_reason LIKE ?""",
                    (period_start, period_end, f"{reason_prefix}%"),
                )
                row = await cursor.fetchone()
                rej_counts[reason_prefix] = row["cnt"] if row else 0

            # Size capped count
            cursor = await self._db.execute(
                """SELECT COUNT(*) as cnt FROM trade_costs
                   WHERE was_size_capped=1
                   AND trade_id IN (
                       SELECT trade_id FROM paper_trades
                       WHERE exit_time >= ? AND exit_time < ?
                   )""",
                (period_start, period_end),
            )
            row = await cursor.fetchone()
            size_capped = row["cnt"] if row else 0

            # System events in period
            cursor = await self._db.execute(
                """SELECT COUNT(*) as cnt FROM system_events
                   WHERE timestamp >= ? AND timestamp < ?
                   AND event_type='DATA_TIMEOUT'""",
                (period_start, period_end),
            )
            row = await cursor.fetchone()
            data_timeouts = row["cnt"] if row else 0

            cursor = await self._db.execute(
                """SELECT COUNT(*) as cnt FROM system_events
                   WHERE timestamp >= ? AND timestamp < ?
                   AND event_type='CIRCUIT_BREAKER_TRIGGERED'""",
                (period_start, period_end),
            )
            row = await cursor.fetchone()
            cb_events = row["cnt"] if row else 0

            # Balance at period boundaries
            cursor = await self._db.execute(
                """SELECT balance_after FROM portfolio_state
                   WHERE timestamp < ? ORDER BY id DESC LIMIT 1""",
                (period_start,),
            )
            row = await cursor.fetchone()
            balance_start = row["balance_after"] if row else self._balance

            await self._db.execute(
                """INSERT INTO performance_snapshots
                   (timestamp, period_start, period_end,
                    total_trades, winning_trades, losing_trades, flat_trades,
                    win_rate_pct, mean_raw_pnl_pct, median_raw_pnl_pct,
                    mean_net_pnl_pct, median_net_pnl_pct, total_net_usd_pnl,
                    max_drawdown_pct,
                    trades_rejected_volume, trades_rejected_liquidity,
                    trades_rejected_mcap, trades_rejected_buy_ratio,
                    trades_rejected_max_positions, trades_size_capped,
                    data_timeout_events, circuit_breaker_events,
                    balance_start, balance_end)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, period_start, period_end,
                 total, winning, losing, flat,
                 win_rate, mean_raw, median_raw,
                 mean_net, median_net, total_net,
                 0.0,  # max_drawdown_pct - simplified
                 rej_counts.get("VOLUME_TOO_LOW", 0),
                 rej_counts.get("LIQUIDITY_TOO_LOW", 0),
                 rej_counts.get("MCAP_OUT_OF_RANGE", 0),
                 rej_counts.get("BUY_RATIO_OUT_OF_RANGE", 0),
                 rej_counts.get("MAX_POSITIONS_REACHED", 0),
                 size_capped, data_timeouts, cb_events,
                 balance_start, self._balance),
            )
            await self._db.commit()

            # Alert conditions
            if total > 0:
                if win_rate < 40:
                    await self.insert_system_event(
                        event_type="ALERT_LOW_WIN_RATE", severity="CRITICAL",
                        description=f"Win rate {win_rate:.1f}% below 40% threshold",
                    )
                if median_net < -0.02:
                    await self.insert_system_event(
                        event_type="ALERT_NEGATIVE_MEDIAN", severity="CRITICAL",
                        description=f"Median net PnL {median_net:.2%} below -2%",
                    )
                if data_timeouts > 0 and data_timeouts / total > 0.10:
                    await self.insert_system_event(
                        event_type="ALERT_DATA_TIMEOUTS", severity="CRITICAL",
                        description=f"Data timeouts {data_timeouts} exceed 10% of trades",
                    )
            elif total < 5:
                # Fewer than 5 trades in snapshot period may indicate data feed failure
                await self.insert_system_event(
                    event_type="ALERT_LOW_TRADE_COUNT", severity="CRITICAL",
                    description=f"Only {total} trades in last hour — possible data feed failure",
                )

            # Check rejection rate
            total_rejections = sum(rej_counts.values())
            total_evaluations = total + total_rejections
            if total_evaluations > 0 and total_rejections / total_evaluations > 0.80:
                await self.insert_system_event(
                    event_type="ALERT_HIGH_REJECTION_RATE", severity="CRITICAL",
                    description=f"Rejection rate {total_rejections}/{total_evaluations} exceeds 80%",
                )

            # Check circuit breakers in last 24h
            cursor = await self._db.execute(
                """SELECT COUNT(*) as cnt FROM system_events
                   WHERE event_type='CIRCUIT_BREAKER_TRIGGERED'
                   AND timestamp >= ?""",
                (now - 86400,),
            )
            row = await cursor.fetchone()
            if row and row["cnt"] > 3:
                await self.insert_system_event(
                    event_type="ALERT_CIRCUIT_BREAKERS", severity="CRITICAL",
                    description=f"Circuit breaker triggered {row['cnt']} times in 24h",
                )

            log.info(
                f"📸 Snapshot: {total} trades, {win_rate:.1f}% win, "
                f"net PnL {mean_net:+.2%} avg / {median_net:+.2%} med"
            )
        except Exception as exc:
            log.error(f"insert_performance_snapshot error: {exc}\n{traceback.format_exc()}")
            await self._db.rollback()

    # ══════════════════════════════════════════════════════════
    # Daily Loss Tracking
    # ══════════════════════════════════════════════════════════

    async def get_daily_realized_loss(self, day_start_ts: float) -> float:
        """Sum of all negative net_usd_pnl since day_start_ts."""
        try:
            cursor = await self._db.execute(
                """SELECT COALESCE(SUM(net_usd_pnl), 0) as total_loss
                   FROM paper_trades
                   WHERE status='CLOSED' AND exit_time >= ?
                   AND net_usd_pnl < 0""",
                (day_start_ts,),
            )
            row = await cursor.fetchone()
            return row["total_loss"] if row else 0.0
        except Exception:
            return 0.0

    async def get_balance_at(self, ts: float) -> float:
        """Get balance closest to a timestamp."""
        try:
            cursor = await self._db.execute(
                """SELECT balance_after FROM portfolio_state
                   WHERE timestamp <= ? ORDER BY id DESC LIMIT 1""",
                (ts,),
            )
            row = await cursor.fetchone()
            return row["balance_after"] if row else self._balance
        except Exception:
            return self._balance

    # ══════════════════════════════════════════════════════════
    # Portfolio Events (v4.1 — for partial exits)
    # ══════════════════════════════════════════════════════════

    async def insert_portfolio_event(self, *, event_type: str, trade_id: str,
                                     balance_before: float, balance_after: float,
                                     usd_change: float, description: str):
        """Insert a portfolio state event (used for partial exits, etc.)."""
        try:
            await self._db.execute(
                """INSERT INTO portfolio_state
                   (timestamp, event_type, trade_id, balance_before,
                    balance_after, usd_change, description)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), event_type, trade_id, balance_before,
                 balance_after, usd_change, description),
            )
            await self._db.commit()
        except Exception as exc:
            log.error(f"insert_portfolio_event error: {exc}")
            await self._db.rollback()

    # ══════════════════════════════════════════════════════════
    # Extended Stats (v4.1 — for enhanced reporting)
    # ══════════════════════════════════════════════════════════

    async def get_extended_stats(self) -> Optional[dict]:
        """Get extended performance metrics: payoff ratio, profit factor, expectancy, avg hold time."""
        try:
            cursor = await self._db.execute(
                """SELECT net_pnl_pct, net_usd_pnl, entry_time, exit_time
                   FROM paper_trades WHERE status='CLOSED'"""
            )
            trades = await cursor.fetchall()
            if not trades:
                return None

            wins = [t for t in trades if (t["net_pnl_pct"] or 0) > 0]
            losses = [t for t in trades if (t["net_pnl_pct"] or 0) < 0]

            avg_win = sum(t["net_pnl_pct"] or 0 for t in wins) / len(wins) if wins else 0.0
            avg_loss = abs(sum(t["net_pnl_pct"] or 0 for t in losses) / len(losses)) if losses else 0.0
            payoff_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

            gross_profit = sum(t["net_usd_pnl"] or 0 for t in wins)
            gross_loss = abs(sum(t["net_usd_pnl"] or 0 for t in losses))
            profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

            n = len(trades)
            win_rate = len(wins) / n if n > 0 else 0.0
            loss_rate = len(losses) / n if n > 0 else 0.0
            expectancy = (win_rate * avg_win) - (loss_rate * avg_loss)

            hold_times = []
            for t in trades:
                if t["entry_time"] and t["exit_time"]:
                    hold_times.append((t["exit_time"] - t["entry_time"]) / 60.0)
            avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0.0

            return {
                "payoff_ratio": payoff_ratio,
                "profit_factor": profit_factor,
                "expectancy": expectancy,
                "avg_hold_minutes": avg_hold,
            }
        except Exception as exc:
            log.error(f"get_extended_stats error: {exc}")
            return None

    async def get_filter_rejection_counts(self, since: float) -> dict:
        """Get counts of filter rejections by reason prefix since a timestamp."""
        try:
            result = {}
            for prefix in [
                "ACTIVITY_TOO_LOW", "LIQUIDITY_TOO_LOW", "VOLUME_TOO_LOW",
                "MCAP_OUT_OF_RANGE", "BUY_RATIO_OUT_OF_RANGE",
                "MOMENTUM_SMA_FAIL", "MOMENTUM_DELTA_FAIL",
                "HURST_TOO_LOW", "GINI_TOO_HIGH", "CVD_SLOPE_OUT_OF_RANGE",
            ]:
                cursor = await self._db.execute(
                    """SELECT COUNT(*) as cnt FROM filter_rejections
                       WHERE timestamp >= ? AND rejection_reason LIKE ?""",
                    (since, f"{prefix}%"),
                )
                row = await cursor.fetchone()
                result[prefix] = row["cnt"] if row else 0
            return result
        except Exception as exc:
            log.error(f"get_filter_rejection_counts error: {exc}")
            return {}

    # ══════════════════════════════════════════════════════════
    # Session Stats (for reporting)
    # ══════════════════════════════════════════════════════════

    async def get_session_stats(self) -> dict:
        try:
            stats = {}
            cursor = await self._db.execute("SELECT COUNT(*) as cnt FROM market_ticks")
            row = await cursor.fetchone()
            stats["total_ticks"] = row["cnt"]

            cursor = await self._db.execute("SELECT COUNT(*) as cnt FROM paper_trades")
            row = await cursor.fetchone()
            stats["total_trades"] = row["cnt"]

            cursor = await self._db.execute(
                "SELECT COUNT(*) as cnt FROM paper_trades WHERE status='OPEN'"
            )
            row = await cursor.fetchone()
            stats["open_trades"] = row["cnt"]

            cursor = await self._db.execute(
                "SELECT COUNT(*) as cnt FROM paper_trades WHERE status='CLOSED'"
            )
            row = await cursor.fetchone()
            stats["closed_trades"] = row["cnt"]

            if stats["closed_trades"] > 0:
                cursor = await self._db.execute(
                    """SELECT
                         AVG(CASE WHEN net_pnl_pct IS NOT NULL THEN net_pnl_pct ELSE pnl_pct END) as avg_pnl,
                         MAX(CASE WHEN net_pnl_pct IS NOT NULL THEN net_pnl_pct ELSE pnl_pct END) as best_pnl,
                         MIN(CASE WHEN net_pnl_pct IS NOT NULL THEN net_pnl_pct ELSE pnl_pct END) as worst_pnl,
                         SUM(CASE WHEN COALESCE(net_pnl_pct, pnl_pct, 0) > 0 THEN 1 ELSE 0 END) as wins
                       FROM paper_trades WHERE status='CLOSED'"""
                )
                row = await cursor.fetchone()
                stats["avg_pnl"] = row["avg_pnl"] or 0.0
                stats["best_pnl"] = row["best_pnl"] or 0.0
                stats["worst_pnl"] = row["worst_pnl"] or 0.0
                stats["win_rate"] = (row["wins"] or 0) / stats["closed_trades"]
            else:
                stats["avg_pnl"] = 0.0
                stats["best_pnl"] = 0.0
                stats["worst_pnl"] = 0.0
                stats["win_rate"] = 0.0

            stats["balance"] = self._balance
            return stats
        except Exception as exc:
            log.error(f"get_session_stats error: {exc}")
            return {
                "total_ticks": 0, "total_trades": 0,
                "open_trades": 0, "closed_trades": 0,
                "avg_pnl": 0.0, "best_pnl": 0.0,
                "worst_pnl": 0.0, "win_rate": 0.0,
                "balance": self._balance,
            }
