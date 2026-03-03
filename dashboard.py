#!/usr/bin/env python3
"""
Solana Meme Coin Quantitative Harvester — Dashboard
dashboard.py
"""

import sqlite3
import shutil
import time
import math
import os
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# PAGE CONFIG — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Harvester",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CONSTANTS — Apple minimalist palette
# ---------------------------------------------------------------------------
DB_PATH = "data/quant_harvest.db"
DB_URI = f"file:{DB_PATH}?mode=ro"

ACCENT   = "#0A84FF"   # Apple system blue
PROFIT   = "#30D158"   # Apple system green
LOSS     = "#FF453A"   # Apple system red
TEXT_PRI = "#FFFFFF"
TEXT_SEC = "rgba(235,235,245,0.6)"
TEXT_TER = "rgba(235,235,245,0.3)"
BORDER   = "rgba(255,255,255,0.06)"

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(
        family="-apple-system,BlinkMacSystemFont,'SF Pro Display','Inter',system-ui,sans-serif",
        color=TEXT_SEC,
        size=11,
    ),
    margin=dict(l=48, r=16, t=48, b=32),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.04)",
        zerolinecolor="rgba(255,255,255,0.04)",
        tickfont=dict(size=10),
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,0.04)",
        zerolinecolor="rgba(255,255,255,0.04)",
        tickfont=dict(size=10),
    ),
)

# ---------------------------------------------------------------------------
# APPLE MINIMALIST CSS
# ---------------------------------------------------------------------------
_CSS = """
<style>
    #MainMenu {visibility: hidden;}
    footer    {visibility: hidden;}
    header    {visibility: hidden;}
    .stDeployButton {display: none;}

    .stApp {background: #000000;}

    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0 !important;
    }

    section[data-testid="stSidebar"] {
        background: rgba(18,18,20,0.95) !important;
        border-right: 1px solid rgba(255,255,255,0.06);
    }

    div[data-testid="stMetric"] {
        background: rgba(28,28,30,0.8);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 16px 20px;
    }
    div[data-testid="stMetric"] label {
        font-size: 0.72rem !important;
        color: rgba(235,235,245,0.45) !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 1.4rem !important;
        font-weight: 600 !important;
        color: #ffffff !important;
        font-variant-numeric: tabular-nums;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: rgba(28,28,30,0.6);
        border-radius: 10px;
        padding: 4px;
        border: 1px solid rgba(255,255,255,0.06);
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 600;
        font-size: 0.85rem;
        color: rgba(235,235,245,0.3);
        border-radius: 8px;
        padding: 8px 16px;
        transition: color 0.15s ease;
    }
    .stTabs [aria-selected="true"] {
        color: #ffffff !important;
        background: rgba(255,255,255,0.08) !important;
    }
    .stTabs [data-baseweb="tab-border"] {display: none;}
    .stTabs [data-baseweb="tab-highlight"] {display: none;}

    .stDataFrame {
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 10px;
        overflow: hidden;
    }

    ::-webkit-scrollbar {width: 4px; height: 4px;}
    ::-webkit-scrollbar-track {background: transparent;}
    ::-webkit-scrollbar-thumb {background: rgba(255,255,255,0.15); border-radius: 2px;}

    @keyframes pulse {
        0%, 100% {opacity: 1;}
        50%       {opacity: 0.35;}
    }
    .live-dot {
        display: inline-block;
        width: 7px; height: 7px;
        background: #30D158;
        border-radius: 50%;
        animation: pulse 1.8s ease-in-out infinite;
        margin-right: 5px;
        vertical-align: middle;
    }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# DATABASE HELPERS — connection pooling via session_state
# ---------------------------------------------------------------------------
def get_connection() -> sqlite3.Connection | None:
    """Return a cached read-only SQLite connection stored in session_state."""
    conn = st.session_state.get("db_conn")
    if conn is None:
        try:
            conn = sqlite3.connect(DB_URI, uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            st.session_state["db_conn"] = conn
        except Exception:
            return None
    return conn


def safe_query(query: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a read-only query and return a DataFrame. Returns empty DF on error."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        # Connection may be stale — drop it so next call reconnects
        st.session_state.pop("db_conn", None)
        return pd.DataFrame()


def table_exists(table_name: str) -> bool:
    df = safe_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return len(df) > 0


# ---------------------------------------------------------------------------
# DATA FETCHING FUNCTIONS  (all queries hit indexes; none are unbounded scans)
# ---------------------------------------------------------------------------

# TTL 10 s — index-leaf read, essentially free
@st.cache_data(ttl=10)
def fetch_tick_count() -> int:
    """O(1) — reads the PK B-tree leaf, no table scan."""
    df = safe_query("SELECT MAX(id) as cnt FROM market_ticks")
    return int(df["cnt"].iloc[0]) if len(df) > 0 and df["cnt"].iloc[0] is not None else 0


@st.cache_data(ttl=5)
def fetch_all_trades() -> pd.DataFrame:
    return safe_query("SELECT * FROM paper_trades ORDER BY entry_time DESC")


@st.cache_data(ttl=5)
def fetch_closed_trades() -> pd.DataFrame:
    return safe_query(
        "SELECT * FROM paper_trades WHERE status='CLOSED' ORDER BY exit_time DESC"
    )


@st.cache_data(ttl=5)
def fetch_open_trades() -> pd.DataFrame:
    return safe_query(
        "SELECT * FROM paper_trades WHERE status='OPEN' ORDER BY entry_time DESC"
    )


# TTL 30 s — keeps latest 500 rows for equity curve; much cheaper than unbounded fetch
@st.cache_data(ttl=30)
def fetch_portfolio_state() -> pd.DataFrame:
    """Fetch at most the last 500 portfolio events (plenty for equity curve)."""
    df = safe_query(
        "SELECT * FROM portfolio_state ORDER BY id DESC LIMIT 500"
    )
    if len(df) > 0:
        df = df.iloc[::-1].reset_index(drop=True)
    return df


# TTL 10 s — uses PK MAX(id) per group: index-leaf lookup, no full scan
@st.cache_data(ttl=10)
def fetch_latest_ticks_per_token() -> pd.DataFrame:
    """Get the most recent tick for each tracked token — O(distinct mints)."""
    return safe_query("""
        SELECT * FROM market_ticks
        WHERE id IN (
            SELECT MAX(id) FROM market_ticks GROUP BY mint
        )
        ORDER BY volume_5m DESC
    """)


# TTL 5 s — ORDER BY id DESC avoids sort; reversed in Python
@st.cache_data(ttl=5)
def fetch_ticks_for_mint(mint: str, limit: int = 300) -> pd.DataFrame:
    """Fetch the most recent `limit` ticks for a mint using PK order (no sort)."""
    df = safe_query(
        "SELECT * FROM market_ticks WHERE mint=? ORDER BY id DESC LIMIT ?",
        (mint, limit),
    )
    if len(df) > 0:
        df = df.iloc[::-1].reset_index(drop=True)
    return df


@st.cache_data(ttl=5)
def fetch_signals_for_trade(trade_id: str) -> pd.DataFrame:
    """Fetch quant signals for a single trade (uses idx_signals_trade index)."""
    return safe_query(
        "SELECT * FROM quant_signals WHERE trade_id=? ORDER BY timestamp ASC",
        (trade_id,),
    )


@st.cache_data(ttl=10)
def fetch_signals_for_trades(trade_ids: tuple) -> pd.DataFrame:
    """Fetch quant signals for a list of trade_ids — bounded by trade count.

    SQLite's variable limit is 999; batch into chunks if needed.
    """
    if not trade_ids:
        return pd.DataFrame()
    # SQLite SQLITE_MAX_VARIABLE_NUMBER default is 999; batch for safety
    CHUNK = 999
    if len(trade_ids) <= CHUNK:
        placeholders = ",".join("?" * len(trade_ids))
        return safe_query(
            f"SELECT * FROM quant_signals WHERE trade_id IN ({placeholders}) ORDER BY timestamp ASC",
            trade_ids,
        )
    # Large set: fetch in chunks then concatenate
    chunks = []
    for i in range(0, len(trade_ids), CHUNK):
        batch = trade_ids[i : i + CHUNK]
        placeholders = ",".join("?" * len(batch))
        chunk_df = safe_query(
            f"SELECT * FROM quant_signals WHERE trade_id IN ({placeholders}) ORDER BY timestamp ASC",
            batch,
        )
        if len(chunk_df) > 0:
            chunks.append(chunk_df)
    return pd.concat(chunks, ignore_index=True).sort_values("timestamp") if chunks else pd.DataFrame()


@st.cache_data(ttl=5)
def fetch_ticks_for_mint_timerange(
    mint: str, start_ts: float, end_ts: float
) -> pd.DataFrame:
    """Fetch ticks for a specific mint + time window (for trade trajectory charts)."""
    return safe_query(
        "SELECT * FROM market_ticks WHERE mint=? AND timestamp>=? AND timestamp<=? ORDER BY id ASC",
        (mint, start_ts, end_ts),
    )


# ---------------------------------------------------------------------------
# COMPUTATION HELPERS
# ---------------------------------------------------------------------------
def compute_win_rate(closed_df: pd.DataFrame) -> float:
    if len(closed_df) == 0:
        return 0.0
    wins = len(closed_df[closed_df["pnl_pct"] > 0])
    return wins / len(closed_df)


def compute_kelly_criterion(closed_df: pd.DataFrame) -> float:
    """Compute fractional Kelly: f* = (p * b - q) / b, where p=win rate, q=loss rate, b=avg_win/avg_loss."""
    if len(closed_df) < 2:
        return 0.0

    winners = closed_df[closed_df["pnl_pct"] > 0]["pnl_pct"]
    losers = closed_df[closed_df["pnl_pct"] <= 0]["pnl_pct"]
    
    if len(winners) == 0 or len(losers) == 0:
        return 0.0
        
    p = len(winners) / len(closed_df)
    q = 1 - p
    avg_win = winners.mean()
    avg_loss = abs(losers.mean())
    
    if avg_loss == 0:
        return 0.0
        
    b = avg_win / avg_loss
    kelly = (p * b - q) / b
    
    # Return quarter-Kelly for safety
    return max(0.0, kelly * 0.25)


def _coalesce_peak_high(df: pd.DataFrame) -> pd.Series:
    """Return peak_high, falling back to entry_price when peak_high is null."""
    return df["peak_high"].fillna(df["entry_price"]).fillna(0)


def compute_mfe(row: pd.Series) -> float:
    """Maximum Favorable Excursion: (peak_high - entry_price) / entry_price."""
    if row["entry_price"] is None or row["entry_price"] == 0:
        return 0.0
    peak_high = row.get("peak_high", row["entry_price"])
    if peak_high is None:
        peak_high = row["entry_price"]
    return (peak_high - row["entry_price"]) / row["entry_price"]


def compute_mae(row: pd.Series) -> float:
    """Maximum Adverse Excursion: (entry_price - peak_low) / entry_price."""
    if row["entry_price"] is None or row["entry_price"] == 0:
        return 0.0
    peak_low = row.get("peak_low", row["entry_price"])
    if peak_low is None:
        peak_low = row["entry_price"]
    return (row["entry_price"] - peak_low) / row["entry_price"]


def compute_optimal_tp(closed_df: pd.DataFrame):
    """
    Sweep TP levels from 0.5% to 80%.
    For each level T: if MFE >= T, simulated PnL = T, else actual pnl_pct.
    Vectorized with NumPy 2D broadcasting — O(N+M) instead of O(N*M) Python loops.
    Returns: sweep_df with columns [tp_level, ev, hit_rate], optimal_tp, optimal_ev, optimal_hit_rate
    """
    if len(closed_df) == 0:
        return pd.DataFrame(), 0.0, 0.0, 0.0

    df = closed_df.copy()
    entry_prices = df["entry_price"].fillna(0).to_numpy(dtype=float)
    # Use entry_price as fallback when peak_high is missing
    peak_highs   = _coalesce_peak_high(df).to_numpy(dtype=float)
    actual_pnl   = df["pnl_pct"].fillna(0).to_numpy(dtype=float)

    valid  = entry_prices > 0
    mfe_arr = np.where(valid, (peak_highs - entry_prices) / entry_prices, 0.0)

    tp_levels = np.arange(0.005, 0.81, 0.005)

    # 2D broadcast: (n_trades, 1) vs (1, n_tp_levels)
    mfe_2d        = mfe_arr[:, np.newaxis]
    tp_2d         = tp_levels[np.newaxis, :]
    actual_pnl_2d = actual_pnl[:, np.newaxis]

    sim_pnl_2d = np.where(mfe_2d >= tp_2d, tp_2d, actual_pnl_2d)
    ev_arr       = sim_pnl_2d.mean(axis=0)
    hit_rate_arr = (mfe_2d >= tp_2d).mean(axis=0)

    sweep_df = pd.DataFrame({
        "tp_level": tp_levels,
        "tp_pct":   tp_levels * 100,
        "ev":       ev_arr,
        "ev_pct":   ev_arr * 100,
        "hit_rate": hit_rate_arr,
    })

    optimal_idx      = sweep_df["ev"].idxmax()
    optimal_tp       = sweep_df.loc[optimal_idx, "tp_level"]
    optimal_ev       = sweep_df.loc[optimal_idx, "ev"]
    optimal_hit_rate = sweep_df.loc[optimal_idx, "hit_rate"]

    return sweep_df, optimal_tp, optimal_ev, optimal_hit_rate


def ts_to_dt(ts) -> str:
    """Convert unix timestamp to readable datetime string."""
    if ts is None or (isinstance(ts, float) and math.isnan(ts)):
        return "—"
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "—"


def format_usd(val) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "$0.00"
    if abs(val) >= 1_000_000:
        return f"${val/1_000_000:,.2f}M"
    if abs(val) >= 1_000:
        return f"${val/1_000:,.2f}K"
    return f"${val:,.4f}"


def format_price(val) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "$0"
    if val >= 1.0:
        return f"${val:,.4f}"
    if val >= 0.001:
        return f"${val:,.6f}"
    return f"${val:.10f}"


def format_pnl_pct(val) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "0.00%"
    return f"{val * 100:+.2f}%"


def build_ohlc_from_ticks(ticks_df: pd.DataFrame, freq_seconds: int = 60) -> pd.DataFrame:
    """Build OHLC candles from raw tick data."""
    if len(ticks_df) == 0:
        return pd.DataFrame()
        
    df = ticks_df.copy()
    df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.set_index("dt").sort_index()
    
    freq_str = f"{freq_seconds}s"
    ohlc = df["price_usd"].resample(freq_str).agg(["first", "max", "min", "last"]).dropna()
    ohlc.columns = ["open", "high", "low", "close"]
    
    if "volume_5m" in df.columns:
        vol = df["volume_5m"].resample(freq_str).last().fillna(0)
        ohlc["volume"] = vol
        
    ohlc = ohlc.reset_index()
    return ohlc


# ---------------------------------------------------------------------------
# PLOTLY CHART BUILDERS
# ---------------------------------------------------------------------------
def apply_default_layout(fig: go.Figure, title: str = "", height: int = 400) -> go.Figure:
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(
            text=title,
            font=dict(
                family="-apple-system,BlinkMacSystemFont,'SF Pro Display',system-ui,sans-serif",
                size=13,
                color=TEXT_PRI,
            ),
            x=0.0,
        ),
        height=height,
        showlegend=True,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=BORDER,
            borderwidth=1,
            font=dict(size=10, color=TEXT_SEC),
        ),
    )
    return fig


def build_candlestick_chart(ohlc_df: pd.DataFrame, title: str = "Price") -> go.Figure:
    if len(ohlc_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="No tick data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_SEC, size=14),
        )
        return apply_default_layout(fig, title, 350)

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=ohlc_df["dt"],
        open=ohlc_df["open"],
        high=ohlc_df["high"],
        low=ohlc_df["low"],
        close=ohlc_df["close"],
        increasing=dict(line=dict(color=PROFIT, width=1), fillcolor="rgba(48,209,88,0.55)"),
        decreasing=dict(line=dict(color=LOSS,   width=1), fillcolor="rgba(255,69,58,0.55)"),
        name="Price",
    ))
    fig.update_xaxes(rangeslider_visible=False)
    return apply_default_layout(fig, title, 350)


def build_hurst_chart(ticks_df: pd.DataFrame, signals_df: pd.DataFrame) -> go.Figure:
    """Build Hurst exponent timeline from quant_signals linked to a token."""
    fig = go.Figure()

    if len(signals_df) > 0 and "hurst_value" in signals_df.columns:
        sig = signals_df.copy()
        sig["dt"] = pd.to_datetime(sig["timestamp"], unit="s", utc=True)
        sig = sig.sort_values("dt")

        fig.add_trace(go.Scatter(
            x=sig["dt"], y=sig["hurst_value"],
            mode="lines+markers",
            line=dict(color=ACCENT, width=2),
            marker=dict(size=4, color=ACCENT),
            name="Hurst H",
        ))

        # Fill above 0.6 (trending) with green
        h_above = sig["hurst_value"].clip(lower=0.6)
        fig.add_trace(go.Scatter(
            x=sig["dt"], y=h_above,
            fill="tonexty" if len(sig) > 1 else None,
            mode="none",
            fillcolor="rgba(48,209,88,0.1)",
            showlegend=False,
        ))
        
        # Threshold baseline
        fig.add_trace(go.Scatter(
            x=sig["dt"], y=[0.6] * len(sig),
            mode="lines",
            line=dict(color=PROFIT, width=1, dash="dot"),
            name="H=0.6 (Trend)",
        ))

        # 0.5 random walk line
        fig.add_trace(go.Scatter(
            x=sig["dt"], y=[0.5] * len(sig),
            mode="lines",
            line=dict(color="rgba(255,255,255,0.2)", width=1, dash="dash"),
            name="H=0.5 (Random)",
        ))
    else:
        fig.add_annotation(
            text="No Hurst data — waiting for signals",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_SEC, size=13),
        )

    fig.update_yaxes(range=[0, 1])
    return apply_default_layout(fig, "Hurst Exponent (H)", 250)


def build_cvd_chart(ticks_df: pd.DataFrame, signals_df: pd.DataFrame) -> go.Figure:
    """Build micro-CVD from tick buy/sell data + signal overlays."""
    fig = go.Figure()

    if len(ticks_df) > 0 and "buys_5m" in ticks_df.columns and "sells_5m" in ticks_df.columns:
        df = ticks_df.copy()
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df = df.sort_values("dt")

        df["delta"] = df["buys_5m"].fillna(0) - df["sells_5m"].fillna(0)
        df["cvd"] = df["delta"].cumsum()

        fig.add_trace(go.Scatter(
            x=df["dt"], y=df["cvd"],
            fill="tozeroy",
            mode="lines",
            line=dict(color=ACCENT, width=1.5),
            fillcolor="rgba(10,132,255,0.08)",
            name="Micro-CVD",
        ))

        # Overlay signal CVD values if present
        if len(signals_df) > 0 and "cvd_value" in signals_df.columns:
            sig = signals_df.copy()
            sig["dt"] = pd.to_datetime(sig["timestamp"], unit="s", utc=True)
            sig = sig.sort_values("dt")

            fig.add_trace(go.Scatter(
                x=sig["dt"], y=sig["cvd_value"],
                mode="markers",
                marker=dict(size=7, color=PROFIT),
                name="Signal CVD",
            ))

    if len(ticks_df) == 0 and len(signals_df) == 0:
        fig.add_annotation(
            text="No CVD data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_SEC, size=13),
        )

    return apply_default_layout(fig, "Cumulative Volume Delta", 250)


def build_equity_curve(portfolio_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    if len(portfolio_df) == 0:
        fig.add_annotation(
            text="No portfolio data yet",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_SEC, size=14),
        )
        return apply_default_layout(fig, "Equity Curve", 350)

    df = portfolio_df.copy()
    df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.sort_values("dt")

    fig.add_trace(go.Scatter(
        x=df["dt"], y=df["balance_after"],
        mode="lines+markers",
        line=dict(color=ACCENT, width=2, shape="spline"),
        marker=dict(
            size=5,
            color=[PROFIT if c >= 0 else LOSS for c in df["usd_change"].fillna(0)],
        ),
        fill="tozeroy",
        fillcolor="rgba(10,132,255,0.06)",
        name="Balance",
    ))

    return apply_default_layout(fig, "Equity Curve", 350)


def build_mfe_mae_scatter(closed_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    if len(closed_df) == 0:
        fig.add_annotation(
            text="No closed trades",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_SEC, size=14),
        )
        return apply_default_layout(fig, "MFE vs MAE", 400)

    df = closed_df.copy()
    df["mfe"] = df.apply(compute_mfe, axis=1) * 100
    df["mae"] = df.apply(compute_mae, axis=1) * 100
    df["pnl_display"] = df["pnl_pct"].fillna(0) * 100

    colors = [PROFIT if p > 0 else LOSS for p in df["pnl_display"]]

    fig.add_trace(go.Scatter(
        x=df["mae"], y=df["mfe"],
        mode="markers+text",
        marker=dict(
            size=10,
            color=colors,
            opacity=0.85,
        ),
        text=df["symbol"],
        textposition="top center",
        textfont=dict(size=9, color="#aaaaaa"),
        name="Trades",
        hovertemplate=(
            "<b>%{text}</b><br>"
            "MAE: %{x:.2f}%<br>"
            "MFE: %{y:.2f}%<br>"
            "PnL: %{customdata:.2f}%<extra></extra>"
        ),
        customdata=df["pnl_display"],
    ))
    
    # Diagonal reference line (MFE = MAE)
    max_val = max(df["mfe"].max(), df["mae"].max(), 5)
    fig.add_trace(go.Scatter(
        x=[0, max_val], y=[0, max_val],
        mode="lines",
        line=dict(color="rgba(255,255,255,0.12)", dash="dash", width=1),
        name="MFE=MAE",
        showlegend=False,
    ))

    fig.update_xaxes(title_text="MAE (Max Drawdown %)", title_font=dict(color=LOSS))
    fig.update_yaxes(title_text="MFE (Max Potential %)", title_font=dict(color=PROFIT))

    return apply_default_layout(fig, "MFE vs MAE", 400)


def build_tp_optimizer_chart(sweep_df: pd.DataFrame, optimal_tp: float, optimal_ev: float) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("Expected Value at TP Level", "Hit Rate at TP Level"),
        row_heights=[0.6, 0.4],
    )

    if len(sweep_df) == 0:
        fig.add_annotation(
            text="Insufficient data for TP optimization",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_SEC, size=14),
        )
        return apply_default_layout(fig, "TP Optimizer", 500)

    # EV curve
    fig.add_trace(go.Scatter(
        x=sweep_df["tp_pct"], y=sweep_df["ev_pct"],
        mode="lines",
        line=dict(color=ACCENT, width=2),
        fill="tozeroy",
        fillcolor="rgba(10,132,255,0.06)",
        name="EV(%)",
    ), row=1, col=1)

    # Optimal T* marker
    fig.add_trace(go.Scatter(
        x=[optimal_tp * 100], y=[optimal_ev * 100],
        mode="markers+text",
        marker=dict(size=12, color=PROFIT),
        text=[f"T*={optimal_tp*100:.1f}%"],
        textposition="top center",
        textfont=dict(color=PROFIT, size=11),
        name="Optimal T*",
        showlegend=True,
    ), row=1, col=1)

    # Hit rate curve
    fig.add_trace(go.Scatter(
        x=sweep_df["tp_pct"], y=sweep_df["hit_rate"],
        mode="lines",
        line=dict(color=PROFIT, width=2),
        fill="tozeroy",
        fillcolor="rgba(48,209,88,0.06)",
        name="Hit Rate",
    ), row=2, col=1)

    # Vertical line at optimal
    for row_idx in [1, 2]:
        fig.add_vline(
            x=optimal_tp * 100,
            line_dash="dash", line_color=PROFIT, line_width=1,
            row=row_idx, col=1,
        )

    fig.update_xaxes(title_text="Take Profit Level (%)", row=2, col=1)
    fig.update_yaxes(title_text="EV (%)", row=1, col=1)
    fig.update_yaxes(title_text="Hit Rate", row=2, col=1)

    for annotation in fig["layout"]["annotations"]:
        annotation["font"] = dict(color=TEXT_SEC, size=11)

    return apply_default_layout(fig, "Dynamic TP Optimizer", 500)


def build_trade_trajectory(
    trade_row: pd.Series,
    ticks_df: pd.DataFrame
) -> go.Figure:
    """Build a price trajectory chart for a single trade."""
    fig = go.Figure()
    
    entry_price = trade_row["entry_price"]
    exit_price = trade_row.get("exit_price", None)
    peak_high = trade_row.get("peak_high", None)
    peak_low = trade_row.get("peak_low", None)

    if len(ticks_df) > 0:
        df = ticks_df.copy()
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df = df.sort_values("dt")

        # Price line
        fig.add_trace(go.Scatter(
            x=df["dt"], y=df["price_usd"],
            mode="lines",
            line=dict(color=ACCENT, width=2),
            fill="tozeroy",
            fillcolor="rgba(10,132,255,0.06)",
            name="Price",
        ))
        x_range = [df["dt"].iloc[0], df["dt"].iloc[-1]]
    else:
        entry_dt = datetime.fromtimestamp(trade_row["entry_time"], tz=timezone.utc)
        exit_ts = trade_row.get("exit_time", None)
        if exit_ts and not (isinstance(exit_ts, float) and math.isnan(exit_ts)):
            exit_dt = datetime.fromtimestamp(exit_ts, tz=timezone.utc)
        else:
            exit_dt = entry_dt
        x_range = [entry_dt, exit_dt]

        fig.add_annotation(
            text="Tick data not available for this period",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=TEXT_TER, size=12),
        )

    # Entry price line
    if entry_price is not None:
        fig.add_hline(
            y=entry_price,
            line_dash="solid", line_color=ACCENT, line_width=1.5,
            annotation_text=f"Entry {format_price(entry_price)}",
            annotation_position="top left",
            annotation_font=dict(color=ACCENT, size=10),
        )

    # Exit price line
    if exit_price is not None and not (isinstance(exit_price, float) and math.isnan(exit_price)):
        exit_color = PROFIT if (exit_price > entry_price) else LOSS
        fig.add_hline(
            y=exit_price,
            line_dash="solid", line_color=exit_color, line_width=1.5,
            annotation_text=f"Exit {format_price(exit_price)}",
            annotation_position="bottom left",
            annotation_font=dict(color=exit_color, size=10),
        )

    # Peak high line
    if peak_high is not None and not (isinstance(peak_high, float) and math.isnan(peak_high)):
        fig.add_hline(
            y=peak_high,
            line_dash="dot", line_color=PROFIT, line_width=1,
            annotation_text=f"Peak ▲ {format_price(peak_high)}",
            annotation_position="top right",
            annotation_font=dict(color=PROFIT, size=9),
        )

    # Peak low line
    if peak_low is not None and not (isinstance(peak_low, float) and math.isnan(peak_low)):
        fig.add_hline(
            y=peak_low,
            line_dash="dot", line_color=LOSS, line_width=1,
            annotation_text=f"Trough ▼ {format_price(peak_low)}",
            annotation_position="bottom right",
            annotation_font=dict(color=LOSS, size=9),
        )

    return apply_default_layout(fig, f"Trajectory — {trade_row.get('symbol', '?')}", 350)


# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------
def render_sidebar():
    with st.sidebar:
        st.markdown(
            '<span class="live-dot"></span>'
            '<span style="font-size:0.75rem; color:rgba(235,235,245,0.45);">Live</span>',
            unsafe_allow_html=True,
        )
        st.markdown("### Harvester")

        refresh_interval = st.slider(
            "Auto-refresh (sec)",
            min_value=5, max_value=120, value=15, step=5,
            key="refresh_slider",
        )

        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=refresh_interval * 1000, limit=None, key="auto_refresh")
        except ImportError:
            if st.button("Refresh"):
                st.cache_data.clear()

        st.markdown("---")

        # Check DB existence
        if not os.path.exists(DB_PATH):
            st.error("Database not found")
            st.caption(f"Expected: `{DB_PATH}`")
            st.stop()

        st.caption("System")

        total_ticks  = fetch_tick_count()
        latest_ticks = fetch_latest_ticks_per_token()
        tracked_tokens = len(latest_ticks)
        closed_df    = fetch_closed_trades()
        all_trades   = fetch_all_trades()

        win_rate     = compute_win_rate(closed_df)
        closed_count = len(closed_df)
        open_count   = len(all_trades) - closed_count

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Ticks", f"{total_ticks:,}")
        with col2:
            st.metric("Tokens", f"{tracked_tokens}")

        col3, col4 = st.columns(2)
        with col3:
            st.metric("Win Rate", f"{win_rate*100:.1f}%", delta=f"{closed_count} closed")
        with col4:
            st.metric("Open", f"{open_count}", delta=f"{closed_count + open_count} total")

        st.markdown("---")
        st.caption("Optimal TP")

        if len(closed_df) >= 3:
            sweep_df, optimal_tp, optimal_ev, optimal_hit_rate = compute_optimal_tp(closed_df)
            c1, c2 = st.columns(2)
            with c1:
                st.metric("T* Level", f"{optimal_tp*100:.1f}%")
            with c2:
                st.metric("EV at T*", f"{optimal_ev*100:.2f}%")
            st.metric("Hit Rate", f"{optimal_hit_rate*100:.1f}%")
        else:
            st.caption("Need ≥3 closed trades")

        st.markdown("---")
        st.caption("Export")

        # Export only runs when user explicitly clicks — not on every render
        if st.button("Prepare database ZIP"):
            st.session_state["export_ready"] = False
            if os.path.exists(DB_PATH):
                try:
                    shutil.make_archive("quant_harvest_backup", "zip", "data")
                    st.session_state["export_ready"] = True
                except Exception as e:
                    st.error(f"Export failed: {e}")

        if st.session_state.get("export_ready") and os.path.exists("quant_harvest_backup.zip"):
            with open("quant_harvest_backup.zip", "rb") as f:
                st.download_button(
                    label="Download ZIP",
                    data=f,
                    file_name="quant_harvest_backup.zip",
                    mime="application/zip",
                )

        st.markdown("---")
        st.caption(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))


# ---------------------------------------------------------------------------
# TAB 1: LIVE  (was "War Room")
# ---------------------------------------------------------------------------
def render_live():
    st.markdown('<span class="live-dot"></span> **Live**', unsafe_allow_html=True)

    latest_ticks = fetch_latest_ticks_per_token()

    if len(latest_ticks) == 0:
        st.info("No market data yet. Waiting for the harvester to start…")
        return

    # Token overview table
    display_df = latest_ticks[
        ["symbol", "mint", "price_usd", "market_cap", "liquidity_usd", "volume_5m", "buys_5m", "sells_5m"]
    ].copy()
    display_df["price_usd"]     = display_df["price_usd"].apply(format_price)
    display_df["market_cap"]    = display_df["market_cap"].apply(format_usd)
    display_df["liquidity_usd"] = display_df["liquidity_usd"].apply(format_usd)
    display_df["volume_5m"]     = display_df["volume_5m"].apply(format_usd)
    display_df.columns = ["Symbol", "Mint", "Price", "MCap", "Liquidity", "Vol 5m", "Buys", "Sells"]

    st.dataframe(
        display_df,
        use_container_width=True,
        height=min(250, 35 * len(display_df) + 40),
        hide_index=True,
    )

    # Token selector
    token_options = latest_ticks[["symbol", "mint"]].apply(
        lambda r: f"{r['symbol']} ({r['mint'][:8]}…{r['mint'][-4:]})", axis=1
    ).tolist()

    if not token_options:
        return

    selected_idx = st.selectbox(
        "Token",
        range(len(token_options)),
        format_func=lambda i: token_options[i],
        label_visibility="collapsed",
    )

    selected_mint   = latest_ticks.iloc[selected_idx]["mint"]
    selected_symbol = latest_ticks.iloc[selected_idx]["symbol"]

    st.markdown(f"**{selected_symbol}** `{selected_mint}`")

    # Fetch ticks + signals for selected token
    ticks_df    = fetch_ticks_for_mint(selected_mint, limit=300)
    all_trades  = fetch_all_trades()
    token_trades = all_trades[all_trades["mint"] == selected_mint] if len(all_trades) > 0 else pd.DataFrame()
    token_trade_ids = tuple(token_trades["trade_id"].tolist()) if len(token_trades) > 0 else ()
    token_signals   = fetch_signals_for_trades(token_trade_ids) if token_trade_ids else pd.DataFrame()

    # Candlestick chart
    ohlc_df    = build_ohlc_from_ticks(ticks_df, freq_seconds=60)
    candle_fig = build_candlestick_chart(ohlc_df, f"Price — {selected_symbol}")

    if len(token_trades) > 0:
        for _, t in token_trades[token_trades["status"] == "OPEN"].iterrows():
            candle_fig.add_hline(
                y=t["entry_price"],
                line_dash="dash", line_color=ACCENT, line_width=1,
                annotation_text=f"Open @ {format_price(t['entry_price'])}",
                annotation_font=dict(color=ACCENT, size=9),
            )

    st.plotly_chart(candle_fig, use_container_width=True, config={"displayModeBar": False})

    # Hurst + CVD side by side
    col_h, col_c = st.columns(2)
    with col_h:
        st.plotly_chart(
            build_hurst_chart(ticks_df, token_signals),
            use_container_width=True, config={"displayModeBar": False},
        )
    with col_c:
        st.plotly_chart(
            build_cvd_chart(ticks_df, token_signals),
            use_container_width=True, config={"displayModeBar": False},
        )

    # Active positions for this token
    if len(token_trades) > 0:
        open_here = token_trades[token_trades["status"] == "OPEN"]
        if len(open_here) > 0:
            st.caption("Active positions")
            pos_rows = []
            for _, t in open_here.iterrows():
                mfe = compute_mfe(t)
                mae = compute_mae(t)
                # v4.1 optional columns
                partial_tp  = t.get("partial_tp_hits", None)
                trail_stop  = t.get("trailing_stop_status", None)
                partial_stop = t.get("partial_stop_status", None)
                row = {
                    "Trade ID": t.get("trade_id", "?"),
                    "Entry":    format_price(t.get("entry_price", 0)),
                    "Size":     format_usd(t.get("usd_size") or 0),
                    "MFE":      f"{mfe*100:.2f}%",
                    "MAE":      f"{mae*100:.2f}%",
                }
                if partial_tp is not None:
                    row["Partial TP"] = str(partial_tp)
                if trail_stop is not None:
                    row["Trail Stop"] = str(trail_stop)
                if partial_stop is not None:
                    row["Partial Stop"] = str(partial_stop)
                pos_rows.append(row)
            st.dataframe(pd.DataFrame(pos_rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# TAB 2: PERFORMANCE  (was "Analytics")
# ---------------------------------------------------------------------------
def render_performance():
    st.markdown("### Performance")

    closed_df    = fetch_closed_trades()
    portfolio_df = fetch_portfolio_state()

    # Key metrics row
    if len(closed_df) > 0:
        win_rate  = compute_win_rate(closed_df)
        kelly     = compute_kelly_criterion(closed_df)
        avg_pnl   = closed_df["pnl_pct"].mean() if "pnl_pct" in closed_df.columns else 0
        best      = closed_df["pnl_pct"].max()
        worst     = closed_df["pnl_pct"].min()
        total_usd = closed_df["usd_pnl"].sum() if "usd_pnl" in closed_df.columns else 0
        avg_usd   = closed_df["usd_pnl"].mean() if "usd_pnl" in closed_df.columns else 0

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1: st.metric("¼ Kelly f*", f"{kelly*100:.2f}%")
        with c2: st.metric("Win Rate",   f"{win_rate*100:.1f}%")
        with c3: st.metric("Avg PnL",    format_pnl_pct(avg_pnl))
        with c4: st.metric("Best",       format_pnl_pct(best))
        with c5: st.metric("Worst",      format_pnl_pct(worst))
        with c6: st.metric("Total USD",  format_usd(total_usd))

        c7, c8, c9, c10 = st.columns(4)
        with c7: st.metric("Closed Trades", f"{len(closed_df)}")
        with c8: st.metric("Avg USD PnL",   format_usd(avg_usd))
        with c9:
            gross_profit = closed_df[closed_df["usd_pnl"] > 0]["usd_pnl"].sum() if "usd_pnl" in closed_df.columns else 0
            gross_loss   = abs(closed_df[closed_df["usd_pnl"] <= 0]["usd_pnl"].sum()) if "usd_pnl" in closed_df.columns else 0
            pf           = gross_profit / gross_loss if gross_loss > 0 else float("inf")
            st.metric("Profit Factor", f"{pf:.2f}" if pf < 100 else "∞")
        with c10:
            entry_p  = closed_df["entry_price"].fillna(0).to_numpy(dtype=float)
            peak_h   = _coalesce_peak_high(closed_df).to_numpy(dtype=float)
            valid    = entry_p > 0
            mfe_mean = np.where(valid, (peak_h - entry_p) / entry_p, 0.0).mean() * 100
            st.metric("Avg MFE", f"{mfe_mean:.2f}%")
    else:
        st.info("No closed trades yet — analytics will populate as trades complete.")

    st.markdown("---")

    # Equity curve
    st.plotly_chart(build_equity_curve(portfolio_df), use_container_width=True, config={"displayModeBar": False})

    st.markdown("---")

    # TP Optimizer
    st.caption("Dynamic TP Optimizer")
    if len(closed_df) >= 3:
        sweep_df, optimal_tp, optimal_ev, optimal_hit_rate = compute_optimal_tp(closed_df)
        st.plotly_chart(
            build_tp_optimizer_chart(sweep_df, optimal_tp, optimal_ev),
            use_container_width=True, config={"displayModeBar": False},
        )
        st.caption(
            f"Optimal T* = {optimal_tp*100:.1f}%  ·  "
            f"EV = {optimal_ev*100:.3f}% per trade  ·  "
            f"Hit Rate = {optimal_hit_rate*100:.1f}%"
        )
    else:
        st.caption("Need ≥3 closed trades for TP optimization.")

    st.markdown("---")

    # MFE vs MAE
    st.plotly_chart(build_mfe_mae_scatter(closed_df), use_container_width=True, config={"displayModeBar": False})

    # PnL waterfall
    if len(closed_df) > 0:
        pnl_vals = closed_df["pnl_pct"].fillna(0) * 100
        colors   = [PROFIT if v > 0 else LOSS for v in pnl_vals]
        avg_val  = pnl_vals.mean()

        pnl_fig = go.Figure()
        pnl_fig.add_trace(go.Bar(
            x=list(range(len(pnl_vals))),
            y=pnl_vals.values,
            marker=dict(color=colors),
            name="PnL %",
            hovertemplate="Trade %{x}<br>PnL: %{y:.2f}%<extra></extra>",
        ))
        pnl_fig.add_hline(
            y=avg_val,
            line_dash="dash", line_color=ACCENT,
            annotation_text=f"Avg: {avg_val:.2f}%",
            annotation_font=dict(color=ACCENT, size=10),
        )
        pnl_fig.add_hline(y=0, line_color="rgba(255,255,255,0.08)", line_width=1)
        pnl_fig.update_xaxes(title_text="Trade #")
        pnl_fig.update_yaxes(title_text="PnL (%)")
        pnl_fig = apply_default_layout(pnl_fig, "PnL Distribution", 300)
        st.plotly_chart(pnl_fig, use_container_width=True, config={"displayModeBar": False})

    # Signal distributions — fetch only for closed trades (bounded)
    if len(closed_df) > 0:
        trade_ids  = tuple(closed_df["trade_id"].tolist())
        signals_df = fetch_signals_for_trades(trade_ids)
        if len(signals_df) > 0:
            sig_col1, sig_col2, sig_col3 = st.columns(3)
            with sig_col1:
                if "hurst_value" in signals_df.columns:
                    h_fig = go.Figure()
                    h_fig.add_trace(go.Histogram(
                        x=signals_df["hurst_value"].dropna(), nbinsx=30,
                        marker=dict(color=ACCENT, opacity=0.75), name="Hurst",
                    ))
                    h_fig.add_vline(x=0.5, line_dash="dash", line_color="rgba(255,255,255,0.2)")
                    h_fig.add_vline(x=0.6, line_dash="dash", line_color=PROFIT)
                    st.plotly_chart(apply_default_layout(h_fig, "Hurst Dist", 250), use_container_width=True, config={"displayModeBar": False})
            with sig_col2:
                if "cvd_slope" in signals_df.columns:
                    c_fig = go.Figure()
                    c_fig.add_trace(go.Histogram(
                        x=signals_df["cvd_slope"].dropna(), nbinsx=30,
                        marker=dict(color=TEXT_SEC, opacity=0.75), name="CVD Slope",
                    ))
                    c_fig.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")
                    st.plotly_chart(apply_default_layout(c_fig, "CVD Slope Dist", 250), use_container_width=True, config={"displayModeBar": False})
            with sig_col3:
                if "gini_coeff" in signals_df.columns:
                    g_fig = go.Figure()
                    g_fig.add_trace(go.Histogram(
                        x=signals_df["gini_coeff"].dropna(), nbinsx=30,
                        marker=dict(color=PROFIT, opacity=0.75), name="Gini",
                    ))
                    st.plotly_chart(apply_default_layout(g_fig, "Gini Dist", 250), use_container_width=True, config={"displayModeBar": False})

    # Open positions summary
    open_df = fetch_open_trades()
    if len(open_df) > 0:
        st.markdown("---")
        st.caption("Open positions")
        pos_rows = []
        for _, row in open_df.iterrows():
            mint         = row["mint"]
            entry_price  = row.get("entry_price", 0) or 0
            latest_tick  = fetch_ticks_for_mint(mint, limit=1)
            current_price = latest_tick.iloc[0]["price_usd"] if len(latest_tick) > 0 else entry_price
            live_pnl = (current_price - entry_price) / entry_price if entry_price > 0 else 0
            dur_secs = time.time() - (row.get("entry_time") or time.time())
            dur_str  = f"{int(dur_secs // 3600)}h {int((dur_secs % 3600) // 60)}m"
            r = {
                "Symbol":       row.get("symbol", "?"),
                "Entry":        format_price(entry_price),
                "Current":      format_price(current_price),
                "PnL %":        f"{live_pnl*100:+.2f}%",
                "Duration":     dur_str,
            }
            # v4.1 optional columns
            for col_name, label in [
                ("partial_tp_hits",    "Partial TP"),
                ("trailing_stop_status", "Trail Stop"),
                ("partial_stop_status",  "Partial Stop"),
            ]:
                val = row.get(col_name)
                if val is not None:
                    r[label] = str(val)
            pos_rows.append(r)
        st.dataframe(pd.DataFrame(pos_rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# TAB 3: TRADES  (was "Trade Journal")
# ---------------------------------------------------------------------------
def render_trades():
    st.markdown("### Trades")

    all_trades = fetch_all_trades()

    if len(all_trades) == 0:
        st.info("No trades recorded yet. The paper engine will log entries here.")
        return

    open_count   = len(all_trades[all_trades["status"] == "OPEN"])
    closed_count = len(all_trades[all_trades["status"] == "CLOSED"])

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total", f"{len(all_trades)}")
    with c2: st.metric("Open",   f"{open_count}")
    with c3: st.metric("Closed", f"{closed_count}")

    st.markdown("---")

    status_filter = st.radio(
        "Filter",
        ["ALL", "OPEN", "CLOSED"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if status_filter == "OPEN":
        filtered = all_trades[all_trades["status"] == "OPEN"].copy()
    elif status_filter == "CLOSED":
        filtered = all_trades[all_trades["status"] == "CLOSED"].copy()
    else:
        filtered = all_trades.copy()

    if len(filtered) == 0:
        st.caption("No trades match filter.")
        return

    # Build display dataframe
    filtered["mfe_pct"] = filtered.apply(compute_mfe, axis=1) * 100
    filtered["mae_pct"] = filtered.apply(compute_mae, axis=1) * 100

    table_cols = {
        "symbol":      "Symbol",
        "status":      "Status",
        "pnl_pct":     "PnL %",
        "usd_pnl":     "USD PnL",
        "exit_reason": "Exit Reason",
        "entry_time":  "Entry",
        "exit_time":   "Exit",
        "mfe_pct":     "MFE %",
        "mae_pct":     "MAE %",
    }
    # v4.1 optional columns
    for col_name, label in [
        ("partial_tp_hits",    "Partial TP"),
        ("trailing_stop_status", "Trail Stop"),
        ("partial_stop_status",  "Partial Stop"),
    ]:
        if col_name in filtered.columns:
            table_cols[col_name] = label

    avail_cols = [c for c in table_cols if c in filtered.columns]
    disp = filtered[avail_cols].copy()
    disp.rename(columns={c: table_cols[c] for c in avail_cols}, inplace=True)

    if "PnL %" in disp.columns:
        disp["PnL %"] = disp["PnL %"].apply(lambda v: f"{v*100:+.2f}%" if pd.notna(v) else "—")
    if "USD PnL" in disp.columns:
        disp["USD PnL"] = disp["USD PnL"].apply(lambda v: format_usd(v) if pd.notna(v) else "—")
    if "Entry" in disp.columns:
        disp["Entry"] = disp["Entry"].apply(ts_to_dt)
    if "Exit" in disp.columns:
        disp["Exit"]  = disp["Exit"].apply(ts_to_dt)
    if "MFE %" in disp.columns:
        disp["MFE %"] = disp["MFE %"].apply(lambda v: f"{v:.2f}%")
    if "MAE %" in disp.columns:
        disp["MAE %"] = disp["MAE %"].apply(lambda v: f"{v:.2f}%")

    st.dataframe(disp, use_container_width=True, hide_index=True, height=360)

    # Drill-down
    st.markdown("---")
    st.caption("Drill-down")

    trade_options = filtered["trade_id"].tolist() if "trade_id" in filtered.columns else []
    if not trade_options:
        return

    label_map = {}
    for _, row in filtered.iterrows():
        tid = row.get("trade_id", "?")
        sym = row.get("symbol", "?")
        pnl = row.get("pnl_pct", None)
        pnl_s = f"{pnl*100:+.2f}%" if pnl is not None and pd.notna(pnl) else "open"
        label_map[tid] = f"{sym}  {pnl_s}  ({tid[:12]}…)"

    selected_tid = st.selectbox(
        "Select trade",
        trade_options,
        format_func=lambda tid: label_map.get(tid, tid),
        label_visibility="collapsed",
    )

    trade = filtered[filtered["trade_id"] == selected_tid].iloc[0]

    dc1, dc2, dc3, dc4 = st.columns(4)
    with dc1:
        st.metric("Entry Price",  format_price(trade.get("entry_price", 0)))
        st.caption(ts_to_dt(trade.get("entry_time")))
    with dc2:
        ep = trade.get("exit_price")
        st.metric("Exit Price", format_price(ep) if ep and not (isinstance(ep, float) and math.isnan(ep)) else "—")
        st.caption(ts_to_dt(trade.get("exit_time")))
    with dc3:
        pnl_v = trade.get("pnl_pct", None) or 0
        pnl_c = PROFIT if pnl_v > 0 else LOSS
        st.metric("PnL %", format_pnl_pct(pnl_v))
        st.metric("USD PnL", format_usd(trade.get("usd_pnl", 0) or 0))
    with dc4:
        st.metric("MFE", f"{compute_mfe(trade)*100:.2f}%")
        st.metric("MAE", f"{compute_mae(trade)*100:.2f}%")

    # v4.1 info
    for col_name, label in [
        ("partial_tp_hits",    "Partial TP hits"),
        ("trailing_stop_status", "Trailing stop"),
        ("partial_stop_status",  "Partial stop"),
    ]:
        val = trade.get(col_name)
        if val is not None and not (isinstance(val, float) and math.isnan(val)):
            st.caption(f"{label}: **{val}**")

    # Entry signals
    trade_signals = fetch_signals_for_trade(selected_tid)
    if len(trade_signals) > 0:
        st.caption("Entry signals")
        sig_c1, sig_c2, sig_c3, sig_c4 = st.columns(4)
        latest_sig = trade_signals.iloc[0]
        with sig_c1:
            h_val = latest_sig.get("hurst_value")
            st.metric("Hurst H", f"{h_val:.4f}" if h_val else "—")
        with sig_c2:
            st.metric("CVD", f"{latest_sig.get('cvd_value', 0) or 0:.2f}")
        with sig_c3:
            slope = latest_sig.get("cvd_slope", 0)
            st.metric("CVD Slope", f"{slope:.4f}" if slope else "—")
        with sig_c4:
            gini = latest_sig.get("gini_coeff")
            st.metric("Gini", f"{gini:.4f}" if gini else "—")

    # Trajectory chart
    st.caption("Price trajectory")
    entry_ts  = trade.get("entry_time", 0)
    exit_ts_v = trade.get("exit_time")
    if exit_ts_v and not (isinstance(exit_ts_v, float) and math.isnan(exit_ts_v)):
        start_ts = entry_ts - 300
        end_ts   = exit_ts_v + 300
    else:
        start_ts = entry_ts - 300
        end_ts   = time.time()

    mint_v = trade.get("mint", "")
    traj_ticks = fetch_ticks_for_mint_timerange(mint_v, start_ts, end_ts)
    st.plotly_chart(
        build_trade_trajectory(trade, traj_ticks),
        use_container_width=True, config={"displayModeBar": False},
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    render_sidebar()

    st.markdown("## Quantitative Harvester")
    st.caption("Solana meme coin paper trading engine")

    tab1, tab2, tab3 = st.tabs(["Live", "Performance", "Trades"])

    with tab1:
        render_live()
    with tab2:
        render_performance()
    with tab3:
        render_trades()


if __name__ == "__main__":
    main()

