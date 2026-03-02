#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║ SOLANA MEME COIN QUANTITATIVE HARVESTER — CYBERPUNK DASHBOARD                ║
║ dashboard.py                                                                 ║
║ Senior Full-Stack Quant Developer Build                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sqlite3
import time
import math
import os
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# PAGE CONFIG — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Quant Harvester ⚡",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
DB_PATH = "data/quant_harvest.db"
DB_URI = f"file:{DB_PATH}?mode=ro"

NEON_CYAN = "#00F5FF"
BRIGHT_MAGENTA = "#FF00E5"
EMERALD = "#00E676"
CRIMSON = "#FF1744"
DARK_BG = "#0a0a0f"
CARD_BG = "#111118"

BORDER_GLOW_CYAN = f"0 0 15px {NEON_CYAN}40, 0 0 30px {NEON_CYAN}20"
BORDER_GLOW_MAGENTA = f"0 0 15px {BRIGHT_MAGENTA}40, 0 0 30px {BRIGHT_MAGENTA}20"
BORDER_GLOW_EMERALD = f"0 0 15px {EMERALD}40, 0 0 30px {EMERALD}20"
BORDER_GLOW_CRIMSON = f"0 0 15px {CRIMSON}40, 0 0 30px {CRIMSON}20"

PLOTLY_LAYOUT_DEFAULTS = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="JetBrains Mono, Fira Code, monospace", color="#c0c0c0"),
    margin=dict(l=40, r=20, t=40, b=30),
    xaxis=dict(gridcolor="#1a1a2e", zerolinecolor="#1a1a2e"),
    yaxis=dict(gridcolor="#1a1a2e", zerolinecolor="#1a1a2e"),
)

# ---------------------------------------------------------------------------
# CYBERPUNK CSS INJECTION
# ---------------------------------------------------------------------------
CYBERPUNK_CSS = f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Orbitron:wght@400;700;900&display=swap');

    /* Hide Streamlit chrome */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
    .stDeployButton {{display: none;}}

    /* Root overrides */
    .stApp {{
        background: linear-gradient(135deg, #05050a 0%, #0a0a18 40%, #0d0818 100%);
    }}

    /* Remove top padding */
    .block-container {{
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
    }}

    /* Sidebar styling */
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #08081a 0%, #0d0d22 100%) !important;
        border-right: 1px solid {NEON_CYAN}30;
    }}
    section[data-testid="stSidebar"] .stMarkdown h1,
    section[data-testid="stSidebar"] .stMarkdown h2,
    section[data-testid="stSidebar"] .stMarkdown h3 {{
        font-family: 'Orbitron', monospace !important;
        color: {NEON_CYAN} !important;
        text-shadow: 0 0 10px {NEON_CYAN}60;
    }}

    /* Metric cards with glow */
    div[data-testid="stMetric"] {{
        background: linear-gradient(135deg, #0d0d1a 0%, #12121f 100%);
        border: 1px solid {NEON_CYAN}35;
        border-radius: 8px;
        padding: 12px 16px;
        box-shadow: {BORDER_GLOW_CYAN};
        transition: all 0.3s ease;
    }}
    div[data-testid="stMetric"]:hover {{
        border-color: {NEON_CYAN}70;
        box-shadow: 0 0 20px {NEON_CYAN}50, 0 0 40px {NEON_CYAN}25;
        transform: translateY(-1px);
    }}
    div[data-testid="stMetric"] label {{
        font-family: 'JetBrains Mono', monospace !important;
        color: {NEON_CYAN}cc !important;
        font-size: 0.72rem !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }}
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {{
        font-family: 'Orbitron', monospace !important;
        color: #ffffff !important;
        font-size: 1.4rem !important;
        text-shadow: 0 0 8px {NEON_CYAN}40;
    }}
    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {{
        font-family: 'JetBrains Mono', monospace !important;
    }}

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        background: #0a0a15;
        border-radius: 8px;
        padding: 4px;
        border: 1px solid {NEON_CYAN}20;
    }}
    .stTabs [data-baseweb="tab"] {{
        font-family: 'Orbitron', monospace !important;
        font-size: 0.8rem;
        font-weight: 700;
        color: #666688;
        border-radius: 6px;
        padding: 8px 20px;
        transition: all 0.3s ease;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: {NEON_CYAN};
        background: {NEON_CYAN}10;
    }}
    .stTabs [aria-selected="true"] {{
        color: {NEON_CYAN} !important;
        background: {NEON_CYAN}15 !important;
        border-bottom: 2px solid {NEON_CYAN} !important;
        text-shadow: 0 0 10px {NEON_CYAN}60;
    }}
    .stTabs [data-baseweb="tab-highlight"] {{
        background-color: {NEON_CYAN} !important;
    }}
    .stTabs [data-baseweb="tab-border"] {{
        display: none;
    }}

    /* Expander styling */
    .streamlit-expanderHeader {{
        font-family: 'JetBrains Mono', monospace !important;
        background: linear-gradient(90deg, #0d0d1a 0%, #15152a 100%) !important;
        border: 1px solid {BRIGHT_MAGENTA}30 !important;
        border-radius: 6px !important;
        color: {BRIGHT_MAGENTA} !important;
    }}
    .streamlit-expanderContent {{
        background: #0a0a14 !important;
        border: 1px solid {BRIGHT_MAGENTA}20 !important;
        border-top: none !important;
        border-radius: 0 0 6px 6px !important;
    }}

    /* Dataframe styling */
    .stDataFrame {{
        border: 1px solid {NEON_CYAN}25;
        border-radius: 8px;
        overflow: hidden;
    }}

    /* Button styling */
    .stDownloadButton > button {{
        font-family: 'Orbitron', monospace !important;
        background: linear-gradient(135deg, {NEON_CYAN}20 0%, {BRIGHT_MAGENTA}20 100%) !important;
        border: 1px solid {NEON_CYAN}50 !important;
        color: {NEON_CYAN} !important;
        font-weight: 700;
        letter-spacing: 1px;
        transition: all 0.3s ease;
    }}
    .stDownloadButton > button:hover {{
        background: linear-gradient(135deg, {NEON_CYAN}35 0%, {BRIGHT_MAGENTA}35 100%) !important;
        box-shadow: 0 0 20px {NEON_CYAN}40;
        transform: translateY(-1px);
    }}

    /* Slider styling */
    .stSlider > div > div > div > div {{
        background-color: {NEON_CYAN} !important;
    }}

    /* Selectbox */
    .stSelectbox > div > div {{
        background: #0d0d1a !important;
        border-color: {NEON_CYAN}30 !important;
    }}

    /* Custom glow text class */
    .glow-text {{
        font-family: 'Orbitron', monospace;
        color: {NEON_CYAN};
        text-shadow: 0 0 10px {NEON_CYAN}60, 0 0 20px {NEON_CYAN}30;
    }}
    .glow-magenta {{
        font-family: 'Orbitron', monospace;
        color: {BRIGHT_MAGENTA};
        text-shadow: 0 0 10px {BRIGHT_MAGENTA}60, 0 0 20px {BRIGHT_MAGENTA}30;
    }}

    /* Scrollbar */
    ::-webkit-scrollbar {{
        width: 6px;
        height: 6px;
    }}
    ::-webkit-scrollbar-track {{
        background: #0a0a0f;
    }}
    ::-webkit-scrollbar-thumb {{
        background: {NEON_CYAN}40;
        border-radius: 3px;
    }}
    ::-webkit-scrollbar-thumb:hover {{
        background: {NEON_CYAN}70;
    }}

    /* Animate pulse for live indicator */
    @keyframes pulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.4; }}
    }}
    .live-dot {{
        display: inline-block;
        width: 8px;
        height: 8px;
        background: {EMERALD};
        border-radius: 50%;
        animation: pulse 1.5s ease-in-out infinite;
        box-shadow: 0 0 8px {EMERALD};
        margin-right: 6px;
    }}
</style>
"""
st.markdown(CYBERPUNK_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------------------------
def get_db_connection():
    """Open a read-only SQLite connection using URI mode."""
    try:
        conn = sqlite3.connect(DB_URI, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def safe_query(query: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a read-only query and return a DataFrame. Returns empty DF on error."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    df = safe_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return len(df) > 0


# ---------------------------------------------------------------------------
# DATA FETCHING FUNCTIONS
# ---------------------------------------------------------------------------
@st.cache_data(ttl=5)
def fetch_tick_count() -> int:
    df = safe_query("SELECT COUNT(*) as cnt FROM market_ticks")
    return int(df["cnt"].iloc[0]) if len(df) > 0 else 0


@st.cache_data(ttl=5)
def fetch_tracked_tokens() -> int:
    df = safe_query("SELECT COUNT(DISTINCT mint) as cnt FROM market_ticks")
    return int(df["cnt"].iloc[0]) if len(df) > 0 else 0


@st.cache_data(ttl=5)
def fetch_all_trades() -> pd.DataFrame:
    return safe_query(
        "SELECT * FROM paper_trades ORDER BY entry_time DESC"
    )


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


@st.cache_data(ttl=5)
def fetch_portfolio_state() -> pd.DataFrame:
    return safe_query("SELECT * FROM portfolio_state ORDER BY timestamp ASC")


@st.cache_data(ttl=5)
def fetch_quant_signals() -> pd.DataFrame:
    return safe_query("SELECT * FROM quant_signals ORDER BY timestamp DESC")


@st.cache_data(ttl=5)
def fetch_latest_ticks_per_token() -> pd.DataFrame:
    """Get the most recent tick for each tracked token."""
    return safe_query("""
        SELECT mt.*
        FROM market_ticks mt
        INNER JOIN (
            SELECT mint, MAX(timestamp) as max_ts
            FROM market_ticks
            GROUP BY mint
        ) latest ON mt.mint = latest.mint AND mt.timestamp = latest.max_ts
        ORDER BY mt.volume_5m DESC
    """)


@st.cache_data(ttl=5)
def fetch_ticks_for_mint(mint: str, limit: int = 500) -> pd.DataFrame:
    return safe_query(
        "SELECT * FROM market_ticks WHERE mint=? ORDER BY timestamp ASC LIMIT ?",
        (mint, limit),
    )


@st.cache_data(ttl=5)
def fetch_signals_for_trade(trade_id: str) -> pd.DataFrame:
    return safe_query(
        "SELECT * FROM quant_signals WHERE trade_id=? ORDER BY timestamp ASC",
        (trade_id,),
    )


@st.cache_data(ttl=5)
def fetch_ticks_for_mint_timerange(
    mint: str, start_ts: float, end_ts: float
) -> pd.DataFrame:
    return safe_query(
        "SELECT * FROM market_ticks WHERE mint=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp ASC",
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
    Returns: sweep_df with columns [tp_level, ev, hit_rate], optimal_tp, optimal_ev, optimal_hit_rate
    """
    if len(closed_df) == 0:
        return pd.DataFrame(), 0.0, 0.0, 0.0
        
    closed_df = closed_df.copy()
    closed_df["mfe"] = closed_df.apply(compute_mfe, axis=1)
    
    tp_levels = np.arange(0.005, 0.81, 0.005)
    results = []
    
    for tp in tp_levels:
        sim_pnl = []
        hits = 0
        for _, row in closed_df.iterrows():
            mfe_val = row["mfe"]
            actual_pnl = row["pnl_pct"] if row["pnl_pct"] is not None else 0.0
            
            if mfe_val >= tp:
                sim_pnl.append(tp)
                hits += 1
            else:
                sim_pnl.append(actual_pnl)
        
        ev = np.mean(sim_pnl) if len(sim_pnl) > 0 else 0.0
        hit_rate = hits / len(closed_df) if len(closed_df) > 0 else 0.0
        results.append(
            {"tp_level": tp, "tp_pct": tp * 100, "ev": ev, "ev_pct": ev * 100, "hit_rate": hit_rate}
        )
        
    sweep_df = pd.DataFrame(results)
    if len(sweep_df) == 0:
        return sweep_df, 0.0, 0.0, 0.0
        
    optimal_idx = sweep_df["ev"].idxmax()
    optimal_tp = sweep_df.loc[optimal_idx, "tp_level"]
    optimal_ev = sweep_df.loc[optimal_idx, "ev"]
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
        **PLOTLY_LAYOUT_DEFAULTS,
        title=dict(
            text=title,
            font=dict(family="Orbitron, monospace", size=14, color=NEON_CYAN),
            x=0.01,
        ),
        height=height,
        showlegend=True,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0, 245, 255, 0.3)",
            borderwidth=1,
            font=dict(size=10),
        ),
    )
    fig.update_layout(modebar=dict(bgcolor="rgba(0,0,0,0)", color=NEON_CYAN, activecolor=BRIGHT_MAGENTA))
    return fig


def build_candlestick_chart(ohlc_df: pd.DataFrame, title: str = "PRICE") -> go.Figure:
    if len(ohlc_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="No tick data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=NEON_CYAN, size=16, family="Orbitron"),
        )
        return apply_default_layout(fig, title, 350)

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=ohlc_df["dt"],
        open=ohlc_df["open"],
        high=ohlc_df["high"],
        low=ohlc_df["low"],
        close=ohlc_df["close"],
        increasing=dict(line=dict(color=EMERALD, width=1), fillcolor="rgba(0, 230, 118, 0.5)"),
        decreasing=dict(line=dict(color=CRIMSON, width=1), fillcolor="rgba(255, 23, 68, 0.5)"),
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
            line=dict(color=NEON_CYAN, width=2),
            marker=dict(size=5, color=NEON_CYAN, symbol="diamond"),
            name="Hurst H",
        ))
        
        # Fill above 0.6 (trending) with green
        h_above = sig["hurst_value"].clip(lower=0.6)
        fig.add_trace(go.Scatter(
            x=sig["dt"], y=h_above,
            fill="tonexty" if len(sig) > 1 else None,
            mode="none",
            fillcolor="rgba(0, 230, 118, 0.1)",
            showlegend=False,
        ))
        
        # Threshold baseline
        fig.add_trace(go.Scatter(
            x=sig["dt"], y=[0.6] * len(sig),
            mode="lines",
            line=dict(color=EMERALD, width=1, dash="dot"),
            name="H=0.6 (Trend)",
        ))
        
        # 0.5 random walk line
        fig.add_trace(go.Scatter(
            x=sig["dt"], y=[0.5] * len(sig),
            mode="lines",
            line=dict(color="#666666", width=1, dash="dash"),
            name="H=0.5 (Random)",
        ))
    else:
        fig.add_annotation(
            text="No Hurst data — waiting for signals",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=NEON_CYAN, size=13, family="Orbitron"),
        )
        
    fig.update_yaxes(range=[0, 1])
    return apply_default_layout(fig, "HURST EXPONENT (H)", 250)


def build_cvd_chart(ticks_df: pd.DataFrame, signals_df: pd.DataFrame) -> go.Figure:
    """Build micro-CVD from tick buy/sell data + signal overlays."""
    fig = go.Figure()
    
    if len(ticks_df) > 0 and "buys_5m" in ticks_df.columns and "sells_5m" in ticks_df.columns:
        df = ticks_df.copy()
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df = df.sort_values("dt")
        
        df["delta"] = df["buys_5m"].fillna(0) - df["sells_5m"].fillna(0)
        df["cvd"] = df["delta"].cumsum()
        
        # Separate positive and negative for coloring
        fig.add_trace(go.Scatter(
            x=df["dt"], y=df["cvd"],
            fill="tozeroy",
            mode="lines",
            line=dict(color=NEON_CYAN, width=1.5),
            fillcolor="rgba(0, 245, 255, 0.1)",
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
                marker=dict(
                    size=10, color=BRIGHT_MAGENTA, symbol="star",
                    line=dict(color="white", width=1),
                ),
                name="Signal CVD",
            ))
            
    if len(ticks_df) == 0 and len(signals_df) == 0:
        fig.add_annotation(
            text="No CVD data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=NEON_CYAN, size=13, family="Orbitron"),
        )
        
    return apply_default_layout(fig, "CUMULATIVE VOLUME DELTA", 250)


def build_equity_curve(portfolio_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    
    if len(portfolio_df) == 0:
        fig.add_annotation(
            text="No portfolio data yet",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=NEON_CYAN, size=14, family="Orbitron"),
        )
        return apply_default_layout(fig, "EQUITY CURVE", 350)
        
    df = portfolio_df.copy()
    df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.sort_values("dt")
    
    fig.add_trace(go.Scatter(
        x=df["dt"], y=df["balance_after"],
        mode="lines+markers",
        line=dict(color=NEON_CYAN, width=2.5, shape="spline"),
        marker=dict(
            size=6,
            color=[EMERALD if c >= 0 else CRIMSON for c in df["usd_change"].fillna(0)],
            line=dict(color="white", width=1),
        ),
        fill="tozeroy",
        fillcolor="rgba(0, 245, 255, 0.05)",
        name="Balance",
    ))
    
    return apply_default_layout(fig, "⚡ EQUITY CURVE", 350)


def build_mfe_mae_scatter(closed_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    
    if len(closed_df) == 0:
        fig.add_annotation(
            text="No closed trades",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color=NEON_CYAN, size=14, family="Orbitron"),
        )
        return apply_default_layout(fig, "MFE vs MAE — TRADE QUALITY", 400)
        
    df = closed_df.copy()
    df["mfe"] = df.apply(compute_mfe, axis=1) * 100
    df["mae"] = df.apply(compute_mae, axis=1) * 100
    df["pnl_display"] = df["pnl_pct"].fillna(0) * 100
    
    colors = [EMERALD if p > 0 else CRIMSON for p in df["pnl_display"]]
    
    fig.add_trace(go.Scatter(
        x=df["mae"], y=df["mfe"],
        mode="markers+text",
        marker=dict(
            size=12,
            color=colors,
            line=dict(color="white", width=1),
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
        line=dict(color="#444466", dash="dash", width=1),
        name="MFE=MAE",
        showlegend=False,
    ))
    
    fig.update_xaxes(title_text="MAE (Max Drawdown %)", title_font=dict(color=CRIMSON))
    fig.update_yaxes(title_text="MFE (Max Potential %)", title_font=dict(color=EMERALD))
    
    return apply_default_layout(fig, "MFE vs MAE — TRADE QUALITY", 400)


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
            font=dict(color=NEON_CYAN, size=14, family="Orbitron"),
        )
        return apply_default_layout(fig, "DYNAMIC TP OPTIMIZER", 500)

    # EV curve
    fig.add_trace(go.Scatter(
        x=sweep_df["tp_pct"], y=sweep_df["ev_pct"],
        mode="lines",
        line=dict(color=NEON_CYAN, width=2.5),
        fill="tozeroy",
        fillcolor="rgba(0, 245, 255, 0.06)",
        name="EV(%)",
    ), row=1, col=1)
    
    # Optimal T* star marker
    fig.add_trace(go.Scatter(
        x=[optimal_tp * 100], y=[optimal_ev * 100],
        mode="markers+text",
        marker=dict(size=18, color=BRIGHT_MAGENTA, symbol="star", line=dict(color="white", width=2)),
        text=[f"T*={optimal_tp*100:.1f}%"],
        textposition="top center",
        textfont=dict(color=BRIGHT_MAGENTA, size=12, family="Orbitron"),
        name="Optimal T*",
        showlegend=True,
    ), row=1, col=1)
    
    # Hit rate curve
    fig.add_trace(go.Scatter(
        x=sweep_df["tp_pct"], y=sweep_df["hit_rate"],
        mode="lines",
        line=dict(color=EMERALD, width=2),
        fill="tozeroy",
        fillcolor="rgba(0, 230, 118, 0.06)",
        name="Hit Rate",
    ), row=2, col=1)
    
    # Vertical line at optimal
    for row_idx in [1, 2]:
        fig.add_vline(
            x=optimal_tp * 100,
            line_dash="dash", line_color=BRIGHT_MAGENTA, line_width=1,
            row=row_idx, col=1,
        )
        
    fig.update_xaxes(title_text="Take Profit Level (%)", row=2, col=1)
    fig.update_yaxes(title_text="EV (%)", row=1, col=1)
    fig.update_yaxes(title_text="Hit Rate", row=2, col=1)
    
    # Apply style to subplots
    for annotation in fig['layout']['annotations']:
        annotation['font'] = dict(color=NEON_CYAN, size=11, family="Orbitron")

    return apply_default_layout(fig, "⚡ DYNAMIC TP OPTIMIZER", 500)


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
            line=dict(color=NEON_CYAN, width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 245, 255, 0.05)",
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
            font=dict(color="#666688", size=12),
        )

    # Entry price line
    if entry_price is not None:
        fig.add_hline(
            y=entry_price,
            line_dash="solid", line_color=NEON_CYAN, line_width=1.5,
            annotation_text=f"ENTRY {format_price(entry_price)}",
            annotation_position="top left",
            annotation_font=dict(color=NEON_CYAN, size=10, family="Orbitron"),
        )
        
    # Exit price line
    if exit_price is not None and not (isinstance(exit_price, float) and math.isnan(exit_price)):
        exit_color = EMERALD if (exit_price > entry_price) else CRIMSON
        fig.add_hline(
            y=exit_price,
            line_dash="solid", line_color=exit_color, line_width=1.5,
            annotation_text=f"EXIT {format_price(exit_price)}",
            annotation_position="bottom left",
            annotation_font=dict(color=exit_color, size=10, family="Orbitron"),
        )
        
    # Peak high line
    if peak_high is not None and not (isinstance(peak_high, float) and math.isnan(peak_high)):
        fig.add_hline(
            y=peak_high,
            line_dash="dot", line_color=EMERALD, line_width=1,
            annotation_text=f"PEAK ▲ {format_price(peak_high)}",
            annotation_position="top right",
            annotation_font=dict(color=EMERALD, size=9),
        )
        
    # Peak low line
    if peak_low is not None and not (isinstance(peak_low, float) and math.isnan(peak_low)):
        fig.add_hline(
            y=peak_low,
            line_dash="dot", line_color=CRIMSON, line_width=1,
            annotation_text=f"TROUGH ▼ {format_price(peak_low)}",
            annotation_position="bottom right",
            annotation_font=dict(color=CRIMSON, size=9),
        )
        
    return apply_default_layout(fig, f"TRAJECTORY — {trade_row.get('symbol', '?')}", 350)


# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------
def render_sidebar():
    with st.sidebar:
        st.markdown(
            f"""
            <div style="text-align:center; margin-bottom:20px;">
                <h1 style="font-family: Orbitron, monospace; font-size:1.3rem; color:{NEON_CYAN}; text-shadow: 0 0 15px {NEON_CYAN}80; margin:0; line-height:1.3;">
                    ⚡ QUANT<br/>HARVESTER
                </h1>
                <p style="font-family: JetBrains Mono, monospace; font-size:0.65rem; color:{BRIGHT_MAGENTA}; letter-spacing:3px; margin-top:4px;">
                    SOLANA MEME COIN ENGINE
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        
        st.markdown(
            f'<p style="font-size:0.75rem; color:{NEON_CYAN}cc;"><span class="live-dot"></span>LIVE DATA STREAM</p>',
            unsafe_allow_html=True,
        )
        
        refresh_interval = st.slider(
            "Auto-Refresh (seconds)",
            min_value=5, max_value=120, value=15, step=5,
            key="refresh_slider",
        )
        
        # Attempt to use streamlit_autorefresh
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=refresh_interval * 1000, limit=None, key="auto_refresh")
        except ImportError:
            # Fallback: manual refresh button
            if st.button("🔄 Refresh Now"):
                st.cache_data.clear()
        
        st.markdown("---")
        
        # Check DB existence
        db_exists = os.path.exists(DB_PATH)
        if not db_exists:
            st.error("⚠️ Database not found")
            st.caption(f"Expected: `{DB_PATH}`")
            st.stop()
            
        # Sidebar metrics
        st.markdown(
            f'<p style="font-family:Orbitron; font-size:0.7rem; color:{NEON_CYAN}; letter-spacing:2px;">SYSTEM METRICS</p>',
            unsafe_allow_html=True,
        )
        
        total_ticks = fetch_tick_count()
        tracked_tokens = fetch_tracked_tokens()
        closed_df = fetch_closed_trades()
        all_trades = fetch_all_trades()
        
        win_rate = compute_win_rate(closed_df)
        closed_count = len(closed_df)
        open_count = len(all_trades) - closed_count
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Ticks", f"{total_ticks:,}")
        with col2:
            st.metric("Tokens", f"{tracked_tokens}")
            
        col3, col4 = st.columns(2)
        with col3:
            st.metric("Win Rate", f"{win_rate*100:.1f}%", delta=f"{closed_count} closed")
        with col4:
            st.metric("Open", f"{open_count}", delta=f"{closed_count + open_count} total")
            
        st.markdown("---")
        
        # Dynamic TP metric
        st.markdown(
            f'<p style="font-family:Orbitron; font-size:0.7rem; color:{BRIGHT_MAGENTA}; letter-spacing:2px;">OPTIMAL TP</p>',
            unsafe_allow_html=True,
        )
        
        if len(closed_df) >= 3:
            sweep_df, optimal_tp, optimal_ev, optimal_hit_rate = compute_optimal_tp(closed_df)
            c1, c2 = st.columns(2)
            with c1:
                st.metric("T* Level", f"{optimal_tp*100:.1f}%")
            with c2:
                st.metric("EV at T*", f"{optimal_ev*100:.2f}%")
            st.metric("Hit Rate", f"{optimal_hit_rate*100:.1f}%")
        else:
            st.caption("Need ≥3 closed trades for TP optimization")
            
        st.markdown("---")
        
        # Export button
        st.markdown(
            f'<p style="font-family:Orbitron; font-size:0.7rem; color:{NEON_CYAN}; letter-spacing:2px;">EXPORT</p>',
            unsafe_allow_html=True,
        )
        
        if os.path.exists(DB_PATH):
            import shutil
            shutil.make_archive("quant_harvest_backup", "zip", "data")
            with open("quant_harvest_backup.zip", "rb") as f:
                st.download_button(
                    label="⬇️ Download DB (ZIP)",
                    data=f,
                    file_name="quant_harvest_backup.zip",
                    mime="application/zip",
                )
        
        # Footer
        st.markdown("---")
        st.markdown(
            f"""
            <div style="text-align:center; padding:8px 0;">
                <p style="font-family: JetBrains Mono; font-size:0.6rem; color:#444466;">
                    Built with ⚡ by Quant Engine<br/>
                    {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# TAB 1: WAR ROOM
# ---------------------------------------------------------------------------
def render_war_room():
    st.markdown(
        f"""
        <h2 style="font-family:Orbitron; color:{NEON_CYAN}; margin:0 0 4px 0; font-size:1.1rem; text-shadow: 0 0 15px {NEON_CYAN}60;">
            <span class="live-dot"></span> LIVE BATTLEFIELD
        </h2>
        <p style="font-family:JetBrains Mono; font-size:0.7rem; color:#555577; margin-bottom:16px;">
            Real-time token tracking & signal visualization
        </p>
        """,
        unsafe_allow_html=True,
    )
    
    latest_ticks = fetch_latest_ticks_per_token()
    
    if len(latest_ticks) == 0:
        st.info("🔍 No market data ingested yet. Waiting for the harvester to start…")
        return

    # Styled dataframe of tracked tokens
    display_df = latest_ticks[["symbol", "mint", "price_usd", "market_cap", "liquidity_usd", "volume_5m", "buys_5m", "sells_5m"]].copy()
    display_df["price_usd"] = display_df["price_usd"].apply(format_price)
    display_df["market_cap"] = display_df["market_cap"].apply(format_usd)
    display_df["liquidity_usd"] = display_df["liquidity_usd"].apply(format_usd)
    display_df["volume_5m"] = display_df["volume_5m"].apply(format_usd)
    
    display_df.columns = ["Symbol", "Mint", "Price", "MCap", "Liquidity", "Vol 5m", "Buys", "Sells"]
    
    st.dataframe(
        display_df,
        use_container_width=True,
        height=min(250, 35 * len(display_df) + 40),
        hide_index=True,
    )
    
    # Token selector
    st.markdown(
        f'<p style="font-family:Orbitron; font-size:0.75rem; color:{BRIGHT_MAGENTA}; letter-spacing:1px; margin-top:16px;">SELECT TARGET</p>',
        unsafe_allow_html=True,
    )
    
    token_options = latest_ticks[["symbol", "mint"]].apply(
        lambda r: f"{r['symbol']} ({r['mint'][:8]}…{r['mint'][-4:]})", axis=1
    ).tolist()
    
    if len(token_options) == 0:
        return
        
    selected_idx = st.selectbox(
        "Token",
        range(len(token_options)),
        format_func=lambda i: token_options[i],
        label_visibility="collapsed",
    )
    
    selected_mint = latest_ticks.iloc[selected_idx]["mint"]
    selected_symbol = latest_ticks.iloc[selected_idx]["symbol"]
    
    st.markdown(
        f"""
        <div style="background:linear-gradient(90deg, {NEON_CYAN}10, transparent); border-left:3px solid {NEON_CYAN}; padding:8px 14px; margin:12px 0; border-radius:0 6px 6px 0;">
            <span style="font-family:Orbitron; color:{NEON_CYAN}; font-size:0.85rem;">
                ⚔️ {selected_symbol}
            </span>
            <span style="font-family:JetBrains Mono; color:#555577; font-size:0.65rem; margin-left:12px;">
                {selected_mint}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    # Fetch data for selected token
    ticks_df = fetch_ticks_for_mint(selected_mint, limit=1000)
    
    # Get signals related to trades for this token
    all_trades = fetch_all_trades()
    token_trades = all_trades[all_trades["mint"] == selected_mint] if len(all_trades) > 0 else pd.DataFrame()
    
    token_trade_ids = token_trades["trade_id"].tolist() if len(token_trades) > 0 else []
    
    all_signals = fetch_quant_signals()
    token_signals = pd.DataFrame()
    if len(all_signals) > 0 and len(token_trade_ids) > 0:
        token_signals = all_signals[all_signals["trade_id"].isin(token_trade_ids)]
        
    # 3-pane visualization
    st.markdown(
        f'<p style="font-family:Orbitron; font-size:0.7rem; color:{NEON_CYAN}; letter-spacing:2px; margin-top:10px;">SIGNAL MATRIX</p>',
        unsafe_allow_html=True,
    )
    
    # Pane 1: Candlestick
    ohlc_df = build_ohlc_from_ticks(ticks_df, freq_seconds=60)
    candle_fig = build_candlestick_chart(ohlc_df, f"PRICE — {selected_symbol}")
    
    # Mark open trades on candlestick
    if len(token_trades) > 0:
        open_trades = token_trades[token_trades["status"] == "OPEN"]
        for _, t in open_trades.iterrows():
            candle_fig.add_hline(
                y=t["entry_price"],
                line_dash="dash", line_color=BRIGHT_MAGENTA, line_width=1,
                annotation_text=f"OPEN @ {format_price(t['entry_price'])}",
                annotation_font=dict(color=BRIGHT_MAGENTA, size=9),
            )
            
    st.plotly_chart(candle_fig, use_container_width=True, config={"displayModeBar": False})
    
    # Panes 2 & 3 side by side
    col_h, col_c = st.columns(2)
    with col_h:
        hurst_fig = build_hurst_chart(ticks_df, token_signals)
        st.plotly_chart(hurst_fig, use_container_width=True, config={"displayModeBar": False})
    with col_c:
        cvd_fig = build_cvd_chart(ticks_df, token_signals)
        st.plotly_chart(cvd_fig, use_container_width=True, config={"displayModeBar": False})
        
    # Open positions for this token
    if len(token_trades) > 0:
        open_here = token_trades[token_trades["status"] == "OPEN"]
        if len(open_here) > 0:
            st.markdown(
                f'<p style="font-family:Orbitron; font-size:0.7rem; color:{EMERALD}; letter-spacing:2px; margin-top:8px;">ACTIVE POSITIONS</p>',
                unsafe_allow_html=True,
            )
            for _, t in open_here.iterrows():
                mfe = compute_mfe(t)
                mae = compute_mae(t)
                st.markdown(
                    f"""
                    <div style="background:#0d0d1a; border:1px solid {EMERALD}40; border-radius:6px; padding:10px 14px; margin:4px 0;">
                        <span style="font-family:Orbitron; color:{EMERALD}; font-size:0.75rem;">🟢 {t['trade_id']}</span>
                        <span style="font-family:JetBrains Mono; color:#888; font-size:0.7rem; margin-left:12px;">
                            Entry: {format_price(t['entry_price'])} | Size: {format_usd(t['usd_size'] if t['usd_size'] else 0)} | MFE: {mfe*100:.2f}% | MAE: {mae*100:.2f}%
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


# ---------------------------------------------------------------------------
# TAB 2: ANALYTICS
# ---------------------------------------------------------------------------
def render_analytics():
    st.markdown(
        f"""
        <h2 style="font-family:Orbitron; color:{BRIGHT_MAGENTA}; margin:0 0 4px 0; font-size:1.1rem; text-shadow: 0 0 15px {BRIGHT_MAGENTA}60;">
            📊 PERFORMANCE ANALYTICS
        </h2>
        <p style="font-family:JetBrains Mono; font-size:0.7rem; color:#555577; margin-bottom:16px;">
            Paper trading engine quantitative analysis
        </p>
        """,
        unsafe_allow_html=True,
    )
    
    closed_df = fetch_closed_trades()
    all_trades = fetch_all_trades()
    portfolio_df = fetch_portfolio_state()
    
    # ---- HUD METRICS ----
    st.markdown(
        f'<p style="font-family:Orbitron; font-size:0.7rem; color:{NEON_CYAN}; letter-spacing:2px;">COMMAND HUD</p>',
        unsafe_allow_html=True,
    )
    
    if len(closed_df) > 0:
        win_rate = compute_win_rate(closed_df)
        kelly = compute_kelly_criterion(closed_df)
        avg_pnl = closed_df["pnl_pct"].mean() if "pnl_pct" in closed_df.columns else 0
        best_trade = closed_df["pnl_pct"].max() if len(closed_df) > 0 else 0
        worst_trade = closed_df["pnl_pct"].min() if len(closed_df) > 0 else 0
        
        total_usd_pnl = closed_df["usd_pnl"].sum() if "usd_pnl" in closed_df.columns else 0
        avg_usd_pnl = closed_df["usd_pnl"].mean() if "usd_pnl" in closed_df.columns else 0
        
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1: st.metric("¼ Kelly f*", f"{kelly*100:.2f}%")
        with c2: st.metric("Win Rate", f"{win_rate*100:.1f}%")
        with c3: st.metric("Avg PnL", format_pnl_pct(avg_pnl))
        with c4: st.metric("Best Trade", format_pnl_pct(best_trade))
        with c5: st.metric("Worst Trade", format_pnl_pct(worst_trade))
        with c6: st.metric("Total USD PnL", format_usd(total_usd_pnl))
        
        st.markdown("<br/>", unsafe_allow_html=True)
        
        # Additional row
        c7, c8, c9, c10 = st.columns(4)
        with c7: st.metric("Closed Trades", f"{len(closed_df)}")
        with c8: st.metric("Avg USD PnL", format_usd(avg_usd_pnl))
        with c9:
            # Profit factor
            gross_profit = closed_df[closed_df["usd_pnl"] > 0]["usd_pnl"].sum() if "usd_pnl" in closed_df.columns else 0
            gross_loss = abs(closed_df[closed_df["usd_pnl"] <= 0]["usd_pnl"].sum()) if "usd_pnl" in closed_df.columns else 0
            pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")
            pf_display = f"{pf:.2f}" if pf < 100 else "∞"
            st.metric("Profit Factor", pf_display)
        with c10:
            # Avg MFE
            closed_df_copy = closed_df.copy()
            closed_df_copy["mfe"] = closed_df_copy.apply(compute_mfe, axis=1)
            avg_mfe = closed_df_copy["mfe"].mean() * 100
            st.metric("Avg MFE", f"{avg_mfe:.2f}%")
            
    else:
        st.info("No closed trades yet — analytics will populate as trades complete.")
        
    st.markdown("---")
    
    # ---- EQUITY CURVE ----
    equity_fig = build_equity_curve(portfolio_df)
    st.plotly_chart(equity_fig, use_container_width=True, config={"displayModeBar": False})
    
    st.markdown("---")
    
    # ---- DYNAMIC TP OPTIMIZER ----
    st.markdown(
        f'<p style="font-family:Orbitron; font-size:0.7rem; color:{BRIGHT_MAGENTA}; letter-spacing:2px;">DYNAMIC TP OPTIMIZER</p>',
        unsafe_allow_html=True,
    )
    
    if len(closed_df) >= 3:
        sweep_df, optimal_tp, optimal_ev, optimal_hit_rate = compute_optimal_tp(closed_df)
        tp_fig = build_tp_optimizer_chart(sweep_df, optimal_tp, optimal_ev)
        st.plotly_chart(tp_fig, use_container_width=True, config={"displayModeBar": False})
        
        # Summary
        st.markdown(
            f"""
            <div style="background:linear-gradient(90deg, {BRIGHT_MAGENTA}08, transparent); border-left:3px solid {BRIGHT_MAGENTA}; padding:10px 16px; border-radius:0 6px 6px 0; margin:8px 0;">
                <span style="font-family:Orbitron; color:{BRIGHT_MAGENTA}; font-size:0.8rem;">
                    Optimal T* = {optimal_tp*100:.1f}%
                </span>
                <span style="font-family:JetBrains Mono; color:#888; font-size:0.7rem; margin-left:16px;">
                    EV = {optimal_ev*100:.3f}% per trade | Hit Rate = {optimal_hit_rate*100:.1f}%
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.caption("Need ≥3 closed trades for TP optimization sweep.")
        
    st.markdown("---")
    
    # ---- MFE vs MAE SCATTER ----
    st.markdown(
        f'<p style="font-family:Orbitron; font-size:0.7rem; color:{NEON_CYAN}; letter-spacing:2px;">TRADE QUALITY MATRIX</p>',
        unsafe_allow_html=True,
    )
    mfe_mae_fig = build_mfe_mae_scatter(closed_df)
    st.plotly_chart(mfe_mae_fig, use_container_width=True, config={"displayModeBar": False})
    
    # ---- PNL DISTRIBUTION ----
    if len(closed_df) > 0:
        st.markdown(
            f'<p style="font-family:Orbitron; font-size:0.7rem; color:{NEON_CYAN}; letter-spacing:2px; margin-top:12px;">PNL DISTRIBUTION</p>',
            unsafe_allow_html=True,
        )
        pnl_dist_fig = go.Figure()
        pnl_vals = closed_df["pnl_pct"].fillna(0) * 100
        colors = [EMERALD if v > 0 else CRIMSON for v in pnl_vals]
        
        pnl_dist_fig.add_trace(go.Bar(
            x=list(range(len(pnl_vals))),
            y=pnl_vals.values,
            marker=dict(color=colors, line=dict(color="white", width=0.5)),
            name="PnL %",
            hovertemplate="Trade %{x}<br>PnL: %{y:.2f}%<extra></extra>",
        ))
        
        # Average line
        avg_val = pnl_vals.mean()
        pnl_dist_fig.add_hline(
            y=avg_val,
            line_dash="dash", line_color=NEON_CYAN,
            annotation_text=f"Avg: {avg_val:.2f}%",
            annotation_font=dict(color=NEON_CYAN, size=10, family="Orbitron"),
        )
        
        pnl_dist_fig.add_hline(
            y=0,
            line_color="#333355",
            line_width=1,
        )
        
        pnl_dist_fig.update_xaxes(title_text="Trade #")
        pnl_dist_fig.update_yaxes(title_text="PnL (%)")
        pnl_dist_fig = apply_default_layout(pnl_dist_fig, "PnL WATERFALL", 300)
        st.plotly_chart(pnl_dist_fig, use_container_width=True, config={"displayModeBar": False})
        
    # ---- SIGNAL DISTRIBUTIONS ----
    signals_df = fetch_quant_signals()
    if len(signals_df) > 0:
        st.markdown(
            f'<p style="font-family:Orbitron; font-size:0.7rem; color:{BRIGHT_MAGENTA}; letter-spacing:2px; margin-top:12px;">SIGNAL DISTRIBUTIONS</p>',
            unsafe_allow_html=True,
        )
        
        sig_col1, sig_col2, sig_col3 = st.columns(3)
        
        with sig_col1:
            if "hurst_value" in signals_df.columns:
                hist_fig = go.Figure()
                hist_fig.add_trace(go.Histogram(
                    x=signals_df["hurst_value"].dropna(),
                    nbinsx=30,
                    marker=dict(color=NEON_CYAN, line=dict(color="white", width=0.5)),
                    opacity=0.75,
                    name="Hurst",
                ))
                hist_fig.add_vline(x=0.5, line_dash="dash", line_color="#666", line_width=1)
                hist_fig.add_vline(x=0.6, line_dash="dash", line_color=EMERALD, line_width=1)
                hist_fig = apply_default_layout(hist_fig, "HURST DIST", 250)
                st.plotly_chart(hist_fig, use_container_width=True, config={"displayModeBar": False})
                
        with sig_col2:
            if "cvd_slope" in signals_df.columns:
                hist_fig = go.Figure()
                hist_fig.add_trace(go.Histogram(
                    x=signals_df["cvd_slope"].dropna(),
                    nbinsx=30,
                    marker=dict(color=BRIGHT_MAGENTA, line=dict(color="white", width=0.5)),
                    opacity=0.75,
                    name="CVD Slope",
                ))
                hist_fig.add_vline(x=0, line_dash="dash", line_color="#666", line_width=1)
                hist_fig = apply_default_layout(hist_fig, "CVD SLOPE DIST", 250)
                st.plotly_chart(hist_fig, use_container_width=True, config={"displayModeBar": False})
                
        with sig_col3:
            if "gini_coeff" in signals_df.columns:
                hist_fig = go.Figure()
                hist_fig.add_trace(go.Histogram(
                    x=signals_df["gini_coeff"].dropna(),
                    nbinsx=30,
                    marker=dict(color=EMERALD, line=dict(color="white", width=0.5)),
                    opacity=0.75,
                    name="Gini",
                ))
                hist_fig = apply_default_layout(hist_fig, "GINI DIST", 250)
                st.plotly_chart(hist_fig, use_container_width=True, config={"displayModeBar": False})

    # ---- OPEN POSITIONS (ALL) ----
    open_df = fetch_open_trades()
    if len(open_df) > 0:
        st.markdown("---")
        st.markdown(
            f'<p style="font-family:Orbitron; font-size:0.7rem; color:{EMERALD}; letter-spacing:2px;">🔓 ALL OPEN POSITIONS</p>',
            unsafe_allow_html=True,
        )
        
        cols = st.columns(3)
        for idx, row in open_df.iterrows():
            mint = row["mint"]
            symbol = row.get("symbol", "???")
            entry_price = row.get("entry_price", 0)
            
            # Fetch latest tick for Live PnL
            latest_tick_df = fetch_ticks_for_mint(mint, limit=1)
            current_price = entry_price
            if len(latest_tick_df) > 0:
                current_price = latest_tick_df.iloc[0]["price_usd"]
            
            live_pnl = (current_price - entry_price) / entry_price if entry_price > 0 else 0
            pnl_color = EMERALD if live_pnl >= 0 else CRIMSON
            
            with cols[idx % 3]:
                st.markdown(
                    f'''
                    <div style="background:{CARD_BG}; border:1px solid {pnl_color}40; border-radius:8px; padding:12px; margin-bottom:12px;">
                        <div style="font-family:Orbitron; color:{pnl_color}; font-size:1rem; margin-bottom:8px;">{symbol}</div>
                        <div style="font-family:JetBrains Mono; color:#888; font-size:0.75rem;">
                            Entry: {format_price(entry_price)}<br>
                            Current: {format_price(current_price)}<br>
                            <span style="color:{pnl_color}; font-weight:bold; font-size:0.9rem;">PnL: {live_pnl*100:+.2f}%</span>
                        </div>
                    </div>
                    ''',
                    unsafe_allow_html=True
                )


# ---------------------------------------------------------------------------
# TAB 3: TRADE JOURNAL
# ---------------------------------------------------------------------------
def render_trade_journal():
    st.markdown(
        f"""
        <h2 style="font-family:Orbitron; color:{EMERALD}; margin:0 0 4px 0; font-size:1.1rem; text-shadow: 0 0 15px {EMERALD}60;">
            📓 TRADE JOURNAL
        </h2>
        <p style="font-family:JetBrains Mono; font-size:0.7rem; color:#555577; margin-bottom:16px;">
            Complete audit trail of all entries and exits
        </p>
        """,
        unsafe_allow_html=True,
    )
    
    all_trades = fetch_all_trades()
    
    if len(all_trades) == 0:
        st.info("📝 No trades recorded yet. The paper engine will log entries here.")
        return
        
    # Summary
    open_count = len(all_trades[all_trades["status"] == "OPEN"])
    closed_count = len(all_trades[all_trades["status"] == "CLOSED"])
    
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total Entries", f"{len(all_trades)}")
    with c2: st.metric("Open", f"{open_count}", delta="ACTIVE" if open_count > 0 else None)
    with c3: st.metric("Closed", f"{closed_count}")
    
    st.markdown("---")
    
    # Filter
    status_filter = st.radio(
        "Filter",
        ["ALL", "OPEN", "CLOSED"],
        horizontal=True,
        label_visibility="collapsed",
    )
    
    if status_filter == "OPEN":
        filtered = all_trades[all_trades["status"] == "OPEN"]
    elif status_filter == "CLOSED":
        filtered = all_trades[all_trades["status"] == "CLOSED"]
    else:
        filtered = all_trades
        
    if len(filtered) == 0:
        st.caption("No trades match filter.")
        return
        
    # Iterate through trades with expanders
    for idx, (_, trade) in enumerate(filtered.iterrows()):
        status = trade["status"]
        symbol = trade.get("symbol", "???")
        trade_id = trade.get("trade_id", "?")
        pnl_pct = trade.get("pnl_pct", 0) or 0
        usd_pnl = trade.get("usd_pnl", 0) or 0
        exit_reason = trade.get("exit_reason", "—") or "—"
        
        if status == "OPEN":
            icon = "🟢"
            border_color = EMERALD
            pnl_str = "OPEN"
        elif pnl_pct > 0:
            icon = "🟩"
            border_color = EMERALD
            pnl_str = f"+{pnl_pct*100:.2f}%"
        else:
            icon = "🟥"
            border_color = CRIMSON
            pnl_str = f"{pnl_pct*100:.2f}%"
            
        # Expander header
        header = f"{icon} {symbol} | {trade_id} | {pnl_str} | {exit_reason}"
        
        with st.expander(header, expanded=False):
            # Trade details in columns
            dc1, dc2, dc3, dc4 = st.columns(4)
            with dc1:
                st.markdown(f"**Entry Time**")
                st.code(ts_to_dt(trade.get("entry_time")))
                st.markdown(f"**Entry Price**")
                st.code(format_price(trade.get("entry_price", 0)))
            with dc2:
                st.markdown(f"**Exit Time**")
                st.code(ts_to_dt(trade.get("exit_time")))
                st.markdown(f"**Exit Price**")
                exit_p = trade.get("exit_price", None)
                st.code(format_price(exit_p) if exit_p and not (isinstance(exit_p, float) and math.isnan(exit_p)) else "—")
            with dc3:
                st.markdown(f"**PnL %**")
                pnl_color = EMERALD if pnl_pct > 0 else CRIMSON
                st.markdown(
                    f'<span style="color:{pnl_color}; font-family:Orbitron; font-size:1.1rem;">{format_pnl_pct(pnl_pct)}</span>',
                    unsafe_allow_html=True,
                )
                st.markdown(f"**USD PnL**")
                st.markdown(
                    f'<span style="color:{pnl_color}; font-family:JetBrains Mono;">{format_usd(usd_pnl)}</span>',
                    unsafe_allow_html=True,
                )
            with dc4:
                mfe_val = compute_mfe(trade) * 100
                mae_val = compute_mae(trade) * 100
                st.markdown(f"**MFE (Max Potential)**")
                st.markdown(
                    f'<span style="color:{EMERALD}; font-family:JetBrains Mono;">▲ {mfe_val:.2f}%</span>',
                    unsafe_allow_html=True,
                )
                st.markdown(f"**MAE (Max Drawdown)**")
                st.markdown(
                    f'<span style="color:{CRIMSON}; font-family:JetBrains Mono;">▼ {mae_val:.2f}%</span>',
                    unsafe_allow_html=True,
                )
                
            # Additional info row
            info_c1, info_c2, info_c3 = st.columns(3)
            with info_c1:
                st.markdown(f"**Status:** `{status}`")
                st.markdown(f"**Exit Reason:** `{exit_reason}`")
            with info_c2:
                usd_size = trade.get("usd_size", 0) or 0
                st.markdown(f"**Position Size:** {format_usd(usd_size)}")
                st.markdown(f"**Mint:** `{trade.get('mint', '?')[:20]}…`")
            with info_c3:
                peak_h = trade.get("peak_high", None)
                peak_l = trade.get("peak_low", None)
                st.markdown(f"**Peak High:** {format_price(peak_h) if peak_h and not (isinstance(peak_h, float) and math.isnan(peak_h)) else '—'}")
                st.markdown(f"**Peak Low:** {format_price(peak_l) if peak_l and not (isinstance(peak_l, float) and math.isnan(peak_l)) else '—'}")
                
            # Quant signals for this trade
            trade_signals = fetch_signals_for_trade(trade_id)
            if len(trade_signals) > 0:
                st.markdown(
                    f'<p style="font-family:Orbitron; font-size:0.65rem; color:{NEON_CYAN}; letter-spacing:2px; margin-top:8px;">ENTRY SIGNALS</p>',
                    unsafe_allow_html=True,
                )
                sig_c1, sig_c2, sig_c3, sig_c4 = st.columns(4)
                latest_sig = trade_signals.iloc[0]
                
                with sig_c1:
                    h_val = latest_sig.get("hurst_value", None)
                    h_color = EMERALD if h_val and h_val > 0.6 else CRIMSON if h_val else "#888"
                    st.metric("Hurst H", f"{h_val:.4f}" if h_val else "—")
                with sig_c2:
                    st.metric("CVD", f"{latest_sig.get('cvd_value', 0):.2f}")
                with sig_c3:
                    slope = latest_sig.get("cvd_slope", 0)
                    st.metric("CVD Slope", f"{slope:.4f}" if slope else "—")
                with sig_c4:
                    gini = latest_sig.get("gini_coeff", None)
                    st.metric("Gini", f"{gini:.4f}" if gini else "—")
                    
            # Trajectory chart
            st.markdown(
                f'<p style="font-family:Orbitron; font-size:0.65rem; color:{BRIGHT_MAGENTA}; letter-spacing:2px; margin-top:8px;">PRICE TRAJECTORY</p>',
                unsafe_allow_html=True,
            )
            
            entry_ts = trade.get("entry_time", 0)
            exit_ts = trade.get("exit_time", None)
            
            # Determine time range for tick fetching
            if exit_ts and not (isinstance(exit_ts, float) and math.isnan(exit_ts)):
                # Add some buffer (5 min before entry, 5 min after exit)
                start_ts = entry_ts - 300
                end_ts = exit_ts + 300
            else:
                # For open trades, fetch from entry to now
                start_ts = entry_ts - 300
                end_ts = time.time()
                
            mint = trade.get("mint", "")
            trajectory_ticks = fetch_ticks_for_mint_timerange(mint, start_ts, end_ts)
            
            traj_fig = build_trade_trajectory(trade, trajectory_ticks)
            st.plotly_chart(traj_fig, use_container_width=True, config={"displayModeBar": False})


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    render_sidebar()
    
    # Header
    st.markdown(
        f"""
        <div style="text-align:center; margin-bottom:6px;">
            <h1 style="font-family:Orbitron; color:{NEON_CYAN}; font-size:1.6rem; margin:0; text-shadow: 0 0 20px {NEON_CYAN}80, 0 0 40px {NEON_CYAN}30;">
                ⚡ QUANTITATIVE HARVESTER
            </h1>
            <p style="font-family:JetBrains Mono; color:{BRIGHT_MAGENTA}; font-size:0.7rem; letter-spacing:4px; margin:2px 0 0 0;">
                SOLANA MEME COIN ALPHA ENGINE
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["⚔️ War Room", "📊 Analytics", "📓 Trade Journal"])
    
    with tab1:
        render_war_room()
    with tab2:
        render_analytics()
    with tab3:
        render_trade_journal()


if __name__ == "__main__":
    main()
