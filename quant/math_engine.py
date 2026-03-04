"""
Quantitative Mathematics Engine.
Pure-function implementations of:
1. Hurst Exponent via R/S (rescaled range) analysis
2. Micro-CVD Cumulative Volume Delta proxy
3. Gini Coefficient holder-concentration measure
4. ATR (Average True Range) for per-trade TP/SL
5. Price Efficiency Ratio for trend detection
6. Volume Acceleration for momentum confirmation
All functions are stateless and operate on numpy arrays.
"""
import numpy as np
from typing import Optional, Tuple, NamedTuple
from utils.logger import get_logger

log = get_logger("QuantMath")

# ═══════════════════════════════════════════════════════════════
# 1. HURST EXPONENT — Regime Detection
# ═══════════════════════════════════════════════════════════════

def hurst_exponent(prices: np.ndarray) -> Optional[float]:
    """
    Compute the Hurst exponent H from a price series using rescaled-range (R/S)
    analysis with log-log regression.
    """
    if prices is None or len(prices) < 20:
        return None
    try:
        # Log-returns are more stationary than raw prices
        log_ret = np.diff(np.log(prices[prices > 0]))
        n = len(log_ret)
        if n < 16:
            return None

        # Build a set of sub-window sizes, spaced geometrically
        min_w = max(8, n // 10)
        max_w = n // 2
        if max_w <= min_w: min_w = 4
        if max_w <= min_w: return None

        windows: list[int] = []
        w = min_w
        while w <= max_w:
            if w not in windows:
                windows.append(w)
            next_w = int(w * 1.4)
            w = next_w if next_w > w else w + 1
            
        if len(windows) < 3:
            return None

        log_n_list: list[float] = []
        log_rs_list: list[float] = []

        for w_size in windows:
            num_segs = n // w_size
            if num_segs < 1: continue
            
            rs_accum: list[float] = []
            for seg_i in range(num_segs):
                seg = log_ret[seg_i * w_size : (seg_i + 1) * w_size]
                mean_seg = np.mean(seg)
                dev = seg - mean_seg
                cum_dev = np.cumsum(dev)
                
                R = float(np.max(cum_dev) - np.min(cum_dev))
                S = float(np.std(seg, ddof=1))
                
                if S > 1e-14 and R > 0:
                    rs_accum.append(R / S)
            
            if rs_accum:
                log_n_list.append(np.log(w_size))
                log_rs_list.append(np.log(np.mean(rs_accum)))

        if len(log_n_list) < 3:
            return None

        X = np.vstack([np.array(log_n_list), np.ones(len(log_n_list))]).T
        y = np.array(log_rs_list)
        coefs, *_ = np.linalg.lstsq(X, y, rcond=None)
        
        H = float(np.clip(coefs[0], 0.0, 1.0))
        return H
    except Exception as exc:
        log.error(f"Hurst computation error: {exc}")
        return None

# ═══════════════════════════════════════════════════════════════
# 2. MICRO-CVD — Cumulative Volume Delta Proxy
# ═══════════════════════════════════════════════════════════════

def micro_cvd(
    buy_counts: np.ndarray,
    sell_counts: np.ndarray,
    volumes: np.ndarray,
    prices: np.ndarray,
    lookback: int = 10,
) -> Tuple[Optional[float], Optional[float], bool]:
    min_len = min(len(buy_counts), len(sell_counts), len(volumes), len(prices))
    if min_len < lookback or min_len < 3:
        return None, None, False

    try:
        buys = buy_counts[-min_len:].astype(np.float64)
        sells = sell_counts[-min_len:].astype(np.float64)
        vols = volumes[-min_len:].astype(np.float64)
        px = prices[-min_len:].astype(np.float64)

        total = buys + sells
        safe_total = np.where(total > 0, total, 1.0)
        
        v_buy, v_sell = vols * (buys / safe_total), vols * (sells / safe_total)
        cvd = np.cumsum(v_buy - v_sell)
        cvd_current = float(cvd[-1])

        cvd_tail = cvd[-lookback:]
        x = np.arange(len(cvd_tail), dtype=np.float64)
        cvd_slope = float(np.polyfit(x, cvd_tail, 1)[0])

        px_tail = px[-lookback:]
        xp = np.arange(len(px_tail), dtype=np.float64)
        px_slope = float(np.polyfit(xp, px_tail, 1)[0])
        
        avg_px = float(np.mean(px_tail))
        px_slope_pct = (px_slope / avg_px) if avg_px > 0 else 0.0

        is_bullish = (px_slope_pct <= 0.02 and cvd_slope > 0 and cvd_current > 0)
        return cvd_current, cvd_slope, is_bullish
    except Exception as exc:
        log.error(f"CVD computation error: {exc}")
        return None, None, False

# ═══════════════════════════════════════════════════════════════
# 3. GINI COEFFICIENT — Holder Concentration
# ═══════════════════════════════════════════════════════════════

def gini_coefficient(balances: np.ndarray) -> Optional[float]:
    if balances is None or len(balances) < 2:
        return None
    try:
        b = balances[balances > 0].astype(np.float64)
        if len(b) < 2: return None
        n = len(b)
        total = np.sum(b)
        if total <= 0: return None
        sorted_b = np.sort(b)
        idx = np.arange(1, n + 1, dtype=np.float64)
        G = (2.0 * np.dot(idx, sorted_b)) / (n * total) - (n + 1.0) / n
        return float(np.clip(G, 0.0, 1.0))
    except Exception as exc:
        log.error(f"Gini computation error: {exc}")
        return None

# ═══════════════════════════════════════════════════════════════
# 4. DYNAMIC MFE-OPTIMIZED TAKE PROFIT
# ═══════════════════════════════════════════════════════════════

def calculate_optimal_tp(
    entry_prices: np.ndarray,
    peak_highs: np.ndarray,
    actual_pnls: np.ndarray,
    min_tp: float = 0.05,
    max_tp: float = 1.00,
    step: float = 0.01,
    min_hit_rate: float = 0.10,
    default_tp: float = 0.20,
) -> tuple[float, dict]:
    """
    Find the take-profit level T* that maximizes historical simulated Expected Value across closed trades.
    """
    n = len(entry_prices)
    if n < 5:
        log.debug(f"MFE optimizer: insufficient data ({n} trades), using default TP={default_tp:.0%}")
        return default_tp, {
            "ev_curve": [], "best_ev": 0.0, "sample_size": n,
            "hit_rate": 0.0, "confidence": "INSUFFICIENT_DATA",
            "reason": f"Need ≥5 trades, have {n}"
        }

    safe_entry = np.where(entry_prices > 0, entry_prices, 1e-18)
    mfe_array = (peak_highs - entry_prices) / safe_entry
    valid_mask = np.isfinite(mfe_array) & np.isfinite(actual_pnls)
    mfe_valid, pnl_valid = mfe_array[valid_mask], actual_pnls[valid_mask]
    n_valid = len(mfe_valid)

    if n_valid < 5:
        log.debug(f"MFE optimizer: only {n_valid} valid trades after filtering")
        return default_tp, {
            "ev_curve": [], "best_ev": 0.0, "sample_size": n_valid,
            "hit_rate": 0.0, "confidence": "INSUFFICIENT_DATA",
            "reason": f"Only {n_valid} valid trades"
        }

    candidates = np.arange(min_tp, max_tp + step / 2, step)
    ev_curve, best_tp, best_ev, best_hit_rate = [], default_tp, -np.inf, 0.0

    for T in candidates:
        would_have_hit = mfe_valid >= T
        sim_pnl = np.where(would_have_hit, T, pnl_valid)
        ev = float(np.mean(sim_pnl))
        hit_rate = float(np.mean(would_have_hit))
        ev_curve.append((float(T), ev, hit_rate))
        if hit_rate >= min_hit_rate and ev > best_ev:
            best_ev, best_tp, best_hit_rate = ev, float(T), hit_rate

    if n_valid >= 30 and best_hit_rate >= 0.25: confidence = "HIGH"
    elif n_valid >= 15 and best_hit_rate >= 0.15: confidence = "MEDIUM"
    elif n_valid >= 5: confidence = "LOW"
    else: confidence = "INSUFFICIENT_DATA"

    best_tp = float(np.clip(best_tp, min_tp, max_tp))
    log.info(f"📐 MFE Optimizer: T*={best_tp:.0%} | EV={best_ev:+.2%} | Hit={best_hit_rate:.0%} of {n_valid} trades | Confidence={confidence}")
    
    return best_tp, {
        "ev_curve": ev_curve, "best_ev": best_ev, "sample_size": n_valid,
        "hit_rate": best_hit_rate, "confidence": confidence, "reason": "OK"
    }

def calculate_optimal_tp_from_dataframe(trades_df, min_tp: float = 0.05, max_tp: float = 1.00, step: float = 0.01, default_tp: float = 0.20) -> tuple[float, dict]:
    try:
        if hasattr(trades_df, "empty"):
            if trades_df.empty:
                return default_tp, {
                    "ev_curve": [], "best_ev": 0.0, "sample_size": 0,
                    "hit_rate": 0.0, "confidence": "INSUFFICIENT_DATA",
                    "reason": "Empty DataFrame"
                }
            entry_prices = trades_df["entry_price"].values.astype(np.float64)
            peak_highs = trades_df["peak_high"].values.astype(np.float64)
            actual_pnls = trades_df["pnl_pct"].values.astype(np.float64)
        else:
            entry_prices = np.array([t["entry_price"] for t in trades_df], dtype=np.float64)
            peak_highs = np.array([t["peak_high"] for t in trades_df], dtype=np.float64)
            actual_pnls = np.array([t["pnl_pct"] for t in trades_df], dtype=np.float64)
        return calculate_optimal_tp(entry_prices=entry_prices, peak_highs=peak_highs, actual_pnls=actual_pnls, min_tp=min_tp, max_tp=max_tp, step=step, default_tp=default_tp)
    except Exception as e:
        log.error(f"MFE optimizer wrapper error: {e}")
        return default_tp, {
            "ev_curve": [], "best_ev": 0.0, "sample_size": 0,
            "hit_rate": 0.0, "confidence": "ERROR", "reason": str(e)
        }

# ═══════════════════════════════════════════════════════════════
# 5. ATR — Average True Range (per-trade TP/SL)
# ═══════════════════════════════════════════════════════════════

def compute_atr(prices: np.ndarray, periods: int = 5) -> Optional[float]:
    """
    Compute a simplified ATR from a price series.
    Uses abs(price[i] - price[i-1]) as the true range proxy.
    Returns ATR as a fraction of the latest price (percentage-like).
    Returns None if insufficient data (need at least periods+1 prices).
    """
    if prices is None or len(prices) < periods + 1:
        return None
    try:
        px = np.asarray(prices, dtype=np.float64)
        true_ranges = np.abs(np.diff(px))
        atr_raw = float(np.mean(true_ranges[-periods:]))
        latest_price = float(px[-1])
        if latest_price <= 0:
            return None
        return atr_raw / latest_price
    except Exception as exc:
        log.error(f"ATR computation error: {exc}")
        return None

# ═══════════════════════════════════════════════════════════════
# 6. PRICE EFFICIENCY RATIO — Trend vs. Range Detection
# ═══════════════════════════════════════════════════════════════

def price_efficiency_ratio(prices: np.ndarray, lookback: int = 30) -> Optional[float]:
    """
    Compute the Price Efficiency Ratio (PER).
    PER = |prices[-1] - prices[-lookback]| / sum(|prices[i] - prices[i-1]|)
    Values near 1.0 = straight-line trend; near 0.0 = range-bound chop.
    Returns None if insufficient data.
    """
    if prices is None or len(prices) < lookback + 1:
        return None
    try:
        px = np.asarray(prices, dtype=np.float64)
        window = px[-lookback - 1:]
        net_move = abs(float(window[-1]) - float(window[0]))
        total_path = float(np.sum(np.abs(np.diff(window))))
        if total_path <= 0:
            return None
        return float(np.clip(net_move / total_path, 0.0, 1.0))
    except Exception as exc:
        log.error(f"Price efficiency ratio error: {exc}")
        return None

# ═══════════════════════════════════════════════════════════════
# 7. VOLUME ACCELERATION — Momentum Confirmation
# ═══════════════════════════════════════════════════════════════

class VolumeAccelResult(NamedTuple):
    accel_ratio: Optional[float]
    recent_buys_count: int

def volume_acceleration(
    buy_counts: np.ndarray,
    sell_counts: np.ndarray,
    lookback_short: int = 1,
    lookback_long: int = 5,
) -> VolumeAccelResult:
    """
    Compute volume (transaction) acceleration.
    accel_ratio = short_window_rate / long_window_avg_rate
    Returns VolumeAccelResult with accel_ratio and recent_buys_count.
    """
    if buy_counts is None or sell_counts is None:
        return VolumeAccelResult(accel_ratio=None, recent_buys_count=0)

    min_len = min(len(buy_counts), len(sell_counts))
    if min_len < lookback_long:
        return VolumeAccelResult(accel_ratio=None, recent_buys_count=0)

    try:
        buys = np.asarray(buy_counts, dtype=np.float64)[-min_len:]
        sells = np.asarray(sell_counts, dtype=np.float64)[-min_len:]
        total = buys + sells

        short_rate = float(np.sum(total[-lookback_short:]))
        long_avg = float(np.mean(total[-lookback_long:]))
        recent_buys = int(np.sum(buys[-lookback_long:]))

        if long_avg <= 0:
            if short_rate > 0:
                # Large finite value to indicate strong acceleration from zero baseline
                accel = 100.0
            else:
                accel = None
        else:
            accel = short_rate / long_avg

        return VolumeAccelResult(accel_ratio=accel, recent_buys_count=recent_buys)
    except Exception as exc:
        log.error(f"Volume acceleration error: {exc}")
        return VolumeAccelResult(accel_ratio=None, recent_buys_count=0)
