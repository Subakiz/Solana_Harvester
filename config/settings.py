"""
Centralized configuration for the Quant Harvest Bot.
All tunable parameters are defined here and loaded from environment variables
with sensible defaults. No trading parameters should be hardcoded elsewhere.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


def _env(key: str, default, cast=str):
    """Read an environment variable with type casting and default."""
    val = os.getenv(key, None)
    if val is None:
        return default
    try:
        return cast(val)
    except (ValueError, TypeError):
        return default


class Settings:
    """
    Global, read-only configuration singleton.
    Values are read from environment variables at import time.
    """

    # ── Infrastructure ────────────────────────────────────────
    SOLANA_RPC_URL: str = _env("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=DEMO")
    DB_PATH: str = _env("DB_PATH", "data/quant_harvest.db")
    DEXSCREENER_BASE: str = _env("DEXSCREENER_BASE_URL", "https://api.dexscreener.com")
    POLL_INTERVAL: float = _env("POLL_INTERVAL_SECONDS", 4.0, float)
    ROLLING_WINDOW: int = _env("ROLLING_WINDOW_SIZE", 80, int)

    # ── Quant signal gates (used as entry filters when data is available) ──
    HURST_THRESHOLD: float = _env("HURST_THRESHOLD", 0.70, float)
    MAX_GINI: float = _env("MAX_GINI_COEFFICIENT", 0.35, float)
    MIN_CVD_SLOPE: float = _env("MIN_CVD_SLOPE", 10.0, float)
    MAX_CVD_SLOPE: float = _env("MAX_CVD_SLOPE", 2000.0, float)
    CVD_LOOKBACK: int = _env("CVD_LOOKBACK_PERIODS", 10, int)
    MIN_SNAPSHOTS_HURST: int = _env("MIN_SNAPSHOTS_HURST", 15, int)

    # ── Entry Filters ─────────────────────────────────────────
    MIN_VOLUME_5M: float = _env("MIN_VOLUME_5M_USD", 500.0, float)
    MIN_LIQUIDITY: float = _env("MIN_LIQUIDITY_USD", 50000.0, float)
    MIN_MARKET_CAP: float = _env("MIN_MARKET_CAP", 50000.0, float)
    MAX_MARKET_CAP: float = _env("MAX_MARKET_CAP", 2000000.0, float)
    MIN_BUY_RATIO: float = _env("MIN_BUY_RATIO", 0.40, float)
    MAX_BUY_RATIO: float = _env("MAX_BUY_RATIO", 0.75, float)
    MIN_ACTIVITY_TXNS: int = _env("MIN_ACTIVITY_TXNS", 3, int)

    # ── Exit Rules ────────────────────────────────────────────
    TIME_STOP_MINUTES: float = _env("TIME_STOP_MINUTES", 30.0, float)
    TIME_STOP_SECONDS: float = TIME_STOP_MINUTES * 60.0
    RUG_PROTECTION_PCT: float = _env("RUG_PROTECTION_PCT", -0.15, float)

    # ── Scaled Exit System (v4.1) ────────────────────────────
    PARTIAL_TP_SELL_PCT: float = _env("PARTIAL_TP_SELL_PCT", 0.50, float)
    TRAILING_STOP_PCT: float = _env("TRAILING_STOP_PCT", 0.25, float)
    TRAILING_ACTIVATION_PCT: float = _env("TRAILING_ACTIVATION_PCT", 0.05, float)
    MAX_GAIN_PCT: float = _env("MAX_GAIN_PCT", 2.00, float)
    MAX_POSITION_TIME_MINUTES: float = _env("MAX_POSITION_TIME_MINUTES", 180.0, float)
    MAX_POSITION_TIME_SECONDS: float = MAX_POSITION_TIME_MINUTES * 60.0

    # ── Partial Stop Loss (v4.1) ─────────────────────────────
    PARTIAL_STOP_LOSS_PCT: float = _env("PARTIAL_STOP_LOSS_PCT", -0.08, float)
    PARTIAL_STOP_SELL_PCT: float = _env("PARTIAL_STOP_SELL_PCT", 0.50, float)

    # ── Position Sizing ───────────────────────────────────────
    POSITION_PCT: float = _env("POSITION_PCT", 0.10, float)
    MAX_LIQUIDITY_PCT: float = _env("MAX_LIQUIDITY_PCT", 0.02, float)
    MIN_TRADE_SIZE_USD: float = _env("MIN_TRADE_SIZE_USD", 1.00, float)
    INITIAL_BALANCE: float = _env("INITIAL_BALANCE", 10000.0, float)

    # ── Risk Management ───────────────────────────────────────
    MAX_OPEN_TRADES: int = _env("MAX_OPEN_PAPER_TRADES", 5, int)
    MAX_POSITIONS_PER_TOKEN: int = _env("MAX_POSITIONS_PER_TOKEN", 1, int)
    DAILY_LOSS_LIMIT_PCT: float = _env("DAILY_LOSS_LIMIT_PCT", 0.15, float)
    CIRCUIT_BREAKER_MINUTES: float = _env("CIRCUIT_BREAKER_MINUTES", 60.0, float)
    LARGE_LOSS_THRESHOLD_PCT: float = _env("LARGE_LOSS_THRESHOLD_PCT", -0.10, float)
    LOSS_COOLDOWN_MINUTES: float = _env("LOSS_COOLDOWN_MINUTES", 5.0, float)

    # ── Cost Model (paper trading accuracy) ───────────────────
    FEE_PER_SIDE_PCT: float = _env("FEE_PER_SIDE_PCT", 0.005, float)
    SLIPPAGE_FACTOR: float = _env("SLIPPAGE_FACTOR", 5.0, float)
    MAX_SLIPPAGE_PCT: float = _env("MAX_SLIPPAGE_PCT", 3.0, float)
    BASE_SLIPPAGE_PCT: float = _env("BASE_SLIPPAGE_PCT", 0.01, float)
    PRIORITY_FEE_USD: float = _env("PRIORITY_FEE_USD", 0.05, float)

    # ── Data Reliability ──────────────────────────────────────
    DATA_STALE_WARNING_SECONDS: float = _env("DATA_STALE_WARNING_SECONDS", 60.0, float)
    DATA_STALE_ERROR_SECONDS: float = _env("DATA_STALE_ERROR_SECONDS", 120.0, float)
    DATA_TIMEOUT_EXIT_SECONDS: float = _env("DATA_TIMEOUT_EXIT_SECONDS", 180.0, float)
    DATA_RESUME_WAIT_SECONDS: float = _env("DATA_RESUME_WAIT_SECONDS", 30.0, float)

    # ── Rate Limits ───────────────────────────────────────────
    # Legacy AsyncRateLimiter is set high (300) because TieredPoller already
    # enforces the real 55 req/min budget. 300 keeps it as a catastrophic
    # safety net only, preventing it from throttling normal traffic.
    DEX_RPM: int = _env("DEXSCREENER_REQUESTS_PER_MINUTE", 300, int)
    RPC_RPM: int = _env("RPC_REQUESTS_PER_MINUTE", 20, int)

    # ── Tiered Polling (v3.1) ─────────────────────────────────
    MAIN_LOOP_INTERVAL: float = _env("MAIN_LOOP_INTERVAL_SECONDS", 1.0, float)
    TIER1_POLL_INTERVAL: float = _env("TIER1_POLL_INTERVAL_SECONDS", 2.0, float)
    TIER2_POLL_INTERVAL: float = _env("TIER2_POLL_INTERVAL_SECONDS", 3.0, float)
    TIER3_POLL_INTERVAL: float = _env("TIER3_POLL_INTERVAL_SECONDS", 12.0, float)
    DISCOVERY_INTERVAL: float = _env("DISCOVERY_INTERVAL_SECONDS", 25.0, float)
    TIER2_MAX_TOKENS: int = _env("TIER2_MAX_TOKENS", 30, int)
    TIER3_MAX_TOKENS: int = _env("TIER3_MAX_TOKENS", 200, int)
    API_RATE_LIMIT: int = _env("API_RATE_LIMIT_PER_MINUTE", 55, int)

    # Discovery pre-filter (looser than entry filters)
    DISCOVERY_MIN_LIQUIDITY: float = _env("DISCOVERY_MIN_LIQUIDITY_USD", 5000.0, float)
    DISCOVERY_MIN_MCAP: float = _env("DISCOVERY_MIN_MCAP", 10000.0, float)
    DISCOVERY_MAX_MCAP: float = _env("DISCOVERY_MAX_MCAP", 10000000.0, float)

    # Tier 2 promotion thresholds (near entry filters)
    HOT_MIN_VOLUME: float = _env("HOT_MIN_VOLUME_5M_USD", 200.0, float)
    HOT_MIN_LIQUIDITY: float = _env("HOT_MIN_LIQUIDITY_USD", 15000.0, float)
    HOT_MIN_MCAP: float = _env("HOT_MIN_MCAP", 40000.0, float)
    HOT_MAX_MCAP: float = _env("HOT_MAX_MCAP", 2500000.0, float)
    HOT_MIN_ACTIVITY: int = _env("HOT_MIN_ACTIVITY_TXNS", 2, int)

    # Tier 3 demotion/removal thresholds
    DEAD_TOKEN_ZERO_VOL_POLLS: int = _env("DEAD_TOKEN_ZERO_VOL_POLLS", 20, int)
    DEAD_TOKEN_MIN_LIQUIDITY: float = _env("DEAD_TOKEN_MIN_LIQUIDITY_USD", 3000.0, float)
    STALE_TOKEN_MAX_AGE_HOURS: float = _env("STALE_TOKEN_MAX_AGE_HOURS", 2.0, float)

    # ── Trade Cooldown (v4.1) ────────────────────────────────
    TRADE_COOLDOWN_MINUTES: float = _env("TRADE_COOLDOWN_MINUTES", 5.0, float)

    # ── Momentum Confirmation Filter (v4.1) ──────────────────
    MOMENTUM_SMA_LOOKBACK: int = _env("MOMENTUM_SMA_LOOKBACK", 10, int)
    MOMENTUM_DELTA_LOOKBACK: int = _env("MOMENTUM_DELTA_LOOKBACK", 5, int)
    MOMENTUM_CHECK_ENABLED: bool = _env("MOMENTUM_CHECK_ENABLED", True, lambda v: v.lower() in ("1", "true", "yes"))

    # ── Enhanced Discovery (v4.1) ────────────────────────────
    DISCOVERY_ENABLE_LATEST_PAIRS: bool = _env("DISCOVERY_ENABLE_LATEST_PAIRS", True, lambda v: v.lower() in ("1", "true", "yes"))
    DISCOVERY_ENABLE_PROFILES: bool = _env("DISCOVERY_ENABLE_PROFILES", True, lambda v: v.lower() in ("1", "true", "yes"))

    # ── Logging ───────────────────────────────────────────────
    LOG_LEVEL: str = _env("LOG_LEVEL", "DEBUG")
    LOG_FILE: str = _env("LOG_FILE", "logs/harvester.log")

    # ── Misc ──────────────────────────────────────────────────
    TOP_HOLDERS: int = _env("TOP_HOLDERS_COUNT", 50, int)

    @classmethod
    def summary(cls) -> dict[str, str]:
        """Return a dict of key settings for display at startup."""
        return {
            "DB": cls.DB_PATH,
            "Poll": f"{cls.POLL_INTERVAL}s",
            "Window": str(cls.ROLLING_WINDOW),
            "Volume min": f"${cls.MIN_VOLUME_5M:,.0f}",
            "Liquidity min": f"${cls.MIN_LIQUIDITY:,.0f}",
            "Mcap range": f"${cls.MIN_MARKET_CAP:,.0f}-${cls.MAX_MARKET_CAP:,.0f}",
            "Buy ratio": f"{cls.MIN_BUY_RATIO:.0%}-{cls.MAX_BUY_RATIO:.0%}",
            "Rug protect": f"{cls.RUG_PROTECTION_PCT:.0%}",
            "Time stop": f"{cls.TIME_STOP_MINUTES:.0f}min",
            "Position": f"{cls.POSITION_PCT:.0%} bal / {cls.MAX_LIQUIDITY_PCT:.0%} liq",
            "Max open": str(cls.MAX_OPEN_TRADES),
            "Hurst gate": f"≥{cls.HURST_THRESHOLD}",
            "Gini gate": f"≤{cls.MAX_GINI}",
            "CVD slope": f"{cls.MIN_CVD_SLOPE}–{cls.MAX_CVD_SLOPE}",
            "Partial TP": f"{cls.PARTIAL_TP_SELL_PCT:.0%}",
            "Trail stop": f"{cls.TRAILING_STOP_PCT:.0%}",
            "Max gain": f"{cls.MAX_GAIN_PCT:.0%}",
            "Cooldown": f"{cls.TRADE_COOLDOWN_MINUTES:.0f}min",
            "Momentum": "ON" if cls.MOMENTUM_CHECK_ENABLED else "OFF",
        }
