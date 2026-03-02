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
    ROLLING_WINDOW: int = _env("ROLLING_WINDOW_SIZE", 50, int)

    # ── Quant thresholds (kept for calculation, NOT used as gates) ──
    HURST_THRESHOLD: float = _env("HURST_THRESHOLD", 0.6, float)
    MAX_GINI: float = _env("MAX_GINI_COEFFICIENT", 0.85, float)
    CVD_LOOKBACK: int = _env("CVD_LOOKBACK_PERIODS", 10, int)
    MIN_SNAPSHOTS_HURST: int = _env("MIN_SNAPSHOTS_HURST", 20, int)

    # ── Entry Filters ─────────────────────────────────────────
    MIN_VOLUME_5M: float = _env("MIN_VOLUME_5M_USD", 500.0, float)
    MIN_LIQUIDITY: float = _env("MIN_LIQUIDITY_USD", 20000.0, float)
    MIN_MARKET_CAP: float = _env("MIN_MARKET_CAP", 50000.0, float)
    MAX_MARKET_CAP: float = _env("MAX_MARKET_CAP", 2000000.0, float)
    MIN_BUY_RATIO: float = _env("MIN_BUY_RATIO", 0.40, float)
    MAX_BUY_RATIO: float = _env("MAX_BUY_RATIO", 0.75, float)
    MIN_ACTIVITY_TXNS: int = _env("MIN_ACTIVITY_TXNS", 3, int)

    # ── Exit Rules ────────────────────────────────────────────
    TIME_STOP_MINUTES: float = _env("TIME_STOP_MINUTES", 30.0, float)
    TIME_STOP_SECONDS: float = TIME_STOP_MINUTES * 60.0
    RUG_PROTECTION_PCT: float = _env("RUG_PROTECTION_PCT", -0.50, float)
    # TRAILING_STOP: REMOVED
    # HARD_STOP: REMOVED (replaced by rug_protection)

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
    FEE_PER_SIDE_PCT: float = _env("FEE_PER_SIDE_PCT", 0.003, float)
    SLIPPAGE_FACTOR: float = _env("SLIPPAGE_FACTOR", 0.5, float)
    MAX_SLIPPAGE_PCT: float = _env("MAX_SLIPPAGE_PCT", 0.25, float)

    # ── Data Reliability ──────────────────────────────────────
    DATA_STALE_WARNING_SECONDS: float = _env("DATA_STALE_WARNING_SECONDS", 60.0, float)
    DATA_STALE_ERROR_SECONDS: float = _env("DATA_STALE_ERROR_SECONDS", 120.0, float)
    DATA_TIMEOUT_EXIT_SECONDS: float = _env("DATA_TIMEOUT_EXIT_SECONDS", 180.0, float)
    DATA_RESUME_WAIT_SECONDS: float = _env("DATA_RESUME_WAIT_SECONDS", 30.0, float)

    # ── Rate Limits ───────────────────────────────────────────
    DEX_RPM: int = _env("DEXSCREENER_REQUESTS_PER_MINUTE", 30, int)
    RPC_RPM: int = _env("RPC_REQUESTS_PER_MINUTE", 20, int)

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
            "Hurst (log)": f">{cls.HURST_THRESHOLD}",
            "Gini (log)": f"<{cls.MAX_GINI}",
        }
