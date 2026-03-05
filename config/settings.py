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
    MAX_GINI: float = _env("MAX_GINI_COEFFICIENT", 0.85, float)
    MIN_CVD_SLOPE: float = _env("MIN_CVD_SLOPE", 10.0, float)
    MAX_CVD_SLOPE: float = _env("MAX_CVD_SLOPE", 2000.0, float)
    CVD_LOOKBACK: int = _env("CVD_LOOKBACK_PERIODS", 10, int)
    MIN_SNAPSHOTS_HURST: int = _env("MIN_SNAPSHOTS_HURST", 15, int)

    # ── Entry Filters ─────────────────────────────────────────
    MIN_VOLUME_5M: float = _env("MIN_VOLUME_5M_USD", 500.0, float)
    MIN_LIQUIDITY: float = _env("MIN_LIQUIDITY_USD", 75000.0, float)
    MIN_MARKET_CAP: float = _env("MIN_MARKET_CAP", 50000.0, float)
    MAX_MARKET_CAP: float = _env("MAX_MARKET_CAP", 2000000.0, float)
    MIN_BUY_RATIO: float = _env("MIN_BUY_RATIO", 0.65, float)
    MAX_BUY_RATIO: float = _env("MAX_BUY_RATIO", 0.95, float)
    MIN_ACTIVITY_TXNS: int = _env("MIN_ACTIVITY_TXNS", 15, int)

    # ── Exit Rules ────────────────────────────────────────────
    TIME_STOP_MINUTES: float = _env("TIME_STOP_MINUTES", 30.0, float)
    TIME_STOP_SECONDS: float = TIME_STOP_MINUTES * 60.0
    RUG_PROTECTION_PCT: float = _env("RUG_PROTECTION_PCT", -0.25, float)

    # ── Scaled Exit System (v4.1) ────────────────────────────
    PARTIAL_TP_SELL_PCT: float = _env("PARTIAL_TP_SELL_PCT", 0.60, float)
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
    MAX_OPEN_TRADES: int = _env("MAX_OPEN_PAPER_TRADES", 3, int)
    MAX_POSITIONS_PER_TOKEN: int = _env("MAX_POSITIONS_PER_TOKEN", 1, int)
    DAILY_LOSS_LIMIT_PCT: float = _env("DAILY_LOSS_LIMIT_PCT", 0.15, float)
    CIRCUIT_BREAKER_MINUTES: float = _env("CIRCUIT_BREAKER_MINUTES", 60.0, float)
    LARGE_LOSS_THRESHOLD_PCT: float = _env("LARGE_LOSS_THRESHOLD_PCT", -0.10, float)
    LOSS_COOLDOWN_MINUTES: float = _env("LOSS_COOLDOWN_MINUTES", 5.0, float)

    # ── Cost Model (paper trading accuracy) ───────────────────
    # Jupiter/Raydium charges ~0.5% with platform fees (was 0.3%)
    FEE_PER_SIDE_PCT: float = _env("FEE_PER_SIDE_PCT", 0.005, float)
    # Meme coin AMM curves amplify slippage vs size (was 0.5)
    SLIPPAGE_FACTOR: float = _env("SLIPPAGE_FACTOR", 5.0, float)
    # Thin meme coin order books can slip 3%+ on larger trades (was 0.25%)
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

    # Minimum liquidity to include a token in the analyzable set (tracking gate).
    # Intentionally lower than MIN_LIQUIDITY so the paper engine's own entry
    # filter decides what qualifies — tracking is free, exclusion is not.
    TRACKING_MIN_LIQUIDITY: float = _env("TRACKING_MIN_LIQUIDITY_USD", 5000.0, float)

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

    # ── Enhanced Entry Filters (v5.0) ─────────────────────────

    # Entry selectivity
    MIN_GINI: float = _env("MIN_GINI_COEFFICIENT", 0.40, float)
    TOKEN_MIN_AGE_MINUTES: float = _env("TOKEN_MIN_AGE_MINUTES", 30.0, float)
    TOKEN_MAX_AGE_HOURS: float = _env("TOKEN_MAX_AGE_HOURS", 6.0, float)
    MIN_BUY_TXNS_5M: int = _env("MIN_BUY_TXNS_5M", 15, int)
    VOLUME_ACCEL_MULTIPLIER: float = _env("VOLUME_ACCEL_MULTIPLIER", 3.0, float)
    MAX_SINGLE_TXN_CONCENTRATION: float = _env("MAX_SINGLE_TXN_CONCENTRATION", 0.30, float)

    # SOL regime filter
    SOL_REGIME_ENABLED: bool = _env("SOL_REGIME_FILTER_ENABLED", True, lambda v: v.lower() in ("1", "true", "yes"))
    SOL_PAIR_ADDRESS: str = _env("SOL_PAIR_ADDRESS", "")  # Set in .env: Raydium SOL/USDC pair address e.g. 58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWaS2GkQ8stGK
    SOL_SMA_LOOKBACK: int = _env("SOL_SMA_LOOKBACK", 15, int)
    SOL_CRASH_PCT: float = _env("SOL_CRASH_PCT", -0.02, float)
    SOL_CRASH_WINDOW_SECONDS: float = _env("SOL_CRASH_WINDOW_SECONDS", 900.0, float)

    # UTC time blackout filter
    TRADING_BLACKOUT_START_UTC: int = _env("TRADING_BLACKOUT_START_UTC", 2, int)
    TRADING_BLACKOUT_END_UTC: int = _env("TRADING_BLACKOUT_END_UTC", 6, int)

    # Per-trade dynamic exits (v5.0)
    ATR_TP_MULTIPLIER: float = _env("ATR_TP_MULTIPLIER", 1.5, float)
    ATR_TP_COST_BUFFER: float = _env("ATR_TP_COST_BUFFER", 0.02, float)
    ATR_TP_MIN: float = _env("ATR_TP_MIN", 0.04, float)
    ATR_TP_MAX: float = _env("ATR_TP_MAX", 0.15, float)
    ATR_LOOKBACK_PERIODS: int = _env("ATR_LOOKBACK_PERIODS", 5, int)
    SPEED_BONUS_THRESHOLD: float = _env("SPEED_BONUS_THRESHOLD", 0.01, float)
    SPEED_BONUS_MULTIPLIER: float = _env("SPEED_BONUS_MULTIPLIER", 1.3, float)
    SPEED_BONUS_WINDOW_SECONDS: float = _env("SPEED_BONUS_WINDOW_SECONDS", 60.0, float)
    BREAKEVEN_ACTIVATION_PCT: float = _env("BREAKEVEN_ACTIVATION_PCT", 0.04, float)
    BREAKEVEN_BUFFER_PCT: float = _env("BREAKEVEN_BUFFER_PCT", 0.02, float)

    # Liquidity-scaled stop loss
    SL_THIN_POOL_PCT: float = _env("SL_THIN_POOL_PCT", -0.06, float)
    SL_MEDIUM_POOL_PCT: float = _env("SL_MEDIUM_POOL_PCT", -0.08, float)
    SL_DEEP_POOL_PCT: float = _env("SL_DEEP_POOL_PCT", -0.10, float)
    SL_THIN_POOL_THRESHOLD: float = _env("SL_THIN_POOL_THRESHOLD", 100000.0, float)
    SL_DEEP_POOL_THRESHOLD: float = _env("SL_DEEP_POOL_THRESHOLD", 500000.0, float)

    # Trailing stop time decay
    TRAIL_TIGHTEN_1_MINUTES: float = _env("TRAIL_TIGHTEN_1_MINUTES", 15.0, float)
    TRAIL_TIGHTEN_1_FACTOR: float = _env("TRAIL_TIGHTEN_1_FACTOR", 0.7, float)
    TRAIL_TIGHTEN_2_MINUTES: float = _env("TRAIL_TIGHTEN_2_MINUTES", 20.0, float)
    TRAIL_TIGHTEN_2_FACTOR: float = _env("TRAIL_TIGHTEN_2_FACTOR", 0.5, float)
    HARD_TIME_STOP_MINUTES: float = _env("HARD_TIME_STOP_MINUTES", 25.0, float)
    HARD_TIME_STOP_SECONDS: float = HARD_TIME_STOP_MINUTES * 60.0

    # Portfolio-level risk
    PORTFOLIO_HEAT_LIMIT_PCT: float = _env("PORTFOLIO_HEAT_LIMIT_PCT", -0.05, float)
    PORTFOLIO_HEAT_PAUSE_MINUTES: float = _env("PORTFOLIO_HEAT_PAUSE_MINUTES", 30.0, float)

    # ── Safety Enrichment (v5.2) ───────────────────────────
    RUGCHECK_ENABLED: bool = _env("RUGCHECK_ENABLED", True, lambda v: v.lower() in ("1", "true", "yes"))
    RUGCHECK_MAX_SCORE: int = _env("RUGCHECK_MAX_SCORE", 1000, int)
    RUGCHECK_REQUIRE_MINT_RENOUNCED: bool = _env("RUGCHECK_REQUIRE_MINT_RENOUNCED", True, lambda v: v.lower() in ("1", "true", "yes"))
    RUGCHECK_REQUIRE_LP_LOCKED: bool = _env("RUGCHECK_REQUIRE_LP_LOCKED", False, lambda v: v.lower() in ("1", "true", "yes"))
    PUMPFUN_CHECK_ENABLED: bool = _env("PUMPFUN_CHECK_ENABLED", True, lambda v: v.lower() in ("1", "true", "yes"))
    PUMPFUN_MIGRATION_BONUS: bool = _env("PUMPFUN_MIGRATION_BONUS", True, lambda v: v.lower() in ("1", "true", "yes"))

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
            "Hard time stop": f"{cls.HARD_TIME_STOP_MINUTES:.0f}min",
            "Position": f"{cls.POSITION_PCT:.0%} bal / {cls.MAX_LIQUIDITY_PCT:.0%} liq",
            "Max open": str(cls.MAX_OPEN_TRADES),
            "Hurst gate": f"≥{cls.HURST_THRESHOLD}",
            "Gini gate": f"{cls.MIN_GINI}–{cls.MAX_GINI}",
            "CVD slope": f"{cls.MIN_CVD_SLOPE}–{cls.MAX_CVD_SLOPE}",
            "Partial TP": f"{cls.PARTIAL_TP_SELL_PCT:.0%}",
            "Trail stop": f"{cls.TRAILING_STOP_PCT:.0%}",
            "Max gain": f"{cls.MAX_GAIN_PCT:.0%}",
            "Cooldown": f"{cls.TRADE_COOLDOWN_MINUTES:.0f}min",
            "Momentum": "ON" if cls.MOMENTUM_CHECK_ENABLED else "OFF",
            "Token age": f"{cls.TOKEN_MIN_AGE_MINUTES:.0f}min–{cls.TOKEN_MAX_AGE_HOURS:.0f}h",
            "Vol accel": f"≥{cls.VOLUME_ACCEL_MULTIPLIER}x",
            "SOL regime": "ON" if cls.SOL_REGIME_ENABLED else "OFF",
            "ATR TP range": f"{cls.ATR_TP_MIN:.0%}–{cls.ATR_TP_MAX:.0%}",
            "SL (thin/med/deep)": f"{cls.SL_THIN_POOL_PCT:.0%}/{cls.SL_MEDIUM_POOL_PCT:.0%}/{cls.SL_DEEP_POOL_PCT:.0%}",
            "RugCheck": "ON" if cls.RUGCHECK_ENABLED else "OFF",
            "Mint renounced": "REQUIRED" if cls.RUGCHECK_REQUIRE_MINT_RENOUNCED else "optional",
            "LP locked": "REQUIRED" if cls.RUGCHECK_REQUIRE_LP_LOCKED else "optional",
            "Pump.fun bonus": "ON" if cls.PUMPFUN_MIGRATION_BONUS else "OFF",
        }
