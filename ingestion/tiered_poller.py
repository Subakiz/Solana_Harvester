"""
Tiered Polling Architecture for DexScreener API.
v3.1 — Priority-based scheduler replacing the flat 4-second cycle.

Architecture:
  Tier 1 — OPEN_POSITION: Poll every 2s (up to 5 tokens with active trades)
  Tier 2 — HOT_WATCHLIST: Poll every 3s (up to 30 pre-qualified entry candidates)
  Tier 3 — WARM_SCANNER:  Poll every 12s (up to 200 broader-market tokens)
  Tier 4 — DISCOVERY:     Poll every 25s (DexScreener discovery endpoint)

Stays under 55 req/min (DexScreener cap: 60 req/min).
"""
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional, Set


class TokenTier(IntEnum):
    """Priority tiers for polling frequency."""
    OPEN_POSITION = 1   # Tokens with active trades
    HOT_WATCHLIST = 2   # Pre-qualified entry candidates
    WARM_SCANNER = 3    # Broader market monitoring
    DISCOVERY = 4       # New token discovery feed


@dataclass
class TrackedToken:
    """State for a single tracked token."""
    mint: str
    symbol: str
    pair_address: str
    tier: TokenTier = TokenTier.WARM_SCANNER
    last_poll_time: float = 0.0
    last_tick_time: float = 0.0
    last_price: float = 0.0
    last_volume_5m: float = 0.0
    last_liquidity: float = 0.0
    last_mcap: float = 0.0
    last_buys_5m: int = 0
    last_sells_5m: int = 0
    consecutive_zero_vol: int = 0    # Track dead tokens
    added_time: float = field(default_factory=time.time)
    promotion_time: float = 0.0      # When promoted to current tier


@dataclass
class TierConfig:
    """Configuration for each polling tier."""
    poll_interval_sec: float      # Minimum seconds between polls
    max_tokens: int               # Maximum tokens in this tier
    batch_size: int               # Max addresses per API request


TIER_CONFIGS: Dict[TokenTier, TierConfig] = {
    TokenTier.OPEN_POSITION: TierConfig(
        poll_interval_sec=2.0,
        max_tokens=5,       # Matches MAX_OPEN_TRADES
        batch_size=5,
    ),
    TokenTier.HOT_WATCHLIST: TierConfig(
        poll_interval_sec=3.0,
        max_tokens=30,
        batch_size=30,
    ),
    TokenTier.WARM_SCANNER: TierConfig(
        poll_interval_sec=12.0,
        max_tokens=200,
        batch_size=30,
    ),
    TokenTier.DISCOVERY: TierConfig(
        poll_interval_sec=25.0,
        max_tokens=0,       # Discovery doesn't track tokens
        batch_size=0,
    ),
}


class TieredPoller:
    """
    Priority-based polling scheduler.
    Runs every ~1 second, picks the most urgent polling task,
    and stays within the 60 req/min API budget.
    """

    def __init__(
        self,
        tier1_interval: float = 2.0,
        tier2_interval: float = 3.0,
        tier3_interval: float = 12.0,
        discovery_interval: float = 25.0,
        tier2_max_tokens: int = 30,
        tier3_max_tokens: int = 200,
        rate_limit_max: int = 55,
    ):
        self.tokens: Dict[str, TrackedToken] = {}
        self.request_timestamps: List[float] = []  # Rolling window for rate limiting
        self.last_discovery_time: float = 0.0
        self._rate_limit_window = 60.0   # seconds
        self._rate_limit_max = rate_limit_max

        # Allow per-instance tier config overrides
        self._tier_configs: Dict[TokenTier, TierConfig] = {
            TokenTier.OPEN_POSITION: TierConfig(
                poll_interval_sec=tier1_interval,
                max_tokens=5,
                batch_size=5,
            ),
            TokenTier.HOT_WATCHLIST: TierConfig(
                poll_interval_sec=tier2_interval,
                max_tokens=tier2_max_tokens,
                batch_size=30,
            ),
            TokenTier.WARM_SCANNER: TierConfig(
                poll_interval_sec=tier3_interval,
                max_tokens=tier3_max_tokens,
                batch_size=30,
            ),
            TokenTier.DISCOVERY: TierConfig(
                poll_interval_sec=discovery_interval,
                max_tokens=0,
                batch_size=0,
            ),
        }

    # ── Rate limiting ──────────────────────────────────────────

    def _requests_in_window(self) -> int:
        """Count API requests made in the last 60 seconds."""
        now = time.time()
        cutoff = now - self._rate_limit_window
        self.request_timestamps = [t for t in self.request_timestamps if t > cutoff]
        return len(self.request_timestamps)

    def _can_make_request(self) -> bool:
        """Check if we're within rate limit budget."""
        return self._requests_in_window() < self._rate_limit_max

    def record_request(self):
        """Record that an API request was made."""
        self.request_timestamps.append(time.time())

    # ── Token accessors ────────────────────────────────────────

    def get_tokens_by_tier(self, tier: TokenTier) -> List[TrackedToken]:
        """Get all tokens in a specific tier, sorted by staleness (most stale first)."""
        tier_tokens = [t for t in self.tokens.values() if t.tier == tier]
        tier_tokens.sort(key=lambda t: t.last_poll_time)
        return tier_tokens

    # ── Scheduling ─────────────────────────────────────────────

    def get_next_poll_task(self) -> Optional[tuple]:
        """
        Determine what to poll next based on priority and staleness.
        Returns (tier, list_of_mints) or None if nothing is due.

        Priority order:
          1. Open positions (Tier 1)
          2. Hot watchlist (Tier 2)
          3. Discovery (Tier 4)
          4. Warm scanner (Tier 3)
        """
        now = time.time()

        if not self._can_make_request():
            return None

        # Priority 1: Open positions overdue for poll
        t1_cfg = self._tier_configs[TokenTier.OPEN_POSITION]
        t1_due = [
            t for t in self.get_tokens_by_tier(TokenTier.OPEN_POSITION)
            if now - t.last_poll_time >= t1_cfg.poll_interval_sec
        ]
        if t1_due:
            batch = t1_due[:t1_cfg.batch_size]
            return (TokenTier.OPEN_POSITION, [t.mint for t in batch])

        # Priority 2: Hot watchlist overdue
        t2_cfg = self._tier_configs[TokenTier.HOT_WATCHLIST]
        t2_due = [
            t for t in self.get_tokens_by_tier(TokenTier.HOT_WATCHLIST)
            if now - t.last_poll_time >= t2_cfg.poll_interval_sec
        ]
        if t2_due:
            batch = t2_due[:t2_cfg.batch_size]
            return (TokenTier.HOT_WATCHLIST, [t.mint for t in batch])

        # Priority 3: Discovery (new tokens)
        disc_cfg = self._tier_configs[TokenTier.DISCOVERY]
        if now - self.last_discovery_time >= disc_cfg.poll_interval_sec:
            return (TokenTier.DISCOVERY, [])

        # Priority 4: Warm scanner (round-robin)
        t3_cfg = self._tier_configs[TokenTier.WARM_SCANNER]
        t3_due = [
            t for t in self.get_tokens_by_tier(TokenTier.WARM_SCANNER)
            if now - t.last_poll_time >= t3_cfg.poll_interval_sec
        ]
        if t3_due:
            batch = t3_due[:t3_cfg.batch_size]
            return (TokenTier.WARM_SCANNER, [t.mint for t in batch])

        return None  # Nothing due yet

    # ── Tier management ────────────────────────────────────────

    def update_token_tiers(
        self,
        open_position_mints: Set[str],
        hot_min_volume: float = 200.0,
        hot_min_liquidity: float = 15000.0,
        hot_min_mcap: float = 40000.0,
        hot_max_mcap: float = 2500000.0,
        hot_min_activity: int = 2,
        hot_promote_max_zero_vol: int = 3,   # Max consecutive zero-vol polls for T2 promotion
        hot_demote_min_volume: float = 50.0,  # Volume below which T2 is demoted
        hot_demote_min_liquidity: float = 10000.0,
        hot_demote_min_mcap: float = 20000.0,
        hot_demote_max_mcap: float = 3000000.0,
        hot_demote_zero_vol: int = 5,         # Zero-vol polls that trigger T2 demotion
        hot_max_age_hours: float = 6.0,       # Age above which T2 tokens are demoted
        dead_zero_vol_polls: int = 20,
        dead_min_liquidity: float = 3000.0,
        dead_min_mcap: float = 5000.0,        # Mcap below this → token effectively rugged
        stale_max_age_hours: float = 2.0,
        stale_min_volume: float = 10.0,       # Min volume to keep a stale-age token
    ):
        """
        Reclassify tokens into tiers based on current state.
        Called after each poll cycle.

        Args:
            open_position_mints: Set of mint addresses with open trades.
        """
        now = time.time()
        t2_cfg = self._tier_configs[TokenTier.HOT_WATCHLIST]

        for mint, token in self.tokens.items():
            old_tier = token.tier

            # TIER 1: Any token with an open position
            if mint in open_position_mints:
                token.tier = TokenTier.OPEN_POSITION
                if old_tier != TokenTier.OPEN_POSITION:
                    token.promotion_time = now
                continue

            # TIER 2 DEMOTION CRITERIA (evaluated before promotion so that an
            # already-hot token that has grown stale is not silently re-promoted)
            if token.tier == TokenTier.HOT_WATCHLIST:
                token_age_hours = (now - token.added_time) / 3600.0
                demote = (
                    token.last_volume_5m < hot_demote_min_volume
                    or token.last_liquidity < hot_demote_min_liquidity
                    or token.last_mcap < hot_demote_min_mcap
                    or token.last_mcap > hot_demote_max_mcap
                    or token.consecutive_zero_vol >= hot_demote_zero_vol
                    or token_age_hours > hot_max_age_hours
                )
                if demote:
                    token.tier = TokenTier.WARM_SCANNER
                    continue

            # TIER 2 PROMOTION CRITERIA
            promotes_to_hot = (
                token.last_volume_5m >= hot_min_volume
                and token.last_liquidity >= hot_min_liquidity
                and hot_min_mcap <= token.last_mcap <= hot_max_mcap
                and token.consecutive_zero_vol < hot_promote_max_zero_vol
                and (token.last_buys_5m + token.last_sells_5m) >= hot_min_activity
            )

            if promotes_to_hot:
                current_t2_count = sum(
                    1 for t in self.tokens.values()
                    if t.tier == TokenTier.HOT_WATCHLIST
                )
                if current_t2_count < t2_cfg.max_tokens:
                    token.tier = TokenTier.HOT_WATCHLIST
                    if old_tier != TokenTier.HOT_WATCHLIST:
                        token.promotion_time = now
                    continue

            # Tokens that were Tier 1 but position closed: ensure they leave Tier 1.
            # If they didn't qualify for Tier 2 above (or T2 is full), put in Tier 3.
            if old_tier == TokenTier.OPEN_POSITION and mint not in open_position_mints:
                if token.tier == TokenTier.OPEN_POSITION:
                    # Still has old tier — wasn't promoted to Tier 2 above
                    token.tier = TokenTier.WARM_SCANNER

        # Clean up dead tokens (iterate over a copy to allow deletion)
        stale_age_seconds = stale_max_age_hours * 3600.0
        to_remove = [
            mint for mint, t in self.tokens.items()
            if mint not in open_position_mints
            and t.tier != TokenTier.OPEN_POSITION
            and (
                (t.tier == TokenTier.WARM_SCANNER and (
                    t.consecutive_zero_vol >= dead_zero_vol_polls
                    or t.last_mcap < dead_min_mcap
                    or t.last_liquidity < dead_min_liquidity
                    or (
                        now - t.added_time > stale_age_seconds
                        and t.last_volume_5m < stale_min_volume
                    )
                ))
                or
                # Also evict HOT_WATCHLIST tokens that are too old with no volume
                (t.tier == TokenTier.HOT_WATCHLIST
                 and now - t.added_time > stale_age_seconds
                 and t.last_volume_5m < stale_min_volume)
            )
        ]
        for mint in to_remove:
            del self.tokens[mint]

        return to_remove  # Return list of removed mints for logging

    def promote_to_tier1(self, mint: str):
        """Immediately promote a token to Tier 1 (open position)."""
        if mint in self.tokens:
            token = self.tokens[mint]
            if token.tier != TokenTier.OPEN_POSITION:
                token.tier = TokenTier.OPEN_POSITION
                token.promotion_time = time.time()

    def tier_counts(self) -> Dict[str, int]:
        """Return a dict of tier name -> token count."""
        counts: Dict[str, int] = {t.name: 0 for t in TokenTier if t != TokenTier.DISCOVERY}
        for token in self.tokens.values():
            name = token.tier.name
            if name in counts:
                counts[name] += 1
        return counts
