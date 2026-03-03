"""
Data Ingestion Layer — DexScreener API + Solana RPC (for Gini).
v3.1 — Tiered Polling Architecture
────────────────────────────────────
Replaces the flat 4-second cycle with a priority-based scheduler:

  Tier 1 — OPEN_POSITION (every 2s):  tokens with active trades
  Tier 2 — HOT_WATCHLIST (every 3s):  pre-qualified entry candidates
  Tier 3 — WARM_SCANNER  (every 12s): broader market monitoring (up to 200)
  Tier 4 — DISCOVERY     (every 25s): new token discovery

API budget: ~55 req/min (under 60 cap).
Main loop interval: 1 second (was 4 seconds).

Legacy poll() is preserved for backward compatibility.
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional, Set
import aiohttp
import numpy as np
import orjson
from config.settings import Settings
from ingestion.tiered_poller import TieredPoller, TrackedToken, TokenTier, TIER_CONFIGS
from utils.logger import get_logger
from utils.rate_limiter import AsyncRateLimiter

log = get_logger("Harvester")

# ── Constants ────────────────────────────────────────────────
BULK_CHUNK_SIZE = 30           # DexScreener max per bulk request
STALE_THRESHOLD_SECONDS = 3.0  # Token considered stale if older than this
PRUNE_THRESHOLD_SECONDS = 300.0 # Remove token if no data for 5 minutes
DISCOVERY_COOLDOWN_SECONDS = 15.0 # Don't re-discover too aggressively (legacy)

# ── In-memory snapshot model ────────────────────────────────

@dataclass
class Tick:
    timestamp: float
    price_usd: float
    liquidity_usd: float
    volume_5m: float
    buys_5m: int
    sells_5m: int
    market_cap: float

@dataclass
class TokenBuffer:
    """Rolling window of ticks for one token, kept in memory."""
    mint: str
    symbol: str
    name: str
    pair_address: str
    dex_id: str
    ticks: list[Tick] = field(default_factory=list)
    first_seen: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)

    # ── Convenience numpy accessors ──────────────────────
    @property
    def prices(self) -> np.ndarray:
        return np.array([t.price_usd for t in self.ticks], dtype=np.float64)

    @property
    def volumes(self) -> np.ndarray:
        return np.array([t.volume_5m for t in self.ticks], dtype=np.float64)

    @property
    def buys(self) -> np.ndarray:
        return np.array([t.buys_5m for t in self.ticks], dtype=np.float64)

    @property
    def sells(self) -> np.ndarray:
        return np.array([t.sells_5m for t in self.ticks], dtype=np.float64)

    @property
    def count(self) -> int:
        return len(self.ticks)

    @property
    def latest_price(self) -> float:
        return self.ticks[-1].price_usd if self.ticks else 0.0

    @property
    def latest_liquidity(self) -> float:
        return self.ticks[-1].liquidity_usd if self.ticks else 0.0

    def append(self, tick: Tick, max_window: int):
        self.ticks.append(tick)
        if len(self.ticks) > max_window:
            self.ticks = self.ticks[-max_window:]
        self.last_updated = time.time()

class DataHarvester:
    """
    Polls DexScreener and manages in-memory token buffers.
    Also provides a Gini helper that calls Solana RPC.

    v2 optimizations:
    • Bulk token fetching (30 per request)
    • Highest-liquidity pair selection
    • Aggressive staleness detection (3s)
    • Timed pruning (5 min inactivity)
    """
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._rpc_session: Optional[aiohttp.ClientSession] = None
        self.tokens: dict[str, TokenBuffer] = {} # mint -> buffer
        self._discovery_total = 0
        self._last_discovery_time: float = 0.0
        
        self.dex_limiter = AsyncRateLimiter(
            "DexScreener", Settings.DEX_RPM, period=60.0
        )
        self.rpc_limiter = AsyncRateLimiter(
            "SolanaRPC", Settings.RPC_RPM, period=60.0
        )
        
        # Cache Gini results to avoid hitting RPC repeatedly
        self._gini_cache: dict[str, tuple[float, float]] = {} # mint -> (gini, ts)
        self._gini_cache_ttl = 180.0 # 3 minutes

        # Helius RPC 401 tracking — short-circuit after consecutive failures
        self._rpc_consecutive_401s: int = 0
        self._rpc_401_threshold: int = 5       # Disable after this many consecutive 401s
        self._rpc_eval_count: int = 0           # Total holder queries attempted
        self._rpc_retry_interval: int = 50      # Retry every N evaluations when disabled
        self._rpc_disabled_logged: bool = False  # Only log the disable warning once

        # Performance counters (reset each poll for logging)
        self._poll_stats: dict[str, int] = {}

        # ── Tiered polling scheduler (v3.1) ──────────────────
        self.poller = TieredPoller(
            tier1_interval=Settings.TIER1_POLL_INTERVAL,
            tier2_interval=Settings.TIER2_POLL_INTERVAL,
            tier3_interval=Settings.TIER3_POLL_INTERVAL,
            discovery_interval=Settings.DISCOVERY_INTERVAL,
            tier2_max_tokens=Settings.TIER2_MAX_TOKENS,
            tier3_max_tokens=Settings.TIER3_MAX_TOKENS,
            rate_limit_max=Settings.API_RATE_LIMIT,
        )

    # ══════════════════════════════════════════════════════════
    # Session Management
    # ══════════════════════════════════════════════════════════

    async def _dex_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=12, connect=6),
                headers={"Accept": "application/json"},
            )
        return self._session

    async def _solana_session(self) -> aiohttp.ClientSession:
        if self._rpc_session is None or self._rpc_session.closed:
            self._rpc_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15, connect=6),
            )
        return self._rpc_session

    async def close(self):
        for s in (self._session, self._rpc_session):
            if s and not s.closed:
                await s.close()

    # ══════════════════════════════════════════════════════════
    # HTTP Helpers
    # ══════════════════════════════════════════════════════════

    async def _fetch_json(self, url: str) -> Optional[dict | list]:
        """
        GET request with rate limiting, 429 backoff, and error handling.
        Returns parsed JSON or None on any failure.
        """
        await self.dex_limiter.acquire()
        session = await self._dex_session()
        try:
            async with session.get(url) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "10"))
                    log.warning(f"DexScreener 429 — backing off {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return None
                if resp.status != 200:
                    log.warning(f"DexScreener HTTP {resp.status} ← …{url[-80:]}")
                    return None
                return await resp.json(content_type=None)
        except asyncio.TimeoutError:
            log.warning(f"DexScreener timeout ← …{url[-80:]}")
        except aiohttp.ClientError as e:
            log.error(f"DexScreener client error: {e}")
        except Exception as e:
            log.error(f"DexScreener unexpected error: {e}")
        return None

    # ══════════════════════════════════════════════════════════
    # Parsing Helpers (static, no side effects)
    # ══════════════════════════════════════════════════════════

    # Tokens we never want to track — stables, infrastructure, etc.
    _SKIP_SYMBOLS: frozenset[str] = frozenset({
        "SOL", "USDC", "USDT", "WBTC", "WETH", "RAY", "SRM", "BONK",
        "WSOL", "MSOL", "JITOSOL", "BSOL", "JSOL",
    })

    @staticmethod
    def _parse_tick(pair: dict) -> Optional[Tick]:
        """Extract a Tick from a DexScreener pair object."""
        try:
            price = float(pair.get("priceUsd") or 0)
            if price <= 0:
                return None
            
            vol = pair.get("volume") or {}
            txns = (pair.get("txns") or {}).get("m5") or {}
            liq = pair.get("liquidity") or {}
            
            return Tick(
                timestamp=time.time(),
                price_usd=price,
                liquidity_usd=float(liq.get("usd") or 0),
                volume_5m=float(vol.get("m5") or 0),
                buys_5m=int(txns.get("buys") or 0),
                sells_5m=int(txns.get("sells") or 0),
                market_cap=float(pair.get("marketCap") or pair.get("fdv") or 0),
            )
        except (ValueError, TypeError):
            return None

    @classmethod
    def _parse_identity(cls, pair: dict) -> Optional[tuple[str, str, str, str, str]]:
        """
        Extract (mint, symbol, name, pair_address, dex_id) from a pair.
        Returns None if the pair should be skipped.
        """
        try:
            bt = pair.get("baseToken") or {}
            mint = (bt.get("address") or "").strip()
            symbol = (bt.get("symbol") or "???").strip()
            name = (bt.get("name") or "").strip()
            pa = (pair.get("pairAddress") or "").strip()
            dex = (pair.get("dexId") or "").strip()
            
            if not mint or not pa:
                return None
            
            if symbol.upper() in cls._SKIP_SYMBOLS:
                return None
            
            if pair.get("chainId") != "solana":
                return None
                
            return mint, symbol, name, pa, dex
        except Exception:
            return None

    @classmethod
    def _select_best_pair(cls, pairs: list[dict]) -> Optional[dict]:
        """
        Given multiple pairs for the same base token (e.g., Raydium, Orca, Meteora),
        return the pair with the highest liquidity.usd.
        This ensures we always price-track from the deepest pool.
        """
        best_pair: Optional[dict] = None
        best_liq: float = -1.0
        
        for p in pairs:
            if p.get("chainId") != "solana":
                continue
            try:
                liq = float((p.get("liquidity") or {}).get("usd") or 0)
            except (ValueError, TypeError):
                liq = 0.0
                
            if liq > best_liq:
                best_liq = liq
                best_pair = p
        return best_pair

    # ══════════════════════════════════════════════════════════
    # Discovery Phase (boosted / trending tokens)
    # ══════════════════════════════════════════════════════════

    async def _discover_tokens(self) -> list[dict]:
        """
        Fetch Solana pairs from DexScreener discovery endpoints.
        Returns raw pair dicts for parsing.
        Rate-limited and throttled to avoid excessive API calls:
        runs at most once every DISCOVERY_COOLDOWN_SECONDS.

        v4.1: Added latest pairs and token profiles endpoints.
        """
        now = time.time()
        if now - self._last_discovery_time < DISCOVERY_COOLDOWN_SECONDS:
            log.debug(f" Discovery cooldown ({DISCOVERY_COOLDOWN_SECONDS - (now - self._last_discovery_time):.0f}s remaining)")
            return []
        
        self._last_discovery_time = now
        base = Settings.DEXSCREENER_BASE
        solana_pairs: list[dict] = []
        seen_mints: set[str] = set()

        def _dedup_add(pairs: list[dict], source: str):
            """Add pairs, deduplicating by mint address."""
            for p in pairs:
                if p.get("chainId") != "solana":
                    continue
                bt = p.get("baseToken") or {}
                mint = (bt.get("address") or "").strip()
                if mint and mint not in seen_mints:
                    seen_mints.add(mint)
                    p["_discovery_source"] = source
                    solana_pairs.append(p)
        
        # ── Endpoint 1: Boosted tokens ───────────────────
        boosts = await self._fetch_json(f"{base}/token-boosts/top/v1")
        if isinstance(boosts, list):
            # Collect boosted Solana token addresses for bulk fetch
            boosted_addrs: list[str] = []
            for item in boosts:
                if item.get("chainId") == "solana" and item.get("tokenAddress"):
                    addr = item["tokenAddress"].strip()
                    if addr and addr not in boosted_addrs:
                        boosted_addrs.append(addr)
            
            # Bulk-fetch boosted tokens (up to 30 at a time)
            for chunk in _chunk_list(boosted_addrs, BULK_CHUNK_SIZE):
                joined = ",".join(chunk)
                url = f"{base}/latest/dex/tokens/{joined}"
                data = await self._fetch_json(url)
                if isinstance(data, dict):
                    _dedup_add(data.get("pairs") or [], "boosted")
        
        # ── Endpoint 2: Search fallback ──────────────────
        search = await self._fetch_json(f"{base}/latest/dex/search?q=SOL")
        if isinstance(search, dict):
            _dedup_add(search.get("pairs") or [], "search")

        # ── Endpoint 3: Latest pairs (v4.1) ──────────────
        if Settings.DISCOVERY_ENABLE_LATEST_PAIRS:
            latest = await self._fetch_json(f"{base}/latest/dex/pairs/solana")
            if isinstance(latest, dict):
                _dedup_add(latest.get("pairs") or [], "latest_pairs")
            elif isinstance(latest, list):
                _dedup_add(latest, "latest_pairs")

        # ── Endpoint 4: Token profiles (v4.1) ────────────
        if Settings.DISCOVERY_ENABLE_PROFILES:
            profiles = await self._fetch_json(f"{base}/token-profiles/latest/v1")
            if isinstance(profiles, list):
                profile_addrs: list[str] = []
                for item in profiles:
                    if item.get("chainId") == "solana" and item.get("tokenAddress"):
                        addr = item["tokenAddress"].strip()
                        if addr and addr not in profile_addrs and addr not in seen_mints:
                            profile_addrs.append(addr)
                # Bulk-fetch profile tokens
                for chunk in _chunk_list(profile_addrs, BULK_CHUNK_SIZE):
                    joined = ",".join(chunk)
                    url = f"{base}/latest/dex/tokens/{joined}"
                    data = await self._fetch_json(url)
                    if isinstance(data, dict):
                        _dedup_add(data.get("pairs") or [], "profiles")

        log.debug(
            f" Discovery sources: {len(solana_pairs)} unique pairs "
            f"(deduped to {len(seen_mints)} unique mints)"
        )
        return solana_pairs

    # ══════════════════════════════════════════════════════════
    # Bulk Refresh Phase (the core optimization)
    # ══════════════════════════════════════════════════════════

    async def _bulk_refresh_stale_tokens(self, seen_in_discovery: set[str]) -> int:
        """
        Identify stale tokens, chunk them into groups of 30, and fetch
        each chunk in a single bulk API request.
        For each token that returns multiple pairs (different DEXes),
        we select the pair with the highest liquidity.
        Returns the number of ticks successfully processed.
        """
        now = time.time()
        stale_cutoff = now - STALE_THRESHOLD_SECONDS
        prune_cutoff = now - PRUNE_THRESHOLD_SECONDS
        
        # ── Step 1: Identify stale tokens ────────────────
        stale_mints: list[str] = []
        prune_mints: list[str] = []
        
        for mint, buf in self.tokens.items():
            # Skip tokens that were already updated in discovery
            if mint in seen_in_discovery:
                continue
            
            if buf.last_updated < prune_cutoff:
                # Mark for pruning (no data for >5 minutes)
                prune_mints.append(mint)
            elif buf.last_updated < stale_cutoff:
                # Needs refresh
                stale_mints.append(mint)

        # ── Step 2: Prune dead tokens ────────────────────
        for mint in prune_mints:
            symbol = self.tokens[mint].symbol
            age = now - self.tokens[mint].last_updated
            log.info(f"🗑️ Pruning stale token: {symbol} ({mint[:8]}…) — no data for {age:.0f}s")
            del self.tokens[mint]

        if not stale_mints:
            log.debug(" No stale tokens to refresh")
            return 0
            
        log.debug(f" Refreshing {len(stale_mints)} stale tokens in {len(list(_chunk_list(stale_mints, BULK_CHUNK_SIZE)))} bulk request(s)")

        # ── Step 3: Chunk and bulk-fetch ─────────────────
        ticks_processed = 0
        base = Settings.DEXSCREENER_BASE
        
        for chunk in _chunk_list(stale_mints, BULK_CHUNK_SIZE):
            joined = ",".join(chunk)
            url = f"{base}/latest/dex/tokens/{joined}"
            data = await self._fetch_json(url)
            
            if not isinstance(data, dict):
                log.warning(f" Bulk fetch failed for chunk of {len(chunk)} tokens")
                continue
                
            all_pairs = data.get("pairs") or []
            if not all_pairs:
                log.debug(f" Bulk response returned 0 pairs for {len(chunk)} tokens")
                continue
            
            # ── Step 4: Group pairs by base token address ─
            pairs_by_mint: dict[str, list[dict]] = {}
            for pair in all_pairs:
                if pair.get("chainId") != "solana":
                    continue
                bt = pair.get("baseToken") or {}
                mint_addr = (bt.get("address") or "").strip()
                if mint_addr:
                    pairs_by_mint.setdefault(mint_addr, []).append(pair)
            
            # ── Step 5: Select best pair and update buffer ─
            for mint_addr in chunk:
                candidate_pairs = pairs_by_mint.get(mint_addr, [])
                if not candidate_pairs:
                    # Token returned no pairs — might be dead
                    continue
                    
                # Select the pair with the highest liquidity
                best_pair = self._select_best_pair(candidate_pairs)
                if best_pair is None:
                    continue
                
                tick = self._parse_tick(best_pair)
                if tick is None:
                    continue
                
                # Apply minimum filters
                if tick.liquidity_usd < Settings.MIN_LIQUIDITY:
                    continue
                if tick.volume_5m < Settings.MIN_VOLUME_5M:
                    continue
                
                if mint_addr in self.tokens:
                    buf = self.tokens[mint_addr]
                    
                    # Update pair_address if a higher-liquidity pool
                    # emerged since we first discovered this token
                    new_pa = (best_pair.get("pairAddress") or "").strip()
                    if new_pa and new_pa != buf.pair_address:
                        old_pa = buf.pair_address[:12]
                        log.debug(f" 🔄 {buf.symbol}: pair updated {old_pa}… → {new_pa[:12]}… (higher liquidity)")
                        buf.pair_address = new_pa
                        buf.dex_id = best_pair.get("dexId") or buf.dex_id
                        
                    buf.append(tick, Settings.ROLLING_WINDOW)
                    ticks_processed += 1
            
            log.debug(f" Chunk of {len(chunk)}: {len(pairs_by_mint)} tokens responded, {ticks_processed} ticks ingested")
            
        return ticks_processed

    # ══════════════════════════════════════════════════════════
    # Main Poll Cycle (the public entry point)
    # ══════════════════════════════════════════════════════════

    async def poll(self) -> int:
        """
        Execute one full polling cycle:
        1. Discovery — find new tokens from DexScreener trending/boosted
        2. Bulk Refresh — update all stale tracked tokens in chunked bulk requests
        3. Prune — remove tokens with no data for >5 minutes
        Returns the total number of ticks processed this cycle.
        """
        cycle_start = time.time()
        self._poll_stats = {
            "discovery_pairs": 0, "new_tokens": 0,
            "discovery_ticks": 0, "bulk_ticks": 0, "pruned": 0
        }
        
        log.debug("📡 Polling DexScreener…")
        
        # ═══════════════════════════════════════════════════
        # Phase 1: Discovery
        # ═══════════════════════════════════════════════════
        raw_pairs = await self._discover_tokens()
        self._poll_stats["discovery_pairs"] = len(raw_pairs)
        log.debug(f" Discovery returned {len(raw_pairs)} raw Solana pairs")
        
        seen_in_discovery: set[str] = set()
        
        # Group discovery pairs by mint to select best liquidity
        discovery_by_mint: dict[str, list[dict]] = {}
        for pair in raw_pairs:
            ident = self._parse_identity(pair)
            if not ident: continue
            mint = ident[0]
            discovery_by_mint.setdefault(mint, []).append(pair)
            
        # Process each discovered token (best-pair selection)
        for mint, pairs in discovery_by_mint.items():
            best_pair = self._select_best_pair(pairs)
            if best_pair is None: continue
            
            ident = self._parse_identity(best_pair)
            if not ident: continue
            mint, symbol, name, pa, dex = ident
            
            tick = self._parse_tick(best_pair)
            if tick is None: continue
            
            # Pre-filters
            if tick.liquidity_usd < Settings.MIN_LIQUIDITY: continue
            if tick.volume_5m < Settings.MIN_VOLUME_5M: continue
            
            seen_in_discovery.add(mint)
            
            if mint not in self.tokens:
                buf = TokenBuffer(
                    mint=mint, symbol=symbol, name=name,
                    pair_address=pa, dex_id=dex,
                )
                buf.append(tick, Settings.ROLLING_WINDOW)
                self.tokens[mint] = buf
                self._discovery_total += 1
                self._poll_stats["new_tokens"] += 1
                log.info(
                    f"🆕 Discovered: {symbol} ({mint[:8]}…) │ "
                    f"${tick.price_usd:.10f} │ Liq ${tick.liquidity_usd:,.0f} │ "
                    f"Vol5m ${tick.volume_5m:,.0f} │ DEX: {dex}"
                )
            else:
                self.tokens[mint].append(tick, Settings.ROLLING_WINDOW)
                self._poll_stats["discovery_ticks"] += 1

        # ═══════════════════════════════════════════════════
        # Phase 2: Bulk Refresh of Stale Tokens
        # ═══════════════════════════════════════════════════
        bulk_ticks = await self._bulk_refresh_stale_tokens(seen_in_discovery)
        self._poll_stats["bulk_ticks"] = bulk_ticks

        # ═══════════════════════════════════════════════════
        # Phase 3: Summary
        # ═══════════════════════════════════════════════════
        total_ticks = self._poll_stats["discovery_ticks"] + self._poll_stats["bulk_ticks"]
        cycle_ms = (time.time() - cycle_start) * 1000
        
        log.info(
            f"📊 Poll complete in {cycle_ms:.0f}ms │ {len(self.tokens)} tracked │ "
            f"{total_ticks} ticks │ {self._poll_stats['new_tokens']} new │ "
            f"discovery={self._poll_stats['discovery_ticks']} bulk={self._poll_stats['bulk_ticks']} │ "
            f"Lifetime: {self._discovery_total}"
        )
        
        return total_ticks

    # ══════════════════════════════════════════════════════════
    # Tiered Polling (v3.1 — replaces flat poll() loop)
    # ══════════════════════════════════════════════════════════

    async def poll_tiered(self, open_position_mints: Optional[Set[str]] = None) -> int:
        """
        Execute one tiered polling task (called every ~1 second).
        Picks the most-overdue tier and issues a single batch request.
        Returns the number of ticks processed (0 if nothing was due).

        Args:
            open_position_mints: Set of mints with open trades, used for
                                 tier promotion/demotion after the poll.
        """
        if open_position_mints is None:
            open_position_mints = set()

        task = self.poller.get_next_poll_task()
        if task is None:
            return 0

        tier, mints = task

        if tier == TokenTier.DISCOVERY:
            added = await self._poll_discovery_tiered()
            self.poller.last_discovery_time = time.time()
            self.poller.record_request()
            self._sync_tiered_to_legacy()
            self.poller.update_token_tiers(
                open_position_mints,
                hot_min_volume=Settings.HOT_MIN_VOLUME,
                hot_min_liquidity=Settings.HOT_MIN_LIQUIDITY,
                hot_min_mcap=Settings.HOT_MIN_MCAP,
                hot_max_mcap=Settings.HOT_MAX_MCAP,
                hot_min_activity=Settings.HOT_MIN_ACTIVITY,
                dead_zero_vol_polls=Settings.DEAD_TOKEN_ZERO_VOL_POLLS,
                dead_min_liquidity=Settings.DEAD_TOKEN_MIN_LIQUIDITY,
                stale_max_age_hours=Settings.STALE_TOKEN_MAX_AGE_HOURS,
            )
            log.debug(
                f"Poll [DISCOVERY] │ {added} new tokens │ "
                f"Budget: {self.poller._requests_in_window()}/"
                f"{self.poller._rate_limit_max} │ "
                f"Tiers: {self.poller.tier_counts()}"
            )
            return added

        # Data-fetch tier (1, 2, or 3): batch request for specific tokens
        if not mints:
            return 0

        # Map mints to pair addresses for DexScreener batch call
        pair_addresses = []
        mint_to_pair: dict[str, str] = {}
        for mint in mints:
            token = self.poller.tokens.get(mint)
            if token and token.pair_address:
                pair_addresses.append(token.pair_address)
                mint_to_pair[token.pair_address] = mint

        if not pair_addresses:
            # Update poll timestamps even with no data to avoid tight-loops
            now = time.time()
            for mint in mints:
                if mint in self.poller.tokens:
                    self.poller.tokens[mint].last_poll_time = now
            return 0

        base = Settings.DEXSCREENER_BASE
        joined = ",".join(pair_addresses[:BULK_CHUNK_SIZE])
        url = f"{base}/latest/dex/pairs/solana/{joined}"
        data = await self._fetch_json(url)
        self.poller.record_request()

        now = time.time()
        ticks_processed = 0

        if isinstance(data, dict):
            all_pairs = data.get("pairs") or []
            for pair in all_pairs:
                pa = (pair.get("pairAddress") or "").strip()
                mint = mint_to_pair.get(pa)
                if not mint:
                    # Try by base token address as fallback
                    bt = pair.get("baseToken") or {}
                    mint = (bt.get("address") or "").strip()

                if not mint or mint not in self.poller.tokens:
                    continue

                token = self.poller.tokens[mint]
                token.last_poll_time = now

                tick = self._parse_tick(pair)
                if tick is None:
                    continue

                # Update TrackedToken state
                token.last_price = tick.price_usd
                token.last_liquidity = tick.liquidity_usd
                token.last_volume_5m = tick.volume_5m
                token.last_mcap = tick.market_cap
                token.last_buys_5m = tick.buys_5m
                token.last_sells_5m = tick.sells_5m
                token.last_tick_time = now

                if tick.volume_5m < 1:
                    token.consecutive_zero_vol += 1
                else:
                    token.consecutive_zero_vol = 0

                # Keep the legacy TokenBuffer in sync
                self._update_legacy_buffer(mint, token, pair, tick)
                ticks_processed += 1

        # Stamp all polled mints regardless of whether data was returned
        for mint in mints:
            if mint in self.poller.tokens:
                self.poller.tokens[mint].last_poll_time = now

        # Reclassify tiers after every poll
        removed = self.poller.update_token_tiers(
            open_position_mints,
            hot_min_volume=Settings.HOT_MIN_VOLUME,
            hot_min_liquidity=Settings.HOT_MIN_LIQUIDITY,
            hot_min_mcap=Settings.HOT_MIN_MCAP,
            hot_max_mcap=Settings.HOT_MAX_MCAP,
            hot_min_activity=Settings.HOT_MIN_ACTIVITY,
            dead_zero_vol_polls=Settings.DEAD_TOKEN_ZERO_VOL_POLLS,
            dead_min_liquidity=Settings.DEAD_TOKEN_MIN_LIQUIDITY,
            stale_max_age_hours=Settings.STALE_TOKEN_MAX_AGE_HOURS,
        )
        for mint in removed:
            self.tokens.pop(mint, None)

        log.debug(
            f"Poll [{tier.name}] │ {len(mints)} tokens │ "
            f"{ticks_processed} ticks │ "
            f"Budget: {self.poller._requests_in_window()}/"
            f"{self.poller._rate_limit_max} │ "
            f"Tiers: {self.poller.tier_counts()}"
        )
        return ticks_processed

    async def _poll_discovery_tiered(self) -> int:
        """
        Fetch newly listed/boosted tokens from DexScreener.
        Add qualifying ones to Tier 3 (Warm Scanner).
        Returns the number of new tokens added.
        """
        raw_pairs = await self._discover_tokens()
        if not raw_pairs:
            return 0

        added = 0
        rejected_reasons: dict[str, int] = {}
        t3_cfg = self.poller._tier_configs[TokenTier.WARM_SCANNER]

        for pair in raw_pairs:
            # Check chain before parsing identity (wrong_chain is a common drop)
            chain_id = pair.get("chainId", "")
            if chain_id != "solana":
                rejected_reasons["wrong_chain"] = rejected_reasons.get("wrong_chain", 0) + 1
                continue

            # Check for skipped symbols (SOL, USDC, etc.) before identity parsing
            bt = pair.get("baseToken") or {}
            symbol_raw = (bt.get("symbol") or "").strip().upper()
            if symbol_raw in self._SKIP_SYMBOLS:
                rejected_reasons["skipped_symbol"] = rejected_reasons.get("skipped_symbol", 0) + 1
                continue

            ident = self._parse_identity(pair)
            if not ident:
                # Distinguish no mint vs no pair address
                bt = pair.get("baseToken") or {}
                mint_raw = (bt.get("address") or "").strip()
                pa_raw = (pair.get("pairAddress") or "").strip()
                if not mint_raw:
                    rejected_reasons["no_mint_address"] = rejected_reasons.get("no_mint_address", 0) + 1
                elif not pa_raw:
                    rejected_reasons["no_pair_address"] = rejected_reasons.get("no_pair_address", 0) + 1
                else:
                    rejected_reasons["parse_failed"] = rejected_reasons.get("parse_failed", 0) + 1
                    log.debug(f"Discovery parse failed for pair: {pair}")
                continue
            mint, symbol, name, pa, dex = ident

            # Skip already tracked
            if mint in self.poller.tokens:
                rejected_reasons["already_tracked"] = rejected_reasons.get("already_tracked", 0) + 1
                continue

            liquidity = float((pair.get("liquidity") or {}).get("usd") or 0)
            mcap = float(pair.get("marketCap") or pair.get("fdv") or 0)
            volume_5m = float((pair.get("volume") or {}).get("m5") or 0)
            price = float(pair.get("priceUsd") or 0)

            if price <= 0:
                rejected_reasons["zero_price"] = rejected_reasons.get("zero_price", 0) + 1
                continue
            if liquidity < Settings.DISCOVERY_MIN_LIQUIDITY:
                rejected_reasons["low_liquidity"] = rejected_reasons.get("low_liquidity", 0) + 1
                continue
            if mcap < Settings.DISCOVERY_MIN_MCAP:
                rejected_reasons["low_mcap"] = rejected_reasons.get("low_mcap", 0) + 1
                continue
            if mcap > Settings.DISCOVERY_MAX_MCAP:
                rejected_reasons["high_mcap"] = rejected_reasons.get("high_mcap", 0) + 1
                continue

            # Check Tier 3 capacity
            t3_count = sum(
                1 for t in self.poller.tokens.values()
                if t.tier == TokenTier.WARM_SCANNER
            )
            if t3_count >= t3_cfg.max_tokens:
                rejected_reasons["scanner_full"] = rejected_reasons.get("scanner_full", 0) + 1
                break

            buys_5m = int((pair.get("txns") or {}).get("m5", {}).get("buys") or 0)
            sells_5m = int((pair.get("txns") or {}).get("m5", {}).get("sells") or 0)

            tracked = TrackedToken(
                mint=mint,
                symbol=symbol,
                pair_address=pa,
                tier=TokenTier.WARM_SCANNER,
                last_poll_time=0.0,
                last_price=price,
                last_liquidity=liquidity,
                last_volume_5m=volume_5m,
                last_mcap=mcap,
                last_buys_5m=buys_5m,
                last_sells_5m=sells_5m,
            )
            self.poller.tokens[mint] = tracked

            # Also seed the legacy TokenBuffer so analyzable_tokens() works
            if mint not in self.tokens:
                buf = TokenBuffer(
                    mint=mint, symbol=symbol, name=name,
                    pair_address=pa, dex_id=dex,
                )
                tick = self._parse_tick(pair)
                if tick:
                    buf.append(tick, Settings.ROLLING_WINDOW)
                self.tokens[mint] = buf
                self._discovery_total += 1

            added += 1
            log.info(
                f"🆕 Discovered: {symbol} ({mint[:8]}…) │ "
                f"${price:.10f} │ Liq ${liquidity:,.0f} │ "
                f"Vol5m ${volume_5m:,.0f} │ DEX: {dex}"
            )

        # Always log discovery results with full accounting
        total_accounted = added + sum(rejected_reasons.values())
        log.info(
            f"🔍 Discovery: {len(raw_pairs)} pairs → "
            f"{added} added │ rejected: {rejected_reasons} │ "
            f"accounted: {total_accounted}/{len(raw_pairs)}"
        )
        return added

    def _update_legacy_buffer(
        self,
        mint: str,
        token: TrackedToken,
        pair: dict,
        tick: "Tick",
    ):
        """Keep the legacy TokenBuffer in sync with the TrackedToken."""
        if mint not in self.tokens:
            ident = self._parse_identity(pair)
            if ident:
                _, symbol, name, pa, dex = ident
                self.tokens[mint] = TokenBuffer(
                    mint=mint, symbol=symbol, name=name,
                    pair_address=pa, dex_id=dex,
                )
        if mint in self.tokens:
            buf = self.tokens[mint]
            # Update pair_address if a higher-liquidity pool emerged
            new_pa = (pair.get("pairAddress") or "").strip()
            if new_pa and new_pa != buf.pair_address:
                buf.pair_address = new_pa
                buf.dex_id = pair.get("dexId") or buf.dex_id
            buf.append(tick, Settings.ROLLING_WINDOW)

    def _sync_tiered_to_legacy(self):
        """Remove legacy buffers for tokens no longer tracked by the tiered poller."""
        poller_mints = set(self.poller.tokens.keys())
        stale = [m for m in list(self.tokens.keys()) if m not in poller_mints]
        for mint in stale:
            del self.tokens[mint]

    # ══════════════════════════════════════════════════════════
    # Accessors (unchanged interface for PaperTradingEngine)
    # ══════════════════════════════════════════════════════════

    def analyzable_tokens(self) -> list[TokenBuffer]:
        """Tokens with enough snapshots for Hurst analysis."""
        return [
            t for t in self.tokens.values()
            if t.count >= Settings.MIN_SNAPSHOTS_HURST
            and t.latest_liquidity >= Settings.MIN_LIQUIDITY
        ]

    def get(self, mint: str) -> Optional[TokenBuffer]:
        return self.tokens.get(mint)

    # ══════════════════════════════════════════════════════════
    # Solana RPC: Top Holders for Gini (unchanged)
    # ══════════════════════════════════════════════════════════

    async def fetch_top_holder_balances(self, mint: str) -> Optional[np.ndarray]:
        """Call getTokenLargestAccounts on Solana RPC. Returns raw balances."""
        if mint in self._gini_cache:
            _, cached_ts = self._gini_cache[mint]
            if time.time() - cached_ts < self._gini_cache_ttl:
                return None

        # Short-circuit if Helius RPC is returning 401s
        self._rpc_eval_count += 1
        if self._rpc_consecutive_401s >= self._rpc_401_threshold:
            # Periodically retry to detect if the key starts working again
            if self._rpc_eval_count % self._rpc_retry_interval != 0:
                if not self._rpc_disabled_logged:
                    log.warning(
                        f"Helius RPC disabled — last {self._rpc_consecutive_401s} "
                        f"queries returned 401. Retrying every "
                        f"{self._rpc_retry_interval} evaluations."
                    )
                    self._rpc_disabled_logged = True
                return None

        await self.rpc_limiter.acquire()
        session = await self._solana_session()
        payload = orjson.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint],
        })
        try:
            async with session.post(
                Settings.SOLANA_RPC_URL, data=payload, headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status == 401:
                    self._rpc_consecutive_401s += 1
                    if self._rpc_consecutive_401s <= self._rpc_401_threshold:
                        log.warning(f"RPC HTTP 401 for holders of {mint[:8]}… (consecutive: {self._rpc_consecutive_401s})")
                    return None
                if resp.status != 200:
                    log.warning(f"RPC HTTP {resp.status} for holders of {mint[:8]}…")
                    return None
                # Successful non-401 response — reset the counter
                self._rpc_consecutive_401s = 0
                self._rpc_disabled_logged = False
                data = await resp.json(content_type=None)
                if "error" in data:
                    log.error(f"RPC error: {data['error']}")
                    return None
                accounts = (data.get("result") or {}).get("value") or []
                if not accounts: return None
                balances: list[float] = []
                for acc in accounts[:Settings.TOP_HOLDERS]:
                    try:
                        amt = float(acc.get("amount") or "0")
                        if amt > 0: balances.append(amt)
                    except (ValueError, TypeError): continue
                if len(balances) < 2: return None
                return np.array(balances, dtype=np.float64)
        except asyncio.TimeoutError:
            log.warning(f"RPC timeout fetching holders for {mint[:8]}…")
        except aiohttp.ClientError as e:
            log.error(f"RPC client error: {e}")
        except Exception as e:
            log.error(f"RPC unexpected error: {e}")
        return None

    def cache_gini(self, mint: str, gini: float):
        self._gini_cache[mint] = (gini, time.time())

    def get_cached_gini(self, mint: str) -> Optional[float]:
        if mint in self._gini_cache:
            g, ts = self._gini_cache[mint]
            if time.time() - ts < self._gini_cache_ttl: return g
        return None

# ══════════════════════════════════════════════════════════════
# Module-level utility
# ══════════════════════════════════════════════════════════════

def _chunk_list(lst: list, size: int):
    """Yield successive chunks of `size` from `lst`."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]
