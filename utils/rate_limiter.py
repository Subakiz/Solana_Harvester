"""
Async token-bucket rate limiter to prevent IP bans.
Enforces a maximum number of requests per minute per service.
"""
import asyncio
import time
from utils.logger import get_logger

log = get_logger("RateLimiter")

class AsyncRateLimiter:
    """
    Token-bucket rate limiter.
    Guarantees that no more than `max_calls` happen within a sliding window of `period` seconds.
    Callers that exceed the limit are transparently delayed (not rejected).
    """
    def __init__(self, name: str, max_calls: int, period: float = 60.0):
        self.name = name
        self.max_calls = max_calls
        self.period = period
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()
        self._total_waits = 0
        self._total_calls = 0

    async def acquire(self):
        """Wait until a request slot is available, then consume it."""
        async with self._lock:
            now = time.monotonic()
            
            # Purge timestamps older than the window
            cutoff = now - self.period
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            
            if len(self._timestamps) >= self.max_calls:
                # Must wait until the oldest timestamp expires
                sleep_until = self._timestamps[0] + self.period
                wait_time = sleep_until - now
                if wait_time > 0:
                    self._total_waits += 1
                    log.debug(
                        f"[{self.name}] Rate limit reached — "
                        f"sleeping {wait_time:.2f}s "
                        f"(waits={self._total_waits})"
                    )
                    await asyncio.sleep(wait_time)
            
            self._timestamps.append(time.monotonic())
            self._total_calls += 1

    @property
    def stats(self) -> str:
        return (
            f"{self.name}: {self._total_calls} calls, "
            f"{self._total_waits} throttled waits"
        )
