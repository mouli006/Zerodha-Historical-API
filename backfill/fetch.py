import time
from datetime import datetime, timedelta

import requests
from kiteconnect import exceptions as kex

from .job import STOP, job_incr, job_update

# Max days per request allowed by Kite for each interval
INTERVALS = {"minute": 60, "day": 2000}
MAX_RANGE_DAYS = 365          # when equity stocks are selected
MAX_INDEX_RANGE_DAYS = 7305   # 20 years, indices only (a few requests per index)
MAX_TRIES = 10


class StopJob(Exception):
    pass


class AuthStop(Exception):
    pass


class ChunkFailed(Exception):
    pass


class RateLimiter:
    """Spaces request starts at least 1/rate seconds apart; halves the rate on a
    429 and doubles it back (up to the configured rate) after 20 clean requests."""

    def __init__(self, rate):
        self.max_rate = self.rate = rate
        self.last = None
        self.ok = 0

    def wait(self):
        if self.last is not None:
            delay = self.last + 1 / self.rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        self.last = time.monotonic()

    def on_429(self):
        self.rate = max(0.5, self.rate / 2)
        self.ok = 0

    def on_success(self):
        self.ok += 1
        if self.ok >= 20 and self.rate < self.max_rate:
            self.rate = min(self.max_rate, self.rate * 2)
            self.ok = 0


def make_chunks(start, end, interval):
    """Split [start 00:00, end 23:59:59] into windows of Kite's max size."""
    chunks, cur = [], datetime.combine(start, datetime.min.time())
    last = datetime.combine(end, datetime.max.time()).replace(microsecond=0)
    step = timedelta(days=INTERVALS[interval])
    while cur <= last:
        chunk_end = min(cur + step - timedelta(seconds=1), last)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(seconds=1)
    return chunks


def fetch_chunk(kite, limiter, token, start, end, interval):
    """One stock + one chunk, with retries. Raises AuthStop, StopJob or ChunkFailed."""
    for attempt in range(1, MAX_TRIES + 1):
        if STOP.is_set():
            raise StopJob()
        limiter.wait()
        job_incr("requests")
        try:
            candles = kite.historical_data(token, start, end, interval)
            limiter.on_success()
            job_update(rate=limiter.rate)
            return candles
        except kex.TokenException as e:
            raise AuthStop(f"Session expired or invalid ({e}). Sign in again and click Resume download.")
        except kex.KiteException as e:
            if e.code == 403:
                raise AuthStop(f"Access denied ({e}). Sign in again and click Resume download.")
            if e.code == 429 or "too many" in str(e).lower():
                job_incr("n429")
                limiter.on_429()
                job_update(rate=limiter.rate)
            elif isinstance(e, kex.NetworkException):
                job_incr("net_errors")
            else:
                raise ChunkFailed(f"{type(e).__name__}: {e}")
            err = e
        except requests.exceptions.RequestException as e:
            job_incr("net_errors")
            err = e
        if attempt < MAX_TRIES and STOP.wait(min(2 ** attempt, 60)):
            raise StopJob()
    raise ChunkFailed(f"gave up after {MAX_TRIES} tries: {type(err).__name__}: {err}")
