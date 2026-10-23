"""Kite 1-minute history, phase 2: download every stock of phase 1's plan.csv from its own minute_first_day, check
every day against Kite's day candle of the same run, store by security_id (backfill/equity/minute_store.py), and a
daily update. docs/dev/equity_minute_download.md has the rules; the Part 0 findings they rest on are in
PART0_FINDINGS.

Inputs, only read: phase 1's plan folder (plan.csv, kite_tokens.csv; plan.csv is rebuilt offline with phase 1's
gaps step when it is older than the master / adjustments), the bhavcopy, the security master, the factors, the exit
list and the index membership (through minute_plan.Inputs). Network: Kite only, in download() and update().
Output under <folder>/equity_1min/ (every file atomic):
    download_progress.sqlite                one row per chunk, day fetch, stock and run (resume)
    staging/<security_id>/                  finished chunks of a stock not built yet (removed after the build)
    data/<security_id>/<year>.parquet       Kite's 1-minute candles unchanged
    data/<security_id>/days.parquet         per day: Kite day candle (same run), bhavcopy, conversion, check
    check/minute_check.csv / minute_empty_days.csv / scale_steps.csv / summary.json
    bhav_cache/<year>.parquet, cache_info.json   the bhavcopy rows of the plan's universe by security_id, read one
                                            stock at a time (derived from the store; rebuilt per changed year file)
    update/kite_instruments_NSE_<date>.csv, update/tokens.csv, update/new_listings.csv   (update / retoken)
    update/retoken_<date>.csv              the table of the last retoken --apply of that day
    check/no_kite_data.csv                  stocks Kite no longer lists (NO_KITE_DATA): for the vendor list
    archive/<security_id>_<date>/           a stock's old files after its Kite token changed (never deleted)
<folder>/equity_1min/.lock: the same lock as phase 1 (one tokens / probe / gaps / download / update at a time).
Memory: the whole bhavcopy is never loaded (mp.Inputs(bhav=False); rows come per stock from bhav_cache), a build
holds one staged chunk plus one year of one stock, and the check files are rebuilt stock by stock."""
import gc
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from bisect import bisect_left, bisect_right
from datetime import date, datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from kiteconnect import exceptions as kex

import config
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.fetch import AuthStop, ChunkFailed, RateLimiter, StopJob, fetch_chunk, make_chunks
from backfill.job import STOP
from backfill.locks import RunLock

SPEED = mp.SPEED                 # requests per second, shared by all workers
MAX_SPEED = mp.MAX_SPEED
WORKERS = 2                      # Part 0: a minute request takes ~0.8 s, so one worker reaches only ~1.3 req/s
MAX_WORKERS = 4                  # priority 1 measured ~1.5 s per minute request: 2 workers reach only ~1.1 req/s
MINUTE_SECONDS = 0.79            # Part 0 median time of a minute request (used until 200 requests are timed)
DAY_SECONDS = 0.12               # ... of a day request
TIMED_MIN = 200
BYTES_PER_DAY = {1: 6600, 2: 5000, 3: 3300}   # parquet bytes per Kite day: RELIANCE 373 rows x 17.7, ENERGYDEV
#                                               267 x 12.5 (Part 0); priority 2 in between (assumption)
MEASURED_MIN = 3                 # DONE stocks of a priority before their measured bytes per day replace the constant
DISK_MARGIN = 1.2                # a real run needs free space >= estimate x 1.2
SCALE_AGREE = 0.001              # minute_scale is measured where day/minute high and low agree within 0.1% (+ ticks)
SCALE_WINDOW = 11                # centred median of 11 measured days: steps exactly at a change
SCALE_STEP = 0.001               # a new minute_scale level: the median moves by more than 0.1% ...
STEP_MIN_DAYS = 5                # ... (or volume: 20%) and the new level lasts 5 measured days (shorter: a blip)
SCALE_ONE = 0.0005               # a level within 0.05% of 1 is 1.0
VOLUME_STEP = 0.2                # minute_volume_scale changes only where day / minute volume moves by more than 20%
VOLUME_HIGH = 1.01               # VOLUME_DIFF: scaled minute volume above day x 1.01 + 1 ...
VOLUME_LOW = 0.5                 # ... or below day x 0.5 (pre-open and closing-session trades are not in minutes)
ACTION_DAYS = 5                  # a scale step with a corporate action within 5 trading days is explained
EMPTY_SHARE = 0.5                # a date more than half of 10+ downloaded stocks with a day candle have no minutes
EMPTY_MIN = 10                   # ... for is one market-wide minute_empty_days row
PROGRESS_EVERY = 10
AUCTION_FROM, AUCTION_TO = "15:00", "15:29"   # CLOSING_AUCTION: the last minute bar in [15:00, 15:29) (F&O stocks end
#                                               at 15:14 from 2026-08-03; the 2021-02-24 outage ended 11:43)
CHECKS = ["EXTRA_SESSION", "DAY_EMPTY", "MINUTE_EMPTY", "HIGHLOW_DIFF", "CLOSE_DIFF", "CLOSING_AUCTION", "VOLUME_DIFF",
          "SCALE_STEP", "CANDLE_UNCONFIRMED"]
FINISHED_AT = dtime(1, 0)        # a day's Kite day candle is final from 01:00 IST the next day (22-23 Oct 2026: fetched
#                                  on its own day it is provisional, around midnight Kite returned none)
DEFER_DAYS = 3                   # an unfinished day up to 3 trading days before the target is deferred, older: stored
LIST_LINES = 20                  # the run summary lists at most 20 deferred / unconfirmed days
FIELDS = mp.FIELDS

INPUT_COLUMNS = ["date"] + [f"kite_{f}" for f in FIELDS] + \
    ["minute_rows", "first_bar", "last_bar", "minute_open", "minute_high", "minute_low", "minute_close",
     "minute_volume", "fetched_at", "run_id"]
DAY_COLUMNS = ["date", "symbol", "series"] + [f"kite_{f}" for f in FIELDS] + [f"bhav_{f}" for f in FIELDS] + \
    ["our_factor", "ratio", "kite_multiplier", "kite_difference", "kind", "vol_ratio", "minute_scale",
     "minute_scale_measured", "minute_volume_scale", "minute_rows", "first_bar", "last_bar", "minute_open",
     "minute_high", "minute_low", "minute_close", "minute_volume", "check", "conversion_from", "note", "fetched_at",
     "run_id"]
CHECK_COLUMNS = ["security_id", "symbol", "priority", "date", "check", "kite_high", "kite_low", "kite_close",
                 "kite_volume", "minute_high", "minute_low", "minute_close", "minute_volume", "minute_scale",
                 "minute_volume_scale", "bhav_close", "note"]
EMPTY_COLUMNS = ["security_id", "symbol", "from_date", "to_date", "days", "note"]
STEP_COLUMNS = ["security_id", "symbol", "kind", "date", "from_value", "to_value", "near_action", "flagged"]
NEW_LISTING_COLUMNS = ["security_id", "symbol", "instrument_token", "minute_first_day", "added"]
NO_KITE = "NO_KITE_DATA"         # a plan stock Kite no longer lists: download skips it, update does not wait for it
NO_KITE_COLUMNS = ["security_id", "symbol", "isin", "last_bhav_day", "reason"]
RETOKEN_COLUMNS = ["security_id", "symbol", "old_token", "new_token", "kite_tradingsymbol", "result", "price_check",
                   "note", "action"]
PROBE_RECOVERED = "PROBE_RECOVERED"   # retoken: the probe found no day candles, a new probe does (price checked)
PLAN_TARGETS = ["plan.csv", "kite_vs_bhav.parquet", "kite_adjustment.csv", "eod_mismatch.csv", "probe_progress.sqlite"]
PRICE_DAYS = 10                  # the price check: Kite's day candles over the 10 days to the last bhavcopy day
FAILED_LINES = 30                # status lists at most 30 unresolved failures

PART0_FINDINGS = """Part 0 findings (Kite, 2026-10-19; see docs/dev/equity_minute_download.md):
- A minute request takes ~0.79 s, a day request ~0.12 s: one worker reaches ~1.3 req/s, two reach the 2.5 limit.
- 373 rows per Kite day (RELIANCE), 267 (ENERGYDEV); zstd parquet 17.7 / 12.5 bytes per row.
- The last minute close is not the day close (NSE's close is the average of the last 30 minutes); summed minute
  volume is 0.5-1.5% below the day volume (pre-open / closing session), up to 15-27% on some days.
- Kite's minute and day histories are not always on one scale: RELIANCE day / minute = 1.00945 until 2020-05-13.
- Kite has minutes without a day candle (Muhurat 2017-10-19, 2018-11-07; ENERGYDEV 2018-12) and day candles
  without minutes (2016-01-01). F&O stocks end at 15:14 from August 2026 (closing auction: CLOSING_AUCTION)."""


class NotEnoughDisk(mp.MinutePlanError):
    pass


class HistoryNotDone(mp.MinutePlanError):
    pass


class NoBhavToday(mp.MinutePlanError):
    pass


class CacheStale(mp.MinutePlanError):
    pass


class ApplyFailed(mp.MinutePlanError):
    pass


def now_ist():
    return datetime.now(mp.IST)


def as_ist(now=None):
    """now (default: the clock) in IST; a naive datetime is taken as IST."""
    now = now or now_ist()
    return now.replace(tzinfo=mp.IST) if now.tzinfo is None else now.astimezone(mp.IST)


def finished_at(d):
    """When the day d counts as finished: (d + 1) 01:00 IST."""
    return datetime.combine(d + timedelta(days=1), FINISHED_AT, tzinfo=mp.IST)


def bhav_holidays(folder):
    """The weekdays cm_bhav.py found to be NSE holidays (a real 404; NSE_CM_bhav/days.csv), or an empty set."""
    path = os.path.join(mp.store_dir(folder), "days.csv")
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except (OSError, ValueError):
        return set()
    if "date" not in df or "status" not in df:
        return set()
    return {sm.as_date(d) for d, s in zip(df["date"], df["status"]) if s == "holiday"}


def target_day(days, now=None, holidays=()):
    """(T, why, in_store): the latest trading day T with now >= (T + 1) 01:00 IST. Trading days are the bhavcopy
    dates (days); after the newest of them a weekday counts too unless cm_bhav found it a holiday (its bhavcopy is not
    in the store yet: in_store False). A weekend session counts only once its bhavcopy is in the store."""
    if not days:
        raise mp.NotBuilt("No bhavcopy days: run python equity_update.py first.")
    now = as_ist(now)
    limit = (now - timedelta(hours=1)).date() - timedelta(days=1)
    newest = days[-1]
    when = f"now {now:%Y-%m-%d %H:%M} IST; a day is finished from 01:00 IST the next day"
    d = limit
    while d > newest:
        if d.weekday() < 5 and d not in holidays:
            return d, (f"latest trading day finished, {when}; no bhavcopy for it yet (newest {newest}): update would "
                       "refuse, run python equity_update.py first"), False
        d -= timedelta(days=1)
    i = bisect_right(days, d)
    if not i:
        raise mp.NotBuilt(f"No bhavcopy day on or before {d}.")
    return days[i - 1], f"latest trading day finished, {when}; bhavcopy in the store", True


def unfinished(inputs, bhav):
    """{date: reason} of the days (INPUT_COLUMNS rows) whose Kite day candle does not look final: minutes and a
    bhavcopy row but no day candle, or a candle volume equal to the summed minute volume while not equal to the
    bhavcopy volume (Kite's provisional candle of the same day). Days without a bhavcopy row are never unfinished."""
    bv = {} if bhav is None else dict(zip(bhav["date"], bhav["volume"]))
    out = {}
    for d, rows, kc, kv, mv in zip(inputs["date"], inputs["minute_rows"], inputs["kite_close"], inputs["kite_volume"],
                                   inputs["minute_volume"]):
        b = bv.get(d)
        if b is None or pd.isna(b):
            continue
        if pd.isna(kc):
            if rows > 0:
                out[d] = "no day candle"
        elif rows > 0 and kv == mv and kv != b:
            out[d] = f"candle volume {kv:,.0f} = summed minute volume, bhavcopy {b:,.0f}"
    return out


def stamp():
    return f"{now_ist():%Y-%m-%d %H:%M:%S}"


def iso(d):
    return d.isoformat() if d else ""


def out_dir(folder):
    return mp.out_dir(folder)


def staging_dir(folder, sid=None):
    p = os.path.join(out_dir(folder), "staging")
    return os.path.join(p, sid) if sid else p


def check_dir(folder):
    return os.path.join(out_dir(folder), "check")


def update_dir(folder):
    return os.path.join(out_dir(folder), "update")


def quiet(*a, **k):
    pass


def memory():
    """(current, peak) resident memory of this process in bytes: the working set on Windows (what psutil calls rss
    and peak_wset), ru_maxrss elsewhere (current then = peak)."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + \
                [(n, ctypes.c_size_t) for n in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                                                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                                                "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
        c = Counters()
        c.cb = ctypes.sizeof(c)
        get = ctypes.windll.psapi.GetProcessMemoryInfo
        get.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        me = ctypes.windll.kernel32.GetCurrentProcess
        me.restype = wintypes.HANDLE
        if get(me(), ctypes.byref(c), c.cb):
            return c.WorkingSetSize, c.PeakWorkingSetSize
        return 0, 0
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    return peak, peak


def gb(n):
    return f"{n / 1e9:.2f} GB"


# ---- progress (sqlite, WAL, one lock for every statement) ----

SQL = ["""CREATE TABLE IF NOT EXISTS chunks (security_id TEXT, token TEXT, chunk_from TEXT, chunk_to TEXT,
          rows INTEGER, status TEXT, fetched_at TEXT, run_id TEXT, seconds REAL, error TEXT,
          PRIMARY KEY (security_id, chunk_from))""",
       """CREATE TABLE IF NOT EXISTS day_fetches (security_id TEXT, token TEXT, run_id TEXT, day_from TEXT,
          day_to TEXT, rows INTEGER, fetched_at TEXT, PRIMARY KEY (security_id, run_id))""",
       """CREATE TABLE IF NOT EXISTS stocks (security_id TEXT PRIMARY KEY, token TEXT, status TEXT, first_day TEXT,
          last_day TEXT, days INTEGER, minute_rows INTEGER, bytes INTEGER, built_at TEXT)""",
       """CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, command TEXT, started TEXT, ended TEXT,
          requests INTEGER, n429 INTEGER, result TEXT, peak_mb REAL)"""]
ERRORS_SQL = """CREATE TABLE IF NOT EXISTS errors (security_id TEXT, token TEXT, run_id TEXT, kind TEXT, chunk_from TEXT,
          error TEXT, at TEXT, PRIMARY KEY (security_id, run_id, kind, chunk_from))"""
CHUNK_FIELDS = ["security_id", "token", "chunk_from", "chunk_to", "rows", "status", "fetched_at", "run_id", "seconds",
                "error"]
STOCK_FIELDS = ["security_id", "token", "status", "first_day", "last_day", "days", "minute_rows", "bytes", "built_at",
                "reason", "reason_at"]


class Progress:
    """download_progress.sqlite, shared by the workers: every statement runs under one lock, each write commits."""

    def __init__(self, folder, write=True):
        os.makedirs(out_dir(folder), exist_ok=True)
        self.path = os.path.join(out_dir(folder), "download_progress.sqlite")
        self.lock = threading.Lock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        for s in SQL:
            self.db.execute(s)
        if write and "peak_mb" not in [r[1] for r in self.db.execute("PRAGMA table_info(runs)")]:
            self.db.execute("ALTER TABLE runs ADD COLUMN peak_mb REAL")     # files written before the memory fix
        if write:                       # files written before the fixes: errors, and the NO_KITE_DATA reason
            self.db.execute(ERRORS_SQL)
            have = [r[1] for r in self.db.execute("PRAGMA table_info(stocks)")]
            for c in ("reason", "reason_at"):
                if c not in have:
                    self.db.execute(f"ALTER TABLE stocks ADD COLUMN {c} TEXT")
        self.db.commit()

    def close(self):
        with self.lock:
            self.db.close()

    def rows(self, sql, args=()):
        with self.lock:
            cur = self.db.execute(sql, args)
            names = [c[0] for c in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    def write(self, sql, args=()):
        with self.lock:
            self.db.execute(sql, args)
            self.db.commit()

    def chunks(self, sid=None):
        if sid is None:
            return self.rows("SELECT * FROM chunks")
        return {r["chunk_from"]: r for r in self.rows("SELECT * FROM chunks WHERE security_id=?", (sid,))}

    def save_chunk(self, **row):
        self.write(f"INSERT OR REPLACE INTO chunks ({','.join(CHUNK_FIELDS)}) VALUES ({','.join('?' * len(CHUNK_FIELDS))})",
                   [row.get(k) for k in CHUNK_FIELDS])

    def day_fetch(self, sid, run_id):
        r = self.rows("SELECT * FROM day_fetches WHERE security_id=? AND run_id=?", (sid, run_id))
        return r[0] if r else None

    def save_day_fetch(self, sid, token, run_id, a, b, n):
        self.write("INSERT OR REPLACE INTO day_fetches VALUES (?,?,?,?,?,?,?)",
                   (sid, str(token), run_id, iso(a), iso(b), n, stamp()))

    def stocks(self):
        return {r["security_id"]: r for r in self.rows("SELECT * FROM stocks")}

    def save_stock(self, **row):
        self.write(f"INSERT OR REPLACE INTO stocks ({','.join(STOCK_FIELDS)}) VALUES ({','.join('?' * len(STOCK_FIELDS))})",
                   [row.get(k) for k in STOCK_FIELDS])

    def drop_stock(self, sid):
        """Only the stocks row (a NO_KITE_DATA stock back in Kite's list); its chunks stay for the resume."""
        self.write("DELETE FROM stocks WHERE security_id=?", (sid,))

    def set_no_kite(self, sid, token, reason, day):
        self.save_stock(security_id=sid, token=str(token), status=NO_KITE, reason=reason, reason_at=iso(day))

    def save_error(self, sid, token, run_id, kind, error, chunk_from=""):
        self.write("INSERT OR REPLACE INTO errors VALUES (?,?,?,?,?,?,?)",
                   (sid, str(token), run_id, kind, chunk_from, error, stamp()))

    def errors(self):
        """Every errors row, oldest first ([] in a file written before the table existed)."""
        with self.lock:
            if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='errors'").fetchone():
                return []
        return self.rows("SELECT * FROM errors ORDER BY at")

    def reset_stock(self, sid):
        for table in ("chunks", "day_fetches", "stocks"):
            self.write(f"DELETE FROM {table} WHERE security_id=?", (sid,))

    def start_run(self, run_id, command):
        self.write("INSERT OR REPLACE INTO runs (run_id, command, started, ended, requests, n429, result) "
                   "VALUES (?,?,?,?,?,?,?)", (run_id, command, stamp(), "", 0, 0, "running"))

    def end_run(self, run_id, requests, n429, result, peak_mb=None):
        self.write("UPDATE runs SET ended=?, requests=?, n429=?, result=?, peak_mb=? WHERE run_id=?",
                   (stamp(), requests, n429, result, peak_mb, run_id))

    def mark_aborted(self):
        """Runs still 'running' become ABORTED (their process died: the caller holds the lock). Their ended stays
        empty: when they stopped is not known. Returns their run_ids."""
        ids = [r["run_id"] for r in self.rows("SELECT run_id FROM runs WHERE result='running'")]
        if ids:
            self.write("UPDATE runs SET result='ABORTED' WHERE result='running'")
        return ids

    def runs(self):
        return self.rows("SELECT * FROM runs ORDER BY started")


def read_progress(folder):
    """(chunks, stocks, runs) without creating the file (status, dry run)."""
    path = os.path.join(out_dir(folder), "download_progress.sqlite")
    if not os.path.isfile(path):
        return [], {}, []
    db = Progress(folder, write=False)
    try:
        return db.chunks(), db.stocks(), db.runs()
    finally:
        db.close()


def read_errors(folder):
    """The errors rows, oldest first, without creating the file or the table (status)."""
    if not os.path.isfile(os.path.join(out_dir(folder), "download_progress.sqlite")):
        return []
    db = Progress(folder, write=False)
    try:
        return db.errors()
    finally:
        db.close()


class SharedLimiter(RateLimiter):
    """RateLimiter for several workers: one lock around it (request starts stay 1/rate apart across workers), and
    counters of requests and 429 answers."""

    def __init__(self, rate):
        super().__init__(rate)
        self.lock = threading.Lock()
        self.requests = 0
        self.n429 = 0

    def wait(self):
        with self.lock:
            super().wait()
            self.requests += 1

    def on_429(self):
        with self.lock:
            self.n429 += 1
            super().on_429()

    def on_success(self):
        with self.lock:
            super().on_success()


# ---- Part 1: work list ----

def plan_path(folder):
    return os.path.join(mp.plan_dir(folder), "plan.csv")


def plan_stale(folder):
    """The input build plan.csv is older than (the security master or the adjustments build_info.json), else None."""
    p = plan_path(folder)
    if not os.path.isfile(p):
        raise mp.NotBuilt(f"No {p}. Run phase 1 first: python equity_minute.py tokens, then probe")
    for name, path in (("security master", os.path.join(sm.out_dir(folder), "build_info.json")),
                       ("adjustments", os.path.join(adj.out_dir(folder), "build_info.json"))):
        if os.path.isfile(path) and os.path.getmtime(p) < os.path.getmtime(path):
            return name
    return None


def ensure_plan(folder, inp, log=print):
    """Rebuild plan.csv offline (phase 1's gaps step, no Kite request) when it is older than the master or the
    adjustments; the caller holds equity_1min/.lock. The gaps step needs the whole bhavcopy (phase 1's Inputs): it is
    loaded only for this and dropped before the download starts (measured: 0.72 GB held, 0.41 GB after freeing)."""
    stale = plan_stale(folder)
    if stale:
        before = memory()[0]
        full = inp if inp.bhav is not None else mp.Inputs(folder)
        mp.build_outputs(folder, full, log=quiet)
        peak = memory()[1]
        del full
        gc.collect()
        log(f"plan.csv was older than the {stale}: rebuilt offline (gaps), no Kite request.")
        log(f"  memory: {gb(before)} before the rebuild, process peak {gb(peak)}, {gb(memory()[0])} after freeing")
    return stale


def read_plan(folder):
    """plan.csv rows with download_minute = Y, plus update/new_listings.csv; tokens from update/tokens.csv when the
    daily update matched them since."""
    p = plan_path(folder)
    if not os.path.isfile(p):
        raise mp.NotBuilt(f"No {p}. Run phase 1 first: python equity_minute.py tokens, then probe")
    plan = pd.read_csv(p, dtype=str, keep_default_na=False)
    plan = plan[(plan["download_minute"] == "Y") & (plan["minute_first_day"] != "")]
    rows = {r.security_id: {"security_id": r.security_id, "token": int(r.instrument_token),
                            "first": sm.as_date(r.minute_first_day), "new": False} for r in plan.itertuples()}
    new = os.path.join(update_dir(folder), "new_listings.csv")
    if os.path.isfile(new):
        for r in pd.read_csv(new, dtype=str, keep_default_na=False).itertuples():
            rows.setdefault(r.security_id, {"security_id": r.security_id, "token": int(r.instrument_token),
                                            "first": sm.as_date(r.minute_first_day), "new": True})
    toks = os.path.join(update_dir(folder), "tokens.csv")
    if os.path.isfile(toks):
        for r in pd.read_csv(toks, dtype=str, keep_default_na=False).itertuples():
            if r.security_id in rows and r.match_status in mp.MATCHED and r.instrument_token:
                rows[r.security_id]["token"] = int(r.instrument_token)
    return rows


def priorities(membership):
    """{security_id: 1 ever in NIFTY 50, 2 ever in NIFTY 500}; others are 3."""
    out = {}
    for p, name in reversed(mp.PRIORITY_INDEX):
        for sid in membership.loc[membership["index"] == name, "security_id"]:
            out[sid] = p
    return out


def bhav_year(path, series, keys, codes, ids, columns):
    """(Arrow table, id codes) of one bhavcopy year file: the master series rows of the given securities, security_id
    first, sorted by security_id and date, one row per security and day (the first, as mp.read_bhav). keys / codes:
    the ISINs owned by one of `ids` and the position of their owner in `ids` (sorted)."""
    t = pq.read_table(path, columns=["date", "isin"] + [c for c in columns if c not in ("date", "isin")],
                      filters=[("series", "in", series)])
    code = pc.take(codes, pc.index_in(t.column("isin"), value_set=keys))
    keep = pc.is_valid(code)
    t, code = t.filter(keep), code.filter(keep)
    c = code.to_numpy(zero_copy_only=False).astype(np.int64)
    d = t.column("date").cast(pa.int32()).to_numpy(zero_copy_only=False)
    order = np.lexsort((d, c))                      # stable, like read_bhav's sort on the concatenated years
    c, d = c[order], d[order]
    first = np.ones(len(c), bool)
    first[1:] = (c[1:] != c[:-1]) | (d[1:] != d[:-1])
    t = t.take(pa.array(order[first])).select([x for x in columns if x != "isin"])
    return t.add_column(0, "security_id", pc.take(ids, pa.array(c[first]))), c[first]


def bhav_keys(folder, inp):
    """(series, keys, codes, ids) for bhav_year: the master's series and ISIN owners, limited to the universe."""
    m = sm.load_master(folder)
    ids = sorted(inp.by_id)
    pos = {sid: i for i, sid in enumerate(ids)}
    own = [(isin, pos[sid]) for isin, sid in m["isin_owner"].items() if sid in pos]
    keys = pa.array([k for k, _ in own], pa.string())
    codes = pa.array([v for _, v in own], pa.int32())
    return list(m["info"]["series"]), keys, codes, pa.array(ids, pa.string())


def turnover_medians(folder, inp):
    """{security_id: median bhavcopy turnover} of the universe, one year file at a time (never the whole bhavcopy;
    writes nothing, so the dry run can use it)."""
    series, keys, codes, ids = bhav_keys(folder, inp)
    cs, ts = [], []
    for path in sm.year_files(mp.store_dir(folder)):
        t, c = bhav_year(path, series, keys, codes, ids, ["turnover"])
        cs.append(c.astype(np.int32))
        ts.append(t.column("turnover").to_numpy(zero_copy_only=False).astype(float))
        del t
    if not cs:
        return {}
    c, v = np.concatenate(cs), np.concatenate(ts)
    del cs, ts
    order = np.argsort(c, kind="stable")
    c, v = c[order], v[order]
    del order
    starts = np.flatnonzero(np.r_[True, c[1:] != c[:-1]])
    ends = np.r_[starts[1:], len(c)]
    names = ids.to_pylist()
    out = {}
    for a, b in zip(starts, ends):
        x = v[a:b]
        x = x[~np.isnan(x)]                                   # as pandas' median: NaN skipped, none left -> NaN
        out[names[c[a]]] = float(np.median(x)) if len(x) else float("nan")
    return out


CACHE = "bhav_cache"
CACHE_COLUMNS = ["date", "symbol", "series"] + FIELDS + ["turnover"]
CACHE_ROW_GROUP = 20_000            # small row groups: a read filtered on one security_id skips the others


def cache_dir(folder):
    return os.path.join(out_dir(folder), CACHE)


def file_key(path):
    st = os.stat(path)
    return [st.st_size, st.st_mtime_ns]


def bhav_cache(folder, inp, log=quiet, write=True, out=None):
    """bhav_cache/<year>.parquet: the bhavcopy rows of the universe by security_id (bhav_year), rebuilt for the year
    files that changed since (size, mtime), all of them when the master or the universe changed. The caller holds
    equity_1min/.lock. Returns the cache's year file paths. write=False (a dry run): writes nothing and raises
    CacheStale when a year would be rebuilt. out: another cache folder (repair's dry run: a temporary one)."""
    out = out or cache_dir(folder)
    if write:
        os.makedirs(out, exist_ok=True)
    info_path = os.path.join(out, "cache_info.json")
    try:
        with open(info_path, encoding="utf-8") as f:
            old = json.load(f)
    except (OSError, ValueError):
        old = {}
    base = {"master": file_key(os.path.join(sm.out_dir(folder), "build_info.json")),
            "universe": hashlib.sha256(",".join(sorted(inp.by_id)).encode()).hexdigest()}
    same = all(old.get(k) == v for k, v in base.items())
    years, built, keys = {}, [], None
    for path in sm.year_files(mp.store_dir(folder)):
        year = os.path.basename(path)[:4]
        dst = os.path.join(out, f"{year}.parquet")
        years[year] = file_key(path)
        if same and old.get("years", {}).get(year) == years[year] and os.path.isfile(dst):
            continue
        if not write:
            raise CacheStale(f"The bhavcopy cache is older than the store ({year}): run with --apply, which rebuilds "
                             "it first.")
        keys = keys or bhav_keys(folder, inp)
        t, _ = bhav_year(path, *keys, CACHE_COLUMNS)
        sm.write_atomic(dst, lambda tmp: pq.write_table(t, tmp, row_group_size=CACHE_ROW_GROUP, compression="zstd"))
        built.append(year)
        del t
    if write:
        mp.write_json(info_path, {**base, "years": years})
    if built:
        log(f"Bhavcopy cache: {len(built)} year file(s) rebuilt ({', '.join(built)}).")
    return [os.path.join(out, f"{y}.parquet") for y in sorted(years)]


def read_cached_bhav(paths, sid):
    """One security's cached bhavcopy rows (date, symbol, series, OHLCV, turnover), or None."""
    frames = [pq.read_table(p, filters=[("security_id", "==", sid)]).to_pandas() for p in paths]
    frames = [f for f in frames if len(f)]
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True).drop(columns="security_id")


def work_list(folder, inp, priority=None, symbols=None, target=None):
    """Every stock to download, in order: priority 1, 2, 3, then median turnover (largest first). Each range ends at
    the latest bhavcopy day, or at the target day (target_day) when that is earlier: an unfinished day is never
    fetched."""
    rows = read_plan(folder)
    prio = priorities(inp.membership)
    turnover = turnover_medians(folder, inp)
    last = min(inp.days[-1], target) if target else inp.days[-1]
    want = None
    if symbols:
        want = set()
        for s in symbols:
            ids = sm.lookup(s, folder)
            if not ids:
                raise mp.MinutePlanError(f"{s}: no security ever had this symbol or ISIN.")
            want.update(ids)
    items = []
    for sid, r in rows.items():
        if sid not in inp.by_id or (want is not None and sid not in want):
            continue
        p = prio.get(sid, 3)
        if priority and p != priority:
            continue
        t = turnover.get(sid)
        items.append({**r, "symbol": inp.by_id[sid]["symbol"], "priority": p, "last": last,
                      "turnover": None if t is None or pd.isna(t) else float(t),
                      "chunks": make_chunks(r["first"], last, "minute") if r["first"] <= last else []})
    items.sort(key=lambda x: (x["priority"], -(x["turnover"] if x["turnover"] is not None else -1), x["security_id"]))
    return items


def chunk_done(have, chunk, token, folder, sid):
    r = have.get(iso(chunk[0].date()))
    return bool(r and r["status"] == "DONE" and r["token"] == str(token) and r["chunk_to"] == iso(chunk[1].date())
                and os.path.isfile(os.path.join(staging_dir(folder, sid), f"m_{r['chunk_from']}.parquet"))
                and os.path.isfile(os.path.join(staging_dir(folder, sid), f"day_{r['run_id']}.parquet")))


def stock_state(item, stocks, chunks_by_sid, folder):
    """DONE (built with this token), PARTLY (some chunks done), NOT_STARTED, NO_KITE_DATA (not in Kite's list: nothing
    to fetch); and the chunks still to fetch."""
    st = stocks.get(item["security_id"])
    if st and st["status"] == NO_KITE:
        return NO_KITE, []
    if st and st["status"] == "DONE" and st["token"] == str(item["token"]):
        return "DONE", []
    have = chunks_by_sid.get(item["security_id"], {})
    if st and st["status"] == "DONE":            # a new token: everything again
        return "NOT_STARTED", list(item["chunks"])
    todo = [c for c in item["chunks"] if not chunk_done(have, c, item["token"], folder, item["security_id"])]
    return ("PARTLY" if len(todo) < len(item["chunks"]) else "NOT_STARTED"), todo


def free_space(folder):
    p = out_dir(folder)
    while not os.path.isdir(p):
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return shutil.disk_usage(p).free


def estimate(items, folder, days, speed=SPEED, workers=WORKERS):
    """Requests, hours and disk still needed, per priority (Part 0 timings and sizes until measured ones exist)."""
    chunks, stocks, _ = read_progress(folder)
    by_sid = {}
    for c in chunks:
        by_sid.setdefault(c["security_id"], {})[c["chunk_from"]] = c
    timed = [c["seconds"] for c in chunks if c["status"] == "DONE" and c["seconds"]]
    lat = float(np.mean(timed)) if len(timed) >= TIMED_MIN else MINUTE_SECONDS
    prio_of = {i["security_id"]: i["priority"] for i in items}
    per_day = dict(BYTES_PER_DAY)
    for p in (1, 2, 3):
        done = [s for s in stocks.values() if s["status"] == "DONE" and prio_of.get(s["security_id"]) == p and s["days"]]
        if len(done) >= MEASURED_MIN:
            per_day[p] = sum(s["bytes"] for s in done) / sum(s["days"] for s in done)
    groups = {}
    for i in items:
        state, todo = stock_state(i, stocks, by_sid, folder)
        g = groups.setdefault(i["priority"], {"stocks": 0, "DONE": 0, "PARTLY": 0, "NOT_STARTED": 0, NO_KITE: 0,
                                               "minute_requests": 0, "day_requests": 0, "bytes": 0.0})
        g["stocks"] += 1
        g[state] += 1
        if todo:
            g["minute_requests"] += len(todo)
            g["day_requests"] += len(make_chunks(i["first"], i["last"], "day"))
            n = bisect_right(days, i["last"]) - bisect_left(days, i["first"])
            g["bytes"] += n * len(todo) / max(len(i["chunks"]), 1) * per_day[i["priority"]]
    total = {k: sum(g[k] for g in groups.values()) for k in
             ("stocks", "DONE", "PARTLY", "NOT_STARTED", NO_KITE, "minute_requests", "day_requests", "bytes")}
    secs = total["minute_requests"] * max(1 / speed, lat / workers) + \
        total["day_requests"] * max(1 / speed, DAY_SECONDS / workers)
    return {"groups": dict(sorted(groups.items())), "total": total, "hours": secs / 3600, "latency": lat,
            "latency_measured": len(timed) >= TIMED_MIN, "bytes_per_day": per_day, "speed": speed,
            "workers": workers}


def estimate_text(e, free):
    t = e["total"]
    nk = f", {t[NO_KITE]:,} no Kite data" if t.get(NO_KITE) else ""
    lines = [f"Stocks: {t['stocks']:,} ({t['DONE']:,} done, {t['PARTLY']:,} partly done, {t['NOT_STARTED']:,} not "
             f"started{nk})"]
    for p, g in e["groups"].items():
        lines.append(f"  priority {p}: {g['stocks']:,} stocks, {g['DONE']:,} done, {g['PARTLY']:,} partly, "
                     f"{g['minute_requests']:,} minute + {g['day_requests']:,} day requests, "
                     f"about {g['bytes'] / 1e9:.1f} GB")
    need = t["bytes"] * DISK_MARGIN
    lines += [f"Requests left: {t['minute_requests']:,} minute + {t['day_requests']:,} day",
              f"About {e['hours']:.1f} hours at {e['speed']:g} req/s with {e['workers']} worker(s) "
              f"({'measured' if e['latency_measured'] else 'Part 0'} {e['latency']:.2f} s per minute request)",
              f"Disk: about {t['bytes'] / 1e9:.1f} GB more (+20% = {need / 1e9:.1f} GB); free {free / 1e9:.1f} GB"]
    if free < need:
        lines.append("NOT ENOUGH DISK: a real run would refuse.")
    return "\n".join(lines) + "\n"


# ---- Part 2 / 3: per stock ----

def corporate_actions(folder, inp):
    """{security_id: sorted action dates}: applied events (events.csv) and every row of the corporate-actions CSVs
    (any purpose: rights, dividends, demergers, ...) for any symbol the security had."""
    out = {}
    try:
        ev = adj.read_built(folder, "events.csv")
        for r in ev[ev["applied"] == "Y"].itertuples():
            out.setdefault(r.security_id, set()).add(sm.as_date(r.ex_date))
    except adj.AdjustmentError:
        pass
    try:
        ca = adj.read_corp_actions(folder)
    except adj.AdjustmentError:
        ca = pd.DataFrame(columns=["symbol", "ex_date_text"])
    by_symbol = {}
    for sym, text in zip(ca["symbol"], ca["ex_date_text"]):
        try:
            by_symbol.setdefault(sym, set()).add(adj.parse_date(text))
        except (ValueError, TypeError):
            continue
    for sid, rows in inp.history.items():
        for sym in set(rows["symbol"]):
            if sym in by_symbol:
                out.setdefault(sid, set()).update(by_symbol[sym])
    return {sid: sorted(d) for sid, d in out.items()}


def levels(values, rel_tol, min_days=STEP_MIN_DAYS, window=SCALE_WINDOW):
    """Per position: the level of the step-wise series behind noisy measured values (NaN = not measured). A centred
    median of `window` measured values; a new level starts where it moves by more than rel_tol and lasts min_days
    measured days (shorter: a blip, kept in the level before). Each level is the median of its measured values;
    positions before the first measurement take the first level. None when nothing was measured."""
    values = np.asarray(values, float)
    idx = np.flatnonzero(~np.isnan(values))
    if not len(idx):
        return None
    med = pd.Series(values[idx]).rolling(window, center=True, min_periods=1).median().to_numpy()
    groups, level = [[0]], med[0]
    for k in range(1, len(idx)):
        if abs(med[k] - level) > rel_tol * abs(level):
            groups.append([k])
            level = med[k]
        else:
            groups[-1].append(k)
    merged = []
    for g in groups:                              # blips join the level before (or the next one at the start)
        if merged and len(g) < min_days:
            merged[-1] += g
        else:
            merged.append(g)
    if len(merged) > 1 and len(merged[0]) < min_days:
        merged[1] = merged[0] + merged[1]
        merged = merged[1:]
    out = np.empty(len(values))
    starts = [idx[g[0]] for g in merged]
    for j, g in enumerate(merged):
        a = 0 if j == 0 else starts[j]
        b = starts[j + 1] if j + 1 < len(merged) else len(values)
        out[a:b] = float(np.median(values[idx[g]]))
    return out


def volume_levels(ratio):
    """minute_volume_scale per day: 1.0 unless day volume / minute volume steps by more than VOLUME_STEP for
    STEP_MIN_DAYS days; such a period gets its median ratio / the normal ratio of the nearest unadjusted period."""
    lv = levels(ratio, VOLUME_STEP)
    if lv is None:
        return np.ones(len(ratio))
    normal = np.abs(lv - 1) <= VOLUME_STEP
    out = np.ones(len(lv))
    pos = np.flatnonzero(normal)
    for i in np.flatnonzero(~normal):
        if len(pos):
            j = pos[np.argmin(np.abs(pos - i))]
            out[i] = lv[i] / lv[j]
        else:
            out[i] = lv[i]
    return np.round(out, 6)


def day_inputs(minutes, runs_of_day, day_frames):
    """The INPUT_COLUMNS frame of one stock: per date, the minute aggregates and Kite's day candle of the run that
    fetched that date's chunk. minutes: the stored columns plus run_id / fetched_at; runs_of_day(date) -> (run_id,
    fetched_at); day_frames: {run_id: mp.day_frame()}."""
    return inputs_from(minute_aggregates(minutes), runs_of_day, day_frames)


def minute_aggregates(minutes):
    """{date: minute aggregates} of minute rows (the stored columns plus run_id / fetched_at). Every date's rows must
    be in the frame: a build calls it per staged chunk (chunks start at 00:00, so they never split a day). The bars go
    through ms.clean_bars first (as load_minute's default): Kite's all-zero padding bars do not count, so a day of
    only zero bars has no minutes (MINUTE_EMPTY). The stored files keep every bar."""
    rows = {}
    minutes = ms.clean_bars(minutes)[0]
    if len(minutes):
        m = minutes.sort_values("ts", kind="stable")
        m = m.assign(_d=m["ts"].dt.date, _hm=m["ts"].dt.strftime("%H:%M"))
        g = m.groupby("_d", sort=True)
        agg = pd.DataFrame({"minute_rows": g.size(), "first_bar": g["_hm"].first(), "last_bar": g["_hm"].last(),
                            "minute_open": g["open"].first(), "minute_high": g["high"].max(),
                            "minute_low": g["low"].min(), "minute_close": g["close"].last(),
                            "minute_volume": g["volume"].sum(), "run_id": g["run_id"].last(),
                            "fetched_at": g["fetched_at"].last()})
        for d, r in agg.iterrows():
            rows[d] = r.to_dict()
    return rows


def inputs_from(rows, runs_of_day, day_frames):
    """The INPUT_COLUMNS frame from minute_aggregates rows (changed in place) and the day frames (day_inputs)."""
    for run_id, df in day_frames.items():
        for r in df.itertuples():
            d = r.date
            run, fetched = runs_of_day(d)
            if run != run_id:
                continue
            row = rows.setdefault(d, {"minute_rows": 0, "first_bar": "", "last_bar": "", "run_id": run,
                                      "fetched_at": fetched})
            for f in FIELDS:
                row[f"kite_{f}"] = getattr(r, f)
    out = pd.DataFrame([{"date": d, **r} for d, r in sorted(rows.items())])
    for c in INPUT_COLUMNS:
        if c not in out:
            out[c] = np.nan if c not in ("first_bar", "last_bar", "run_id", "fetched_at") else ""
    out["minute_rows"] = out["minute_rows"].fillna(0).astype("int64")
    return out[INPUT_COLUMNS]


class Calc:
    """What every stock's days need: the bhavcopy rows (read per stock from bhav_cache), factors and corporate
    actions (from minute_plan.Inputs). Building it refreshes the cache: the caller holds equity_1min/.lock."""

    def __init__(self, folder, inp, log=quiet, write=True, cache_out=None):
        self.inp = inp
        self.days = inp.days
        self.cache = bhav_cache(folder, inp, log, write, cache_out)
        self.actions = corporate_actions(folder, inp)

    def bhav_of(self, sid):
        return read_cached_bhav(self.cache, sid)

    def near_action(self, sid, d):
        acts = self.actions.get(sid, [])
        if not acts:
            return ""
        i = bisect_left(self.days, d)
        lo = self.days[max(i - ACTION_DAYS, 0)]
        hi = self.days[min(i + ACTION_DAYS, len(self.days) - 1)]
        near = [a for a in acts if lo <= a <= hi]
        return iso(near[0]) if near else ""

    def age(self, d, end):
        """Trading days from d to end (end itself 0)."""
        return bisect_right(self.days, end) - bisect_right(self.days, d)

    def derive(self, sid, inputs, bhav=False):
        """The days.parquet frame of one stock from its INPUT_COLUMNS frame (every date of the stock). bhav: the
        stock's bhav_of rows when the caller has read them already."""
        d = inputs.sort_values("date", kind="stable").reset_index(drop=True)
        b = self.bhav_of(sid) if bhav is False else bhav
        b = (b[["date", "symbol", "series"] + FIELDS].rename(columns={f: f"bhav_{f}" for f in FIELDS})
             if b is not None else pd.DataFrame(columns=["date", "symbol", "series"] + [f"bhav_{f}" for f in FIELDS]))
        d = d.merge(b, on="date", how="left")
        has_min = d["minute_rows"].to_numpy() > 0
        has_day = d["kite_close"].notna().to_numpy()
        has_bhav = d["bhav_close"].notna().to_numpy()
        n = len(d)

        # Kite's conversion: phase 1's functions on the days with a day candle and a bhavcopy row
        own = has_day & has_bhav
        conv = d.loc[own, ["date", "symbol"] + [f"kite_{f}" for f in FIELDS] + [f"bhav_{f}" for f in FIELDS]].copy()
        conv.insert(0, "security_id", sid)
        for c in ("ratio", "kite_multiplier", "kite_difference", "half"):
            d[c] = np.nan
        d["kind"] = ""
        if len(conv):
            conv = mp.with_factor(conv, self.inp.factors)
            with np.errstate(divide="ignore", invalid="ignore"):
                conv["ratio"] = conv["bhav_close"] / conv["kite_close"]
                conv["difference"] = conv["bhav_close"] - conv["kite_close"]
                conv["adj_difference"] = conv["bhav_close"] / conv["our_factor"] - conv["kite_close"]
                conv["kite_multiplier"] = mp.kite_multiplier(conv)
                conv["kite_difference"] = conv["bhav_close"] / conv["kite_multiplier"] - conv["kite_close"]
            _, _, conv = mp.adjustments_and_mismatches(conv, self.inp)
            conv["half"] = mp.half_units(conv)
            pos = {dt: i for i, dt in enumerate(d["date"])}
            ix = [pos[dt] for dt in conv["date"]]
            for c in ("ratio", "kite_multiplier", "kite_difference", "half"):
                d.loc[ix, c] = conv[c].to_numpy(float)
            d.loc[ix, "kind"] = conv["kind"].to_numpy(object)
        f = mp.with_factor(pd.DataFrame({"security_id": sid, "date": d["date"]}), self.inp.factors)
        d["our_factor"] = d["date"].map(dict(zip(f["date"], f["our_factor"]))).astype(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            d["vol_ratio"] = np.where(own & (d["kite_volume"].to_numpy(float) > 0),
                                      d["bhav_volume"].to_numpy(float) / d["kite_volume"].to_numpy(float), np.nan)

        # minute_scale: Kite day / minute from high and low where both agree
        kh, kl, kc = (d[f"kite_{x}"].to_numpy(float) for x in ("high", "low", "close"))
        mh, ml = d["minute_high"].to_numpy(float), d["minute_low"].to_numpy(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            sh, sl = kh / mh, kl / ml
            agree = np.abs(sh - sl) <= SCALE_AGREE + 2 * mp.HALF_TICK / kc
        measured = np.where(has_min & has_day & agree & (mh > 0) & (ml > 0), (sh + sl) / 2, np.nan)
        lv = levels(measured, SCALE_STEP)
        scale = np.ones(n) if lv is None else np.where(np.abs(lv - 1) <= SCALE_ONE, 1.0, np.round(lv, 6))
        d["minute_scale"] = scale
        d["minute_scale_measured"] = np.round(measured, 6)
        mv, kv = d["minute_volume"].to_numpy(float), d["kite_volume"].to_numpy(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            vr = np.where(has_min & has_day & (mv > 0) & (kv > 0), kv / mv, np.nan)
        d["minute_volume_scale"] = volume_levels(vr)

        # days without their own conversion take the nearest day's (same minute_scale and factor first)
        d["conversion_from"] = ""
        src = np.flatnonzero(own & ~np.isnan(d["ratio"].to_numpy(float)))
        dates = np.array([x.toordinal() for x in d["date"]])
        for i in np.flatnonzero(~own | np.isnan(d["ratio"].to_numpy(float))):
            if not len(src):
                break
            same = src[(scale[src] == scale[i]) & (d["our_factor"].to_numpy()[src] == d["our_factor"].iat[i])]
            pool = same if len(same) else src
            j = pool[np.argmin(np.abs(dates[pool] - dates[i]))]
            for c in ("ratio", "kite_multiplier", "kite_difference", "vol_ratio", "kind"):
                d.at[i, c] = d.at[j, c]
            d.at[i, "conversion_from"] = iso(d.at[j, "date"])
        d["kind"] = d["kind"].replace("", "MULTIPLICATIVE")
        d["symbol"] = d["symbol"].ffill().bfill()
        d["series"] = d["series"].ffill().bfill()

        # checks (scaled minutes vs the same-run day candle)
        half = np.where(np.isnan(d["half"].to_numpy(float)), mp.HALF_TICK, d["half"].to_numpy(float))
        both = has_min & has_day
        with np.errstate(invalid="ignore"):
            tol_h, tol_l, tol_c = (mp.PRICE_REL * np.abs(x) + half for x in (kh, kl, kc))
            hl = both & ((mh * scale > kh + tol_h) | (ml * scale < kl - tol_l))
            cl = both & ((kc > mh * scale + tol_c) | (kc < ml * scale - tol_c))
            vs = mv * d["minute_volume_scale"].to_numpy(float)
            vol = both & ((vs > kv * VOLUME_HIGH + 1) | (vs < kv * VOLUME_LOW))
        last_bar = d["last_bar"].fillna("").astype(str).to_numpy(object)
        auction = cl & (last_bar >= AUCTION_FROM) & (last_bar < AUCTION_TO)    # the close came from the auction
        bv = d["bhav_volume"].to_numpy(float)
        unconfirmed = has_min & has_day & has_bhav & (kv == mv) & (kv != bv)   # unfinished(): stored after the cap
        flags = {"EXTRA_SESSION": ~has_bhav, "DAY_EMPTY": has_min & has_bhav & ~has_day,
                 "MINUTE_EMPTY": has_day & has_bhav & ~has_min, "HIGHLOW_DIFF": hl, "CLOSE_DIFF": cl & ~auction,
                 "CLOSING_AUCTION": auction, "VOLUME_DIFF": vol, "SCALE_STEP": np.zeros(n, bool),
                 "CANDLE_UNCONFIRMED": unconfirmed}
        notes = [[] for _ in range(n)]
        vscale = d["minute_volume_scale"].to_numpy(float)
        for i in range(1, n):
            for kind, arr in (("minute_scale", scale), ("minute_volume_scale", vscale)):
                if arr[i] != arr[i - 1]:
                    act = self.near_action(sid, d.at[i, "date"])
                    notes[i].append(f"{kind} {arr[i - 1]:g} -> {arr[i]:g}" +
                                    (f" (corporate action {act})" if act else " (no corporate action near)"))
                    if not act:
                        flags["SCALE_STEP"][i] = True
        for i in np.flatnonzero(flags["DAY_EMPTY"] | flags["EXTRA_SESSION"]):
            if d.at[i, "conversion_from"]:
                notes[i].append(f"converted as {d.at[i, 'conversion_from']}")
        d["check"] = ["|".join(c for c in CHECKS if flags[c][i]) or "OK" for i in range(n)]
        d["note"] = ["; ".join(x) for x in notes]
        return d[DAY_COLUMNS]


def scale_steps(sid, symbol, days):
    """scale_steps.csv rows of one stock's days.parquet."""
    out = []
    for kind in ("minute_scale", "minute_volume_scale"):
        v = days[kind].to_numpy(float)
        for i in np.flatnonzero(v[1:] != v[:-1]) + 1:
            note = days["note"].iat[i]
            act = note.split("(corporate action ")[1].split(")")[0] if f"{kind} " in note and \
                "(corporate action " in note else ""
            out.append({"security_id": sid, "symbol": symbol, "kind": kind.upper(), "date": iso(days["date"].iat[i]),
                        "from_value": v[i - 1], "to_value": v[i], "near_action": act,
                        "flagged": "Y" if "SCALE_STEP" in days["check"].iat[i] else "N"})
    return out


# ---- the run ----

class Runner:
    """One download or update run: workers take whole stocks from a shared queue (never the same stock), one
    shared rate limiter, every progress write through Progress's lock."""

    def __init__(self, folder, inp, kite, speed, workers, command, log=print):
        self.folder, self.inp, self.kite, self.log = folder, inp, kite, log
        self.calc = Calc(folder, inp, log)
        self.limiter = SharedLimiter(speed)
        self.workers = workers
        self.db = Progress(folder)
        self.run_id = f"{now_ist():%Y%m%d-%H%M%S}-{secrets.token_hex(2)}"
        self.db.start_run(self.run_id, command)
        self.lock = threading.Lock()
        self.stats = {"stocks": 0, "built": 0, "updated": 0, "up_to_date": 0, "failed_stocks": 0, "chunks": 0,
                      "failed_chunks": 0, "rows": 0, "new_days": 0, "problem_days": 0, "stopped": False,
                      "auth": None, "started": time.monotonic(), "done": 0, "deferred_days": 0, "deferred_stocks": 0,
                      "unconfirmed_days": 0}
        self.deferred, self.unconfirmed = [], []     # (symbol, date, reason[, days held back])
        self.priority = None

    def add(self, **kw):
        with self.lock:
            for k, v in kw.items():
                self.stats[k] += v

    def fetch(self, token, start, end, interval):
        return fetch_chunk(self.kite, self.limiter, token, start, end, interval)

    def fetch_days(self, token, a, b):
        candles = []
        for s, e in make_chunks(a, b, "day"):
            candles += self.fetch(token, s, e, "day")
        return mp.day_frame(candles)

    def hold_back(self, item, inputs, bhav, end, window=False):
        """The first deferred date of a stock's new days (INPUT_COLUMNS rows), or None. An unfinished day (unfinished())
        at most DEFER_DAYS trading days before end is deferred with every day after it: nothing of them is stored and
        the next update fetches them again. An older one is stored anyway (CANDLE_UNCONFIRMED, or DAY_EMPTY without a
        candle) and counted as unconfirmed, so no stock is deferred forever. window: only the days at most
        DEFER_DAYS before end are looked at (a full history: the older days are finished)."""
        if window:
            inputs = inputs[[self.calc.age(d, end) <= DEFER_DAYS for d in inputs["date"]]]
        bad = unfinished(inputs, bhav)
        cut = next((d for d in sorted(bad) if self.calc.age(d, end) <= DEFER_DAYS), None)
        with self.lock:
            for d in sorted(bad):
                if cut is not None and d >= cut:
                    break
                self.unconfirmed.append((item["symbol"], d, bad[d]))
                self.stats["unconfirmed_days"] += 1
            if cut is not None:
                held = int((inputs["date"] >= cut).sum())
                self.stats["deferred_days"] += held
                self.stats["deferred_stocks"] += 1
                self.deferred.append((item["symbol"], cut, bad[cut], held))
        return cut

    # history of one stock
    def history(self, item):
        sid, token = item["security_id"], item["token"]
        st = self.db.stocks().get(sid)
        if st and st["status"] == "DONE" and st["token"] != str(token):
            archive(self.folder, sid, f"token {st['token']} -> {token}")
            self.db.reset_stock(sid)
            shutil.rmtree(staging_dir(self.folder, sid), ignore_errors=True)
        if not item["chunks"]:                      # listed after the target day: the next run fetches it
            return "NOT_YET"
        stage = staging_dir(self.folder, sid)
        os.makedirs(stage, exist_ok=True)
        have = self.db.chunks(sid)
        todo = [c for c in item["chunks"] if not chunk_done(have, c, token, self.folder, sid)]
        failed = 0
        if todo:
            if self.db.day_fetch(sid, self.run_id) is None or \
                    not os.path.isfile(os.path.join(stage, f"day_{self.run_id}.parquet")):
                try:
                    df = self.fetch_days(token, item["first"], item["last"])
                except ChunkFailed as e:
                    self.log(f"  {item['symbol']}: day candles FAILED {e}")
                    self.db.save_error(sid, token, self.run_id, "DAY_CANDLES", str(e))
                    self.add(failed_stocks=1, failed_chunks=1)
                    return "FAILED"
                sm.write_parquet(df, os.path.join(stage, f"day_{self.run_id}.parquet"))
                self.db.save_day_fetch(sid, token, self.run_id, item["first"], item["last"], len(df))
            for start, end in todo:
                if STOP.is_set():
                    raise StopJob()
                t0 = time.monotonic()
                row = {"security_id": sid, "token": str(token), "chunk_from": iso(start.date()),
                       "chunk_to": iso(end.date()), "run_id": self.run_id}
                try:
                    candles = self.fetch(token, start, end, "minute")
                except ChunkFailed as e:
                    self.db.save_chunk(**row, rows=0, status="FAILED", fetched_at=stamp(),
                                       seconds=time.monotonic() - t0, error=str(e))
                    self.add(failed_chunks=1)
                    failed += 1
                    continue
                df = ms.minute_frame(candles)
                sm.write_parquet(df, os.path.join(stage, f"m_{row['chunk_from']}.parquet"))
                self.db.save_chunk(**row, rows=len(df), status="DONE", fetched_at=stamp(),
                                   seconds=time.monotonic() - t0, error="")
                self.add(chunks=1, rows=len(df))
        if failed:
            self.add(failed_stocks=1)
            return "FAILED"
        if not self.build(item):
            return "DEFERRED"
        self.add(built=1)
        return "BUILT"

    def build(self, item):
        """All chunks done: write data/<sid>/ (into a temporary folder, then renamed) and mark the stock DONE. The
        staged chunks are read one at a time; at most one year of the stock is held, for its year file. Deferred days
        at the end (hold_back) are left out of both files; when nothing is left, nothing is built (False) and the
        staging stays."""
        sid = item["security_id"]
        stage = staging_dir(self.folder, sid)
        have = self.db.chunks(sid)
        final = ms.stock_dir(self.folder, sid)
        tmp = final + ".building"
        shutil.rmtree(tmp, ignore_errors=True)
        os.makedirs(tmp)
        spans, aggs, year, buf, n = [], {}, None, [], 0

        def flush():
            if buf:
                ms.write_minutes(pd.concat(buf, ignore_index=True), os.path.join(tmp, f"{year}.parquet"))
                buf.clear()

        for start, end in item["chunks"]:
            r = have[iso(start.date())]
            spans.append((start.date(), end.date(), r["run_id"], r["fetched_at"]))
            df = pq.read_table(os.path.join(stage, f"m_{r['chunk_from']}.parquet")).to_pandas()
            if not len(df):
                continue
            df["ts"] = pd.to_datetime(df["ts"]).dt.tz_convert(ms.TZ)
            df = df.drop_duplicates("ts", keep="last").sort_values("ts", kind="stable")
            aggs.update(minute_aggregates(df.assign(run_id=r["run_id"], fetched_at=r["fetched_at"])))
            n += len(df)
            for y, g in df.groupby(df["ts"].dt.year):
                if y != year:
                    flush()
                    year = y
                buf.append(g)
            del df
        flush()
        frames = {}
        for run in {s[2] for s in spans}:
            df = pq.read_table(os.path.join(stage, f"day_{run}.parquet")).to_pandas()
            df["date"] = [sm.as_date(x) for x in df["date"]]
            frames[run] = df

        def runs_of_day(d):
            for a, b, run, fetched in spans:
                if a <= d <= b:
                    return run, fetched
            return spans[-1][2], spans[-1][3]

        inputs = inputs_from(aggs, runs_of_day, frames)
        bhav = self.calc.bhav_of(sid)
        cut = self.hold_back(item, inputs, bhav, item["last"], window=True)
        if cut is not None:
            inputs = inputs[inputs["date"] < cut].reset_index(drop=True)
            if inputs.empty:
                shutil.rmtree(tmp, ignore_errors=True)
                return False
            n -= cut_minutes(tmp, cut)
        days = self.calc.derive(sid, inputs, bhav=bhav)
        ms.write_days(days, os.path.join(tmp, ms.DAYS_FILE))
        if os.path.isdir(final):
            archive(self.folder, sid, "rebuilt")
        os.replace(tmp, final)
        self.save_stock(item, days, n)
        shutil.rmtree(stage, ignore_errors=True)
        del days, frames, aggs
        gc.collect()
        return True

    def save_stock(self, item, days, rows):
        path = ms.stock_dir(self.folder, item["security_id"])
        size = sum(os.path.getsize(os.path.join(path, n)) for n in os.listdir(path))
        self.db.save_stock(security_id=item["security_id"], token=str(item["token"]), status="DONE",
                           first_day=iso(days["date"].iat[0]) if len(days) else iso(item["first"]),
                           last_day=iso(days["date"].iat[-1]) if len(days) else iso(item["last"]),
                           days=len(days), minute_rows=int(rows), bytes=size, built_at=stamp())

    # daily update of one DONE stock
    def append(self, item, st, target):
        """The days after the stock's last stored day up to the target day (target_day, fixed for the run); deferred
        days (hold_back) are not stored, so last_day stays before them."""
        sid, token = item["security_id"], item["token"]
        last = sm.as_date(st["last_day"])
        a = last + timedelta(days=1)
        if a > target:
            self.add(up_to_date=1)
            return "UP_TO_DATE"
        try:
            dayf = self.fetch_days(token, a, target)
            candles = []
            for s, e in make_chunks(a, target, "minute"):
                if STOP.is_set():
                    raise StopJob()
                candles += self.fetch(token, s, e, "minute")
        except ChunkFailed as e:
            self.log(f"  {item['symbol']}: FAILED {e}")
            self.db.save_error(sid, token, self.run_id, "UPDATE", str(e))
            self.add(failed_stocks=1, failed_chunks=1)
            return "FAILED"
        fetched = stamp()
        new = ms.minute_frame(candles)
        path = ms.stock_dir(self.folder, sid)
        stored = ms.read_days(path)
        inputs_new = day_inputs(new.assign(run_id=self.run_id, fetched_at=fetched),
                                lambda d: (self.run_id, fetched), {self.run_id: dayf})
        if inputs_new.empty:
            self.add(up_to_date=1)
            return "NO_NEW_DAYS"
        bhav = self.calc.bhav_of(sid)
        cut = self.hold_back(item, inputs_new, bhav, target)
        if cut is not None:
            inputs_new = inputs_new[inputs_new["date"] < cut]
            if len(new):
                new = new[new["ts"].dt.date < cut].reset_index(drop=True)
            if inputs_new.empty:
                return "DEFERRED"
        derived = self.calc.derive(sid, pd.concat([stored[INPUT_COLUMNS], inputs_new], ignore_index=True), bhav=bhav)
        added = derived[derived["date"] > last]
        days = pd.concat([stored, added], ignore_index=True)[DAY_COLUMNS]
        if len(new):
            for year, g in new.groupby(new["ts"].dt.year):
                p = os.path.join(path, f"{year}.parquet")
                old = pq.read_table(p).to_pandas() if os.path.isfile(p) else None
                merged = pd.concat([old, g], ignore_index=True) if old is not None else g
                merged = merged.drop_duplicates("ts", keep="last").sort_values("ts", kind="stable")
                ms.write_minutes(merged, p)
        ms.write_days(days, os.path.join(path, ms.DAYS_FILE))
        self.save_stock(item, days, int(st["minute_rows"] or 0) + len(new))
        self.add(updated=1, rows=len(new), new_days=len(added),
                 problem_days=int((added["check"] != "OK").sum()))
        del stored, derived, days, new
        gc.collect()
        return "UPDATED"

    def progress(self, total, force=False):
        s = self.stats
        if not force and s["done"] % PROGRESS_EVERY:
            return
        el = max(time.monotonic() - s["started"], 1e-9)
        rate = self.limiter.requests / el
        left = total - s["done"]
        eta = (el / s["done"] * left / 3600) if s["done"] else 0
        now, peak = memory()
        self.log(f"  {s['done']:,} / {total:,} stocks, {s['chunks']:,} chunks, {s['rows']:,} rows, {rate:.2f} req/s, "
                 f"{self.limiter.n429:,} x 429, about {eta:.1f} h left, disk {disk_used(self.folder) / 1e9:.2f} GB, "
                 f"memory {gb(now)} (peak {gb(peak)})")

    def run(self, items, task):
        """Run task(item) for every item on `workers` threads. Returns when all are done, or stopped (Ctrl+C,
        StopJob) or a sign-in is needed (AuthStop: both workers stop)."""
        STOP.clear()
        queue = list(items)
        qlock = threading.Lock()
        errors = []
        total = len(queue)

        def worker():
            while not STOP.is_set():
                with qlock:
                    if not queue:
                        return
                    item = queue.pop(0)
                    if item["priority"] != self.priority:
                        if self.priority is not None:
                            self.progress(total, force=True)
                            self.log(f"Priority {item['priority']}:")
                        self.priority = item["priority"]
                try:
                    task(item)
                except AuthStop as e:
                    with self.lock:
                        self.stats["auth"] = str(e)
                    STOP.set()
                    return
                except (StopJob, KeyboardInterrupt):
                    with self.lock:
                        self.stats["stopped"] = True
                    STOP.set()
                    return
                except Exception as e:          # an unexpected error stops the run; it is raised below
                    errors.append(e)
                    STOP.set()
                    return
                with self.lock:
                    self.stats["done"] += 1
                    self.stats["stocks"] += 1
                    self.progress(total)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(min(self.workers, max(total, 1)))]
        for t in threads:
            t.start()
        try:
            for t in threads:
                while t.is_alive():
                    t.join(0.2)
        except KeyboardInterrupt:
            self.stats["stopped"] = True
            STOP.set()
            for t in threads:
                t.join()
        STOP.clear()
        self.progress(total, force=True)
        if errors:
            raise errors[0]

    def finish(self):
        s = self.stats
        result = "auth" if s["auth"] else "stopped" if s["stopped"] else "failed" if s["failed_chunks"] else "done"
        s["peak_mb"] = round(memory()[1] / 1e6, 1)
        self.db.end_run(self.run_id, self.limiter.requests, self.limiter.n429, result, s["peak_mb"])
        self.db.close()
        s["requests"], s["n429"] = self.limiter.requests, self.limiter.n429
        s["deferred"], s["unconfirmed"] = sorted(self.deferred), sorted(self.unconfirmed)
        return s


def cut_minutes(path, cut):
    """Drop the bars from the date cut on from a stock folder's year files (a build's temporary folder); returns how
    many were dropped."""
    dropped = 0
    for y, p in sorted(ms.year_paths(path).items()):
        if y < cut.year:
            continue
        df = pq.read_table(p).to_pandas()
        keep = (pd.to_datetime(df["ts"]).dt.tz_convert(ms.TZ).dt.date < cut).to_numpy()
        dropped += int((~keep).sum())
        if keep.all():
            continue
        if keep.any():
            ms.write_minutes(df[keep], p)
        else:
            os.remove(p)
    return dropped


def held_text(stats):
    """The summary lines of the deferred and the stored-unconfirmed days (at most LIST_LINES each)."""
    lines = []
    for key, what in (("deferred", "Deferred (not stored, the next update tries again)"),
                      ("unconfirmed", f"Stored unconfirmed (more than {DEFER_DAYS} trading days old)")):
        rows = stats.get(key) or []
        if not rows:
            continue
        lines.append(f"{what}: {len(rows):,}")
        for r in rows[:LIST_LINES]:
            more = f", {r[3]} day(s) held back" if len(r) > 3 else ""
            lines.append(f"  {r[0]} {iso(r[1])}: {r[2]}{more}")
        if len(rows) > LIST_LINES:
            lines.append(f"  ... {len(rows) - LIST_LINES:,} more")
    return lines


def archive(folder, sid, why):
    """Move a stock's data folder to archive/<sid>_<date>[_n]/ (never deleted)."""
    src = ms.stock_dir(folder, sid)
    if not os.path.isdir(src):
        return None
    base = os.path.join(out_dir(folder), "archive", f"{sid}_{now_ist():%Y-%m-%d}")
    dst, n = base, 1
    while os.path.exists(dst):
        n += 1
        dst = f"{base}_{n}"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.move(src, dst)
    sm.write_text(os.path.join(dst, "archived.txt"), f"{stamp()} {why}\n")
    return dst


def disk_used(folder):
    """Bytes under data/ and staging/. The other worker may remove a built stock's staging meanwhile: a file gone
    between the listing and its size counts 0."""
    total = 0
    for sub in ("data", "staging"):
        for root, _, files in os.walk(os.path.join(out_dir(folder), sub)):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    return total


# ---- Part 3: check files ----

def rebuild_checks(folder, prio):
    """check/minute_check.csv, minute_empty_days.csv, scale_steps.csv and summary.json from every days.parquet, one
    stock at a time: problem rows are appended to minute_check.csv as they come (a temporary file, replaced at the
    end); only per-date counts, scale steps and the MINUTE_EMPTY dates of stocks that have any are kept."""
    base = ms.data_dir(folder)
    sids = sorted(n for n in os.listdir(base) if os.path.isfile(os.path.join(base, n, ms.DAYS_FILE))) \
        if os.path.isdir(base) else []
    out = check_dir(folder)
    os.makedirs(out, exist_ok=True)
    check_path = os.path.join(out, "minute_check.csv")
    tmp = check_path + ".tmp"
    csv_args = dict(index=False, lineterminator="\n", encoding="utf-8")
    pd.DataFrame(columns=CHECK_COLUMNS).to_csv(tmp, **csv_args)
    empty, steps = {}, []
    counts = {}
    with_day, no_min = {}, {}
    not_one, vol_steps, auction = [], [], []
    total_days = 0
    try:
        for sid in sids:
            days = ms.read_days(ms.stock_dir(folder, sid))
            total_days += len(days)
            symbol = days["symbol"].iat[-1] if len(days) else ""
            p = prio.get(sid, 3)
            c = counts.setdefault(str(p), {})
            for chk in days["check"]:
                for x in chk.split("|"):
                    c[x] = c.get(x, 0) + 1
            bad = days[days["check"] != "OK"]
            if len(bad):
                rows = pd.DataFrame({"security_id": sid, "symbol": bad["symbol"].to_numpy(object), "priority": p,
                                     "date": [iso(d) for d in bad["date"]], "check": bad["check"].to_numpy(object),
                                     **{k: bad[k].to_numpy(float) for k in CHECK_COLUMNS[5:-1]},
                                     "note": bad["note"].to_numpy(object)}, columns=CHECK_COLUMNS)
                rows.to_csv(tmp, mode="a", header=False, **csv_args)
            for d, k in zip(days["date"], days["kite_close"].notna() & days["bhav_close"].notna()):
                if k:
                    with_day[d] = with_day.get(d, 0) + 1
            me = days[days["check"].str.contains("MINUTE_EMPTY")]
            if len(me):
                empty[sid] = (symbol, set(me["date"]))
            for d in me["date"]:
                no_min[d] = no_min.get(d, 0) + 1
            steps += scale_steps(sid, symbol, days)
            if (days["minute_scale"] != 1.0).any():
                not_one.append({"security_id": sid, "symbol": symbol, "periods": spans_of(days, "minute_scale")})
            if (days["minute_volume_scale"] != 1.0).any():
                vol_steps.append({"security_id": sid, "symbol": symbol,
                                  "periods": spans_of(days, "minute_volume_scale")})
            ca = days.loc[days["check"].str.contains("CLOSING_AUCTION"), "date"]
            if len(ca):
                auction.append({"security_id": sid, "symbol": symbol, "first_date": iso(ca.min()), "days": len(ca)})
            del days, bad, me, ca
        os.replace(tmp, check_path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    market = sorted(d for d, k in no_min.items() if with_day.get(d, 0) >= EMPTY_MIN and k / with_day[d] > EMPTY_SHARE)
    rows = [{"security_id": "", "symbol": "", "from_date": iso(d), "to_date": iso(d), "days": 1,
             "note": f"market-wide: {no_min[d]:,} of {with_day[d]:,} downloaded stocks with a day candle have no "
                     "minutes"} for d in market]
    market = set(market)
    for sid, (symbol, dates) in empty.items():
        all_days = [sm.as_date(d) for d in pq.read_table(os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE),
                                                          columns=["date"]).column("date").to_pylist()]
        flags = [d in dates and d not in market for d in all_days]
        for a, b, n in mp.runs_of(all_days, flags):
            rows.append({"security_id": sid, "symbol": symbol, "from_date": iso(a), "to_date": iso(b), "days": n,
                         "note": f"{n} day(s) with a Kite day candle and a bhavcopy row but no 1-minute candles"})
    sm.write_csv(pd.DataFrame(rows, columns=EMPTY_COLUMNS), os.path.join(out, "minute_empty_days.csv"))
    sm.write_csv(pd.DataFrame(steps, columns=STEP_COLUMNS), os.path.join(out, "scale_steps.csv"))
    summary = {"built_at": stamp(), "stocks": len(sids), "days": total_days,
               "checks_per_priority": dict(sorted(counts.items())),
               "checks": {k: sum(c.get(k, 0) for c in counts.values()) for k in ["OK"] + CHECKS},
               "minute_empty_market_days": [iso(d) for d in sorted(market)],
               "minute_scale_not_one": not_one, "volume_steps": vol_steps, "closing_auction": auction,
               "scale_steps_flagged": sum(1 for s in steps if s["flagged"] == "Y")}
    mp.write_json(os.path.join(out, "summary.json"), summary)
    return summary


def spans_of(days, col):
    out = []
    for d, v in zip(days["date"], days[col]):
        if out and out[-1][2] == v:
            out[-1][1] = d
        else:
            out.append([d, d, v])
    return [f"{iso(a)}..{iso(b)}: {v:g}" for a, b, v in out]


# ---- commands ----

def check_workers(workers):
    if not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"workers must be 1 to {MAX_WORKERS}, not {workers}")


def _prepare(folder, command, log, dry_run=False):
    """The inputs without the bhavcopy rows (read per stock later); a real run also takes the lock, marks runs left
    'running' by a killed process ABORTED and rebuilds a stale plan.csv."""
    inp = mp.Inputs(folder, bhav=False)
    if dry_run:
        return inp, None
    mp.check_fno(config.load_env())
    lock = RunLock(mp.lock_path(folder)).acquire(f"equity_minute.py {command}")
    try:
        mark_aborted(folder, log)
        ensure_plan(folder, inp, log)
    except BaseException:
        lock.release()
        raise
    return inp, lock


def mark_aborted(folder, log=print):
    """Runs still 'running' in the progress db while we hold equity_1min/.lock had their process killed (the OS lock
    goes with the process): mark them ABORTED."""
    if not os.path.isfile(os.path.join(out_dir(folder), "download_progress.sqlite")):
        return []
    db = Progress(folder)
    try:
        ids = db.mark_aborted()
    finally:
        db.close()
    if ids:
        log(f"Marked ABORTED (left 'running' by a stopped process): {', '.join(ids)}")
    return ids


def download(folder=None, kite=None, priority=None, symbols=None, limit=None, speed=SPEED, workers=WORKERS,
             dry_run=False, now=None, log=print):
    """The history download of the work list (Parts 1-3). dry_run: figures only, nothing written or fetched. Ranges
    end at the target day at the latest (target_day): an unfinished day is never stored."""
    folder = folder or config.equity_data_dir()
    check_workers(workers)
    inp, lock = _prepare(folder, "download", log, dry_run)
    try:
        target = target_day(inp.days, now, bhav_holidays(folder))[0]
        items = work_list(folder, inp, priority, symbols, target)
        chunks, stocks, _ = read_progress(folder)
        by_sid = {}
        for c in chunks:
            by_sid.setdefault(c["security_id"], {})[c["chunk_from"]] = c
        state = {i["security_id"]: stock_state(i, stocks, by_sid, folder)[0] for i in items}
        todo = [i for i in items if state[i["security_id"]] not in ("DONE", NO_KITE)]
        skipped = [i["symbol"] for i in items if state[i["security_id"]] == NO_KITE]
        if limit:
            todo = todo[:limit]
            keep = {i["security_id"] for i in todo}
            shown = [i for i in items if i["security_id"] in keep]
        else:
            shown = items
        est = estimate(shown, folder, inp.days, speed, workers)
        free = free_space(folder)
        text = estimate_text(est, free)
        if skipped:
            text += f"Skipped, Kite has no data ({NO_KITE}, see check/no_kite_data.csv): {len(skipped):,} " \
                    f"({', '.join(skipped[:20])}{' ...' if len(skipped) > 20 else ''})\n"
        if dry_run:
            stale = plan_stale(folder)
            if stale:
                text = f"plan.csv is older than the {stale}: a real run rebuilds it offline first.\n" + text
            log(text, end="")
            return {"dry_run": True, "estimate": est, "free": free, "items": shown, "report": text}
        if free < est["total"]["bytes"] * DISK_MARGIN:
            raise NotEnoughDisk(f"Not enough disk: about {est['total']['bytes'] * DISK_MARGIN / 1e9:.1f} GB needed "
                                f"(estimate + 20%), {free / 1e9:.1f} GB free on the data drive.")
        log(text, end="")
        kite = kite or mp.kite_client()
        r = Runner(folder, inp, kite, speed, workers, "download", log)
        try:
            r.run(todo, r.history)
        finally:
            stats = r.finish()
            rebuild_checks(folder, {i["security_id"]: i["priority"] for i in items})
        if stats["auth"]:
            raise mp.SignInNeeded("sign in (python app.py) and run the same command again; finished chunks are kept.")
        return {"dry_run": False, "stats": stats, "estimate": est, "items": todo}
    finally:
        if lock:
            lock.release()


def update(folder=None, kite=None, speed=SPEED, workers=WORKERS, now=None, log=print):
    """The daily update (Part 5): new tokens / listings, then each DONE stock from its next day to the target day T
    (target_day: the latest trading day finished by 01:00 IST the next day), fixed once at the start; unfinished days
    are deferred (Runner.hold_back). docs/dev/equity_minute_finished_days.md."""
    folder = folder or config.equity_data_dir()
    check_workers(workers)
    inp, lock = _prepare(folder, "update", log)
    try:
        now = as_ist(now)
        today = now.date()
        target, why, in_store = target_day(inp.days, now, bhav_holidays(folder))
        if not in_store:
            raise NoBhavToday(f"No bhavcopy for the target day {target} {target:%a} in the store yet (newest "
                              f"{inp.days[-1]}). Run python equity_update.py first.")
        items = work_list(folder, inp, target=target)
        _, stocks, _ = read_progress(folder)
        missing = [i for i in items if not i["new"]
                   and (stocks.get(i["security_id"]) or {}).get("status") not in ("DONE", NO_KITE)]
        if missing:
            raise HistoryNotDone(f"{len(missing):,} stocks have no full history yet (e.g. {missing[0]['symbol']}). "
                                 "Run python equity_minute.py download first.")
        kite = kite or mp.kite_client()
        try:
            instruments = kite.instruments("NSE")
        except kex.TokenException as e:
            raise mp.SignInNeeded(f"Kite session expired or invalid ({type(e).__name__}). Sign in (python app.py) "
                                  "and run the same command again.")
        out = update_dir(folder)
        os.makedirs(out, exist_ok=True)
        sm.write_csv(pd.DataFrame(instruments), os.path.join(out, f"kite_instruments_NSE_{today.isoformat()}.csv"))
        toks = pd.DataFrame(mp.match_tokens(inp.universe, instruments), columns=mp.TOKEN_COLUMNS)
        sm.write_csv(toks, os.path.join(out, "tokens.csv"))
        phase1 = set(mp.read_tokens(folder)["security_id"])
        known = {i["security_id"] for i in items}
        new_path = os.path.join(out, "new_listings.csv")
        listed = pd.read_csv(new_path, dtype=str, keep_default_na=False) if os.path.isfile(new_path) else \
            pd.DataFrame(columns=NEW_LISTING_COLUMNS)
        fresh = []
        for t in toks.itertuples():
            if t.match_status in mp.MATCHED and t.security_id not in phase1 and t.security_id not in known:
                u = inp.by_id[t.security_id]
                fresh.append({"security_id": t.security_id, "symbol": t.symbol, "instrument_token": t.instrument_token,
                              "minute_first_day": iso(max(u["first_date"], mp.KITE_MINUTE_FROM)), "added": iso(today)})
        if fresh:
            listed = pd.concat([listed, pd.DataFrame(fresh, columns=NEW_LISTING_COLUMNS)], ignore_index=True)
            sm.write_csv(listed, new_path)
        back = back_in_list(folder, inp, kite, toks, stocks, speed, log)
        if back:
            _, stocks, _ = read_progress(folder)
        items = work_list(folder, inp, target=target)
        last_before = min((sm.as_date(s["last_day"]) for s in stocks.values() if s["status"] == "DONE"),
                          default=target)
        full, daily = [], []
        for i in items:
            st = stocks.get(i["security_id"])
            if st and st["status"] == NO_KITE:
                continue
            if st and st["status"] == "DONE" and st["token"] == str(i["token"]):
                daily.append(i)
            else:
                full.append(i)
        r = Runner(folder, inp, kite, speed, workers, "update", log)
        log(f"Update to {target} {target:%a} (target day: {why})")
        log(f"Update to {target}: {len(daily):,} stocks, {len(full):,} full downloads "
            f"({len(fresh):,} new listings, {sum(1 for i in full if not i['new']):,} token changes)")

        def task(item):
            st = stocks.get(item["security_id"])
            if st and st["status"] == "DONE" and st["token"] == str(item["token"]):
                return r.append(item, st, target)
            return r.history(item)

        try:
            r.run(full + daily, task)
        finally:
            stats = r.finish()
            rebuild_checks(folder, {i["security_id"]: i["priority"] for i in items})
        if stats["auth"]:
            raise mp.SignInNeeded("sign in (python app.py) and run the same command again; finished days are kept.")
        gone = ex.exits_between(last_before, target, folder=folder)
        exits = [f"{x.last_symbol} ({x.security_id}, last day {x.last_date})" for x in gone.itertuples()
                 if x.security_id in known]
        stats.update(new_listings=[f["symbol"] for f in fresh], exits=exits, today=iso(today), target=iso(target),
                     target_why=why)
        log(f"Target day {target}: updated {stats['updated']:,} stocks ({stats['new_days']:,} new days, "
            f"{stats['problem_days']:,} with a problem check), {stats['deferred_days']:,} deferred days "
            f"({stats['deferred_stocks']:,} stocks), {stats['unconfirmed_days']:,} stored unconfirmed, "
            f"{stats['up_to_date']:,} already up to date, {stats['built']:,} full histories built, "
            f"{stats['failed_stocks']:,} failed.")
        for line in held_text(stats):
            log(line)
        if fresh:
            log("New listings: " + ", ".join(stats["new_listings"]))
        if exits:
            log("Exited (no more days): " + "; ".join(exits))
        return {"stats": stats}
    finally:
        lock.release()


def price_check(kite, limiter, token, u, tol=mp.PRICE_TOL):
    """(ok, text): Kite's day close for `token` on the stock's last bhavcopy day within tol (2%) of the bhavcopy close.
    One day-candle request (PRICE_DAYS days). No candle that day, no bhavcopy close or a failed request is not ok."""
    day, close = u["last_date"], u["last_close"]
    if not close or pd.isna(close):
        return False, f"no bhavcopy close on {day}"
    candles = []
    try:
        for s, e in make_chunks(day - timedelta(days=PRICE_DAYS), day, "day"):
            candles += fetch_chunk(kite, limiter, token, s, e, "day")
    except AuthStop as e:
        raise mp.SignInNeeded(f"Kite session expired or invalid ({e}). Sign in (python app.py) and run the same "
                              "command again.")
    except ChunkFailed as e:
        return False, f"day candles failed: {e}"
    df = mp.day_frame(candles)
    hit = df[df["date"] == day]
    if not len(hit):
        return False, f"no Kite day candle on {day}"
    k = float(hit["close"].iloc[0])
    diff = abs(k - close) / close
    ok = diff <= tol
    return ok, f"{'OK' if ok else 'OFF'} {diff:.2%}: Kite {k:g} vs bhavcopy {close:g} on {day}"


def write_no_kite(folder, inp):
    """check/no_kite_data.csv from the NO_KITE_DATA rows of the progress db (for the vendor list and phase 1's
    gaps)."""
    _, stocks, _ = read_progress(folder)
    rows = []
    for sid, st in sorted(stocks.items()):
        if st["status"] != NO_KITE:
            continue
        u = inp.by_id.get(sid, {})
        rows.append({"security_id": sid, "symbol": u.get("symbol", ""), "isin": u.get("isin", ""),
                     "last_bhav_day": iso(u.get("last_date")), "reason": st.get("reason") or ""})
    os.makedirs(check_dir(folder), exist_ok=True)
    path = os.path.join(check_dir(folder), "no_kite_data.csv")
    sm.write_csv(pd.DataFrame(rows, columns=NO_KITE_COLUMNS), path)
    return path


def back_in_list(folder, inp, kite, toks, stocks, speed=SPEED, log=print):
    """update: NO_KITE_DATA stocks matched again in today's token match get retoken's price check (one day-candle
    request each). Passed: the state is cleared (a full download follows). Failed or no candle: they stay
    NO_KITE_DATA and are logged as REVIEW. Returns the symbols that came back."""
    nk = sorted(sid for sid, s in stocks.items() if s["status"] == NO_KITE and sid in inp.by_id)
    matched = {t.security_id: t for t in toks.itertuples() if t.match_status in mp.MATCHED and t.instrument_token}
    nk = [sid for sid in nk if sid in matched]
    if not nk:
        return []
    limiter = SharedLimiter(speed)
    back = []
    db = Progress(folder)
    try:
        for sid in nk:
            t, u = matched[sid], inp.by_id[sid]
            ok, text = price_check(kite, limiter, int(t.instrument_token), u)
            if ok:
                db.drop_stock(sid)
                back.append(u["symbol"])
                log(f"Back in Kite's list: {u['symbol']} ({sid}) as {t.kite_tradingsymbol} ({t.instrument_token}), "
                    f"price {text}: full download.")
            else:
                log(f"REVIEW {u['symbol']} ({sid}): back in Kite's list as {t.kite_tradingsymbol} "
                    f"({t.instrument_token}), but {text}; stays {NO_KITE}.")
    finally:
        db.close()
    if back:
        write_no_kite(folder, inp)
    return back


def probe_empty(folder, inp):
    """{security_id: token} of the ACTIVE plan stocks the probe found no day candles for (phase 1's BOTH /
    NO_KITE_DATA gap row "the token returned no day candles"): matched to a token, probed DONE with that token, a blank
    eod_first_day, download_minute = N. A NO_TOKEN listing has no token and a not probed / FAILED probe no DONE row:
    neither is here."""
    plan = pd.read_csv(plan_path(folder), dtype=str, keep_default_na=False)
    probes = mp.read_probes(folder)
    out = {}
    for r in plan.itertuples():
        p = probes.get(r.security_id)
        if (r.status == "ACTIVE" and r.match_status in mp.MATCHED and r.instrument_token and r.download_minute == "N"
                and not r.eod_first_day and r.security_id in inp.by_id and p and p["status"] == "DONE"
                and p["instrument_token"] == r.instrument_token and not p["eod_first_day"]):
            out[r.security_id] = int(r.instrument_token)
    return out


def reprobe(kite, limiter, token, u, bhav, today, out):
    """Probe a probe-empty stock again the way phase 1 did (mp.Prober: day candles in chunks, the 2% price check, the
    first minute day), the price check on the latest finished bhavcopy day before today. Writes only
    <out>/kite_day/<security_id>.parquet. Returns (result, price_check, note, probe row or None)."""
    days = bhav[bhav["date"] < today] if bhav is not None else None
    if days is None or not len(days):
        return "REVIEW", "", f"no bhavcopy day before {today}", None
    day, close = days["date"].iloc[-1], float(days["close"].iloc[-1])
    try:
        row = mp.Prober(kite, limiter).probe(token, {**u, "last_close": close}, list(days["date"]), out, mp.PRICE_TOL)
    except AuthStop as e:
        raise mp.SignInNeeded(f"Kite session expired or invalid ({e}). Sign in (python app.py) and run the same "
                              "command again.")
    except ChunkFailed as e:
        return "REVIEW", "", f"probe found no day candles; the new probe failed: {e}", None
    if not row["eod_first_day"]:
        return NO_KITE, "", "the token still returns no day candles", row
    note = (f"probe found no day candles; now day candles from {row['eod_first_day']}, minutes from "
            f"{row['minute_first_day'] or 'none'}")
    if row["price_kite_close"] is None:
        return "REVIEW", f"no Kite day candle on {day}", note, row
    k = row["price_kite_close"]
    diff = abs(k - close) / close
    text = f"{'OK' if row['price_status'] == 'MATCHED' else 'OFF'} {diff:.2%}: Kite {k:g} vs bhavcopy {close:g} on {day}"
    return (PROBE_RECOVERED if row["price_status"] == "MATCHED" else "REVIEW"), text, note, row


def _replace(tmp, path):
    os.replace(tmp, path)


def build_recovered(folder, recovered, work, now=None):
    """Phase 1's build (mp.build_outputs) in <work>/build, never in the data folder: a copy of the plan folder's tokens,
    probe db (plus the new probe rows) and kite_day files (plus the re-probed ones); the inputs are read from the real
    folder. Its gap rows lack the download's corrections (no data/ or check/ there) and are not used. Returns the
    temp plan folder."""
    tmp = os.path.join(work, "build")
    tplan, pdir = mp.plan_dir(tmp), mp.plan_dir(folder)
    os.makedirs(os.path.join(tplan, "kite_day"))
    for name in ("kite_tokens.csv", "tokens_info.json", "probe_progress.sqlite"):
        if os.path.isfile(os.path.join(pdir, name)):
            shutil.copy2(os.path.join(pdir, name), os.path.join(tplan, name))
    kd = os.path.join(pdir, "kite_day")
    for name in (os.listdir(kd) if os.path.isdir(kd) else []):
        if name.endswith(".parquet"):
            shutil.copy2(os.path.join(kd, name), os.path.join(tplan, "kite_day", name))
    for sid, (row, path) in recovered.items():
        shutil.copy2(path, os.path.join(tplan, "kite_day", f"{sid}.parquet"))
    db = sqlite3.connect(os.path.join(tplan, "probe_progress.sqlite"))
    try:
        db.execute(mp.PROBE_SQL)
        for row, _ in recovered.values():
            mp.save_probe(db, row)
    finally:
        db.close()
    mp.build_outputs(tmp, mp.Inputs(folder), now=now, log=quiet)
    return tplan


def spliced(old, new, ids):
    """old's rows of every other stock unchanged plus new's rows of `ids`, in security_id order when old was."""
    out = pd.concat([old[~old["security_id"].isin(ids)], new.loc[new["security_id"].isin(ids), list(old.columns)]],
                    ignore_index=True)
    if old["security_id"].is_monotonic_increasing:
        out = out.sort_values("security_id", kind="stable")
    return out


def apply_recovered(folder, recovered, work, now=None):
    """retoken --apply for the PROBE_RECOVERED stocks ({security_id: (probe row, re-probed kite_day file)}): what the
    probe would have written. Backs up the targets (plan.csv, kite_vs_bhav.parquet, kite_adjustment.csv,
    eod_mismatch.csv, probe_progress.sqlite, kite_day/<sid>.parquet) to archive/plan_retoken_<time>/, builds in a temp
    folder, writes every target to <target>.tmp, then replaces them all. kite_gaps.csv and the other phase 1 outputs
    are not written (the next gaps run). All or nothing: any error restores every target from the backup
    (ApplyFailed). Returns the backup folder."""
    pdir = mp.plan_dir(folder)
    ids = set(recovered)
    targets = [os.path.join(pdir, n) for n in PLAN_TARGETS] + \
        [os.path.join(pdir, "kite_day", f"{sid}.parquet") for sid in sorted(ids)]
    backup = os.path.join(out_dir(folder), "archive", f"plan_retoken_{now_ist():%Y-%m-%d_%H%M%S}")
    existed = {}
    for p in targets:
        existed[p] = os.path.isfile(p)
        if existed[p]:
            dst = os.path.join(backup, os.path.relpath(p, pdir))
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(p, dst)
    tmps = []
    try:
        tplan = build_recovered(folder, recovered, work, now)
        for name in ("plan.csv", "kite_adjustment.csv", "eod_mismatch.csv"):
            p = os.path.join(pdir, name)
            new = pd.read_csv(os.path.join(tplan, name), dtype=str, keep_default_na=False)
            old = pd.read_csv(p, dtype=str, keep_default_na=False) if existed[p] else new.iloc[:0]
            tmps.append((p + ".tmp", p))
            spliced(old, new, ids).to_csv(p + ".tmp", index=False, lineterminator="\n", encoding="utf-8")
        p = os.path.join(pdir, "kite_vs_bhav.parquet")
        new = pq.read_table(os.path.join(tplan, "kite_vs_bhav.parquet")).to_pandas()
        old = pq.read_table(p).to_pandas() if existed[p] else new.iloc[:0]
        kvb = spliced(old, new, ids)
        if old["security_id"].is_monotonic_increasing:
            kvb = kvb.sort_values(["security_id", "date"], kind="stable")
        tmps.append((p + ".tmp", p))
        pq.write_table(pa.Table.from_pandas(kvb, preserve_index=False), p + ".tmp")
        p = os.path.join(pdir, "probe_progress.sqlite")
        tmps.append((p + ".tmp", p))
        if existed[p]:
            shutil.copy2(p, p + ".tmp")
        db = sqlite3.connect(p + ".tmp")
        try:
            db.execute(mp.PROBE_SQL)
            for row, _ in recovered.values():
                mp.save_probe(db, row)
        finally:
            db.close()
        os.makedirs(os.path.join(pdir, "kite_day"), exist_ok=True)
        for sid in sorted(ids):
            p = os.path.join(pdir, "kite_day", f"{sid}.parquet")
            tmps.append((p + ".tmp", p))
            shutil.copy2(os.path.join(tplan, "kite_day", f"{sid}.parquet"), p + ".tmp")
        for tmp, p in tmps:
            _replace(tmp, p)
    except Exception as e:
        for tmp, _ in tmps:
            if os.path.exists(tmp):
                os.remove(tmp)
        for p in targets:
            if existed[p]:
                shutil.copy2(os.path.join(backup, os.path.relpath(p, pdir)), p)
            elif os.path.exists(p):
                os.remove(p)
        raise ApplyFailed(f"retoken --apply failed ({type(e).__name__}: {e}); plan files restored from {backup}") \
            from e
    return backup


def retoken(folder=None, kite=None, apply=False, speed=SPEED, now=None, log=print):
    """Match every plan stock that is not DONE again in today's Kite instrument list (one request; phase 1's
    match_tokens): SAME_TOKEN, NEW_TOKEN, SERIES_SWITCH (both need the 2% price check, one day-candle request each),
    NO_KITE_DATA (not in the list) or REVIEW (several candidates, not ACTIVE, a failed price check; never guessed).
    Dry run (default): the table only, nothing written. apply: takes equity_1min/.lock, saves the list, merges the
    checked new tokens into update/tokens.csv (which download and update read), marks NO_KITE_DATA stocks (and clears
    the state of those found again), writes check/no_kite_data.csv and update/retoken_<date>.csv.
    Also the ACTIVE stocks the probe found no day candles for (probe_empty): probed again (reprobe) -> PROBE_RECOVERED
    (price check on the latest finished day OK; apply updates their plan rows as the probe would have,
    apply_recovered), NO_KITE_DATA (still empty; nothing written) or REVIEW (never applied)."""
    folder = folder or config.equity_data_dir()
    mp.check_fno(config.load_env())
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py retoken --apply") if apply else None
    work = None
    try:
        inp = mp.Inputs(folder, bhav=False)
        today = (now or now_ist()).date()
        plan = read_plan(folder)
        _, stocks, _ = read_progress(folder)
        cands = [sid for sid in sorted(plan) if sid in inp.by_id
                 and (stocks.get(sid) or {}).get("status") != "DONE"]
        empty = {sid: tok for sid, tok in probe_empty(folder, inp).items() if sid not in plan}
        if not cands and not empty:
            log("Every plan stock is DONE: nothing to retoken.")
            return {"applied": apply, "rows": [], "counts": {}, "review": 0}
        kite = kite or mp.kite_client()
        try:
            instruments = kite.instruments("NSE")
        except kex.TokenException as e:
            raise mp.SignInNeeded(f"Kite session expired or invalid ({type(e).__name__}). Sign in (python app.py) "
                                  "and run the same command again.")
        matched = {m["security_id"]: m for m in mp.match_tokens([inp.by_id[s] for s in cands + sorted(empty)],
                                                                instruments)}
        limiter = SharedLimiter(speed)
        rows, new_tokens, recovered = [], {}, {}
        for sid in cands:
            u, old = inp.by_id[sid], plan[sid]["token"]
            was_nk = (stocks.get(sid) or {}).get("status") == NO_KITE
            m = matched.get(sid)
            row = {"security_id": sid, "symbol": u["symbol"], "old_token": str(old), "new_token": "",
                   "kite_tradingsymbol": "", "result": "REVIEW", "price_check": "", "note": "", "action": "none"}
            if m is None:
                row["note"] = f"not ACTIVE ({u['status'] or 'no status'}): not matched"
            elif m["match_status"] == "NO_TOKEN":
                row.update(result=NO_KITE, note=m["note"],
                           action="stays " + NO_KITE if was_nk else f"mark {NO_KITE}, skipped by download")
            elif m["match_status"] == "REVIEW":
                row["note"] = m["note"]
            else:
                tok = int(m["instrument_token"])
                row.update(new_token=str(tok), kite_tradingsymbol=m["kite_tradingsymbol"])
                kind = "NEW_TOKEN" if m["match_rule"] == "EXACT" else "SERIES_SWITCH"
                if tok == old:
                    row.update(result="SAME_TOKEN", note=m["match_rule"],
                               action=f"clear {NO_KITE}; download fetches it" if was_nk else "download retries it")
                else:
                    ok, text = price_check(kite, limiter, tok, u)
                    row.update(price_check=text, note=m["note"] if kind == "SERIES_SWITCH" else "")
                    if ok:
                        new_tokens[sid] = m
                        row.update(result=kind, action="write update/tokens.csv; download fetches it" +
                                   (f" (clear {NO_KITE})" if was_nk else ""))
                    else:
                        row["note"] = f"{kind} to {tok}: price check failed" + (f"; {row['note']}" if row["note"]
                                                                                 else "")
            rows.append(row)
        # the stocks the probe found no day candles for (download_minute = N, not in read_plan): probed again
        work = tempfile.mkdtemp(prefix="retoken_")
        bhav = {}
        if empty:
            b = mp.read_bhav(folder, sm.load_master(folder), set(empty))
            bhav = {sid: g.reset_index(drop=True) for sid, g in b.groupby("security_id")}
            del b
        for sid in sorted(empty):
            u, tok, m = inp.by_id[sid], empty[sid], matched.get(sid)
            row = {"security_id": sid, "symbol": u["symbol"], "old_token": str(tok), "new_token": "",
                   "kite_tradingsymbol": "", "result": "REVIEW", "price_check": "", "note": "", "action": "none"}
            if m is None or m["match_status"] not in mp.MATCHED or int(m["instrument_token"] or 0) != tok:
                listed = f"gives token {m['instrument_token']}" if m and m["instrument_token"] else "has no single match"
                row["note"] = f"probe found no day candles; today's Kite list {listed}: run tokens, then probe"
            else:
                row.update(new_token=str(tok), kite_tradingsymbol=m["kite_tradingsymbol"])
                out = os.path.join(work, "probe", sid)
                result, text, note, prow = reprobe(kite, limiter, tok, u, bhav.get(sid), today, out)
                row.update(result=result, price_check=text, note=note)
                if result == PROBE_RECOVERED:
                    recovered[sid] = (prow, os.path.join(out, "kite_day", f"{sid}.parquet"))
                    row["action"] = "plan.csv download_minute = Y (as the probe); download fetches it"
                elif result == NO_KITE:
                    row["action"] = "stays N in plan.csv (phase 1 gap row)"
            rows.append(row)
        table = pd.DataFrame(rows, columns=RETOKEN_COLUMNS)
        counts = table["result"].value_counts().to_dict()
        with pd.option_context("display.width", 250, "display.max_colwidth", 90, "display.max_rows", 500):
            text = table.drop(columns="security_id").to_string(index=False)
        log(f"Kite instrument list {today}: {len(instruments):,} rows; {len(cands):,} plan stock(s) not DONE" +
            (f", {len(empty):,} the probe found no day candles for" if empty else "") +
            f" ({'apply' if apply else 'dry run, nothing written'}):")
        log(text)
        log("Results: " + ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())) +
            f"; {limiter.requests:,} " + ("Kite history request(s)" if empty else "day-candle request(s)"))
        if apply:
            backup = apply_recovered(folder, recovered, work, now) if recovered else None
            out = update_dir(folder)
            os.makedirs(out, exist_ok=True)
            sm.write_csv(pd.DataFrame(instruments), os.path.join(out, f"kite_instruments_NSE_{today.isoformat()}.csv"))
            tpath = os.path.join(out, "tokens.csv")
            old_t = pd.read_csv(tpath, dtype=str, keep_default_na=False) if os.path.isfile(tpath) else \
                pd.DataFrame(columns=mp.TOKEN_COLUMNS)
            if new_tokens:
                add = pd.DataFrame([{**m, "match_status": "MATCHED",
                                     "note": (m["note"] + "; " if m["note"] else "") + f"retoken {today}: " +
                                     table.loc[table["security_id"] == sid, "price_check"].iat[0]}
                                    for sid, m in new_tokens.items()], columns=mp.TOKEN_COLUMNS)
                merged = pd.concat([old_t[~old_t["security_id"].isin(new_tokens)], add], ignore_index=True)
                sm.write_csv(merged.sort_values("security_id", kind="stable"), tpath)
            db = Progress(folder)
            try:
                for r in rows:
                    sid = r["security_id"]
                    if sid in empty:
                        continue
                    was_nk = (stocks.get(sid) or {}).get("status") == NO_KITE
                    if r["result"] == NO_KITE and not was_nk:
                        db.set_no_kite(sid, r["old_token"], f"not in Kite's NSE list of {today}", today)
                    elif r["result"] in ("SAME_TOKEN", "NEW_TOKEN", "SERIES_SWITCH") and was_nk:
                        db.drop_stock(sid)
            finally:
                db.close()
            write_no_kite(folder, inp)
            sm.write_csv(table, os.path.join(out, f"retoken_{today.isoformat()}.csv"))
            nk = sum(1 for r in rows if r["result"] == NO_KITE and r["security_id"] not in empty)
            log(f"Applied: {len(new_tokens):,} new token(s) in {tpath}; {nk:,} {NO_KITE} in check/no_kite_data.csv.")
            if recovered:
                log(f"Applied: {len(recovered):,} probe-recovered stock(s): plan.csv download_minute = Y; backup in "
                    f"{backup}")
        return {"applied": apply, "rows": rows, "counts": counts, "review": counts.get("REVIEW", 0),
                "report": text}
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)
        if lock:
            lock.release()


def failures(chunks, stocks, errors, names):
    """Unresolved failures for status: FAILED chunks, and the newest error of a stock not built since (NO_KITE_DATA
    stocks left out): [(symbol, security_id, kind, error, run_id)]."""
    out = []
    for c in chunks:
        st = stocks.get(c["security_id"]) or {}
        if c["status"] == "FAILED" and st.get("status") != NO_KITE:
            out.append((names.get(c["security_id"], c["security_id"]), c["security_id"],
                        f"MINUTE_CHUNK {c['chunk_from']}", c["error"] or "", c["run_id"]))
    newest = {}
    for e in errors:
        newest[e["security_id"]] = e
    for sid, e in sorted(newest.items()):
        st = stocks.get(sid) or {}
        if st.get("status") == NO_KITE or (st.get("status") == "DONE" and (st.get("built_at") or "") >= e["at"]):
            continue
        out.append((names.get(sid, sid), sid, e["kind"], e["error"] or "", e["run_id"]))
    return sorted(out)


def status(folder=None, speed=SPEED, workers=WORKERS, now=None, log=print):
    """Offline: per priority DONE / partly / not started, chunks, FAILED chunks, rows, disk, ETA, 429s, unresolved
    failures with their error text, NO_KITE_DATA stocks, last run, and the target day update would use now."""
    folder = folder or config.equity_data_dir()
    rows = read_plan(folder)
    try:
        last = ex.load_exits(folder)["calendar"].days[-1]
        days = ex.load_exits(folder)["calendar"].days
    except ex.ExitsError:
        last, days = date.today(), []
    try:
        prio = priorities(im.load_built(folder)["membership"])
    except im.IndexMembershipError:
        prio = {}
    items = []
    for sid, r in rows.items():
        items.append({**r, "symbol": sid, "priority": prio.get(sid, 3), "last": last,
                      "chunks": make_chunks(r["first"], last, "minute") if r["first"] <= last else []})
    chunks, stocks, runs = read_progress(folder)
    est = estimate(items, folder, days, speed, workers)
    failed = sum(1 for c in chunks if c["status"] == "FAILED")
    done_rows = sum(int(s["minute_rows"] or 0) for s in stocks.values() if s["status"] == "DONE")
    staged = sum(int(c["rows"] or 0) for c in chunks if c["status"] == "DONE"
                 and (stocks.get(c["security_id"]) or {}).get("status") != "DONE")
    n429 = sum(int(r["n429"] or 0) for r in runs)
    lines = [estimate_text(est, free_space(folder)).rstrip("\n"),
             f"Chunks: {sum(1 for c in chunks if c['status'] == 'DONE'):,} done, {failed:,} FAILED",
             f"Rows: {done_rows:,} in built stocks, {staged:,} staged",
             f"Disk used: {disk_used(folder) / 1e9:.2f} GB",
             f"429 answers: {n429:,} in {len(runs):,} run(s)"]
    target = None
    if days:
        target, why, _ = target_day(days, now, bhav_holidays(folder))
        lines.append(f"Target day for update now: {target} {target:%a} ({why})")
    try:
        names = dict(zip(*[mp.read_tokens(folder)[c] for c in ("security_id", "symbol")]))
    except mp.NotBuilt:
        names = {}
    fails = failures(chunks, stocks, read_errors(folder), names)
    if fails:
        lines.append(f"Failed (not built yet, run download again or retoken): {len(fails):,}")
        for sym, sid, kind, err, run in fails[:FAILED_LINES]:
            err = err if len(err) <= 150 else err[:147] + "..."
            lines.append(f"  {sym} ({sid}) {kind}: {err} [run {run}]")
        if len(fails) > FAILED_LINES:
            lines.append(f"  ... {len(fails) - FAILED_LINES:,} more")
    nk = sorted((names.get(sid, sid), s.get("reason_at") or "") for sid, s in stocks.items() if s["status"] == NO_KITE)
    if nk:
        lines.append(f"No Kite data ({NO_KITE}): {len(nk):,} stocks ({', '.join(n for n, _ in nk)}), since "
                     f"{min(d for _, d in nk) or '?'}; check/no_kite_data.csv")
    aborted = [r for r in runs if r["result"] == "ABORTED"]
    if aborted:
        lines.append(f"ABORTED runs (process killed, e.g. low memory): {len(aborted):,} "
                     f"({', '.join(r['run_id'] for r in aborted)})")
    if runs:
        r = runs[-1]
        result = r["result"]
        if result == "running":
            result = "running now" if mp.held(mp.lock_path(folder)) else \
                "running, but no live process: the next download / update marks it ABORTED"
        peak = f", peak memory {r['peak_mb'] / 1e3:.2f} GB" if r.get("peak_mb") else ""
        lines.append(f"Last run: {r['command']} {r['started']} .. {r['ended'] or '-'} ({result}, "
                     f"{r['requests'] or 0:,} requests, {r['n429'] or 0:,} x 429{peak})")
    text = "\n".join(lines) + "\n"
    log(text, end="")
    return {"estimate": est, "failed_chunks": failed, "rows": done_rows, "staged_rows": staged, "n429": n429,
            "runs": runs, "aborted": len(aborted), "failures": fails, "no_kite_data": [n for n, _ in nk],
            "target": target, "report": text}


def same_column(a, b):
    """Two days.parquet columns hold the same values (NaN equal to NaN)."""
    try:
        return np.array_equal(a.to_numpy(float), b.to_numpy(float), equal_nan=True)
    except (TypeError, ValueError):
        return [str(x) for x in a] == [str(x) for x in b]


def checks(folder=None, apply=False, log=print):
    """Offline: the check labels of every built stock again with the current rules, re-derived from its stored
    days.parquet input columns (as update's append does). Only `check` may change: a stock where any other column
    would differ is listed and never written. Dry run (default) writes nothing; apply takes equity_1min/.lock,
    rewrites the days.parquet files whose labels changed, then rebuilds check/."""
    folder = folder or config.equity_data_dir()
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py checks --apply") if apply else None
    try:
        inp = mp.Inputs(folder, bhav=False)
        calc = Calc(folder, inp, log, write=apply)
        base = ms.data_dir(folder)
        sids = sorted(n for n in os.listdir(base) if os.path.isfile(os.path.join(base, n, ms.DAYS_FILE))) \
            if os.path.isdir(base) else []
        before, after, changed, refused = {}, {}, [], []
        for sid in sids:
            path = ms.stock_dir(folder, sid)
            stored = ms.read_days(path)
            new = calc.derive(sid, stored[INPUT_COLUMNS])
            for counts, labels in ((before, stored["check"]), (after, new["check"])):
                for chk in labels:
                    for x in chk.split("|"):
                        counts[x] = counts.get(x, 0) + 1
            symbol = stored["symbol"].iat[-1] if len(stored) else ""
            other = [c for c in DAY_COLUMNS if c not in ("check", "date")]
            if len(new) != len(stored) or list(new["date"]) != list(stored["date"]) or \
                    not all(same_column(stored[c], new[c]) for c in other):
                refused.append(f"{symbol} ({sid})")
            else:
                n = int((stored["check"].to_numpy(object) != new["check"].to_numpy(object)).sum())
                if n:
                    changed.append((sid, symbol, n))
                    if apply:
                        ms.write_days(new, os.path.join(path, ms.DAYS_FILE))
            del stored, new
        keys = ["OK"] + CHECKS
        log(f"Checks of {len(sids):,} built stocks with the current rules ({'applied' if apply else 'dry run'}):")
        for k in keys:
            if before.get(k, 0) or after.get(k, 0):
                log(f"  {k:<16} {before.get(k, 0):>9,} -> {after.get(k, 0):,}")
        log(f"{len(changed):,} stock(s) with new labels ({sum(n for _, _, n in changed):,} days)"
            + (": " + ", ".join(f"{s} {n}" for _, s, n in changed[:20]) + (" ..." if len(changed) > 20 else "")
               if changed else ""))
        if refused:
            log(f"Not changed, other columns would differ (a newer bhavcopy or rule): {', '.join(refused)}")
        summary = None
        if apply:
            prio = priorities(inp.membership)
            summary = rebuild_checks(folder, {sid: prio.get(sid, 3) for sid in sids})
            for a in summary["closing_auction"]:
                log(f"  CLOSING_AUCTION {a['symbol']} ({a['security_id']}): first {a['first_date']}, {a['days']:,} days")
        return {"applied": apply, "before": before, "after": after, "changed": changed, "refused": refused,
                "summary": summary}
    finally:
        if lock:
            lock.release()
