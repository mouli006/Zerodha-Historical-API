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
    update/kite_instruments_NSE_<date>.csv, update/tokens.csv, update/new_listings.csv   (update only)
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
import threading
import time
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timedelta

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
MAX_WORKERS = 2
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
CHECKS = ["EXTRA_SESSION", "DAY_EMPTY", "MINUTE_EMPTY", "HIGHLOW_DIFF", "CLOSE_DIFF", "VOLUME_DIFF", "SCALE_STEP"]
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

PART0_FINDINGS = """Part 0 findings (Kite, 2026-10-19; see docs/dev/equity_minute_download.md):
- A minute request takes ~0.79 s, a day request ~0.12 s: one worker reaches ~1.3 req/s, two reach the 2.5 limit.
- 373 rows per Kite day (RELIANCE), 267 (ENERGYDEV); zstd parquet 17.7 / 12.5 bytes per row.
- The last minute close is not the day close (NSE's close is the average of the last 30 minutes); summed minute
  volume is 0.5-1.5% below the day volume (pre-open / closing session), up to 15-27% on some days.
- Kite's minute and day histories are not always on one scale: RELIANCE day / minute = 1.00945 until 2020-05-13.
- Kite has minutes without a day candle (Muhurat 2017-10-19, 2018-11-07; ENERGYDEV 2018-12) and day candles
  without minutes (2016-01-01). F&O stocks end at 15:14 from August 2026 (closing auction)."""


class NotEnoughDisk(mp.MinutePlanError):
    pass


class HistoryNotDone(mp.MinutePlanError):
    pass


class NoBhavToday(mp.MinutePlanError):
    pass


def now_ist():
    return datetime.now(mp.IST)


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
CHUNK_FIELDS = ["security_id", "token", "chunk_from", "chunk_to", "rows", "status", "fetched_at", "run_id", "seconds",
                "error"]
STOCK_FIELDS = ["security_id", "token", "status", "first_day", "last_day", "days", "minute_rows", "bytes", "built_at"]


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


def bhav_cache(folder, inp, log=quiet):
    """bhav_cache/<year>.parquet: the bhavcopy rows of the universe by security_id (bhav_year), rebuilt for the year
    files that changed since (size, mtime), all of them when the master or the universe changed. The caller holds
    equity_1min/.lock. Returns the cache's year file paths."""
    out = cache_dir(folder)
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
        keys = keys or bhav_keys(folder, inp)
        t, _ = bhav_year(path, *keys, CACHE_COLUMNS)
        sm.write_atomic(dst, lambda tmp: pq.write_table(t, tmp, row_group_size=CACHE_ROW_GROUP, compression="zstd"))
        built.append(year)
        del t
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


def work_list(folder, inp, priority=None, symbols=None):
    """Every stock to download, in order: priority 1, 2, 3, then median turnover (largest first)."""
    rows = read_plan(folder)
    prio = priorities(inp.membership)
    turnover = turnover_medians(folder, inp)
    last = inp.days[-1]
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
    """DONE (built with this token), PARTLY (some chunks done), NOT_STARTED; and the chunks still to fetch."""
    st = stocks.get(item["security_id"])
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
        g = groups.setdefault(i["priority"], {"stocks": 0, "DONE": 0, "PARTLY": 0, "NOT_STARTED": 0,
                                               "minute_requests": 0, "day_requests": 0, "bytes": 0.0})
        g["stocks"] += 1
        g[state] += 1
        if todo:
            g["minute_requests"] += len(todo)
            g["day_requests"] += len(make_chunks(i["first"], i["last"], "day"))
            n = bisect_right(days, i["last"]) - bisect_left(days, i["first"])
            g["bytes"] += n * len(todo) / max(len(i["chunks"]), 1) * per_day[i["priority"]]
    total = {k: sum(g[k] for g in groups.values()) for k in
             ("stocks", "DONE", "PARTLY", "NOT_STARTED", "minute_requests", "day_requests", "bytes")}
    secs = total["minute_requests"] * max(1 / speed, lat / workers) + \
        total["day_requests"] * max(1 / speed, DAY_SECONDS / workers)
    return {"groups": dict(sorted(groups.items())), "total": total, "hours": secs / 3600, "latency": lat,
            "latency_measured": len(timed) >= TIMED_MIN, "bytes_per_day": per_day, "speed": speed,
            "workers": workers}


def estimate_text(e, free):
    t = e["total"]
    lines = [f"Stocks: {t['stocks']:,} ({t['DONE']:,} done, {t['PARTLY']:,} partly done, {t['NOT_STARTED']:,} not "
             "started)"]
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
    be in the frame: a build calls it per staged chunk (chunks start at 00:00, so they never split a day)."""
    rows = {}
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

    def __init__(self, folder, inp, log=quiet):
        self.inp = inp
        self.days = inp.days
        self.cache = bhav_cache(folder, inp, log)
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

    def derive(self, sid, inputs):
        """The days.parquet frame of one stock from its INPUT_COLUMNS frame (every date of the stock)."""
        d = inputs.sort_values("date", kind="stable").reset_index(drop=True)
        b = self.bhav_of(sid)
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
        flags = {"EXTRA_SESSION": ~has_bhav, "DAY_EMPTY": has_min & has_bhav & ~has_day,
                 "MINUTE_EMPTY": has_day & has_bhav & ~has_min, "HIGHLOW_DIFF": hl, "CLOSE_DIFF": cl,
                 "VOLUME_DIFF": vol, "SCALE_STEP": np.zeros(n, bool)}
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
                      "auth": None, "started": time.monotonic(), "done": 0}
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

    # history of one stock
    def history(self, item):
        sid, token = item["security_id"], item["token"]
        st = self.db.stocks().get(sid)
        if st and st["status"] == "DONE" and st["token"] != str(token):
            archive(self.folder, sid, f"token {st['token']} -> {token}")
            self.db.reset_stock(sid)
            shutil.rmtree(staging_dir(self.folder, sid), ignore_errors=True)
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
        self.build(item)
        self.add(built=1)
        return "BUILT"

    def build(self, item):
        """All chunks done: write data/<sid>/ (into a temporary folder, then renamed) and mark the stock DONE. The
        staged chunks are read one at a time; at most one year of the stock is held, for its year file."""
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

        days = self.calc.derive(sid, inputs_from(aggs, runs_of_day, frames))
        ms.write_days(days, os.path.join(tmp, ms.DAYS_FILE))
        if os.path.isdir(final):
            archive(self.folder, sid, "rebuilt")
        os.replace(tmp, final)
        self.save_stock(item, days, n)
        shutil.rmtree(stage, ignore_errors=True)
        del days, frames, aggs
        gc.collect()

    def save_stock(self, item, days, rows):
        path = ms.stock_dir(self.folder, item["security_id"])
        size = sum(os.path.getsize(os.path.join(path, n)) for n in os.listdir(path))
        self.db.save_stock(security_id=item["security_id"], token=str(item["token"]), status="DONE",
                           first_day=iso(days["date"].iat[0]) if len(days) else iso(item["first"]),
                           last_day=iso(days["date"].iat[-1]) if len(days) else iso(item["last"]),
                           days=len(days), minute_rows=int(rows), bytes=size, built_at=stamp())

    # daily update of one DONE stock
    def append(self, item, st, today):
        sid, token = item["security_id"], item["token"]
        last = sm.as_date(st["last_day"])
        a = last + timedelta(days=1)
        if a > today:
            self.add(up_to_date=1)
            return "UP_TO_DATE"
        try:
            dayf = self.fetch_days(token, a, today)
            candles = []
            for s, e in make_chunks(a, today, "minute"):
                if STOP.is_set():
                    raise StopJob()
                candles += self.fetch(token, s, e, "minute")
        except ChunkFailed as e:
            self.log(f"  {item['symbol']}: FAILED {e}")
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
        derived = self.calc.derive(sid, pd.concat([stored[INPUT_COLUMNS], inputs_new], ignore_index=True))
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
        return s


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
    not_one, vol_steps = [], []
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
            del days, bad, me
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
               "minute_scale_not_one": not_one, "volume_steps": vol_steps,
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
             dry_run=False, log=print):
    """The history download of the work list (Parts 1-3). dry_run: figures only, nothing written or fetched."""
    folder = folder or config.equity_data_dir()
    inp, lock = _prepare(folder, "download", log, dry_run)
    try:
        items = work_list(folder, inp, priority, symbols)
        chunks, stocks, _ = read_progress(folder)
        by_sid = {}
        for c in chunks:
            by_sid.setdefault(c["security_id"], {})[c["chunk_from"]] = c
        todo = [i for i in items if stock_state(i, stocks, by_sid, folder)[0] != "DONE"]
        if limit:
            todo = todo[:limit]
            keep = {i["security_id"] for i in todo}
            shown = [i for i in items if i["security_id"] in keep]
        else:
            shown = items
        est = estimate(shown, folder, inp.days, speed, workers)
        free = free_space(folder)
        text = estimate_text(est, free)
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
    """The evening update (Part 5): new tokens / listings, then each DONE stock from its next day to today."""
    folder = folder or config.equity_data_dir()
    inp, lock = _prepare(folder, "update", log)
    try:
        today = (now or now_ist()).date()
        if inp.days[-1] < today:
            raise NoBhavToday(f"Today's bhavcopy ({today}) is not in the store yet (newest {inp.days[-1]}). "
                              "Run python equity_update.py first.")
        items = work_list(folder, inp)
        _, stocks, _ = read_progress(folder)
        missing = [i for i in items if not i["new"] and (stocks.get(i["security_id"]) or {}).get("status") != "DONE"]
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
        items = work_list(folder, inp)
        last_before = min((sm.as_date(s["last_day"]) for s in stocks.values() if s["status"] == "DONE"), default=today)
        full, daily = [], []
        for i in items:
            st = stocks.get(i["security_id"])
            if st and st["status"] == "DONE" and st["token"] == str(i["token"]):
                daily.append(i)
            else:
                full.append(i)
        r = Runner(folder, inp, kite, speed, workers, "update", log)
        log(f"Update to {today}: {len(daily):,} stocks, {len(full):,} full downloads "
            f"({len(fresh):,} new listings, {sum(1 for i in full if not i['new']):,} token changes)")

        def task(item):
            st = stocks.get(item["security_id"])
            if st and st["status"] == "DONE" and st["token"] == str(item["token"]):
                return r.append(item, st, today)
            return r.history(item)

        try:
            r.run(full + daily, task)
        finally:
            stats = r.finish()
            rebuild_checks(folder, {i["security_id"]: i["priority"] for i in items})
        if stats["auth"]:
            raise mp.SignInNeeded("sign in (python app.py) and run the same command again; finished days are kept.")
        gone = ex.exits_between(last_before, today, folder=folder)
        exits = [f"{x.last_symbol} ({x.security_id}, last day {x.last_date})" for x in gone.itertuples()
                 if x.security_id in known]
        stats.update(new_listings=[f["symbol"] for f in fresh], exits=exits, today=iso(today))
        log(f"Updated {stats['updated']:,} stocks ({stats['new_days']:,} new days, {stats['problem_days']:,} with a "
            f"problem check), {stats['up_to_date']:,} already up to date, {stats['built']:,} full histories built, "
            f"{stats['failed_stocks']:,} failed.")
        if fresh:
            log("New listings: " + ", ".join(stats["new_listings"]))
        if exits:
            log("Exited (no more days): " + "; ".join(exits))
        return {"stats": stats}
    finally:
        lock.release()


def status(folder=None, speed=SPEED, workers=WORKERS, log=print):
    """Offline: per priority DONE / partly / not started, chunks, FAILED chunks, rows, disk, ETA, 429s, last run."""
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
            "runs": runs, "aborted": len(aborted), "report": text}
