"""ICICI 1-minute source, parts B and C: the download (icici_minute.py download), the per-day conversion to the
bhavcopy and checks, status, checks and coverage. docs/dev/equity_icici_source.md has the rules and the evidence.

Inputs, only read: the ICICI plan (backfill/equity/icici_plan.py), the bhavcopy (our cache under
equity_1min_icici/bhav_cache), the factors, the corporate actions, the exit list and the index membership. The Kite
store (equity_1min/) is never written. Network: ICICI Breeze only (customerdetails once per run, historical candles).
Output under <folder>/equity_1min_icici/ (every file atomic):
    download_progress.sqlite      calls (per IST day: the quota), chunks, stocks, runs, errors (resume)
    staging/<security_id>/        fetched chunks not built yet (removed after the build)
    data/<security_id>/           <year>.parquet (ICICI candles unchanged), icici_day.parquet, days.parquet
    archive/<security_id>_<time>/ the staging of a SYMBOL_ONLY stock whose price check failed (REVIEW)
    check/icici_check.csv, summary.json, scale_steps.csv, coverage.csv
One stock at a time; a build holds one stock's minutes (~1 million rows at most)."""
import json
import os
import shutil
import sqlite3
import threading
import time
from bisect import bisect_left, bisect_right
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config
from backfill import fetch
from backfill.equity import icici_client as ic
from backfill.equity import icici_plan as ip
from backfill.equity import icici_store as st
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.fetch import RateLimiter, StopJob
from backfill.locks import RunLock

BACKOFF = 60                     # seconds to wait after a rate-limit answer; a second one in a row stops the run
RETRIES = 3                      # tries of a call that fails otherwise (network, server error)
PRICE_TOL = mp.PRICE_TOL         # SYMBOL_ONLY: close / high / low within 2% of the bhavcopy after conversion
SCALE_AGREE = md.SCALE_AGREE     # icici_scale is measured where bhav/ICICI high and low agree within 0.1% (+ ticks)
SCALE_STEP = md.SCALE_STEP       # a new icici_scale level: the median moves by more than 0.1% for 5 measured days
SCALE_ONE = md.SCALE_ONE         # a price level within 0.05% of 1 is 1.0
VOLUME_STEP = md.VOLUME_STEP     # a new volume level: the ratio moves by more than 20% for 5 measured days
VOLUME_ONE = 0.05                # a volume level within 5% of 1 is 1.0
VOLUME_HIGH, VOLUME_LOW = md.VOLUME_HIGH, md.VOLUME_LOW
SHORT_FROM, SHORT_TO = "10:00", "15:00"   # SHORT_DAY: a regular session's first bar after 10:00 or last before 15:00
EVENING = "16:00"                # a session starting at or after 16:00 (Muhurat) is not a short day
AUCTION_FROM, AUCTION_TO = md.AUCTION_FROM, md.AUCTION_TO
FAILED_LINES = 30
BEFORE_START = "ICICI_BEFORE_START"
EMPTY = "ICICI_EMPTY"
CHECKS = [BEFORE_START, EMPTY, "EXTRA_SESSION", "DAY_EMPTY", "SCALE_UNSURE", "SHORT_DAY", "HIGHLOW_DIFF",
          "CLOSE_DIFF", "CLOSING_AUCTION", "VOLUME_DIFF", "SCALE_STEP"]
FIELDS = mp.FIELDS
DAY_COLUMNS = ["date", "symbol", "series"] + [f"icici_{f}" for f in FIELDS] + [f"bhav_{f}" for f in FIELDS] + \
    ["our_factor", "ratio", "icici_scale", "icici_scale_measured", "icici_volume_scale", "vol_ratio", "kind",
     "minute_rows", "bars_outside", "first_bar", "last_bar", "minute_open", "minute_high", "minute_low",
     "minute_close", "minute_volume", "check", "conversion_from", "note", "icici_code", "match_kind", "fetched_at",
     "run_id"]
CHECK_COLUMNS = ["security_id", "symbol", "priority", "date", "check", "icici_scale", "icici_volume_scale",
                 "minute_high", "minute_low", "minute_volume", "bars_outside", "bhav_high", "bhav_low", "bhav_close",
                 "bhav_volume", "note"]
COVERAGE_COLUMNS = ["row", "data_missing", "security_id", "symbols", "reason", "need_from", "need_to", "priority",
                    "stocks", "days", "kite_covered", "icici_needed", "icici_stored", "icici_empty",
                    "icici_before_start", "to_do", "still_missing", "state"]
STEP_COLUMNS = md.STEP_COLUMNS

SQL = ["""CREATE TABLE IF NOT EXISTS calls (n INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT, ist_date TEXT, run_id TEXT,
          kind TEXT, security_id TEXT, result TEXT, seconds REAL, bars INTEGER)""",
       "CREATE INDEX IF NOT EXISTS calls_day ON calls (ist_date)",
       """CREATE TABLE IF NOT EXISTS chunks (security_id TEXT, interval TEXT, chunk_from TEXT, chunk_to TEXT,
          code TEXT, bars INTEGER, day_bars TEXT, status TEXT, built INTEGER, fetched_at TEXT, run_id TEXT,
          seconds REAL, error TEXT, PRIMARY KEY (security_id, interval, chunk_from))""",
       """CREATE TABLE IF NOT EXISTS stocks (security_id TEXT PRIMARY KEY, code TEXT, match_kind TEXT, status TEXT,
          first_minute_day TEXT, price_check TEXT, reason TEXT, days INTEGER, minute_rows INTEGER, built_at TEXT)""",
       """CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, command TEXT, started TEXT, ended TEXT,
          calls INTEGER, result TEXT, peak_mb REAL)""",
       """CREATE TABLE IF NOT EXISTS errors (security_id TEXT, run_id TEXT, kind TEXT, chunk_from TEXT, error TEXT,
          at TEXT)"""]
CHUNK_FIELDS = ["security_id", "interval", "chunk_from", "chunk_to", "code", "bars", "day_bars", "status", "built",
                "fetched_at", "run_id", "seconds", "error"]
STOCK_FIELDS = ["security_id", "code", "match_kind", "status", "first_minute_day", "price_check", "reason", "days",
                "minute_rows", "built_at"]
NONE = "NONE"                    # first_minute_day when ICICI had no minutes even on the last needed day


class CapReached(mp.MinutePlanError):
    pass


class RateStop(mp.MinutePlanError):
    pass


def quiet(*a, **k):
    pass


def iso(d):
    return d.isoformat() if d else ""


def stamp(t):
    return f"{t:%Y-%m-%d %H:%M:%S}"


def parse_stamp(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=mp.IST)


def db_path(folder):
    return os.path.join(st.out_dir(folder), "download_progress.sqlite")


# ---- progress ----

class Progress:
    """download_progress.sqlite: every statement under one lock, each write commits. write=False opens an existing
    file read-only (status) and is empty when there is none."""

    def __init__(self, folder, write=True):
        self.lock = threading.Lock()
        path = db_path(folder)
        self.con = None
        if write:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.con = sqlite3.connect(path, check_same_thread=False)
            self.con.row_factory = sqlite3.Row
            self.con.execute("PRAGMA journal_mode=WAL")
            for s in SQL:
                self.con.execute(s)
            self.con.commit()
        elif os.path.isfile(path):
            self.con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
            self.con.row_factory = sqlite3.Row

    def close(self):
        if self.con:
            self.con.close()

    def rows(self, sql, args=()):
        if not self.con:
            return []
        with self.lock:
            try:
                return [dict(r) for r in self.con.execute(sql, args)]
            except sqlite3.OperationalError:
                return []

    def write(self, sql, args=()):
        with self.lock:
            self.con.execute(sql, args)
            self.con.commit()

    def save(self, table, fields, row):
        self.write(f"INSERT OR REPLACE INTO {table} ({', '.join(fields)}) VALUES ({', '.join('?' * len(fields))})",
                   [row.get(f) for f in fields])

    def calls_on(self, day):
        r = self.rows("SELECT COUNT(*) AS n FROM calls WHERE ist_date = ?", (iso(day),))
        return r[0]["n"] if r else 0

    def chunks(self, sid=None):
        if sid is None:
            return self.rows("SELECT * FROM chunks")
        return self.rows("SELECT * FROM chunks WHERE security_id = ?", (sid,))

    def stocks(self):
        return {r["security_id"]: r for r in self.rows("SELECT * FROM stocks")}

    def stock(self, sid):
        r = self.rows("SELECT * FROM stocks WHERE security_id = ?", (sid,))
        return r[0] if r else {}

    def save_stock(self, sid, **kw):
        row = {**{f: None for f in STOCK_FIELDS}, **self.stock(sid), "security_id": sid, **kw}
        self.save("stocks", STOCK_FIELDS, row)

    def mark_aborted(self):
        n = self.rows("SELECT COUNT(*) AS n FROM runs WHERE result = 'running'")[0]["n"]
        if n:
            self.write("UPDATE runs SET result = 'ABORTED' WHERE result = 'running'")
        return n


# ---- the conversion and checks of one stock (pure: tested on a known example) ----

def aggregates(minutes):
    """{date: minute aggregates} of stored bars: bars_outside counts the stored bars outside 09:15-15:29; the other
    aggregates use minute_store.clean_bars (non-positive bars dropped), as Kite's."""
    out = {}
    if not len(minutes):
        return out
    m = minutes.sort_values("ts", kind="stable")
    d = m["ts"].dt.date
    outside = pd.Series(st.outside_session(m["ts"]), index=m.index).groupby(d).sum()
    c = ms.clean_bars(m)[0]
    c = c.assign(_d=c["ts"].dt.date, _hm=c["ts"].dt.strftime("%H:%M"))
    g = c.groupby("_d", sort=True)
    agg = pd.DataFrame({"minute_rows": g.size(), "first_bar": g["_hm"].first(), "last_bar": g["_hm"].last(),
                        "minute_open": g["open"].first(), "minute_high": g["high"].max(),
                        "minute_low": g["low"].min(), "minute_close": g["close"].last(),
                        "minute_volume": g["volume"].sum()})
    for day in sorted(set(d)):
        row = agg.loc[day].to_dict() if day in agg.index else {"minute_rows": 0, "first_bar": "", "last_bar": ""}
        row["bars_outside"] = int(outside.get(day, 0))
        out[day] = row
    return out


def snap(lv, one):
    return np.where(np.abs(lv - 1) <= one, 1.0, np.round(lv, 6))


def short_day(first_bar, last_bar):
    return bool(first_bar) and first_bar < EVENING and (first_bar > SHORT_FROM or last_bar < SHORT_TO)


def derive(rows, day_candles, bhav, factors, sid, near_action=lambda d: "", code="", match_kind=""):
    """The days.parquet frame of one stock. rows: one row per stored day (date, minute aggregates incl. bars_outside,
    fetched (bool), before_start (bool), fetched_at, run_id); day_candles: ICICI's day candles (all fetched, the
    levels are measured on them); bhav: the stock's bhavcopy rows (date, symbol, series, OHLCV) or None; factors:
    adjustments' factors; near_action(date) -> the date of a corporate action within 5 trading days, or ""."""
    d = rows.sort_values("date", kind="stable").reset_index(drop=True)
    n = len(d)
    bcols = ["date", "symbol", "series"] + [f"bhav_{f}" for f in FIELDS]
    b = (bhav[["date", "symbol", "series"] + FIELDS].rename(columns={f: f"bhav_{f}" for f in FIELDS})
         if bhav is not None and len(bhav) else pd.DataFrame(columns=bcols))
    dc = day_candles.sort_values("date", kind="stable").drop_duplicates("date", keep="last").reset_index(drop=True)
    dc = dc.rename(columns={f: f"icici_{f}" for f in FIELDS})
    d = d.merge(dc, on="date", how="left").merge(b, on="date", how="left")
    for c in ("minute_rows", "bars_outside"):
        d[c] = d[c].fillna(0).astype("int64")
    for c in ("first_bar", "last_bar", "fetched_at", "run_id"):
        d[c] = d[c].fillna("").astype(str)

    # levels on every ICICI day candle with a bhavcopy row
    lv = dc.merge(b, on="date", how="left")
    sdays = {x: short_day(f, l) for x, f, l in zip(d["date"], d["first_bar"], d["last_bar"])}
    ih, il, icl, iv = (lv[f"icici_{x}"].to_numpy(float) for x in ("high", "low", "close", "volume"))
    bh, bl, bv = (lv[f"bhav_{x}"].to_numpy(float) for x in ("high", "low", "volume"))
    with np.errstate(divide="ignore", invalid="ignore"):
        sh, sl = bh / ih, bl / il
        agree = np.abs(sh - sl) <= SCALE_AGREE + 2 * mp.HALF_TICK / icl
        measured = np.where(agree & (ih > 0) & (il > 0) & ~np.isnan(bh), (sh + sl) / 2, np.nan)
        normal = np.array([not sdays.get(x, False) for x in lv["date"]], bool)
        vr = np.where(normal & (iv > 0) & (bv > 0), bv / iv, np.nan)
    p_lv = md.levels(measured, SCALE_STEP)
    v_lv = md.levels(vr, VOLUME_STEP)
    dc_dates = list(lv["date"])
    dc_scale = np.ones(len(lv)) if p_lv is None else snap(p_lv, SCALE_ONE)
    dc_vscale = np.ones(len(lv)) if v_lv is None else snap(v_lv, VOLUME_ONE)
    by_date = {x: i for i, x in enumerate(dc_dates)}

    scale, vscale, meas = np.ones(n), np.ones(n), np.full(n, np.nan)
    conv_from = [""] * n
    unsure = np.zeros(n, bool)
    for i, x in enumerate(d["date"]):
        j = by_date.get(x)
        if j is not None:
            scale[i], vscale[i], meas[i] = dc_scale[j], dc_vscale[j], measured[j]
            continue
        k = bisect_left(dc_dates, x)
        prev, nxt = (k - 1 if k > 0 else None), (k if k < len(dc_dates) else None)
        src = prev if prev is not None else nxt
        if src is None:
            unsure[i] = True
            continue
        scale[i], vscale[i] = dc_scale[src], dc_vscale[src]
        conv_from[i] = iso(dc_dates[src])
        if prev is not None and nxt is not None and (dc_scale[prev] != dc_scale[nxt] or
                                                     dc_vscale[prev] != dc_vscale[nxt]):
            unsure[i] = True
    d["icici_scale"], d["icici_volume_scale"], d["icici_scale_measured"] = scale, vscale, np.round(meas, 6)
    d["conversion_from"] = conv_from
    with np.errstate(divide="ignore", invalid="ignore"):
        d["ratio"] = d["bhav_close"].to_numpy(float) / d["icici_close"].to_numpy(float)
        d["vol_ratio"] = np.where(d["icici_volume"].to_numpy(float) > 0,
                                  d["bhav_volume"].to_numpy(float) / d["icici_volume"].to_numpy(float), np.nan)
    f = mp.with_factor(pd.DataFrame({"security_id": sid, "date": d["date"]}), factors)
    d["our_factor"] = d["date"].map(dict(zip(f["date"], f["our_factor"]))).astype(float)
    d["kind"] = "MULTIPLICATIVE"
    d["symbol"] = d["symbol"].ffill().bfill()
    d["series"] = d["series"].ffill().bfill()
    d["icici_code"], d["match_kind"] = code, match_kind

    # checks: scaled minutes vs the bhavcopy (ICICI's day candle is built from its own minutes)
    has_min = d["minute_rows"].to_numpy() > 0
    has_day = d["icici_close"].notna().to_numpy()
    has_bhav = d["bhav_close"].notna().to_numpy()
    before = d["before_start"].to_numpy(bool)
    fetched = d["fetched"].to_numpy(bool)
    mh, ml, mv = (d[f"minute_{x}"].to_numpy(float) for x in ("high", "low", "volume"))
    bhh, bll, bcc, bvv = (d[f"bhav_{x}"].to_numpy(float) for x in ("high", "low", "close", "volume"))
    half = mp.HALF_TICK * scale
    with np.errstate(invalid="ignore"):
        both = has_min & has_bhav
        hl = both & ((mh * scale > bhh + mp.PRICE_REL * np.abs(bhh) + half) |
                     (ml * scale < bll - mp.PRICE_REL * np.abs(bll) - half))
        tol_c = mp.PRICE_REL * np.abs(bcc) + half
        cl = both & ((bcc > mh * scale + tol_c) | (bcc < ml * scale - tol_c))
        vs = mv * vscale
        vol = both & ((vs > bvv * VOLUME_HIGH + 1) | (vs < bvv * VOLUME_LOW))
    last_bar = d["last_bar"].to_numpy(object)
    auction = cl & (last_bar >= AUCTION_FROM) & (last_bar < AUCTION_TO)
    short = np.array([both[i] and short_day(d.at[i, "first_bar"], d.at[i, "last_bar"]) for i in range(n)], bool)
    flags = {BEFORE_START: before & ~has_min, EMPTY: fetched & ~before & ~has_min & has_bhav,
             "EXTRA_SESSION": has_min & ~has_bhav, "DAY_EMPTY": has_min & ~has_day, "SCALE_UNSURE": unsure & has_min,
             "SHORT_DAY": short, "HIGHLOW_DIFF": hl, "CLOSE_DIFF": cl & ~auction, "CLOSING_AUCTION": auction,
             "VOLUME_DIFF": vol, "SCALE_STEP": np.zeros(n, bool)}
    notes = [[] for _ in range(n)]
    for i in np.flatnonzero(before):
        notes[i].append("inferred by the first-minute-day search (not fetched)")
    steps = [(dc_dates[j], kind, arr[j - 1], arr[j]) for kind, arr in (("icici_scale", dc_scale),
                                                                       ("icici_volume_scale", dc_vscale))
             for j in range(1, len(dc_dates)) if arr[j] != arr[j - 1]]
    for i in range(1, n):
        for when, kind, a, z in steps:
            if d.at[i - 1, "date"] < when <= d.at[i, "date"]:
                act = near_action(when)
                notes[i].append(f"{kind} {a:g} -> {z:g} on {iso(when)}" +
                                (f" (corporate action {act})" if act else " (no corporate action near)"))
                if not act:
                    flags["SCALE_STEP"][i] = True
    for i in range(n):
        if conv_from[i] and has_min[i]:
            notes[i].append(f"converted as {conv_from[i]}")
        if not len(dc_dates) and has_min[i]:
            notes[i].append("no ICICI day candle: scale 1.0 assumed")
        if (i == 0 or scale[i] != scale[i - 1] or vscale[i] != vscale[i - 1]) and \
                abs(vscale[i] * scale[i] - 1) > VOLUME_ONE:
            notes[i].append(f"volume level {vscale[i]:g} vs 1/scale {1 / scale[i]:g}")
    d["check"] = ["|".join(c for c in CHECKS if flags[c][i]) or "OK" for i in range(n)]
    d["note"] = ["; ".join(x) for x in notes]
    return d[DAY_COLUMNS]


def scale_ok(icici, bhav, scales, tol=PRICE_TOL):
    """(passed, scale) of the SYMBOL_ONLY price check: some allowed scale puts ICICI's close, high and low within tol
    of the bhavcopy."""
    for s in scales:
        if all(abs(icici[f] * s - bhav[f]) <= tol * abs(bhav[f]) for f in ("close", "high", "low")):
            return True, s
    return False, None


def allowed_scales(factors, sid, day):
    """1.0, our cumulative factor of the day, and that factor over each later factor level of the stock: the scales
    a frozen-adjusted history can be on (ICICI applied some of our actions, never others)."""
    f = factors[factors["security_id"] == sid].sort_values("from_date")
    if f.empty:
        return [1.0]
    ff = mp.with_factor(pd.DataFrame({"security_id": [sid], "date": [day]}), factors)["our_factor"].iat[0]
    later = [x for x, a in zip(f["factor"], f["from_date"]) if sm.as_date(a) > day]
    return sorted({1.0, float(ff)} | {float(ff / x) for x in later if x})


# ---- the run ----

class Runner:
    def __init__(self, folder, inp, client, speed, max_calls, days_per_stock, run_id, clock, log):
        self.folder, self.inp, self.client = folder, inp, client
        self.cal = inp.days
        self.limiter = RateLimiter(speed / 60)
        self.max_calls, self.days_per_stock = max_calls, days_per_stock
        self.run_id, self.clock, self.log = run_id, clock, log
        self.db = Progress(folder)
        self.calls = 0
        self.rate_hits = 0
        self.stats = {"stocks": 0, "built": 0, "chunks": 0, "rows": 0, "calls": 0, "failed_chunks": 0,
                      "review": [], "cap": False, "stopped": False, "auth": "", "rate_stop": "", "empty_days": 0,
                      "before_start_days": 0, "deferred_days": 0, "peak_mb": 0}
        self.cache = None
        self.actions = None

    # -- calls --

    def used_today(self):
        return self.db.calls_on(self.clock().date())

    def call(self, kind, sid, fn, *args):
        tries = 0
        while True:
            if fetch.STOP.is_set():
                raise StopJob()
            if self.used_today() >= self.max_calls:
                raise CapReached(f"daily cap reached: {self.used_today():,} calls used today (cap "
                                 f"{self.max_calls:,}); resume tomorrow with the same command")
            self.limiter.wait()
            at, t0 = self.clock(), time.monotonic()
            result, bars = "ok", None
            try:
                out = fn(*args)
                bars = len(out) if kind != "session" else None
                self.limiter.on_success()
                self.rate_hits = 0
                return out
            except ic.IciciRateLimit as e:
                result = f"rate limit: {e}"
                self.rate_hits += 1
                self.limiter.on_429()
                if self.rate_hits >= 2:
                    raise RateStop(f"ICICI answered with a rate limit twice in a row ({e}); stopped, resume later "
                                   "with the same command")
            except ic.IciciSignIn as e:
                result = "sign-in needed"
                raise
            except ic.IciciFailed as e:
                result = f"failed: {e}"
                tries += 1
                if tries >= RETRIES:
                    raise
            finally:
                self.calls += 1
                self.stats["calls"] += 1
                self.db.write("INSERT INTO calls (at, ist_date, run_id, kind, security_id, result, seconds, bars) "
                              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                              (stamp(at), iso(at.date()), self.run_id, kind, sid, result[:300],
                               round(time.monotonic() - t0, 3), bars))
            wait = BACKOFF if self.rate_hits else min(2 ** tries, 30)
            if fetch.STOP.wait(wait):
                raise StopJob()

    # -- per stock state --

    def covered(self, sid, interval):
        """{trading day: (bars that day, chunk row)} of the DONE / EMPTY chunks of a stock."""
        out = {}
        for c in self.db.chunks(sid):
            if c["interval"] != interval or c["status"] not in ("DONE", "EMPTY"):
                continue
            a, b = sm.as_date(c["chunk_from"]), sm.as_date(c["chunk_to"])
            bars = json.loads(c["day_bars"] or "{}")
            for d in self.cal[bisect_left(self.cal, a):bisect_right(self.cal, b)]:
                out[d] = (bars.get(iso(d), 0), c)
        return out

    def save_chunk(self, sid, code, interval, a, b, bars, day_bars, status, fetched, secs, error=""):
        self.db.save("chunks", CHUNK_FIELDS, {
            "security_id": sid, "interval": interval, "chunk_from": iso(a), "chunk_to": iso(b), "code": code,
            "bars": bars, "day_bars": json.dumps(day_bars), "status": status, "built": 0, "fetched_at": fetched,
            "run_id": self.run_id, "seconds": secs, "error": error})

    def error(self, sid, kind, a, text):
        self.db.write("INSERT INTO errors (security_id, run_id, kind, chunk_from, error, at) VALUES (?, ?, ?, ?, ?, ?)",
                      (sid, self.run_id, kind, iso(a), str(text)[:500], stamp(self.clock())))

    # -- fetches --

    def fetch_days(self, item, todo):
        """ICICI day candles over the trading days not covered yet (runs split into 900-day calls), staged."""
        sid, code = item["security_id"], item["icici_code"]
        have = self.covered(sid, "day")
        need = [d for d in self.cal[bisect_left(self.cal, todo[0]):bisect_right(self.cal, todo[-1])] if d not in have]
        for a0, b0, _ in ip.runs_of(need, self.cal):
            for a, b in ip.day_call_ranges(a0, b0, self.cal):
                t0 = time.monotonic()
                try:
                    candles = self.call("day", sid, self.client.historical, code, "1day", a, b)
                except ic.IciciFailed as e:
                    self.save_chunk(sid, code, "day", a, b, 0, {}, "FAILED", stamp(self.clock()),
                                    time.monotonic() - t0, str(e))
                    self.error(sid, "DAY_CANDLES", a, e)
                    self.stats["failed_chunks"] += 1
                    continue
                df = st.day_frame(candles)
                df = df[(df["date"] >= a) & (df["date"] <= b)]
                fetched = stamp(self.clock())
                df = df.assign(fetched_at=fetched, run_id=self.run_id)
                path = os.path.join(st.staging_dir(self.folder, sid), f"day_{iso(a)}.parquet")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                sm.write_parquet(df, path)
                self.save_chunk(sid, code, "day", a, b, len(df), {}, "DONE" if len(df) else "EMPTY", fetched,
                                round(time.monotonic() - t0, 3))

    def fetch_minutes(self, item, a, b):
        """One minute chunk of 1-2 trading days. A 1,000-bar answer is cut by ICICI: split into single days, and a
        single day with 1,000 bars is FAILED. Returns {date: bars} of the days fetched."""
        sid, code = item["security_id"], item["icici_code"]
        t0 = time.monotonic()
        try:
            candles = self.call("minute", sid, self.client.historical, code, "1minute", a, b)
        except ic.IciciFailed as e:
            self.save_chunk(sid, code, "minute", a, b, 0, {}, "FAILED", stamp(self.clock()), time.monotonic() - t0,
                            str(e))
            self.error(sid, "MINUTES", a, e)
            self.stats["failed_chunks"] += 1
            return {}
        if len(candles) >= ic.MAX_BARS:
            if a != b:
                out = self.fetch_minutes(item, a, a)
                out.update(self.fetch_minutes(item, b, b))
                return out
            msg = f"truncated: {len(candles):,} bars on one day (ICICI returns at most {ic.MAX_BARS:,})"
            self.save_chunk(sid, code, "minute", a, b, len(candles), {}, "FAILED", stamp(self.clock()),
                            time.monotonic() - t0, msg)
            self.error(sid, "MINUTES", a, msg)
            self.stats["failed_chunks"] += 1
            return {}
        df = st.minute_frame(candles)
        df = df[(df["ts"].dt.date >= a) & (df["ts"].dt.date <= b)]
        fetched = stamp(self.clock())
        day_bars = {iso(x): 0 for x in self.cal[bisect_left(self.cal, a):bisect_right(self.cal, b)]}
        for x, k in df.groupby(df["ts"].dt.date).size().items():
            day_bars[iso(x)] = int(k)
        if len(df):
            path = os.path.join(st.staging_dir(self.folder, sid), f"m_{iso(a)}.parquet")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            sm.write_parquet(df.assign(fetched_at=fetched, run_id=self.run_id), path)
        self.save_chunk(sid, code, "minute", a, b, len(df), day_bars, "DONE" if len(df) else "EMPTY", fetched,
                        round(time.monotonic() - t0, 3))
        self.stats["chunks"] += 1
        self.stats["rows"] += len(df)
        return {sm.as_date(k): v for k, v in day_bars.items()}

    def has_minutes(self, item, d, have):
        """Whether ICICI has minutes on day d: from a chunk already fetched, else one single-day call (FAILED: None)."""
        if d in have:
            return have[d][0] > 0
        got = self.fetch_minutes(item, d, d)
        if d not in got:
            return None
        have[d] = (got[d], None)
        return got[d] > 0

    def first_minute_day(self, item, days, have):
        """The first needed day with ICICI minutes (binary search over the needed days, as Kite's probe; at most
        about 12 calls, not counted in --days-per-stock), NONE when even the last day has none, None when a probe
        failed (searched again next run; the probed days are kept)."""
        first = self.has_minutes(item, days[0], have)
        if first is None:
            return None
        if first:
            return days[0]
        last = self.has_minutes(item, days[-1], have)
        if last is None:
            return None
        if not last:
            return NONE
        lo, hi = 0, len(days) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            got = self.has_minutes(item, days[mid], have)
            if got is None:
                return None
            if got:
                hi = mid
            else:
                lo = mid
        return days[hi]

    def price_check(self, item, first):
        """SYMBOL_ONLY: ICICI's day candle (else the minute aggregates) of the first day with minutes vs the
        bhavcopy, at one of the allowed scales. Returns (passed, text)."""
        sid = item["security_id"]
        stage = st.staging_dir(self.folder, sid)
        day = None
        for name in sorted(os.listdir(stage)) if os.path.isdir(stage) else []:
            if name.startswith("day_"):
                df = pq.read_table(os.path.join(stage, name)).to_pandas()
                df["date"] = [sm.as_date(x) for x in df["date"]]
                hit = df[df["date"] == first]
                if len(hit):
                    day = {f: float(hit[f].iat[0]) for f in ("close", "high", "low")}
        if day is None:
            parts = [pq.read_table(os.path.join(stage, n)).to_pandas() for n in sorted(os.listdir(stage))
                     if n.startswith("m_")]
            m = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=st.MINUTE_COLUMNS)
            m = m[m["ts"].dt.date == first].sort_values("ts") if len(m) else m
            if not len(m):
                return False, f"no ICICI candle on {first} in the staging"
            day = {"close": float(m["close"].iat[-1]), "high": float(m["high"].max()), "low": float(m["low"].min())}
        b = self.bhav(sid)
        b = b[b["date"] == first] if b is not None else None
        if b is None or not len(b):
            return False, f"no bhavcopy row on {first}"
        bhav = {f: float(b[f].iat[0]) for f in ("close", "high", "low")}
        ok, s = scale_ok(day, bhav, allowed_scales(self.inp.factors, sid, first))
        text = (f"{first}: ICICI close {day['close']:g} high {day['high']:g} low {day['low']:g}, bhavcopy close "
                f"{bhav['close']:g} high {bhav['high']:g} low {bhav['low']:g}")
        return ok, (f"PASS at scale {s:g}; " if ok else "FAIL (no allowed scale within 2%); ") + text

    def bhav(self, sid):
        if self.cache is None:
            self.cache = md.bhav_cache(self.folder, self.inp, quiet, out=ip.cache_out(self.folder))
        return md.read_cached_bhav(self.cache, sid)

    def near_action(self, sid):
        if self.actions is None:
            self.actions = md.corporate_actions(self.folder, self.inp)
        acts = self.actions.get(sid, [])

        def near(d):
            if not acts:
                return ""
            i = bisect_left(self.cal, d)
            lo = self.cal[max(i - md.ACTION_DAYS, 0)]
            hi = self.cal[min(i + md.ACTION_DAYS, len(self.cal) - 1)]
            hit = [a for a in acts if lo <= a <= hi]
            return iso(hit[0]) if hit else ""
        return near

    def stock(self, item, days):
        """Fetch what one stock still needs, then build it."""
        sid = item["security_id"]
        state = self.db.stock(sid)
        if state.get("status") == "REVIEW":
            return
        self.db.save_stock(sid, code=item["icici_code"], match_kind=item["match_kind"],
                           status=state.get("status") or "PARTIAL")
        self.budget = self.days_per_stock if self.days_per_stock else 10 ** 9
        have = self.covered(sid, "minute")
        todo = [d for d in days if d not in have]
        try:
            if todo:
                self.fetch_days(item, days)
            first = state.get("first_minute_day") or ""
            if not first:
                found = self.first_minute_day(item, days, have)
                if found is not None:
                    first = found if found == NONE else iso(found)
                    self.db.save_stock(sid, first_minute_day=first)
            symbol_only = item["match_kind"] == "SYMBOL_ONLY"
            if first == NONE and symbol_only:
                self.db.save_stock(sid, status="DONE", reason="no ICICI minutes on any needed day; nothing stored "
                                                              "(SYMBOL_ONLY, not price checked)")
                return
            if first and first != NONE and symbol_only and not (state.get("price_check") or "").startswith("PASS"):
                ok, text = self.price_check(item, sm.as_date(first))
                if not ok:
                    self.review(sid, text)
                    return
                self.db.save_stock(sid, price_check=text)
            if first and first != NONE:
                start = sm.as_date(first)
                for a, b in ip.pair_chunks([d for d in days if d >= start and d not in have], self.cal):
                    if self.budget <= 0:
                        break
                    span = [x for x in (a, b) if x not in have]
                    if not span:
                        continue
                    a, b = (span[0], span[-1]) if len(span) == 2 else (span[0], span[0])
                    self.budget -= len({a, b})
                    for x, k in self.fetch_minutes(item, a, b).items():
                        have[x] = (k, None)
        finally:
            self.build(item, days)

    def review(self, sid, text):
        stage = st.staging_dir(self.folder, sid)
        if os.path.isdir(stage):
            dst = os.path.join(st.archive_dir(self.folder), f"{sid}_{self.clock():%Y-%m-%d_%H%M%S}")
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(stage, dst)
        self.db.save_stock(sid, status="REVIEW", price_check=text, reason=f"SYMBOL_ONLY price check: {text}")
        self.stats["review"].append(sid)
        self.log(f"REVIEW {sid}: SYMBOL_ONLY price check failed, nothing stored ({text})")

    # -- build --

    def build(self, item, days):
        """Merge the staged chunks into data/<sid>/ (year files, icici_day.parquet, then days.parquet), mark them
        built and remove the staging. A day fetched before (day + 1) 01:00 IST is not stored (fetched again)."""
        sid = item["security_id"]
        state = self.db.stock(sid)
        if state.get("status") == "REVIEW":
            return
        if item["match_kind"] == "SYMBOL_ONLY" and not (state.get("price_check") or "").startswith("PASS"):
            return
        stage = st.staging_dir(self.folder, sid)
        files = sorted(os.listdir(stage)) if os.path.isdir(stage) else []
        chunks = [c for c in self.db.chunks(sid) if c["status"] in ("DONE", "EMPTY")]
        unbuilt = [c for c in chunks if not c["built"]]
        if not unbuilt and not files:
            self.finish_stock(item, days)
            return
        path = st.stock_dir(self.folder, sid)
        os.makedirs(path, exist_ok=True)
        # unfinished days (fetched before (day + 1) 01:00 IST): their chunks are dropped and fetched again
        bad = set()
        for c in unbuilt:
            if c["interval"] != "minute":
                continue
            when = parse_stamp(c["fetched_at"])
            span = self.cal[bisect_left(self.cal, sm.as_date(c["chunk_from"])):
                            bisect_right(self.cal, sm.as_date(c["chunk_to"]))]
            if any(when < md.finished_at(x) for x in span):
                bad.add(c["chunk_from"])
        new_min = [pq.read_table(os.path.join(stage, f)).to_pandas() for f in files
                   if f.startswith("m_") and f[2:12] not in bad]
        new_day = [pq.read_table(os.path.join(stage, f)).to_pandas() for f in files if f.startswith("day_")]
        if new_day:
            nd = pd.concat(new_day, ignore_index=True)
            nd["date"] = [sm.as_date(x) for x in nd["date"]]
            old = st.read_day_candles(path)
            old = old[~old["date"].isin(set(nd["date"]))]
            dcs = pd.concat([old, nd[st.DAY_CANDLE_COLUMNS]], ignore_index=True).sort_values("date")
            ms.write_days(dcs.reset_index(drop=True), os.path.join(path, st.DAY_FILE))
        if new_min:
            nm = pd.concat(new_min, ignore_index=True)
            nm_days = set(nm["ts"].dt.date)
            for year, g in nm.groupby(nm["ts"].dt.year):
                yp = os.path.join(path, f"{year}.parquet")
                old = pq.read_table(yp).to_pandas() if os.path.isfile(yp) else None
                if old is not None:
                    old = old[~old["ts"].dt.date.isin(nm_days)]
                    g = pd.concat([old, g[st.MINUTE_COLUMNS]], ignore_index=True)
                ms.write_minutes(g.sort_values("ts", kind="stable"), yp)
            del nm, new_min
        for c in unbuilt:
            if c["interval"] == "minute" and c["chunk_from"] in bad:
                self.db.write("DELETE FROM chunks WHERE security_id = ? AND interval = 'minute' AND chunk_from = ?",
                              (sid, c["chunk_from"]))
                self.stats["deferred_days"] += 1
        self.write_days(item, days)
        for c in unbuilt:
            if not (c["interval"] == "minute" and c["chunk_from"] in bad):
                self.db.write("UPDATE chunks SET built = 1 WHERE security_id = ? AND interval = ? AND chunk_from = ?",
                              (sid, c["interval"], c["chunk_from"]))
        for f in files:
            os.remove(os.path.join(stage, f))
        if os.path.isdir(stage) and not os.listdir(stage):
            os.rmdir(stage)
        self.stats["built"] += 1
        self.finish_stock(item, days)

    def write_days(self, item, days):
        sid = item["security_id"]
        path = st.stock_dir(self.folder, sid)
        minutes = ms.read_minutes(path)
        agg = aggregates(minutes) if len(minutes) else {}
        del minutes
        have = self.covered(sid, "minute")
        first = self.db.stock(sid).get("first_minute_day") or ""
        start = None if not first else (None if first == NONE else sm.as_date(first))
        rows = {}
        for d, (k, c) in have.items():
            rows[d] = {"fetched": True, "before_start": False, "fetched_at": c["fetched_at"] if c else "",
                       "run_id": c["run_id"] if c else ""}
        if first:
            for d in days:
                if d not in have and (start is None or d < start):
                    rows[d] = {"fetched": False, "before_start": True, "fetched_at": "", "run_id": ""}
        for d in agg:
            rows.setdefault(d, {"fetched": True, "before_start": False, "fetched_at": "", "run_id": ""})
        if not rows:
            return
        frame = pd.DataFrame([{"date": d, **v, **agg.get(d, {"minute_rows": 0, "bars_outside": 0})}
                              for d, v in sorted(rows.items())])
        for c in ("first_bar", "last_bar", "minute_open", "minute_high", "minute_low", "minute_close",
                  "minute_volume"):
            if c not in frame:
                frame[c] = np.nan if c.startswith("minute") else ""
        out = derive(frame, st.read_day_candles(path), self.bhav(sid), self.inp.factors, sid, self.near_action(sid),
                     item["icici_code"], item["match_kind"])
        ms.write_days(out, os.path.join(path, st.DAYS_FILE))

    def finish_stock(self, item, days):
        sid = item["security_id"]
        state = self.db.stock(sid)
        if state.get("status") == "REVIEW":
            return
        dp = st.stock_dir(self.folder, sid)
        stored = ms.read_days(dp)
        resolved = set(stored["date"]) if stored is not None else set()
        done = bool(days) and all(d in resolved for d in days)
        self.db.save_stock(sid, status="DONE" if done else "PARTIAL",
                           days=0 if stored is None else int((stored["minute_rows"] > 0).sum()),
                           minute_rows=0 if stored is None else int(stored["minute_rows"].sum()),
                           built_at=stamp(self.clock()) if stored is not None else state.get("built_at"))


def resolved_days(folder, sid):
    """The days a stock's days.parquet has a row for (stored, ICICI_EMPTY or ICICI_BEFORE_START)."""
    d = stored_days(folder, sid)
    return set() if d is None else set(d["date"])


def work_items(folder, plan, days, inp, priority=None, symbols=None, limit=None, stocks=None, now=None):
    """(items, days by stock, deferred) in plan order: fetchable stocks not DONE / REVIEW, each with its finished
    needed days (a day is fetched only from (day + 1) 01:00 IST)."""
    stocks = stocks or {}
    want = None
    if symbols:
        want = set()
        for s in symbols:
            ids = sm.lookup(s, folder)
            if not ids:
                raise mp.MinutePlanError(f"{s}: no security ever had this symbol or ISIN.")
            want.update(ids)
    now = md.as_ist(now)
    by_sid = {sid: sorted(g["date"]) for sid, g in days.groupby("security_id")}
    items, out, deferred = [], {}, 0
    for r in plan.to_dict("records"):
        sid = r["security_id"]
        if r["match_kind"] not in ip.FETCHABLE or sid not in inp.by_id:
            continue
        if priority and r["priority"] != priority:
            continue
        if want is not None and sid not in want:
            continue
        s = stocks.get(sid, {})
        if s.get("status") == "REVIEW":
            continue
        ds = by_sid.get(sid, [])
        fin = [d for d in ds if now >= md.finished_at(d)]
        deferred += len(ds) - len(fin)
        if not fin:
            continue
        if s.get("status") == "DONE" and (s.get("reason") or "").startswith("no ICICI minutes"):
            continue
        if s.get("status") == "DONE" and set(fin) <= resolved_days(folder, sid):
            continue
        items.append(r)
        out[sid] = fin
    if limit:
        items = items[:limit]
    return items, out, deferred


def download(folder=None, client=None, priority=None, symbols=None, limit=None, max_calls=ip.MAX_CALLS,
             days_per_stock=None, speed=ip.SPEED, now=None, log=print):
    """Part B: fetch the plan's stocks one at a time, paced (speed calls per minute) and capped (max_calls a day,
    counted across runs). now: a fixed datetime or a callable for the clock (tests). Returns {"stats": ...}.
    Raises mp.SignInNeeded (exit 2) when the session is rejected, after building what was fetched."""
    folder = folder or config.equity_data_dir()
    if not 0 < speed <= ip.MAX_SPEED:
        raise ValueError(f"speed must be more than 0 and at most {ip.MAX_SPEED} calls a minute")
    clock = now if callable(now) else ((lambda: md.as_ist(now)) if now else md.now_ist)
    ip.refuse_busy(folder)
    inp = mp.Inputs(folder, bhav=False)
    plan, days = ip.read_plan(folder)
    lock = RunLock(st.lock_path(folder)).acquire("icici_minute.py download")
    run_id = f"{clock():%Y%m%d-%H%M%S}"
    runner = None
    try:
        runner = Runner(folder, inp, client, speed, max_calls, days_per_stock, run_id, clock, log)
        db = runner.db
        n = db.mark_aborted()
        if n:
            log(f"{n} run(s) left 'running' by a killed process marked ABORTED.")
        db.save("runs", ["run_id", "command", "started", "result"],
                {"run_id": run_id, "command": "download", "started": stamp(clock()), "result": "running"})
        items, need, deferred = work_items(folder, plan, days, inp, priority, symbols, limit, db.stocks(), clock())
        stats = runner.stats
        stats["deferred_days"] += deferred
        log(f"Work list: {len(items):,} stocks; calls used today {runner.used_today():,} of {max_calls:,}.")
        result = "done"
        try:
            for item in items:          # chunks left by a killed run are built first (no call)
                runner.build(item, need[item["security_id"]])
            if runner.client is None:
                runner.client = ic.BreezeClient.from_env()
            if items:
                runner.call("session", None, runner.client.customer)
            for k, item in enumerate(items, 1):
                runner.stock(item, need[item["security_id"]])
                stats["stocks"] += 1
                cur, peak = md.memory()
                stats["peak_mb"] = max(stats["peak_mb"], peak / 1e6)
                log(f"[{k}/{len(items)}] {item['symbol']} ({item['icici_code']}): {runner.db.stock(item['security_id']).get('status')}; "
                    f"calls today {runner.used_today():,}; memory {md.gb(cur)} (peak {md.gb(peak)})")
        except CapReached as e:
            stats["cap"], result = True, "cap"
            log(str(e))
        except (StopJob, KeyboardInterrupt):
            stats["stopped"], result = True, "stopped"
            log("Stopped; resume with the same command.")
        except ic.IciciSignIn as e:
            stats["auth"], result = str(e), "sign-in needed"
            log(f"Stopped: {e}")
        except RateStop as e:
            stats["rate_stop"], result = str(e), "rate limit"
            log(str(e))
        for item in items:
            d = stored_days(folder, item["security_id"])
            if d is not None:
                stats["days_stored"] = stats.get("days_stored", 0) + int((d["minute_rows"] > 0).sum())
                stats["empty_days"] += int(d["check"].str.contains(EMPTY).sum())
                stats["before_start_days"] += int(d["check"].str.contains(BEFORE_START).sum())
        cur, peak = md.memory()
        stats["peak_mb"] = max(stats["peak_mb"], peak / 1e6)
        stats["calls_today"] = runner.used_today()
        db.write("UPDATE runs SET ended = ?, calls = ?, result = ?, peak_mb = ? WHERE run_id = ?",
                 (stamp(clock()), stats["calls"], result, round(stats["peak_mb"], 1), run_id))
        rebuild_checks(folder, plan)
        cov = coverage(folder, write=True)
        log(coverage_text(cov))
    finally:
        if runner:
            runner.db.close()
        lock.release()
    if stats["auth"]:
        raise mp.SignInNeeded(stats["auth"])
    return {"stats": stats, "items": [i["security_id"] for i in items]}


# ---- checks, coverage, status ----

def stored_days(folder, sid):
    return ms.read_days(st.stock_dir(folder, sid))


def rebuild_checks(folder, plan=None):
    """check/icici_check.csv (problem days), summary.json (counts per check and priority), scale_steps.csv, from every
    stored days.parquet, stock by stock."""
    if plan is None:
        plan, _ = ip.read_plan(folder)
    prio = dict(zip(plan["security_id"], plan["priority"]))
    out = st.check_dir(folder)
    os.makedirs(out, exist_ok=True)
    data = st.data_dir(folder)
    sids = sorted(os.listdir(data)) if os.path.isdir(data) else []
    problems, steps = [], []
    counts, per_prio, outside, scaled = {}, {}, 0, []
    for sid in sids:
        d = stored_days(folder, sid)
        if d is None:
            continue
        p = int(prio.get(sid, 3))
        sym = d["symbol"].dropna().iat[-1] if d["symbol"].notna().any() else ""
        outside += int(d["bars_outside"].sum())
        if (d["icici_scale"] != 1).any() or (d["icici_volume_scale"] != 1).any():
            scaled.append(sid)
        for c in d["check"]:
            for x in c.split("|"):
                counts[x] = counts.get(x, 0) + 1
                per_prio.setdefault(str(p), {}).setdefault(x, 0)
                per_prio[str(p)][x] += 1
        bad = d[d["check"] != "OK"]
        for r in bad.itertuples():
            problems.append({"security_id": sid, "symbol": r.symbol, "priority": p, "date": iso(r.date),
                             **{c: getattr(r, c) for c in CHECK_COLUMNS[4:]}})
        for kind in ("icici_scale", "icici_volume_scale"):
            v = d[kind].to_numpy(float)
            for i in np.flatnonzero(v[1:] != v[:-1]) + 1:
                note = d["note"].iat[i]
                act = note.split("(corporate action ")[1].split(")")[0] if "(corporate action " in note else ""
                steps.append({"security_id": sid, "symbol": sym, "kind": kind.upper(), "date": iso(d["date"].iat[i]),
                              "from_value": v[i - 1], "to_value": v[i], "near_action": act,
                              "flagged": "Y" if "SCALE_STEP" in d["check"].iat[i] else "N"})
    sm.write_csv(pd.DataFrame(problems, columns=CHECK_COLUMNS), os.path.join(out, "icici_check.csv"))
    sm.write_csv(pd.DataFrame(steps, columns=STEP_COLUMNS), os.path.join(out, "scale_steps.csv"))
    mp.write_json(os.path.join(out, "summary.json"),
                  {"stocks": len(sids), "checks": dict(sorted(counts.items())), "per_priority": per_prio,
                   "bars_outside": outside, "stocks_with_scale": scaled})
    return {"checks": counts, "per_priority": per_prio, "bars_outside": outside, "stocks": len(sids)}


def checks(folder=None, log=print):
    """icici_minute.py checks: rebuild the check files (offline; takes the lock)."""
    folder = folder or config.equity_data_dir()
    lock = RunLock(st.lock_path(folder)).acquire("icici_minute.py checks")
    try:
        res = rebuild_checks(folder)
    finally:
        lock.release()
    log(f"Checks of {res['stocks']:,} stocks: " + ", ".join(f"{k} {v:,}" for k, v in sorted(res["checks"].items())))
    log(f"Bars outside 09:15-15:29 (stored unchanged): {res['bars_outside']:,}")
    log(f"Written to {st.check_dir(folder)}")
    return res


def coverage(folder, write=False):
    """Per kite_gaps.csv row (1MIN / BOTH): days needed, Kite-covered, ICICI stored / empty / before start, to do,
    still missing, and a state (FILLED, PARTLY, MISSING, NOT_IN_ICICI, REVIEW, KITE)."""
    plan, days = ip.read_plan(folder)
    rows = ip.read_rows(folder)
    db = Progress(folder, write=False)
    stocks = db.stocks()
    db.close()
    kind = dict(zip(plan["security_id"], plan["match_kind"]))
    labels = {}
    for sid in sorted(set(days["security_id"])):
        d = stored_days(folder, sid)
        if d is not None:
            labels[sid] = dict(zip(d["date"], zip(d["minute_rows"], d["check"])))
    out = []
    by_row = {int(k): g for k, g in days.groupby("row")}
    for r in rows.to_dict("records"):
        g = by_row.get(int(r["row"]))
        need = 0 if g is None else len(g)
        stored = empty = before = 0
        sids = set() if g is None else set(g["security_id"])
        if g is not None:
            for sid, dd in zip(g["security_id"], g["date"]):
                x = labels.get(sid, {}).get(dd)
                if x is None:
                    continue
                if x[0] > 0:
                    stored += 1
                elif EMPTY in x[1]:
                    empty += 1
                elif BEFORE_START in x[1]:
                    before += 1
        to_do = need - stored - empty - before
        if need == 0:
            state = "KITE" if int(r["days"] or 0) else "NONE"
        elif r["security_id"] and kind.get(r["security_id"]) in ("NOT_IN_ICICI", "REVIEW"):
            state = kind[r["security_id"]]
        elif r["security_id"] and stocks.get(r["security_id"], {}).get("status") == "REVIEW":
            state = "REVIEW"
        elif stored == need:
            state = "FILLED"
        elif stored:
            state = "PARTLY"
        else:
            state = "MISSING"
        out.append({**{c: r[c] for c in ("row", "data_missing", "security_id", "symbols", "reason", "need_from",
                                         "need_to", "priority")},
                    "stocks": len(sids) if not r["security_id"] else r["stocks"], "days": r["days"],
                    "kite_covered": r["kite_covered"], "icici_needed": need, "icici_stored": stored,
                    "icici_empty": empty, "icici_before_start": before, "to_do": to_do,
                    "still_missing": need - stored, "state": state})
    df = pd.DataFrame(out, columns=COVERAGE_COLUMNS)
    if write:
        os.makedirs(st.check_dir(folder), exist_ok=True)
        sm.write_csv(df, os.path.join(st.check_dir(folder), "coverage.csv"))
    return df


def coverage_text(df):
    if df.empty:
        return "Coverage: no kite_gaps.csv rows."
    lines = ["Coverage of the kite_gaps.csv rows (1MIN / BOTH):"]
    for s, g in df.groupby("state", sort=True):
        lines.append(f"  {s:<13} {len(g):>5,} rows, {g['icici_needed'].sum():>9,} ICICI days needed, "
                     f"{g['icici_stored'].sum():>9,} stored, {g['icici_empty'].sum():>7,} ICICI_EMPTY, "
                     f"{g['icici_before_start'].sum():>7,} ICICI_BEFORE_START")
    return "\n".join(lines)


def status(folder=None, max_calls=ip.MAX_CALLS, now=None, log=print):
    """icici_minute.py status, offline: plan totals, done / to do per priority, calls today, errors, REVIEW stocks,
    estimated days left. Writes nothing."""
    folder = folder or config.equity_data_dir()
    plan, days = ip.read_plan(folder)
    db = Progress(folder, write=False)
    stocks = db.stocks()
    today = md.as_ist(now).date()
    used = db.calls_on(today)
    chunks = db.chunks()
    errors = db.rows("SELECT * FROM errors ORDER BY at DESC")
    runs = db.rows("SELECT * FROM runs ORDER BY started")
    db.close()
    by_sid = {sid: len(g) for sid, g in days.groupby("security_id")}
    lines = [f"ICICI plan: {len(plan):,} stocks, {int(plan['trading_days'].sum()):,} stock-days, "
             f"{int(plan['est_calls'].sum()):,} calls estimated"]
    left_calls = 0
    for p, g in plan.groupby("priority", sort=True):
        fetch_ = g[g["match_kind"].isin(ip.FETCHABLE)]
        state = {"DONE": 0, "PARTIAL": 0, "REVIEW": 0, "NOT_STARTED": 0}
        stored = empty = before = 0
        for r in fetch_.itertuples():
            s = stocks.get(r.security_id, {}).get("status") or "NOT_STARTED"
            state[s] = state.get(s, 0) + 1
            d = stored_days(folder, r.security_id)
            done_days = 0
            if d is not None:
                stored += int((d["minute_rows"] > 0).sum())
                empty += int(d["check"].str.contains(EMPTY).sum())
                before += int(d["check"].str.contains(BEFORE_START).sum())
                done_days = len(d)
            if s not in ("DONE", "REVIEW"):
                n = by_sid.get(r.security_id, 0)
                left_calls += round(r.est_calls * max(n - done_days, 0) / max(n, 1))
        lines.append(f"  priority {p}: {len(g):,} stocks ({len(fetch_):,} fetchable: {state['DONE']:,} done, "
                     f"{state['PARTIAL']:,} partly, {state['NOT_STARTED']:,} not started, {state['REVIEW']:,} "
                     f"REVIEW; {(g['match_kind'] == 'NOT_IN_ICICI').sum():,} not in ICICI); days stored "
                     f"{stored:,}, ICICI_EMPTY {empty:,}, ICICI_BEFORE_START {before:,}")
    lines.append(f"Calls used today ({today}): {used:,} of {max_calls:,} ({max(max_calls - used, 0):,} left)")
    lines.append(f"Calls still needed (estimate): about {left_calls:,} = {left_calls / max_calls:.1f} days at "
                 f"{max_calls:,} a day")
    failed = [c for c in chunks if c["status"] == "FAILED"]
    lines.append(f"Chunks: {sum(1 for c in chunks if c['status'] == 'DONE'):,} with bars, "
                 f"{sum(1 for c in chunks if c['status'] == 'EMPTY'):,} empty, {len(failed):,} FAILED")
    for c in failed[:FAILED_LINES]:
        lines.append(f"  FAILED {c['security_id']} {c['interval']} {c['chunk_from']}..{c['chunk_to']}: {c['error']}")
    review = [s for s in stocks.values() if s["status"] == "REVIEW"]
    lines.append(f"REVIEW stocks: {len(review):,}")
    for s in review[:FAILED_LINES]:
        lines.append(f"  {s['security_id']} {s['code']}: {s['reason']}")
    nf = plan[plan["match_kind"] == "NOT_IN_ICICI"]
    lines.append(f"Not in ICICI: {len(nf):,} stocks ({int(nf['trading_days'].sum()):,} days)")
    if errors:
        lines.append(f"Errors: {len(errors):,} (newest first)")
        for e in errors[:10]:
            lines.append(f"  {e['at']} {e['security_id']} {e['kind']} {e['chunk_from']}: {e['error']}")
    aborted = [r for r in runs if r["result"] == "ABORTED"]
    if aborted:
        lines.append(f"ABORTED runs: {len(aborted):,}")
    if runs:
        r = runs[-1]
        lines.append(f"Last run: {r['run_id']} {r['command']} started {r['started']}, ended {r['ended'] or '-'}, "
                     f"{r['calls'] or 0:,} calls, {r['result']}, peak memory {(r['peak_mb'] or 0) / 1e3:.2f} GB")
    text = "\n".join(lines)
    log(text)
    return {"text": text, "calls_today": used, "left_calls": left_calls}
