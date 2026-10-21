"""Kite 1-minute and EOD plan, phase 1: which stocks to download from Kite, their instrument tokens, from which
day Kite has day and minute candles, and the vendor list of every stock / period Kite does not have.

Inputs, only read: the bhavcopy parquet (NSE_CM_bhav/<YYYY>.parquet), the security master, the adjustment factors,
the exit list, the index membership and the NSE reference lists (company names). Network: Kite only, in
tokens() (one instrument-list request) and probe() (day candles and single-day minute requests); gaps() is
offline. Output: <folder>/equity_1min/plan/ (every file atomic):
    kite_instruments_NSE_<IST date>.csv   Kite's NSE instrument list as received (tokens)
    kite_tokens.csv / tokens_info.json    ACTIVE securities matched to a Kite tradingsymbol (tokens)
    probe_progress.sqlite                 one row per probed security, committed after each (probe; resume)
    kite_day/<security_id>.parquet        the day candles the probe fetched, so gaps() works offline
    kite_vs_bhav.parquet                  every Kite day joined with the bhavcopy (ratio, differences)
    kite_adjustment.csv                   additive periods (every other day converts with its own ratio)
    eod_mismatch.csv                      real EOD differences (information only)
    plan.csv / kite_gaps.csv / index_coverage.csv / build_report.txt / build_info.json
<folder>/equity_1min/.lock: one tokens / probe / gaps run at a time (backfill.locks.RunLock).

Findings this rests on (Part 0, 2026-10-18) are in PART0_FINDINGS; docs/dev/equity_minute_plan.md has the rules."""
import json
import os
import sqlite3
from bisect import bisect_left, bisect_right
from datetime import date, datetime, time as dtime
from glob import glob
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kiteconnect import exceptions as kex

import config
from backfill.cm_bhav import store_dir
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import security_master as sm
from backfill.fetch import AuthStop, ChunkFailed, RateLimiter, StopJob, fetch_chunk, make_chunks
from backfill.instruments import T2T_SERIES, series_suffix
from backfill.locks import LockBusy, RunLock, held  # noqa: F401 - LockBusy is re-exported for the CLI

IST = ZoneInfo("Asia/Kolkata")
OUT = "equity_1min"
PLAN = "plan"
DATA_FROM = date(2015, 1, 1)
KITE_MINUTE_FROM = date(2015, 2, 2)   # Part 0: no stock has Kite minute candles before this day
LATER_DAYS = 5           # a first Kite day more than 5 trading days after the start is a gap row
PRICE_TOL = 0.02         # probe: Kite day close vs bhavcopy close on the last bhavcopy day (identical in Part 0)
SPEED = 2.5              # requests per second
MAX_SPEED = 3.0
ADJ_TOL = 0.005          # information: days where Kite's factor (bhav close / Kite close) is off ours by more than 0.5%
HALF_TICK = 0.025        # every price tolerance allows half of Kite's rounding unit per Kite price: 0.025 for 0.05 ...
UNITS = [0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]   # ... or a coarser unit Kite rounds a stock's adjusted prices to:
UNIT_SHARE = 0.8         # ... the largest one all four prices sit on for 80% of the days of a year and our factor
SLOPE_RANGE = 0.02       # Kite's own multiplier M (raw = M x (Kite + d)) is measured on days whose Kite high-low range
SLOPE_UNITS = 20         # ... is at least 2% of the close and 20 rounding units, as bhav range / Kite range, ...
SLOPE_WINDOW = 63        # ... and taken as the median of the 63 measured days around each day, within our factor
PRICE_REL = 0.0005       # EOD check: O/H/L off that day's conversion by more than 0.05% + half ticks is a mismatch
STEP_TOL = 0.003         # an additive period splits where the difference moves by more than 0.3% + two half ticks ...
STEP_DAYS = 3            # ... and stays at the new level for 3 days (shorter: a blip); 3 non-additive days end it
ADD_MIN_DAYS = 3         # an additive period needs at least 3 additive days
VOLUME_TOL = 0.01        # a volume off raw x our factor, raw x the price ratio and raw x Kite's volume factor ...
VOLUME_WINDOW = 11       # ... (median of Kite volume / raw volume over the 11 days around it) by more than 1% + 1 share
SESSION_SHARE = 0.5      # a date Kite lacks for more than half the probed stocks that traded ...
SESSION_MIN = 10         # ... out of at least 10, is a market-wide missing session
MINUTES_PER_DAY = 375
PARQUET_BYTES = 15       # estimated bytes per minute row (assumption, not measured)
CSV_BYTES = 45
TOP_GAPS = 20
CRORE = 1e7

INDEXES = list(im.INDEXES)
PRIORITY_INDEX = [(1, "NIFTY 50"), (2, "NIFTY 500")]
MATCHED = ("MATCHED", "MATCHED_UNCHECKED")
EXIT_REASONS = {"MERGER": "MERGED", "VOLUNTARY_DELISTING": "DELISTED", "COMPULSORY_DELISTING": "DELISTED",
                "UNKNOWN": "UNKNOWN_EXIT"}
REFERENCE_NAMES = {"mergers.csv": ("old_symbol", "old_company"), "voluntary_delisting.csv": ("symbol", "company"),
                   "delisting_circulars.csv": ("symbol", "company"), "compulsory_delisting.csv": ("symbol", "company"),
                   "suspensions.csv": ("symbol", "company")}

TOKEN_COLUMNS = ["security_id", "isin", "symbol", "series", "kite_tradingsymbol", "instrument_token", "kite_name",
                 "kite_last_price", "bhav_close", "match_status", "match_rule", "note"]
PLAN_COLUMNS = ["security_id", "symbols", "isins", "status", "exit_reason", "match_status", "match_rule",
                "instrument_token", "need_from", "need_to", "eod_first_day", "eod_missing_days", "eod_mismatch_days",
                "minute_first_day", "download_eod", "download_minute", "eod_requests", "minute_requests",
                "est_minute_rows", "note"]
GAP_COLUMNS = ["data_missing", "security_id", "symbols", "isins", "company_name", "need_from", "need_to",
               "trading_days", "reason", "exit_note", "indices", "median_turnover", "priority", "note"]
ADJUSTMENT_COLUMNS = ["security_id", "symbol", "from_date", "to_date", "days", "additive_days", "kite_vs_ours",
                      "median_ratio", "kite_multiplier", "median_kite_difference", "median_adj_difference",
                      "our_factor", "kind"]
MISMATCH_COLUMNS = ["security_id", "symbol", "date", "field", "kite", "bhav", "factor", "kind"]
COVERAGE_COLUMNS = ["index", "date", "members", "with_eod", "with_minute", "without_eod", "without_minute"]
FIELDS = ["open", "high", "low", "close", "volume"]
KINDS = ["MULTIPLICATIVE", "ADDITIVE"]   # per day: raw = Kite x ratio, or kite_multiplier x (Kite + kite_difference)
TOP_MISMATCH = 10

PART0_FINDINGS = """Part 0 findings (Kite, 2026-10-18; see docs/dev/equity_minute_plan.md):
- Instrument list: no ISIN column; last_price is 0 on every row (the price check uses the probe's day candle).
- One instrument per stock: today's token covers renames, ISIN changes and every EQ / BE period.
- Day candles from 2015-01-01; 1-minute candles from 2015-02-02 for every stock (January 2015: none).
- Day and minute data start on the same day for listings after 2015; no day-only or minute-only days were seen.
- Kite has no candles for the weekday Muhurat sessions 2017-10-19 and 2018-11-07.
- Kite adjusts for splits / bonuses (volume too) and also for demergers, some dividends, and additively
  (TATASTEEL before 2024): each day converts with its own ratio, kite_adjustment.csv lists the additive periods.
  Recent days without later actions match the bhavcopy exactly.
- A series switch (bhavcopy EQ, Kite only SYMBOL-BE, or the reverse) is the same stock: match_rule SERIES_SWITCH.
- INFIBEAM -> CCAVENUE: Kite lists only CCAVENUE (token 4159745, name AVENUESAI); it returned no day candles."""


class MinutePlanError(Exception):
    pass


class StoreBusy(MinutePlanError):
    pass


class FnoBusy(MinutePlanError):
    pass


class SignInNeeded(MinutePlanError):
    pass


class InputStale(MinutePlanError):
    pass


class NotBuilt(MinutePlanError):
    pass


def now_ist():
    return datetime.now(IST)


def out_dir(folder=None):
    return os.path.join(folder or config.equity_data_dir(), OUT)


def plan_dir(folder=None):
    return os.path.join(out_dir(folder), PLAN)


def lock_path(folder):
    return os.path.join(out_dir(folder), ".lock")


def iso(d):
    return d.isoformat() if d else ""


def write_json(path, data):
    sm.write_text(path, json.dumps(data, indent=2, default=str) + "\n")


# ---- Part 1: inputs and universe ----

def input_paths(folder):
    """{name: (lock, required files, build_info)} of every input build."""
    master, factors, exits, index = sm.out_dir(folder), adj.out_dir(folder), ex.out_dir(folder), im.out_dir(folder)
    return {
        "security master": (os.path.join(master, ".lock"), adj.master_paths(folder),
                            os.path.join(master, "build_info.json"), "security_master.py"),
        "adjustments": (os.path.join(factors, ".lock"),
                        [os.path.join(factors, n) for n in ("factors.parquet", "events.csv", "build_info.json")],
                        os.path.join(factors, "build_info.json"), "adjustments.py"),
        "exit list": (os.path.join(exits, ".lock"),
                      [os.path.join(exits, n) for n in ("exits.parquet", "status.parquet", "calendar.csv",
                                                        "build_info.json")],
                      os.path.join(exits, "build_info.json"), "exits.py"),
        "index membership": (os.path.join(index, ".lock"),
                             [os.path.join(index, n) for n in ("nifty_membership.parquet", "amfi.parquet",
                                                               "build_info.json")],
                             os.path.join(index, "build_info.json"), "index_membership.py"),
    }


def check_inputs(folder):
    """Refuse while a bhavcopy download or an input build holds its lock (StoreBusy), when an input is not built
    (NotBuilt), or stale (InputStale): the master older than the newest bhavcopy year file, the adjustments older
    than the master (the builds' own rules), the exit list older than the adjustments, the index membership
    older than the master."""
    store = store_dir(folder)
    paths = input_paths(folder)
    locks = [(os.path.join(store, ".lock"), "a bhavcopy download (cm_bhav.py)")] + \
        [(lock, f"a {name} build ({cmd})") for name, (lock, _, _, cmd) in paths.items()]
    for lock, what in locks:
        if held(lock):
            raise StoreBusy(f"{what} holds {lock}; run after it ends.")
    for name, (_, need, _, cmd) in paths.items():
        if not all(os.path.isfile(p) for p in need):
            raise NotBuilt(f"No {name} in {os.path.dirname(need[0])}. Run: python {cmd} build")
    years = sm.year_files(store)
    if not years:
        raise NotBuilt(f"No bhavcopy parquet files in {store}. Run cm_bhav.py first.")
    newest = max(years, key=os.path.getmtime)
    info = {name: p[2] for name, p in paths.items()}
    cmds = {name: p[3] for name, p in paths.items()}
    for name, older_than in (("security master", newest), ("adjustments", info["security master"]),
                             ("exit list", info["adjustments"]), ("index membership", info["security master"])):
        if os.path.getmtime(info[name]) < os.path.getmtime(older_than):
            raise InputStale(f"The {name} ({info[name]}) is older than {older_than}. "
                             f"Rebuild it first: python {cmds[name]} build")


class Inputs:
    """Everything the plan reads, loaded once: the universe, the master's history, the exit list, the bhavcopy rows
    of the universe, the trading days, the factors, the index membership and company names. bhav=False skips the
    bhavcopy rows (phase 2 reads them per stock; self.bhav is then None)."""

    def __init__(self, folder, include_etf=False, bhav=True):
        check_inputs(folder)
        self.folder = folder
        m = sm.load_master(folder)
        kinds = ("STOCK", "ETF") if include_etf else ("STOCK",)
        secs = m["securities"][m["securities"]["instrument_kind"].isin(kinds)]
        e = ex.load_exits(folder)
        self.days = list(e["calendar"].days)
        self.status = e["status_by"]
        self.exit = e["exit_by"]
        rows = []
        for r in secs.sort_values("security_id").itertuples():
            first, last = sm.as_date(r.first_date), sm.as_date(r.last_date)
            st = self.status.get(r.security_id, {})
            rows.append({"security_id": r.security_id, "first_date": first, "last_date": last,
                         "symbol": r.last_symbol, "series": r.last_series, "isin": r.last_isin,
                         "last_close": float(r.last_close), "kind": r.instrument_kind,
                         "status": st.get("status", ""),
                         "exit_reason": (self.exit.get(r.security_id) or {}).get("reason", ""),
                         "need_from": max(first, DATA_FROM), "need_to": last})
        self.universe = rows
        self.by_id = {u["security_id"]: u for u in rows}
        hist = m["history"][m["history"]["security_id"].isin(self.by_id)]
        self.history = {sid: g.sort_values("from_date") for sid, g in hist.groupby("security_id")}
        self.bhav = read_bhav(folder, m, set(self.by_id)) if bhav else None
        self.factors = adj.load_factors(folder)["factors"]
        built = im.load_built(folder)
        self.membership = built["membership"]
        self.coverage = built["info"]["coverage"]
        self.names = company_names(folder)


def read_bhav(folder, m, ids):
    """The bhavcopy rows (master series) of the given securities, with security_id; one row per security and day."""
    cols = ["date", "symbol", "series", "isin", "open", "high", "low", "close", "volume", "turnover"]
    tables = [pq.read_table(f, columns=cols, filters=[("series", "in", list(m["info"]["series"]))])
              for f in sm.year_files(store_dir(folder))]
    df = pa.concat_tables(tables).to_pandas()
    df["security_id"] = df["isin"].map(m["isin_owner"])
    df = df[df["security_id"].isin(ids)]
    df = df.sort_values(["security_id", "date"], kind="stable").drop_duplicates(["security_id", "date"])
    return df.reset_index(drop=True)


def company_names(folder):
    """{symbol: company name} from the NSE reference lists (the latest non-blank row wins); missing lists are fine."""
    names = {}
    ref = os.path.join(store_dir(folder), "reference")
    for name, (sym_col, name_col) in REFERENCE_NAMES.items():
        path = os.path.join(ref, name)
        if not os.path.isfile(path):
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        if sym_col not in df or name_col not in df:
            continue
        for sym, company in zip(df[sym_col], df[name_col]):
            if sym.strip() and company.strip():
                names[sym.strip().upper()] = company.strip()
    return names


# ---- Part 2: Kite tokens ----

def kite_client():
    """KiteConnect from .env (KITE_API_KEY, KITE_ACCESS_TOKEN). The values are never printed."""
    from kiteconnect import KiteConnect
    env = config.load_env()
    key, token = env.get("KITE_API_KEY", ""), env.get("KITE_ACCESS_TOKEN", "")
    if not key or not token:
        raise SignInNeeded("No Kite API key or access token in .env. Sign in to Kite (python app.py) first.")
    kite = KiteConnect(api_key=key)
    kite.set_access_token(token)
    return kite


def match_tokens(universe, instruments):
    """kite_tokens.csv rows for the ACTIVE securities: today's symbol and series on both sides. Kite names a
    trade-to-trade stock SYMBOL-BE / SYMBOL-BZ. One candidate with the expected name is a match (EXACT); one
    candidate with the same symbol in another series is a match too (SERIES_SWITCH: Kite keeps one instrument per
    stock, the probe's price check decides); none is NO_TOKEN; two or more are REVIEW. Never guessed."""
    by_base = {}
    for i in instruments:
        if i.get("segment") != "NSE":
            continue
        ts, suffix = i["tradingsymbol"], series_suffix(i)
        if suffix and suffix not in T2T_SERIES:
            continue
        by_base.setdefault(ts.rpartition("-")[0] if suffix else ts, []).append(i)
    rows = []
    for u in universe:
        if u["status"] != "ACTIVE":
            continue
        sym, series = u["symbol"], u["series"]
        expected = sym if series == "EQ" else f"{sym}-{series}"
        row = {"security_id": u["security_id"], "isin": u["isin"], "symbol": sym, "series": series,
               "kite_tradingsymbol": "", "instrument_token": "", "kite_name": "", "kite_last_price": "",
               "bhav_close": u["last_close"], "match_status": "NO_TOKEN", "match_rule": "", "note": ""}
        cands = by_base.get(sym, [])
        if not cands:
            row["note"] = f"no {sym} / {sym}-BE / {sym}-BZ in Kite's list"
        elif len(cands) == 1:
            c = cands[0]
            exact = c["tradingsymbol"] == expected
            row.update(kite_tradingsymbol=c["tradingsymbol"], instrument_token=int(c["instrument_token"]),
                       kite_name=c.get("name") or "", kite_last_price=c.get("last_price"),
                       match_status="MATCHED_UNCHECKED", match_rule="EXACT" if exact else "SERIES_SWITCH",
                       note="price checked by probe" if exact else
                       f"SERIES_SWITCH: bhavcopy series {series}, Kite lists {c['tradingsymbol']} "
                       f"({c['instrument_token']}); price checked by probe")
        else:
            listed = ", ".join(f"{c['tradingsymbol']} ({c['instrument_token']})" for c in cands)
            row.update(match_status="REVIEW", note=f"{len(cands)} candidates: {listed}")
        rows.append(row)
    return rows


def saved_instruments(folder):
    """(path, list date) of the newest saved kite_instruments_NSE_<date>.csv."""
    paths = sorted(glob(os.path.join(plan_dir(folder), "kite_instruments_NSE_*.csv")))
    if not paths:
        raise NotBuilt(f"No saved Kite instrument list in {plan_dir(folder)}. Run: python equity_minute.py tokens")
    name = os.path.basename(paths[-1])
    return paths[-1], sm.as_date(name[len("kite_instruments_NSE_"):-len(".csv")])


def tokens(folder=None, kite=None, include_etf=False, rematch=False, now=None, log=print):
    """One instrument-list request: save it as received, match every ACTIVE security, write kite_tokens.csv.
    rematch: match again from the newest saved list instead, offline (no Kite request)."""
    folder = folder or config.equity_data_dir()
    inp = Inputs(folder, include_etf)
    lock = RunLock(lock_path(folder)).acquire("equity_minute.py tokens")
    try:
        out = plan_dir(folder)
        if rematch:
            path, day = saved_instruments(folder)
            name = os.path.basename(path)
            instruments = pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records")
        else:
            kite = kite or kite_client()
            try:
                instruments = kite.instruments("NSE")
            except kex.TokenException as e:
                raise SignInNeeded(f"Kite session expired or invalid ({type(e).__name__}). "
                                   "Sign in again (python app.py).")
            os.makedirs(out, exist_ok=True)
            day = (now or now_ist()).date()
            name = f"kite_instruments_NSE_{day.isoformat()}.csv"
            sm.write_csv(pd.DataFrame(instruments), os.path.join(out, name))
        rows = match_tokens(inp.universe, instruments)
        sm.write_csv(pd.DataFrame(rows, columns=TOKEN_COLUMNS), os.path.join(out, "kite_tokens.csv"))
        counts = pd.Series([r["match_status"] for r in rows], dtype=object).value_counts().to_dict()
        rules = pd.Series([r["match_rule"] for r in rows if r["match_rule"]], dtype=object).value_counts().to_dict()
        info = {"built_at": f"{now or now_ist():%Y-%m-%d %H:%M:%S}", "instruments_file": name,
                "instruments_date": day.isoformat(), "instruments_rows": len(instruments), "rematch": rematch,
                "bhav_last": iso(inp.days[-1]), "calendar_days_between": int(np.busday_count(inp.days[-1], day)),
                "active": len(rows), "match_status": counts, "match_rule": rules}
        write_json(os.path.join(out, "tokens_info.json"), info)
        lines = [f"Kite instrument list {day}: {len(instruments):,} rows " + (f"(rematched offline from {name})"
                                                                                if rematch else f"-> {name}"),
                 f"Latest bhavcopy day {inp.days[-1]} ({info['calendar_days_between']} weekdays before the list; "
                 "a rename in between shows up as NO_TOKEN)",
                 f"ACTIVE securities: {len(rows):,}"] + [f"  {k}: {v:,}" for k, v in sorted(counts.items())] + \
            ["Match rule:"] + [f"  {k}: {v:,}" for k, v in sorted(rules.items())]
        text = "\n".join(lines) + "\n"
        log(text, end="")
        return {"out": out, "rows": rows, "info": info, "report": text}
    finally:
        lock.release()


def read_tokens(folder):
    path = os.path.join(plan_dir(folder), "kite_tokens.csv")
    if not os.path.isfile(path):
        raise NotBuilt(f"No {path}. Run: python equity_minute.py tokens")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "match_rule" not in df:          # written before the rule existed: every match was exact
        df.insert(df.columns.get_loc("note"), "match_rule",
                  np.where(df["match_status"].isin(MATCHED), "EXACT", ""))
    return df


# ---- Part 3: probe ----

def fno_locks(env):
    """Every Kite F&O root's .lock (<root>/<UNDERLYING>_FnO_1min/.lock) under FNO_RAW_KITE / KITE_DATA_DIR."""
    roots = {config.fno_raw_dir("kite", env=env), config.fno_raw_dir("kite", "with", env),
             config.fno_raw_dir("kite", "without", env)}
    return sorted(p for root in roots for p in glob(os.path.join(root, "*_FnO_1min", ".lock")))


def check_fno(env):
    for path in fno_locks(env):
        if held(path):
            raise FnoBusy(f"A Kite F&O run holds {path}; both would share Kite's request limit. Probe after it ends.")


PROBE_SQL = """CREATE TABLE IF NOT EXISTS probes (
    security_id TEXT PRIMARY KEY, instrument_token TEXT, status TEXT, need_from TEXT, need_to TEXT,
    eod_first_day TEXT, eod_last_day TEXT, kite_days INTEGER, eod_missing TEXT, minute_first_day TEXT,
    price_status TEXT, price_day TEXT, price_kite_close REAL, requests INTEGER, error TEXT, probed_at TEXT)"""
PROBE_FIELDS = ["security_id", "instrument_token", "status", "need_from", "need_to", "eod_first_day", "eod_last_day",
                "kite_days", "eod_missing", "minute_first_day", "price_status", "price_day", "price_kite_close",
                "requests", "error", "probed_at"]


def open_db(folder):
    os.makedirs(plan_dir(folder), exist_ok=True)
    db = sqlite3.connect(os.path.join(plan_dir(folder), "probe_progress.sqlite"))
    db.execute(PROBE_SQL)
    db.commit()
    return db


def read_probes(folder):
    """{security_id: probe row} from probe_progress.sqlite ({} before the first probe)."""
    path = os.path.join(plan_dir(folder), "probe_progress.sqlite")
    if not os.path.isfile(path):
        return {}
    db = sqlite3.connect(path)
    try:
        db.execute(PROBE_SQL)
        return {r[0]: dict(zip(PROBE_FIELDS, r)) for r in db.execute(f"SELECT {','.join(PROBE_FIELDS)} FROM probes")}
    finally:
        db.close()


def save_probe(db, row):
    db.execute(f"INSERT OR REPLACE INTO probes ({','.join(PROBE_FIELDS)}) VALUES ({','.join('?' * len(PROBE_FIELDS))})",
               [row.get(k) for k in PROBE_FIELDS])
    db.commit()


def candle_day(value):
    return value.date() if isinstance(value, datetime) else sm.as_date(value)


def day_frame(candles):
    df = pd.DataFrame([{"date": candle_day(c["date"]), **{f: c[f] for f in FIELDS}} for c in candles],
                      columns=["date"] + FIELDS)
    return df.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)


class Prober:
    """One security at a time: day candles in 2,000-day chunks, the price check, the first minute day."""

    def __init__(self, kite, limiter):
        self.kite, self.limiter, self.requests = kite, limiter, 0

    def fetch(self, token, start, end, interval):
        self.requests += 1
        return fetch_chunk(self.kite, self.limiter, token, start, end, interval)

    def has_minute(self, token, day):
        return bool(self.fetch(token, datetime.combine(day, dtime.min), datetime.combine(day, dtime(23, 59, 59)),
                               "minute"))

    def minute_start(self, token, days):
        """First day with minute candles among the security's Kite days: the first day, else the first day on or
        after KITE_MINUTE_FROM, else a binary search on single days (None when even the last day has none)."""
        if not days:
            return None
        if self.has_minute(token, days[0]):
            return days[0]
        lo = 0                                    # days[lo] has no minute candles
        if days[0] < KITE_MINUTE_FROM:
            j = bisect_left(days, KITE_MINUTE_FROM)
            if j < len(days):
                if self.has_minute(token, days[j]):
                    return days[j]
                lo = j
        hi = len(days) - 1
        if hi == lo or not self.has_minute(token, days[hi]):
            return None
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if self.has_minute(token, days[mid]):
                hi = mid
            else:
                lo = mid
        return days[hi]

    def probe(self, token, u, bhav_days, out, price_tol):
        """Probe one security; writes kite_day/<security_id>.parquet and returns the probe row."""
        self.requests = 0
        candles = []
        for start, end in make_chunks(u["need_from"], u["need_to"], "day"):
            candles += self.fetch(token, start, end, "day")
        df = day_frame(candles)
        kdays = list(df["date"])
        have = set(kdays)
        missing = [d for d in bhav_days if d not in have]
        last = bhav_days[-1] if bhav_days else None
        hit = df[df["date"] == last]
        price_status, kclose = "MATCHED_UNCHECKED", None
        if len(hit) and u["last_close"]:
            kclose = float(hit["close"].iloc[0])
            ok = abs(kclose - u["last_close"]) / u["last_close"] <= price_tol
            price_status = "MATCHED" if ok else "PRICE_MISMATCH"
        minute = self.minute_start(token, kdays)
        os.makedirs(os.path.join(out, "kite_day"), exist_ok=True)
        sm.write_parquet(df, os.path.join(out, "kite_day", f"{u['security_id']}.parquet"))
        return {"security_id": u["security_id"], "instrument_token": str(token), "status": "DONE",
                "need_from": iso(u["need_from"]), "need_to": iso(u["need_to"]),
                "eod_first_day": iso(kdays[0]) if kdays else "", "eod_last_day": iso(kdays[-1]) if kdays else "",
                "kite_days": len(kdays), "eod_missing": json.dumps([iso(d) for d in missing]),
                "minute_first_day": iso(minute), "price_status": price_status, "price_day": iso(last),
                "price_kite_close": kclose, "requests": self.requests, "error": "",
                "probed_at": f"{now_ist():%Y-%m-%d %H:%M:%S}"}


def probe(folder=None, kite=None, speed=SPEED, force=False, price_tol=PRICE_TOL, include_etf=False, now=None,
          log=print):
    """Probe every MATCHED / MATCHED_UNCHECKED security not probed yet (or every one with force), then rebuild
    the outputs. AuthStop stops cleanly (SignInNeeded); done securities are kept for the next run."""
    folder = folder or config.equity_data_dir()
    inp = Inputs(folder, include_etf)
    toks = read_tokens(folder)
    check_fno(config.load_env())
    lock = RunLock(lock_path(folder)).acquire("equity_minute.py probe")
    db = None
    try:
        kite = kite or kite_client()
        db = open_db(folder)
        done = read_probes(folder)
        prober = Prober(kite, RateLimiter(speed))
        out = plan_dir(folder)
        todo = [r for r in toks.itertuples() if r.match_status in MATCHED and r.security_id in inp.by_id]
        bhav_days = {sid: list(g["date"]) for sid, g in inp.bhav.groupby("security_id")}
        stats = {"todo": len(todo), "probed": 0, "skipped": 0, "failed": 0, "requests": 0, "stopped": None}
        for n, t in enumerate(todo, 1):
            u = inp.by_id[t.security_id]
            old = done.get(t.security_id)
            if (not force and old and old["status"] == "DONE" and old["instrument_token"] == t.instrument_token
                    and old["need_from"] == iso(u["need_from"]) and old["need_to"] == iso(u["need_to"])):
                stats["skipped"] += 1
                continue
            try:
                row = prober.probe(int(t.instrument_token), u, bhav_days.get(t.security_id, []), out, price_tol)
                stats["probed"] += 1
            except ChunkFailed as e:
                row = {"security_id": t.security_id, "instrument_token": t.instrument_token, "status": "FAILED",
                       "need_from": iso(u["need_from"]), "need_to": iso(u["need_to"]), "error": str(e),
                       "requests": prober.requests, "probed_at": f"{now_ist():%Y-%m-%d %H:%M:%S}"}
                stats["failed"] += 1
                log(f"  {t.symbol}: FAILED {e}")
            except AuthStop as e:
                stats["requests"] += prober.requests
                raise SignInNeeded(f"Kite sign-in needed ({e}). Sign in (python app.py) and run probe again; "
                                   f"{stats['probed']:,} securities probed in this run are kept.")
            except (StopJob, KeyboardInterrupt):
                stats["stopped"] = t.symbol
                break
            stats["requests"] += prober.requests
            save_probe(db, row)
            if n % 50 == 0 or n == len(todo):
                log(f"  {n:,} / {len(todo):,} ({stats['probed']:,} probed, {stats['skipped']:,} already done, "
                    f"{stats['failed']:,} failed, {stats['requests']:,} requests, rate {prober.limiter.rate:g}/s)")
        db.close()
        db = None
        result = build_outputs(folder, inp, now=now, log=log)
        result["stats"] = stats
        return result
    finally:
        if db is not None:
            db.close()
        lock.release()


# ---- Part 4: outputs ----

def gaps(folder=None, include_etf=False, now=None, log=print):
    """Rebuild every output from the saved tokens, probe rows and day candles. Offline."""
    folder = folder or config.equity_data_dir()
    inp = Inputs(folder, include_etf)
    lock = RunLock(lock_path(folder)).acquire("equity_minute.py gaps")
    try:
        return build_outputs(folder, inp, now=now, log=log)
    finally:
        lock.release()


def with_factor(df, factors):
    """df (security_id, date) plus our cumulative factor of that day (adjustments.factor_on, vectorised)."""
    if df.empty:
        return df.assign(our_factor=pd.Series(dtype=float))
    f = factors[["security_id", "from_date", "factor"]].copy()
    f["_from"] = pd.to_datetime(f["from_date"])
    f = f.sort_values("_from")
    d = df.copy()
    d["_dt"] = pd.to_datetime(d["date"])
    d = d.sort_values("_dt")
    merged = pd.merge_asof(d, f[["security_id", "_from", "factor"]], left_on="_dt", right_on="_from",
                           by="security_id", direction="backward")
    first = f.groupby("security_id")["factor"].first()
    merged["our_factor"] = merged["factor"].fillna(merged["security_id"].map(first)).fillna(1.0).astype(float)
    return merged.drop(columns=["_dt", "_from", "factor"]).sort_values(["security_id", "date"]).reset_index(drop=True)


def kite_vs_bhav(folder, inp, probed):
    """Every Kite day of the probed securities joined with the bhavcopy row of that day."""
    frames = []
    for sid in sorted(probed):
        path = os.path.join(plan_dir(folder), "kite_day", f"{sid}.parquet")
        if os.path.isfile(path):
            k = pq.read_table(path).to_pandas()
            k.insert(0, "security_id", sid)
            frames.append(k)
    cols = ["security_id", "date"] + [f"kite_{f}" for f in FIELDS] + [f"bhav_{f}" for f in FIELDS] + \
        ["our_factor", "ratio", "difference", "adj_difference", "kite_multiplier", "kite_difference"]
    if not frames:
        return pd.DataFrame(columns=cols + ["symbol"])
    k = pd.concat(frames, ignore_index=True).rename(columns={f: f"kite_{f}" for f in FIELDS})
    b = inp.bhav[["security_id", "date", "symbol"] + FIELDS].rename(columns={f: f"bhav_{f}" for f in FIELDS})
    j = k.merge(b, on=["security_id", "date"], how="inner")
    j = with_factor(j, inp.factors)
    j["ratio"] = j["bhav_close"] / j["kite_close"]
    j["difference"] = j["bhav_close"] - j["kite_close"]
    j["adj_difference"] = j["bhav_close"] / j["our_factor"] - j["kite_close"]
    j["kite_multiplier"] = kite_multiplier(j)
    j["kite_difference"] = j["bhav_close"] / j["kite_multiplier"] - j["kite_close"]
    return j[cols + ["symbol"]]


def kite_multiplier(j):
    """Kite's own multiplier M per day (raw = M x (Kite + d)): bhav high-low range / Kite range on days with a wide
    enough range, the median of the SLOPE_WINDOW measured days around each day within one of our factor's periods
    (so it follows a step at an action), our factor where nothing was measured. Equal to the day's ratio when Kite
    is purely multiplicative, to our factor when it is purely additive."""
    rng = (j["kite_high"] - j["kite_low"]).to_numpy(float)
    unit = 2 * half_units(j)
    ok = (rng >= np.maximum(SLOPE_RANGE * j["kite_close"].to_numpy(float), SLOPE_UNITS * unit)) & \
        (j["bhav_high"] > j["bhav_low"]).to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = pd.Series(np.where(ok, (j["bhav_high"] - j["bhav_low"]).to_numpy(float) / rng, np.nan), index=j.index)

    def rolled(x):
        m = x.dropna().rolling(SLOPE_WINDOW, center=True, min_periods=1).median()
        return m.reindex(x.index).ffill().bfill()

    m = slope.groupby([j["security_id"], j["our_factor"]]).transform(rolled)
    return m.fillna(j["our_factor"]).to_numpy(float)


def half_units(kvb):
    """Half of Kite's rounding unit per day. Kite rounds some stocks' adjusted prices to 0.5, 1 or 5 (not tied to
    the factor): the unit is the largest of UNITS all four Kite prices sit on for UNIT_SHARE of the stock's days in
    a year with one of our factors, and for the day itself (one day alone never loosens its own check); at least
    0.05."""
    prices = kvb[["kite_open", "kite_high", "kite_low", "kite_close"]].to_numpy(float)
    keys = [kvb["security_id"], kvb["our_factor"], pd.to_datetime(kvb["date"]).dt.year]
    unit = np.full(len(kvb), UNITS[0])
    for u in UNITS[1:]:
        on = (np.abs(prices / u - np.round(prices / u)) < 1e-6).all(axis=1)
        share = pd.Series(on, index=kvb.index).groupby(keys).transform("mean").to_numpy(float)
        unit = np.where(on & (share >= UNIT_SHARE), u, unit)
    return unit * (HALF_TICK / UNITS[0])


def misfits(kvb):
    """Per O/H/L field and day: off the day's own ratio (multiplicative) / off the additive conversion by more than
    0.05% plus half of Kite's rounding unit per rounded Kite price (the field, and the close inside the ratio or
    kite_difference). Additive: raw = kite_multiplier x (Kite + kite_difference)."""
    kc = kvb["kite_close"].to_numpy(float)
    ratio, km = kvb["ratio"].to_numpy(float), kvb["kite_multiplier"].to_numpy(float)
    kd = kvb["kite_difference"].to_numpy(float)
    half = half_units(kvb)
    m_bad, a_bad = {}, {}
    with np.errstate(divide="ignore", invalid="ignore"):
        for f in ("open", "high", "low"):
            kite, bhav = kvb[f"kite_{f}"].to_numpy(float), kvb[f"bhav_{f}"].to_numpy(float)
            rel = PRICE_REL * np.abs(kite)
            m_bad[f] = np.abs(kite - bhav / ratio) > rel + half * (1 + np.abs(kite / kc))
            a_bad[f] = np.abs(kite - (bhav / km - kd)) > rel + 2 * half
    return m_bad, a_bad


def additive_periods(add_ok, mult_ok, ad, kc, half):
    """[(first, last, additive days)] positions of one security's ADDITIVE periods. An additive day fits the
    additive conversion but not its own ratio. A period ends after STEP_DAYS days that do not fit additively (days
    that fit both, flat candles, are neutral) and splits only on a lasting step: the difference moves by more than
    STEP_TOL x close + two half units and the next STEP_DAYS days are additive at a level of their own. A shorter
    move is a blip and stays inside. Periods with fewer than ADD_MIN_DAYS additive days are dropped."""
    cand = add_ok & ~mult_ok
    n, out, i = len(cand), [], 0
    while i < n:
        if not cand[i]:
            i += 1
            continue
        start, last, level, quiet, count, j = i, i, [ad[i]], 0, 1, i + 1
        while j < n:
            if cand[j]:
                tol = STEP_TOL * abs(kc[j]) + 2 * half[j]
                if abs(ad[j] - np.median(level[-STEP_DAYS:])) > tol:
                    w = slice(j, j + STEP_DAYS)
                    if j + STEP_DAYS <= n and cand[w].all() and np.ptp(ad[w]) <= tol:
                        break                               # a lasting step: a new period starts at j
                else:
                    level.append(ad[j])                     # a blip does not move the level
                last, quiet, count = j, 0, count + 1
            elif not add_ok[j]:
                quiet += 1
                if quiet >= STEP_DAYS:
                    break
            j += 1
        if count >= ADD_MIN_DAYS:
            out.append((start, last, count))
        i = last + 1
    return out


def adjustments_and_mismatches(kvb, inp):
    """kite_adjustment.csv (ADDITIVE periods), the day kind and eod_mismatch.csv rows. Every day outside an additive
    period converts with its own ratio (raw = Kite x ratio), so splits, demergers and small dividend steps need no
    period; a mismatch is a field that does not fit that day's own conversion."""
    periods, mismatches = [], []
    kvb = kvb.reset_index(drop=True)
    kvb["kind"] = "MULTIPLICATIVE"
    if kvb.empty:
        return pd.DataFrame(columns=ADJUSTMENT_COLUMNS), pd.DataFrame(columns=MISMATCH_COLUMNS), kvb
    m_bad, a_bad = misfits(kvb)
    mult_ok = ~(m_bad["open"] | m_bad["high"] | m_bad["low"])
    add_ok = ~(a_bad["open"] | a_bad["high"] | a_bad["low"])
    candidates = add_ok & ~mult_ok
    kinds = kvb["kind"].to_numpy(object)
    ad_all, kc_all = kvb["kite_difference"].to_numpy(float), kvb["kite_close"].to_numpy(float)
    half = half_units(kvb)
    for (sid, _), idx in kvb.groupby(["security_id", "our_factor"], sort=False).indices.items():
        if not candidates[idx].any():
            continue
        for first, last, count in additive_periods(add_ok[idx], mult_ok[idx], ad_all[idx], kc_all[idx], half[idx]):
            run = idx[first:last + 1]
            kinds[run] = "ADDITIVE"
            p = kvb.iloc[run]
            factors = sorted(set(p["our_factor"].round(6)))
            periods.append({"security_id": sid, "symbol": inp.by_id[sid]["symbol"], "from_date": iso(p["date"].iloc[0]),
                            "to_date": iso(p["date"].iloc[-1]), "days": len(p), "additive_days": count,
                            "kite_vs_ours": round(float((p["ratio"] / p["our_factor"]).median()), 6),
                            "median_ratio": round(float(p["ratio"].median()), 6),
                            "kite_multiplier": round(float(p["kite_multiplier"].median()), 6),
                            "median_kite_difference": round(float(p["kite_difference"].median()), 4),
                            "median_adj_difference": round(float(p["adj_difference"].median()), 4),
                            "our_factor": "|".join(f"{x:g}" for x in factors), "kind": "ADDITIVE"})
    kvb["kind"] = kinds
    additive = kinds == "ADDITIVE"
    for f in ("open", "high", "low"):
        bad = np.where(additive, a_bad[f], m_bad[f])
        for r in kvb[bad].itertuples():
            mismatches.append({"security_id": r.security_id, "symbol": r.symbol, "date": iso(r.date), "field": f,
                               "kite": getattr(r, f"kite_{f}"), "bhav": getattr(r, f"bhav_{f}"),
                               "factor": round(r.ratio, 6), "kind": r.kind})
    kv, bv = kvb["kite_volume"].to_numpy(float), kvb["bhav_volume"].to_numpy(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        vr = pd.Series(np.where(bv > 0, kv / bv, np.nan))
    # Kite scales volume by its own factor (not always ours, not the price ratio); it only steps at an action, so
    # a centred median of the neighbours follows a step exactly and a one-day error stands out
    vf = vr.groupby(kvb["security_id"]).transform(
        lambda x: x.rolling(VOLUME_WINDOW, center=True, min_periods=1).median()).to_numpy(float)
    vf = np.where(np.isnan(vf), 1.0, vf)
    off = [np.abs(kv - bv * x) > VOLUME_TOL * bv * x + 1
           for x in (kvb["our_factor"].to_numpy(float), kvb["ratio"].to_numpy(float), vf)]
    for r in kvb[off[0] & off[1] & off[2]].itertuples():
        mismatches.append({"security_id": r.security_id, "symbol": r.symbol, "date": iso(r.date), "field": "volume",
                           "kite": r.kite_volume, "bhav": r.bhav_volume, "factor": round(r.our_factor, 6),
                           "kind": r.kind})
    mm = pd.DataFrame(mismatches, columns=MISMATCH_COLUMNS).sort_values(["security_id", "date", "field"], kind="stable")
    per = pd.DataFrame(periods, columns=ADJUSTMENT_COLUMNS).sort_values(["security_id", "from_date"], kind="stable")
    return per.reset_index(drop=True), mm.reset_index(drop=True), kvb


def spans(rows, field, a=None, b=None):
    """'A 2015-01-01..2020-03-01; B 2020-03-02..2026-10-16' for a security's symbol (or ISIN) periods, clipped."""
    out = []
    for value, start, end in zip(rows[field], rows["from_date"], rows["to_date"]):
        start, end = sm.as_date(start), sm.as_date(end)
        if (a and end < a) or (b and start > b):
            continue
        start, end = max(start, a) if a else start, min(end, b) if b else end
        if out and out[-1][0] == value:
            out[-1][2] = end
        else:
            out.append([value, start, end])
    return "; ".join(f"{v} {s}..{e}" for v, s, e in out)


class Outputs:
    """Shared lookups for the gap rows."""

    def __init__(self, inp, tokens):
        self.inp = inp
        self.tokens = {r.security_id: r for r in tokens.itertuples()}
        self.bhav = {sid: g for sid, g in inp.bhav.groupby("security_id")}
        mem = inp.membership
        self.member = {sid: g for sid, g in mem.groupby("security_id")}
        self.reliable = {i: (sm.as_date(c["reliable_from"]) if c.get("reliable_from") else None)
                         for i, c in inp.coverage.items()}

    def stats(self, sid, a, b):
        g = self.bhav.get(sid)
        if g is None:
            return 0, None
        g = g[(g["date"] >= a) & (g["date"] <= b)]
        return len(g), (round(float(g["turnover"].median()) / CRORE, 2) if len(g) else None)

    def indices(self, sid, a, b):
        g = self.member.get(sid)
        if g is None:
            return "", 3
        parts, priority = [], 3
        for r in g.sort_values(["index", "from_date"]).itertuples():
            start, end = sm.as_date(r.from_date), sm.as_date(r.to_date) if r.to_date else b
            if end < a or start > b:
                continue
            start, end = max(start, a), min(end, b)
            rel = self.reliable.get(r.index)
            note = " (before reliable_from)" if rel is None or start < rel else ""
            parts.append(f"{r.index} {start}..{end}{note}")
            for p, name in PRIORITY_INDEX:
                if r.index == name:
                    priority = min(priority, p)
        return "; ".join(parts), priority

    def company(self, sid):
        t = self.tokens.get(sid)
        if t is not None and t.kite_name:
            return t.kite_name
        rows = self.inp.history.get(sid)
        syms = [self.inp.by_id[sid]["symbol"]] + (list(rows["symbol"])[::-1] if rows is not None else [])
        return next((self.inp.names[s] for s in syms if s in self.inp.names), "")

    def exit_note(self, sid):
        e = self.inp.exit.get(sid)
        if not e:
            return ""
        parts = []
        if e.get("acquirer_symbol"):
            parts.append(f"into {e['acquirer_symbol']}" + (f", {e['swap_ratio']}" if e.get("swap_ratio") else ""))
        if e.get("note"):
            parts.append(str(e["note"]))
        return "; ".join(parts)

    def row(self, data_missing, sid, a, b, reason, note="", trading_days=None):
        rows = self.inp.history.get(sid)
        n, turnover = self.stats(sid, a, b)
        indices, priority = self.indices(sid, a, b)
        return {"data_missing": data_missing, "security_id": sid,
                "symbols": spans(rows, "symbol", a, b) if rows is not None else "",
                "isins": spans(rows, "isin", a, b) if rows is not None else "", "company_name": self.company(sid),
                "need_from": iso(a), "need_to": iso(b), "trading_days": n if trading_days is None else trading_days,
                "reason": reason, "exit_note": self.exit_note(sid), "indices": indices, "median_turnover": turnover,
                "priority": priority, "note": note}


def runs_of(days, flags):
    """[(first, last, count)] of consecutive flagged days."""
    out, start, count = [], None, 0
    for d, f in zip(days, flags):
        if f:
            if start is None:
                start, count = d, 0
            last, count = d, count + 1
        elif start is not None:
            out.append((start, last, count))
            start = None
    if start is not None:
        out.append((start, last, count))
    return out


def sessions_of(states):
    """Market-wide missing sessions: dates at least SESSION_MIN probed securities traded inside their Kite range,
    more than SESSION_SHARE of them without a Kite day candle."""
    traded, lacking = {}, {}
    for s in states.values():
        if not s.get("eod_first"):
            continue
        for d in s["days"]:
            if s["eod_first"] <= d <= s["eod_last"]:
                traded[d] = traded.get(d, 0) + 1
        for d in s["missing"]:
            if s["eod_first"] <= d <= s["eod_last"]:
                lacking[d] = lacking.get(d, 0) + 1
    return {d: (lacking[d], traded[d]) for d in sorted(lacking)
            if traded.get(d, 0) >= SESSION_MIN and lacking[d] / traded[d] > SESSION_SHARE}


def build_outputs(folder, inp, now=None, log=print):
    """plan.csv, kite_gaps.csv, index_coverage.csv, kite_vs_bhav.parquet, kite_adjustment.csv, eod_mismatch.csv,
    build_report.txt and build_info.json, rebuilt in full."""
    out = plan_dir(folder)
    os.makedirs(out, exist_ok=True)
    toks = read_tokens(folder)
    probes = read_probes(folder)
    o = Outputs(inp, toks)
    bhav_days = {sid: list(g["date"]) for sid, g in o.bhav.items()}

    # the state of every security: token, probe, price check
    states = {}
    for u in inp.universe:
        sid = u["security_id"]
        t = o.tokens.get(sid)
        p = probes.get(sid)
        s = {"sid": sid, "u": u, "token": t.instrument_token if t is not None and t.instrument_token else "",
             "match": t.match_status if t is not None else ("NO_TOKEN" if u["status"] == "ACTIVE" else ""),
             "rule": t.match_rule if t is not None else "",
             "token_note": t.note if t is not None else "", "probed": False, "eod_first": None, "eod_last": None,
             "missing": [], "minute_first": None, "days": bhav_days.get(sid, [])}
        if (s["match"] in MATCHED and p and p["status"] == "DONE" and p["instrument_token"] == s["token"]):
            s.update(probed=True, match=p["price_status"],
                     eod_first=sm.as_date(p["eod_first_day"]) if p["eod_first_day"] else None,
                     eod_last=sm.as_date(p["eod_last_day"]) if p["eod_last_day"] else None,
                     missing=[sm.as_date(d) for d in json.loads(p["eod_missing"] or "[]")],
                     minute_first=sm.as_date(p["minute_first_day"]) if p["minute_first_day"] else None)
        elif s["match"] in MATCHED and p and p["status"] == "FAILED":
            s["probe_error"] = p["error"]
        states[sid] = s
    usable = {sid for sid, s in states.items() if s["probed"] and s["match"] in MATCHED}

    sessions = sessions_of({sid: states[sid] for sid in usable})
    for sid in usable:
        s = states[sid]
        s["missing_all"] = s["missing"]                 # index coverage: Kite has no candle on a session either
        s["missing"] = [d for d in s["missing"] if d not in sessions]

    kvb = kite_vs_bhav(folder, inp, usable)
    periods, mismatches, kvb = adjustments_and_mismatches(kvb, inp)
    kite_days = {sid: list(g["date"]) for sid, g in kvb.groupby("security_id")} if len(kvb) else {}
    sm.write_parquet(kvb.drop(columns=["symbol"]), os.path.join(out, "kite_vs_bhav.parquet"))
    sm.write_csv(periods, os.path.join(out, "kite_adjustment.csv"))
    sm.write_csv(mismatches, os.path.join(out, "eod_mismatch.csv"))
    mismatch_days = mismatches.groupby("security_id")["date"].nunique().to_dict() if len(mismatches) else {}
    off = (kvb["ratio"] / kvb["our_factor"] - 1).abs() > ADJ_TOL if len(kvb) else pd.Series(dtype=bool)
    top = sorted(mismatch_days.items(), key=lambda x: (-x[1], x[0]))[:TOP_MISMATCH]

    # plan.csv and kite_gaps.csv
    plan_rows, gap_rows = [], []
    for u in inp.universe:
        sid, s = u["security_id"], states[u["security_id"]]
        rows = inp.history.get(sid)
        a, b = u["need_from"], u["need_to"]
        note = s.get("probe_error", "")
        if s["match"] in MATCHED and not s["probed"]:
            note = note or "not probed yet"
        dl_eod = s["probed"] and s["match"] in MATCHED and s["eod_first"] is not None
        dl_min = dl_eod and s["minute_first"] is not None
        kd = kite_days.get(sid, [])
        plan_rows.append({
            "security_id": sid, "symbols": spans(rows, "symbol") if rows is not None else u["symbol"],
            "isins": spans(rows, "isin") if rows is not None else u["isin"], "status": u["status"],
            "exit_reason": u["exit_reason"], "match_status": s["match"], "match_rule": s["rule"],
            "instrument_token": s["token"],
            "need_from": iso(a), "need_to": iso(b), "eod_first_day": iso(s["eod_first"]),
            "eod_missing_days": len(s["missing"]) if s["probed"] else "",
            "eod_mismatch_days": mismatch_days.get(sid, 0) if s["probed"] else "",
            "minute_first_day": iso(s["minute_first"]), "download_eod": "Y" if dl_eod else "N",
            "download_minute": "Y" if dl_min else "N",
            "eod_requests": len(make_chunks(s["eod_first"], b, "day")) if dl_eod else 0,
            "minute_requests": len(make_chunks(s["minute_first"], b, "minute")) if dl_min else 0,
            "est_minute_rows": sum(1 for d in kd if d >= s["minute_first"]) * MINUTES_PER_DAY if dl_min else 0,
            "note": note})
        gap_rows += gaps_of(o, s, u)
    for d, (lacking, traded) in sessions.items():
        turnover = inp.bhav[(inp.bhav["date"] == d) & inp.bhav["security_id"].isin(usable)]["turnover"].sum()
        gap_rows.append({"data_missing": "BOTH", "security_id": "", "symbols": "", "isins": "", "company_name": "",
                         "need_from": iso(d), "need_to": iso(d), "trading_days": 1, "reason": "KITE_MISSING_SESSION",
                         "exit_note": "", "indices": "", "median_turnover": round(float(turnover) / CRORE, 2),
                         "priority": 1, "note": f"Kite has no day candle for {lacking:,} of {traded:,} probed "
                                                "stocks that traded (market-wide)"})
    early = [d for d in inp.days if d < KITE_MINUTE_FROM]
    early_ids = [sid for sid in usable if states[sid]["eod_first"] and states[sid]["eod_first"] < KITE_MINUTE_FROM]
    if early and early_ids:
        gap_rows.append({"data_missing": "1MIN", "security_id": "", "symbols": "", "isins": "", "company_name": "",
                         "need_from": iso(early[0]), "need_to": iso(early[-1]), "trading_days": len(early),
                         "reason": "KITE_MINUTE_START", "exit_note": "", "indices": "", "median_turnover": None,
                         "priority": 1, "note": f"Kite has no 1-minute candles before {KITE_MINUTE_FROM} for any "
                                                f"stock; {len(early_ids):,} probed stocks traded then"})
    gap_rows = minute_gap_rows(folder, o, gap_rows, states, usable)
    plan = pd.DataFrame(plan_rows, columns=PLAN_COLUMNS)
    gap = pd.DataFrame(gap_rows, columns=GAP_COLUMNS)
    gap["_t"] = pd.to_numeric(gap["median_turnover"], errors="coerce")
    gap = gap.sort_values(["priority", "_t", "security_id", "need_from"], ascending=[True, False, True, True],
                          na_position="last", kind="stable").drop(columns="_t").reset_index(drop=True)
    sm.write_csv(plan, os.path.join(out, "plan.csv"))
    sm.write_csv(gap, os.path.join(out, "kite_gaps.csv"))

    coverage = index_coverage(inp, states, usable)
    sm.write_csv(coverage, os.path.join(out, "index_coverage.csv"))

    extra = {"probe_failed": sum(1 for s in states.values() if s.get("probe_error")),
             "not_probed": sum(1 for s in states.values() if s["match"] in MATCHED and not s["probed"]
                               and not s.get("probe_error")),
             "est_eod_rows": sum(len(kite_days.get(sid, [])) for sid in usable),
             "factor_differs_days": int(off.sum()),
             "factor_differs_securities": int(kvb.loc[off, "security_id"].nunique()) if len(kvb) else 0,
             "eod_mismatch_days": int(sum(mismatch_days.values())),
             "top_mismatch": [{"security_id": sid, "symbol": inp.by_id[sid]["symbol"], "days": n} for sid, n in top]}
    info = build_info(folder, inp, plan, gap, periods, mismatches, sessions, extra, now)
    text = report_text(info, gap, coverage)
    sm.write_text(os.path.join(out, "build_report.txt"), text)
    write_json(os.path.join(out, "build_info.json"), info)
    log(text, end="")
    return {"out": out, "plan": plan, "gaps": gap, "coverage": coverage, "periods": periods,
            "mismatches": mismatches, "info": info, "report": text}


def stored_minute_days(folder):
    """{security_id: dates with stored 1-minute candles} from the phase 2 download's data/<sid>/days.parquet ({} before
    any stock is downloaded)."""
    base = os.path.join(out_dir(folder), "data")
    out = {}
    if not os.path.isdir(base):
        return out
    for sid in sorted(os.listdir(base)):
        path = os.path.join(base, sid, "days.parquet")
        if os.path.isfile(path):
            t = pq.read_table(path, columns=["date", "minute_rows"]).to_pandas()
            out[sid] = {sm.as_date(d) for d, n in zip(t["date"], t["minute_rows"]) if n > 0}
    return out


def minute_gap_rows(folder, o, gap_rows, states, usable):
    """The phase 2 download's corrections: days of KITE_MISSING_SESSION / EOD_EMPTY_DAYS rows that do have stored
    1-minute candles leave the 1-minute part (BOTH -> EOD; a run where only some days have minutes is split), and
    check/minute_empty_days.csv adds 1MIN / MINUTE_EMPTY_DAYS rows, check/no_kite_data.csv (retoken) a BOTH /
    NO_KITE_DATA row over need_from..need_to per stock. Nothing stored yet -> the rows unchanged."""
    have = stored_minute_days(folder)
    out = []
    for g in gap_rows:
        if have and g["reason"] == "EOD_EMPTY_DAYS" and g["security_id"] in have:
            sid, mins = g["security_id"], have[g["security_id"]]
            a, b, missing = sm.as_date(g["need_from"]), sm.as_date(g["need_to"]), set(states[sid]["missing"])
            days = [d for d in states[sid]["days"] if a <= d <= b and d in missing]
            if not any(d in mins for d in days):
                out.append(g)
                continue
            groups = []
            for d in days:
                if groups and groups[-1][0] == (d in mins):
                    groups[-1][1].append(d)
                else:
                    groups.append([d in mins, [d]])
            for with_min, ds in groups:
                n = len(ds)
                out.append(o.row("EOD" if with_min else "BOTH", sid, ds[0], ds[-1], "EOD_EMPTY_DAYS",
                                 f"{n} bhavcopy day(s) without a Kite day candle" +
                                 ("; Kite has 1-minute candles on them" if with_min else ""), trading_days=n))
        elif have and g["reason"] == "KITE_MISSING_SESSION":
            d = sm.as_date(g["need_from"])
            lacking = [sid for sid in usable if d in states[sid].get("missing_all", states[sid]["missing"])]
            k = sum(1 for sid in lacking if d in have.get(sid, ()))
            if not k:
                out.append(g)
            elif k == len(lacking):
                out.append({**g, "data_missing": "EOD",
                            "note": g["note"] + f"; Kite has 1-minute candles for all {k:,} of them"})
            else:
                out.append({**g, "data_missing": "EOD",
                            "note": g["note"] + f"; Kite has 1-minute candles for {k:,} of them"})
                out.append({**g, "data_missing": "BOTH",
                            "note": g["note"] + f"; {len(lacking) - k:,} of them have no 1-minute candles either"})
        else:
            out.append(g)
    path = os.path.join(out_dir(folder), "check", "minute_empty_days.csv")
    if os.path.isfile(path):
        for r in pd.read_csv(path, dtype=str, keep_default_na=False).itertuples():
            a, b, n = sm.as_date(r.from_date), sm.as_date(r.to_date), int(r.days)
            if r.security_id:
                if r.security_id in o.inp.by_id:
                    out.append(o.row("1MIN", r.security_id, a, b, "MINUTE_EMPTY_DAYS", r.note, trading_days=n))
            else:
                out.append({"data_missing": "1MIN", "security_id": "", "symbols": "", "isins": "", "company_name": "",
                            "need_from": iso(a), "need_to": iso(b), "trading_days": n, "reason": "MINUTE_EMPTY_DAYS",
                            "exit_note": "", "indices": "", "median_turnover": None, "priority": 1, "note": r.note})
    path = os.path.join(out_dir(folder), "check", "no_kite_data.csv")     # retoken: stocks Kite no longer lists
    if os.path.isfile(path):
        for r in pd.read_csv(path, dtype=str, keep_default_na=False).itertuples():
            u = o.inp.by_id.get(r.security_id)
            if u:
                out.append(o.row("BOTH", r.security_id, u["need_from"], u["need_to"], "NO_KITE_DATA", r.reason))
    return out


def gaps_of(o, s, u):
    """The kite_gaps.csv rows of one security."""
    sid, a, b = u["security_id"], u["need_from"], u["need_to"]
    if u["status"] == "EXITED":
        reason = u["exit_reason"] or "UNKNOWN"
        return [o.row("BOTH", sid, a, b, EXIT_REASONS.get(reason, reason))]
    if u["status"] != "ACTIVE":
        return [o.row("BOTH", sid, a, b, u["status"] or "NO_KITE_TOKEN", "not matched (only ACTIVE securities are)")]
    if s["match"] == "NO_TOKEN":
        return [o.row("BOTH", sid, a, b, "NO_KITE_TOKEN", s["token_note"])]
    if s["match"] == "REVIEW":
        return [o.row("BOTH", sid, a, b, "TOKEN_REVIEW", s["token_note"])]
    if not s["probed"]:
        return []
    if s["match"] == "PRICE_MISMATCH":
        return [o.row("BOTH", sid, a, b, "PRICE_MISMATCH", "Kite close differs from the bhavcopy on the last day")]
    if s["eod_first"] is None:
        return [o.row("BOTH", sid, a, b, "NO_KITE_DATA", "the token returned no day candles")]
    out, days = [], s["days"]
    missing = set(s["missing"])
    before = [d for d in days if d < s["eod_first"]]
    if len(before) > LATER_DAYS:
        out.append(o.row("BOTH", sid, before[0], before[-1], "EOD_STARTS_LATER",
                         f"Kite's first day candle {s['eod_first']}"))
        missing -= set(before)
    for first, last, n in runs_of(days, [d in missing for d in days]):
        out.append(o.row("BOTH", sid, first, last, "EOD_EMPTY_DAYS", f"{n} bhavcopy day(s) without a Kite day candle",
                         trading_days=n))
    start = max(s["eod_first"], KITE_MINUTE_FROM)
    kdays = [d for d in days if d >= start and d not in s["missing"]]
    if s["minute_first"] is None:
        if kdays:
            out.append(o.row("1MIN", sid, kdays[0], kdays[-1], "MINUTE_STARTS_LATER", "no minute candles found"))
    else:
        lag = [d for d in kdays if d < s["minute_first"]]
        if len(lag) > LATER_DAYS:
            out.append(o.row("1MIN", sid, lag[0], lag[-1], "MINUTE_STARTS_LATER",
                             f"Kite's first minute day {s['minute_first']}"))
    return out


def index_coverage(inp, states, usable):
    """Per index and trading day in its coverage: members, members with Kite EOD / 1-minute, and those without."""
    days = inp.days
    pos = {d: i for i, d in enumerate(days)}
    eod, minute = {}, {}
    for sid in usable:
        s = states[sid]
        if s["eod_first"] is None:
            continue
        e = np.zeros(len(days), dtype=bool)
        e[bisect_left(days, s["eod_first"]):bisect_right(days, s["eod_last"])] = True
        for d in s.get("missing_all", s["missing"]):
            if d in pos:
                e[pos[d]] = False
        eod[sid] = e
        mn = e.copy()
        mn[:bisect_left(days, s["minute_first"]) if s["minute_first"] else len(days)] = False
        minute[sid] = mn
    rows = []
    for index in INDEXES:
        c = inp.coverage.get(index) or {}
        if not c.get("first"):
            continue
        first, last = bisect_left(days, sm.as_date(c["first"])), bisect_right(days, sm.as_date(c["last"])) - 1
        n = last - first + 1
        if n <= 0:
            continue
        members, with_e, with_m = np.zeros(n, int), np.zeros(n, int), np.zeros(n, int)
        no_e, no_m = [[] for _ in range(n)], [[] for _ in range(n)]
        mem = inp.membership[inp.membership["index"] == index]
        for r in mem.itertuples():
            i0 = max(bisect_left(days, sm.as_date(r.from_date)), first)
            i1 = min(bisect_right(days, sm.as_date(r.to_date)) - 1 if r.to_date else last, last)
            if i1 < i0:
                continue
            sl = slice(i0 - first, i1 - first + 1)
            members[sl] += 1
            e = eod.get(r.security_id, np.zeros(len(days), dtype=bool))[i0:i1 + 1]
            mn = minute.get(r.security_id, np.zeros(len(days), dtype=bool))[i0:i1 + 1]
            with_e[sl] += e
            with_m[sl] += mn
            for k in np.flatnonzero(~e):
                no_e[i0 - first + k].append(r.symbol)
            for k in np.flatnonzero(~mn):
                no_m[i0 - first + k].append(r.symbol)
        for k in range(n):
            rows.append({"index": index, "date": iso(days[first + k]), "members": int(members[k]),
                         "with_eod": int(with_e[k]), "with_minute": int(with_m[k]),
                         "without_eod": ";".join(sorted(no_e[k])), "without_minute": ";".join(sorted(no_m[k]))})
    return pd.DataFrame(rows, columns=COVERAGE_COLUMNS)


def coverage_lines(coverage, index, kind, limit=None):
    """'NIFTY 50: 1-minute 49 of 50 from a to b (HDFC)' per run of days with the same shortfall."""
    c = coverage[coverage["index"] == index]
    if c.empty:
        return [f"{index}: not covered"]
    col, without, label = ("with_eod", "without_eod", "EOD") if kind == "eod" else \
        ("with_minute", "without_minute", "1-minute")
    full = int((c[col] == c["members"]).sum())
    lines = [f"{index}: {label} complete on {full:,} of {len(c):,} days"]
    runs, prev = [], None
    for r in c.itertuples():
        key = (getattr(r, col), r.members, getattr(r, without))
        if key[0] == key[1]:
            prev = None
            continue
        if prev == key:
            runs[-1][1] = r.date
        else:
            runs.append([r.date, r.date, key])
        prev = key
    for start, end, (have, members, names) in runs[:limit]:
        span = start if start == end else f"{start} to {end}"
        lines.append(f"  {label} {have} of {members} {'on' if start == end else 'from'} {span} ({names.replace(';', ', ')})")
    if limit and len(runs) > limit:
        lines.append(f"  ... {len(runs) - limit:,} more runs in index_coverage.csv")
    return lines


def counts(series):
    return {str(k): int(v) for k, v in series.value_counts().sort_index().items()}


def build_info(folder, inp, plan, gap, periods, mismatches, sessions, extra, now):
    toks_info = {}
    path = os.path.join(plan_dir(folder), "tokens_info.json")
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as f:
            toks_info = json.load(f)
    eod_req, min_req = int(plan["eod_requests"].sum()), int(plan["minute_requests"].sum())
    rows = int(plan["est_minute_rows"].sum())
    return {
        "built_at": f"{now or now_ist():%Y-%m-%d %H:%M:%S}", "bhav_first": iso(inp.days[0]), "bhav_last": iso(inp.days[-1]),
        "instruments_date": toks_info.get("instruments_date", ""), "instruments_file": toks_info.get("instruments_file", ""),
        "securities": len(plan), "status": counts(plan["status"]), "match_status": counts(plan["match_status"]),
        "match_rule": counts(plan.loc[plan["match_rule"] != "", "match_rule"]),
        "not_probed": extra["not_probed"], "probe_failed": extra["probe_failed"],
        "download_eod": int((plan["download_eod"] == "Y").sum()), "download_minute": int((plan["download_minute"] == "Y").sum()),
        "gap_rows": len(gap), "reason": counts(gap["reason"]), "data_missing": counts(gap["data_missing"]),
        "priority": counts(gap["priority"].astype(str)),
        "adjustment_periods": counts(periods["kind"]) if len(periods) else {},
        "adjustment_securities": int(periods["security_id"].nunique()) if len(periods) else 0,
        "eod_mismatch_rows": len(mismatches), "eod_mismatch_fields": counts(mismatches["field"]) if len(mismatches) else {},
        "eod_mismatch_securities": int(mismatches["security_id"].nunique()) if len(mismatches) else 0,
        "eod_mismatch_days": extra["eod_mismatch_days"], "top_mismatch": extra["top_mismatch"],
        "factor_differs_days": extra["factor_differs_days"],
        "factor_differs_securities": extra["factor_differs_securities"],
        "missing_sessions": {iso(d): {"lacking": a, "traded": b} for d, (a, b) in sessions.items()},
        "kite_minute_from": iso(KITE_MINUTE_FROM),
        "eod_requests": eod_req, "minute_requests": min_req,
        "hours_at_2_5": round((eod_req + min_req) / SPEED / 3600, 1),
        "est_minute_rows": rows, "est_eod_rows": extra["est_eod_rows"],
        "est_disk_gb": {"parquet": round(rows * PARQUET_BYTES / 1e9, 1), "csv": round(rows * CSV_BYTES / 1e9, 1)},
        "disk_assumption": f"{PARQUET_BYTES} bytes per minute row in parquet, {CSV_BYTES} in CSV (estimates)",
        "parameters": {"price_tol_default": PRICE_TOL, "adj_tol": ADJ_TOL, "half_tick": HALF_TICK, "units": UNITS,
                       "unit_share": UNIT_SHARE, "price_rel": PRICE_REL, "step_tol": STEP_TOL, "step_days": STEP_DAYS,
                       "add_min_days": ADD_MIN_DAYS, "slope_range": SLOPE_RANGE, "slope_units": SLOPE_UNITS,
                       "slope_window": SLOPE_WINDOW, "volume_tol": VOLUME_TOL, "volume_window": VOLUME_WINDOW,
                       "later_days": LATER_DAYS, "session_share": SESSION_SHARE, "session_min": SESSION_MIN},
    }


def report_text(info, gap, coverage):
    def block(title, d):
        return [title] + [f"  {k}: {v:,}" for k, v in d.items()]

    lines = ["Kite 1-minute / EOD plan (phase 1)", f"Built {info['built_at']} IST",
             f"Bhavcopy {info['bhav_first']} to {info['bhav_last']}; Kite instrument list {info['instruments_date'] or '-'}",
             "", PART0_FINDINGS, "", f"Securities: {info['securities']:,}"]
    lines += block("Status:", info["status"]) + block("Match status:", info["match_status"])
    lines += block("Match rule:", info["match_rule"])
    if info["not_probed"]:
        lines.append(f"Not probed yet: {info['not_probed']:,} (run: python equity_minute.py probe)")
    if info["probe_failed"]:
        lines.append(f"Probe failed: {info['probe_failed']:,} (run probe again)")
    lines += ["", f"Gap rows (kite_gaps.csv): {info['gap_rows']:,}"]
    lines += block("Per reason:", info["reason"]) + block("Per data_missing:", info["data_missing"])
    lines += block("Per priority:", info["priority"])
    lines += ["", "Market-wide missing sessions: " + (", ".join(info["missing_sessions"]) or "none")]
    lines += ["", "EOD check: every day converts with its own ratio (raw = Kite x bhav close / Kite close); inside an "
                  "additive period raw = kite_multiplier x (Kite + kite_difference).",
              f"Kite's factor differs from ours by more than {ADJ_TOL:.1%} on {info['factor_differs_days']:,} days in "
              f"{info['factor_differs_securities']:,} securities (converted per day, no period needed)",
              f"Additive periods (kite_adjustment.csv): {sum(info['adjustment_periods'].values()):,} in "
              f"{info['adjustment_securities']:,} securities"]
    lines += [f"EOD mismatches (eod_mismatch.csv): {info['eod_mismatch_rows']:,} rows, "
              f"{info['eod_mismatch_days']:,} security-days in {info['eod_mismatch_securities']:,} securities"] + \
        [f"  {k}: {v:,}" for k, v in info["eod_mismatch_fields"].items()]
    if info["top_mismatch"]:
        lines.append(f"Top {TOP_MISMATCH} securities by mismatch days:")
        lines += [f"  {t['symbol']} ({t['security_id']}): {t['days']:,}" for t in info["top_mismatch"]]
    lines += ["", "Index coverage (index_coverage.csv):"]
    for index in INDEXES:
        for kind in ("eod", "minute"):
            lines += coverage_lines(coverage, index, kind, limit=None if index == "NIFTY 50" else 5)
    lines += ["", "Later download:",
              f"  EOD: {info['download_eod']:,} securities, {info['eod_requests']:,} requests, "
              f"about {info['est_eod_rows']:,} rows",
              f"  1-minute: {info['download_minute']:,} securities, {info['minute_requests']:,} requests, "
              f"about {info['est_minute_rows']:,} rows",
              f"  about {info['hours_at_2_5']:,} hours at {SPEED} requests/s",
              f"  disk about {info['est_disk_gb']['parquet']} GB parquet / {info['est_disk_gb']['csv']} GB CSV "
              f"({info['disk_assumption']})",
              "  1-minute days missing inside a range can only be found by downloading it: the download task adds "
              "them to the gap list."]
    for p in (1, 2):
        top = gap[gap["priority"] == p].head(TOP_GAPS)
        lines += ["", f"Top {TOP_GAPS} gaps of priority {p}:"]
        for r in top.itertuples():
            lines.append(f"  {r.data_missing:4} {r.reason:20} {r.security_id or '-':8} {r.need_from}..{r.need_to} "
                         f"{(r.symbols or r.note)[:60]} turnover {r.median_turnover if pd.notna(r.median_turnover) else '-'}")
    return "\n".join(lines) + "\n"


def read_built(folder, name):
    path = os.path.join(plan_dir(folder), name)
    if not os.path.isfile(path):
        raise NotBuilt(f"No {path}. Run: python equity_minute.py gaps")
    return pd.read_csv(path, dtype=str, keep_default_na=False)
