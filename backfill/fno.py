"""Daily F&O download: 1-minute candles with OI for every live futures and options contract
of one underlying (NIFTY on NFO, SENSEX on BFO), saved in the Accelpix file layout:

    <save folder>/<UNDERLYING>_FnO_1min/<day folder>/futures/NIFTY27OCT26.csv
                                                    /options/NIFTY2500013OCT26CE.csv
                                                    /instruments_NFO.csv, summary.csv, run.log

The day folder is <YYYY-MM-DD>_exp-<nearest option expiry>_<withZeroVol|noZeroVol>_dl-<YYYY-MM-DD>_<HHMM> (when it was
first created, IST), e.g. 2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-13_2319; older days are plain <YYYY-MM-DD> (see day_folders).

Kite only serves live contracts. A day D can be downloaded completely only while every contract
that existed on D is still live, i.e. for D after the most recent expiry (see date_window)."""
import csv
import math
import os
import re
import shutil
import time
import glob
from datetime import date, datetime, timedelta
from datetime import time as dtime
from typing import NamedTuple
from zoneinfo import ZoneInfo

from kiteconnect import KiteConnect
from kiteconnect import exceptions as kex

from .dhan_api import SEGMENT_OF as DHAN_SEGMENT_OF
from .fetch import AuthStop, ChunkFailed, RateLimiter, StopJob, fetch_chunk
from .job import JOB, JOB_LOCK, STOP, job_incr, job_update
from .storage import log_error, open_db

IST = ZoneInfo("Asia/Kolkata")
# Underlying -> Kite exchange and strike step (for the ATM strike)
UNDERLYINGS = {"NIFTY": ("NFO", 50), "SENSEX": ("BFO", 100)}
# Underlying -> the exchange whose F&O bhavcopy lists its contracts (backfill/bhav.py SOURCES)
EXCHANGE_OF = {"NIFTY": "NSE", "SENSEX": "BSE"}
# Underlying -> Kite token of its spot index (NSE:NIFTY 50, BSE:SENSEX; Dhan: dhan_api.SPOT_IDS). The holiday check asks it when
# the nearest future has no candles: a thin future (SENSEX on BSE) can go a whole trading day untraded.
SPOT_TOKENS = {"NIFTY": 256265, "SENSEX": 265}
TYPES = {"FUT", "CE", "PE"}
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
WINDOW = (dtime(9, 0), dtime(16, 0))   # every row Kite returns in this window is kept
READY_AT = dtime(15, 40)               # IST; today's run is refused before this
AUTO_BHAV = True                       # Kite runs fetch and check the day's NSE/BSE bhavcopy (tests turn it off)
SESSION = ("09:15:00", "15:30:00")     # bars used for the futures O/H/L/C in the summary
MAX_TRIES = 3
HEADER = ["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "OI"]
# Listed_* come from the instrument dump; Traded_strikes and *_files from the files on disk for the day;
# the Bhav columns from the NSE/BSE bhavcopy check (backfill/bhav.py), blank until a check has run
BHAV_SUMMARY_COLUMNS = ["Bhav_traded_contracts", "Kite_files", "Missing_in_kite", "Extra_in_kite", "OI_diffs",
                        "Price_diffs"]
# Keep_zero_volume (ON/OFF) and Trim_to_bhav (ON / OFF / FALLBACK = ON but no bhavcopy, full list used): the settings
# of the last run that wrote the day (run.log has every run's). Listed_* count the contracts the run requested
# (the trimmed list when Trim_to_bhav is ON).
SUMMARY_COLUMNS = ["Expiry", "Bucket", "Listed_strikes", "Traded_strikes", "Listed_contracts", "CE_files", "PE_files", "ATM_strike",
                   "ATM_CE_bars", "ATM_PE_bars", *BHAV_SUMMARY_COLUMNS, "Keep_zero_volume", "Trim_to_bhav"]
FUTURE_COLUMNS = ["Future", "Expiry", "Bars", "Open", "High", "Low", "Close", "Volume", "Last_OI"]
WEEKLY_EXPIRY_WEEKDAY = 1   # Tuesday (Monday = 0); .env WEEKLY_EXPIRY_WEEKDAY overrides it, see config.py
EXPIRIES_FILE = "expiries_seen.csv"
# Kite builds candle volume from differences of the day's cumulative volume, so a contract that traded
# once can come back with candles (right price, OI) whose volumes are all 0. They are listed here with
# Kept Y/N (setting "keep zero-volume contracts"); progress.sqlite stores why a DONE contract has no file.
ZERO_VOLUME_FILE = "zero_volume_contracts.csv"
ZERO_VOLUME_COLUMNS = ["Contract", "Candles", "Last_close", "Last_OI", "Kept"]
NO_CANDLES, ZERO_VOLUME = "NO_CANDLES", "ZERO_VOLUME"
# Two contracts with the same tradingsymbol or the same file name: all of them FAILED, never overwritten
DUPLICATE_NAME = "DUPLICATE_NAME"
# Data source -> folder suffix and name. Dhan (backfill/dhan_api.DhanClient, a Kite-shaped client) writes
# <UNDERLYING>_FnO_1min_Dhan/ with its own progress keys, so Kite and Dhan days never mix.
FOLDERS = {"kite": "_FnO_1min", "dhan": "_FnO_1min_Dhan"}
BROKER = {"kite": "Kite", "dhan": "Dhan"}


# --- dates and times (always IST, whatever the machine's time zone) -------------------------

def now_ist():
    return datetime.now(IST)


def today_ist(now=None):
    return (now or now_ist()).astimezone(IST).date()


def too_early(run_date, now=None):
    """Message if run_date is today and it is before READY_AT IST (the day isn't over yet), else ""."""
    now = (now or now_ist()).astimezone(IST)
    if run_date == now.date() and now.time() < READY_AT:
        return f"Too early: today's candles can be downloaded after {READY_AT:%H:%M} IST (it is {now:%H:%M} IST)."
    return ""


# --- which dates can still be downloaded completely -----------------------------------------

class Window(NamedTuple):
    min_date: date          # previous expiry + 1 day
    max_date: date          # today (IST)
    current_expiry: date    # earliest live expiry
    previous_expiry: date
    estimated: bool         # previous expiry from the weekday fallback, not from saved history


def weekday_before(d, weekday):
    """The last date strictly before d that falls on weekday (Monday = 0)."""
    d -= timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def live_expiries(instruments, underlying, today):
    return sorted({as_date(i["expiry"]) for i in instruments
                   if i.get("name") == underlying and i.get("instrument_type") in TYPES
                   and i.get("expiry") and as_date(i["expiry"]) >= today})


def date_window(live, history, today, weekday=WEEKLY_EXPIRY_WEEKDAY):
    """Allowed run dates [previous expiry + 1, today]. On any later day every contract that existed
    is still live, so the day is complete; on or before the previous expiry at least one is gone.

    previous expiry = the latest expiry in history before the current week expiry. Without history,
    or when history stops more than a day before the weekday fallback (it was last updated too long
    ago to know the previous expiry), the fallback is used: the last `weekday` before the current
    expiry. That is never earlier than the real previous expiry (expiries only move to the day
    before a holiday), so at worst one complete day is refused."""
    if not live:
        raise ValueError("No live expiries in the instrument list.")
    current = live[0]
    fallback = weekday_before(current, weekday)
    known = [e for e in history if e < current]
    prev = max(known) if known else None
    estimated = prev is None or prev < fallback - timedelta(days=1)
    if estimated:
        prev = fallback
    return Window(min(prev + timedelta(days=1), today), today, current, prev, estimated)


def window_error(run_date, window):
    """Message if run_date is outside the window, else ""."""
    if run_date > window.max_date:
        return f"{run_date} is in the future (today is {window.max_date} in IST)."
    if run_date < window.min_date:
        return (f"Earliest date available is {window.min_date}. Contracts that expired on "
                f"{window.previous_expiry} are no longer served by Kite.")
    return ""


def window_note(window):
    return ("" if not window.estimated else
            f"WARNING: earliest date {window.min_date} is estimated (no saved expiry history covers the "
            f"previous expiry; assumed {window.previous_expiry:%a %d %b %Y}, the weekly expiry weekday).")


def read_expiries(data_root, underlying, source="kite"):
    """Expiry dates seen so far: expiries_seen.csv, or the saved instrument dumps if it doesn't exist yet."""
    base = fno_root(data_root, underlying, source)
    path = os.path.join(base, EXPIRIES_FILE)
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            return {date.fromisoformat(r["expiry"]): r["first_seen"] for r in csv.DictReader(f)}
    seen = {}
    for dump in sorted(glob.glob(os.path.join(base, "*", "instruments_*.csv"))):
        parsed = parse_day_folder(os.path.basename(os.path.dirname(dump)))
        if not parsed:
            continue
        with open(dump, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("name") == underlying and r.get("instrument_type") in TYPES and r.get("expiry"):
                    seen.setdefault(as_date(r["expiry"]), parsed[0].isoformat())
    return seen


def record_expiries(data_root, underlying, instruments, today, source="kite"):
    """Add the dump's expiries to expiries_seen.csv (one row per expiry, sorted); returns all of them."""
    seen = read_expiries(data_root, underlying, source)
    for i in instruments:
        if i.get("name") == underlying and i.get("instrument_type") in TYPES and i.get("expiry"):
            seen.setdefault(as_date(i["expiry"]), today.isoformat())
    base = fno_root(data_root, underlying, source)
    os.makedirs(base, exist_ok=True)
    write_csv(os.path.join(base, EXPIRIES_FILE), ["expiry", "first_seen"],
              [{"expiry": e.isoformat(), "first_seen": seen[e]} for e in sorted(seen)])
    return set(seen)


def load_window(kite, data_root, underlying, weekday=WEEKLY_EXPIRY_WEEKDAY, source="kite"):
    """Fetch today's instrument list, update the expiry history and work out the date window."""
    instruments = kite.instruments(UNDERLYINGS[underlying][0])
    today = today_ist()
    history = record_expiries(data_root, underlying, instruments, today, source)
    return instruments, date_window(live_expiries(instruments, underlying, today), history, today, weekday)


def holidays_logged(data_root, underlying, source="kite"):
    path = os.path.join(fno_root(data_root, underlying, source), "holidays.log")
    days = set()
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) > 2:
                    try:
                        days.add(date.fromisoformat(parts[2].rstrip(":")))
                    except ValueError:
                        pass
    return days


def missing_dates(data_root, underlying, window, now=None, source="kite"):
    """Dates in the window without a complete day (summary.csv written and every contract DONE),
    skipping logged holidays and today before READY_AT. Creates nothing on disk."""
    skip = holidays_logged(data_root, underlying, source)
    out, d = [], window.min_date
    while d <= window.max_date:
        if d not in skip and not too_early(d, now):
            out.append(d)
        d += timedelta(days=1)
    root = fno_root(data_root, underlying, source)
    folders = day_folders(root)
    done = [d for d in out if d in folders and os.path.exists(os.path.join(root, folders[d], "summary.csv"))]
    if done:
        db = open_db(data_root)
        try:
            done = {d for d in done if db.execute("SELECT COUNT(*) FROM chunks WHERE run = ? AND status != 'DONE'",
                                                  (run_key(underlying, d, source),)).fetchone()[0] == 0}
        finally:
            db.close()
    return [d for d in out if d not in done]


def range_dates(d_from, d_to):
    """Every date from d_from to d_to, both included."""
    return [d_from + timedelta(days=n) for n in range((d_to - d_from).days + 1)]


def parse_pick(day="", d_from="", d_to=""):
    """The dashboard's date picker: (run_date, None) for one day, (None, [dates]) for a range
    (date_from/date_to; the same day twice is one day). Raises ValueError with a message."""
    if d_from or d_to:
        try:
            first, last = date.fromisoformat(d_from), date.fromisoformat(d_to)
        except ValueError:
            raise ValueError("Choose the first and last day of the range.") from None
        if first > last:
            raise ValueError("The last day of the range is before the first.")
        return (first, None) if first == last else (None, range_dates(first, last))
    try:
        return date.fromisoformat(day), None
    except ValueError:
        raise ValueError("Choose the date to download.") from None


def to_ist(value):
    """Kite candle time (datetime or "2026-10-12T09:15:00+0530") as an IST datetime.
    Naive datetimes are taken as IST wall-clock time, as Kite returns them."""
    if isinstance(value, str):
        try:
            value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        return value.replace(tzinfo=IST)
    return value.astimezone(IST)


def as_date(value):
    return value if isinstance(value, date) and not isinstance(value, datetime) else \
        value.date() if isinstance(value, datetime) else date.fromisoformat(str(value)[:10])


# --- names and files ------------------------------------------------------------------------

def contract_name(inst_type, expiry, strike=None, underlying="NIFTY"):
    """NIFTY{DD}{MON}{YY} for futures, NIFTY{strike}{DD}{MON}{YY}{CE|PE} for options, built from
    expiry and strike (Kite's weekly tradingsymbols use another format). Raises ValueError."""
    if inst_type not in TYPES:
        raise ValueError(f"unknown instrument type {inst_type!r}")
    e = as_date(expiry)
    stamp = f"{e.day:02d}{MONTHS[e.month - 1]}{e.year % 100:02d}"
    if inst_type == "FUT":
        return f"{underlying}{stamp}"
    try:
        k = float(strike)
    except (TypeError, ValueError):
        raise ValueError(f"strike {strike!r} is not a number")
    if not math.isfinite(k) or k <= 0 or not k.is_integer():
        raise ValueError(f"strike {strike!r} is not a whole positive number")
    return f"{underlying}{int(k)}{stamp}{inst_type}"


def contract_file(day_dir, inst, underlying):
    """(name, path) of a contract's CSV in the day folder. Raises ValueError for a bad name."""
    name = contract_name(inst["instrument_type"], inst["expiry"], inst.get("strike"), underlying)
    sub = "futures" if inst["instrument_type"] == "FUT" else "options"
    return name, os.path.join(day_dir, sub, name + ".csv")


def fmt_num(v):
    """Numbers as Accelpix writes them: 25140 for whole values, 25120.5 otherwise."""
    if v is None or v == "":
        return ""
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)
    return str(v)


def candle_volume(candles):
    return sum(float(c.get("volume") or 0) for c in candles)


def candle_rows(ticker, candles, keep_zero=False):
    """CSV rows for one contract, or None when there are no candles, or when the total volume is 0
    and keep_zero is off. Rows are written as Kite returns them (volume 0 stays 0)."""
    if not candles or (not keep_zero and candle_volume(candles) == 0):
        return None
    rows = []
    for c in candles:
        t = to_ist(c["date"])
        rows.append([ticker, f"{t.year:04d}{t.month:02d}{t.day:02d}", f"{t:%H:%M:%S}",
                     *(fmt_num(c[k]) for k in ("open", "high", "low", "close", "volume")),
                     fmt_num(c.get("oi"))])
    return rows


def write_contract(path, ticker, candles, keep_zero=False):
    """Write the contract's CSV atomically; returns the bar count, or 0 (no file) when candle_rows
    gives None (no candles, or zero volume with keep_zero off)."""
    rows = candle_rows(ticker, candles, keep_zero)
    if rows is None:
        return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(HEADER)
        w.writerows(rows)
    os.replace(tmp, path)
    return len(rows)


def read_bars(path):
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, columns, rows):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


# --- selection and summary ------------------------------------------------------------------

def select_contracts(instruments, underlying, run_date):
    """Live FUT/CE/PE of the underlying: futures by expiry, then options by expiry, strike, type."""
    picked = [i for i in instruments if i.get("name") == underlying and i.get("instrument_type") in TYPES
              and i.get("expiry") and as_date(i["expiry"]) >= run_date]
    futures = sorted((i for i in picked if i["instrument_type"] == "FUT"), key=lambda i: as_date(i["expiry"]))
    options = sorted((i for i in picked if i["instrument_type"] != "FUT"),
                     key=lambda i: (as_date(i["expiry"]), float(i.get("strike") or 0), i["instrument_type"]))
    return futures + options


def bucket_labels(option_expiries, future_expiries):
    """Nearest option expiry "Current week", 2nd "Next week"; nearest future expiry "Current month",
    2nd "Next month". One expiry can have two labels; the rest are "Far"."""
    opt, fut = sorted(set(option_expiries)), sorted(set(future_expiries))
    labels = {e: [] for e in opt}
    for e, label in zip(opt[:2], ("Current week", "Next week")):
        labels[e].append(label)
    for e, label in zip(fut[:2], ("Current month", "Next month")):
        if e in labels:
            labels[e].append(label)
    return {e: " + ".join(ls) or "Far" for e, ls in labels.items()}


def atm_strike(close, step=50):
    return int(math.floor(float(close) / step + 0.5)) * step


def future_stats(name, expiry, rows):
    """Bars, O/H/L/C over 09:15-15:30 (close = last bar <= 15:30), volume of every row, last OI."""
    rows = rows or []
    out = {"Future": name, "Expiry": expiry.isoformat(), "Bars": len(rows), "Open": "", "High": "",
           "Low": "", "Close": "", "Volume": fmt_num(sum(float(r["Volume"] or 0) for r in rows)),
           "Last_OI": rows[-1]["OI"] if rows else ""}
    session = [r for r in rows if SESSION[0] <= r["Time"] <= SESSION[1]]
    if session:
        out.update(Open=session[0]["Open"], Close=session[-1]["Close"],
                   High=fmt_num(max(float(r["High"]) for r in session)),
                   Low=fmt_num(min(float(r["Low"]) for r in session)))
    return out


def build_summary(contracts, files, step):
    """Per option expiry rows (SUMMARY_COLUMNS) and per future rows (FUTURE_COLUMNS). Listed_strikes and
    Listed_contracts count the dump; Traded_strikes, CE_files and PE_files count the files on disk at the
    end of the run, so a resumed run includes files written earlier. files: tradingsymbol -> (name, path)."""
    futures = [c for c in contracts if c["instrument_type"] == "FUT"]
    options = [c for c in contracts if c["instrument_type"] != "FUT"]
    has_file = lambda c: c["tradingsymbol"] in files and os.path.exists(files[c["tradingsymbol"]][1])

    atm = None
    if futures and futures[0]["tradingsymbol"] in files:
        near = read_bars(files[futures[0]["tradingsymbol"]][1]) or []
        first = next((r for r in near if r["Time"] >= SESSION[0]), None)
        if first:
            atm = atm_strike(first["Close"], step)

    expiries = sorted({as_date(c["expiry"]) for c in options})
    labels = bucket_labels(expiries, [as_date(c["expiry"]) for c in futures])
    rows = []
    for e in expiries:
        opts = [c for c in options if as_date(c["expiry"]) == e]

        def atm_bars(kind):
            if atm is None:
                return ""
            c = next((c for c in opts if c["instrument_type"] == kind and float(c["strike"]) == atm), None)
            if c is None:
                return ""
            return len(read_bars(files[c["tradingsymbol"]][1]) or []) if has_file(c) else 0

        traded = [c for c in opts if has_file(c)]
        rows.append({**blank_row(), "Expiry": e.isoformat(), "Bucket": labels[e],
                     "Listed_strikes": len({float(c["strike"]) for c in opts}),
                     "Traded_strikes": len({float(c["strike"]) for c in traded}),
                     "Listed_contracts": len(opts),
                     "CE_files": sum(1 for c in traded if c["instrument_type"] == "CE"),
                     "PE_files": sum(1 for c in traded if c["instrument_type"] == "PE"),
                     "ATM_strike": "" if atm is None else atm,
                     "ATM_CE_bars": atm_bars("CE"), "ATM_PE_bars": atm_bars("PE")})
    for e in sorted({as_date(c["expiry"]) for c in futures}):   # one row per future expiry
        rows.append({**blank_row(), "Expiry": e.isoformat(), "Bucket": "Futures"})
    fut_rows = [future_stats(files[c["tradingsymbol"]][0], as_date(c["expiry"]),
                             read_bars(files[c["tradingsymbol"]][1]))
                for c in futures if c["tradingsymbol"] in files]
    return rows, fut_rows


def blank_row():
    return {c: "" for c in SUMMARY_COLUMNS}


def on_off(flag):
    return "ON" if flag else "OFF"


def saved_setting(day_dir, column):
    """column of the first row of the day's summary.csv, or "" (no summary.csv, or saved before it existed)."""
    path = os.path.join(day_dir, "summary.csv")
    if not os.path.exists(path):
        return ""
    with open(path, newline="", encoding="utf-8") as f:
        first = next(csv.DictReader(f), None)
    return (first or {}).get(column) or ""


def saved_keep_zero(day_dir):
    """Keep_zero_volume of the day's summary.csv ("ON", "OFF", or "" for days saved before it existed)."""
    return saved_setting(day_dir, "Keep_zero_volume")


def saved_trim(day_dir):
    """Trim_to_bhav of the day's summary.csv ("ON", "OFF", "FALLBACK", or "" for days saved before it existed)."""
    return saved_setting(day_dir, "Trim_to_bhav")


def trim_default():
    """The "download only contracts in the bhavcopy" setting (config.trim_to_bhav: .env TRIM_TO_BHAV, else
    config.TRIM_TO_BHAV), read when a run starts."""
    import config
    return config.trim_to_bhav()


def zero_volume_info(candles):
    """Candles, Last_close, Last_OI of a contract's candles (for zero_volume_contracts.csv)."""
    return {"Candles": len(candles), "Last_close": fmt_num(candles[-1]["close"]), "Last_OI": fmt_num(candles[-1].get("oi"))}


def write_zero_volume(day_dir, found):
    """Merge found (name -> row) into the day's zero_volume_contracts.csv, sorted by contract. Rows of
    earlier runs are kept unless found replaces them. Written even when empty (header only)."""
    path = os.path.join(day_dir, ZERO_VOLUME_FILE)
    rows = {}
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            rows = {r["Contract"]: r for r in csv.DictReader(f)}
    rows.update(found)
    write_csv(path, ZERO_VOLUME_COLUMNS, [rows[k] for k in sorted(rows)])
    return rows


def summary_rows(day_dir, contracts, files, step, underlying, keep_zero="", trim=""):
    """build_summary plus the bhavcopy columns when the day has been checked (its <underlying>_bhav.csv
    is compared again with the files on disk, so a Kite re-run keeps the check), Keep_zero_volume
    (keep_zero: "ON", "OFF" or "") and Trim_to_bhav (trim: "ON", "OFF", "FALLBACK" or "").
    Returns (rows, futures rows, bhavcopy status line)."""
    from . import bhav   # bhav imports this module
    rows, fut_rows = build_summary(contracts, files, step)
    for r in rows:
        r["Keep_zero_volume"], r["Trim_to_bhav"] = keep_zero, trim
    items = bhav.saved_items(day_dir, underlying)
    if items is None:
        return rows, fut_rows, bhav.PENDING
    cmp_rows, counts, _ = bhav.compare(items, day_dir, underlying)
    expiry_of = {files[c["tradingsymbol"]][0]: as_date(c["expiry"]).isoformat()
                 for c in contracts if c["tradingsymbol"] in files}
    groups = bhav.bhav_summary(cmp_rows, expiry_of)
    zero = {c: 0 for c in BHAV_SUMMARY_COLUMNS}
    seen = set()
    for r in rows:
        key = (r["Expiry"], "FUT" if r["Bucket"] == "Futures" else "OPT")
        seen.add(key)
        r.update(groups.get(key, zero))
    for key in sorted(set(groups) - seen):   # expiries in the bhavcopy that Kite's list doesn't have
        rows.append({**blank_row(), "Expiry": key[0], "Keep_zero_volume": keep_zero, "Trim_to_bhav": trim,
                     "Bucket": ("Futures" if key[1] == "FUT" else "Options") + " (not in Kite list)", **groups[key]})
    return rows, fut_rows, bhav.check_status(counts)


def rewrite_summary(day_dir, run_date, underlying):
    """Rebuild summary.csv from the day's saved instrument list and files (after a bhavcopy check).
    When the last run trimmed to the bhavcopy (Trim_to_bhav ON) the list is trimmed the same way from the
    saved bhavcopy, so Listed_* keep counting the contracts the run requested.
    Returns the rows, or None when the day has no saved instrument list."""
    from . import bhav   # bhav imports this module
    dumps = sorted(glob.glob(os.path.join(day_dir, "instruments_*.csv")))
    if not dumps:
        return None
    with open(dumps[0], newline="", encoding="utf-8") as f:
        contracts = select_contracts(list(csv.DictReader(f)), underlying, run_date)
    trim = saved_trim(day_dir)
    if trim == "ON":
        items = bhav.saved_items(day_dir, underlying)
        if items is not None:
            contracts = keep_listed(contracts, {i["name"] for i in items if not i["error"]}, underlying)[0]
    files = {}
    for c in contracts:
        try:
            files[c["tradingsymbol"]] = contract_file(day_dir, c, underlying)
        except ValueError:
            pass
    rows, _, _ = summary_rows(day_dir, contracts, files, UNDERLYINGS[underlying][1], underlying,
                              saved_keep_zero(day_dir), trim)
    write_csv(os.path.join(day_dir, "summary.csv"), SUMMARY_COLUMNS, rows)
    return rows


def table(columns, rows):
    """Fixed-width text table for the console and run.log."""
    cells = [columns] + [[str(r[c]) for c in columns] for r in rows]
    widths = [max(len(row[i]) for row in cells) for i in range(len(columns))]
    return "\n".join("  ".join(v.ljust(w) for v, w in zip(row, widths)).rstrip() for row in cells)


# --- cross-process lock ---------------------------------------------------------------------

class LockBusy(Exception):
    pass


class RunLock:
    """Exclusive OS lock on <UNDERLYING>_FnO_1min/.lock, shared by the CLI and the dashboard.
    The OS drops it when the process exits, so a crash never leaves a stale lock."""
    OFFSET = 1 << 20   # Windows locks are mandatory: lock a byte past the text so it stays readable

    def __init__(self, path):
        self.path, self.f = path, None

    def acquire(self, owner):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        f = open(self.path, "a+b")
        try:
            self._lock(f, True)
        except OSError:
            f.seek(0)
            holder = f.read().decode("utf-8", "replace").strip()
            f.close()
            raise LockBusy(f"Another {os.path.basename(os.path.dirname(self.path))} run is in progress"
                           + (f" ({holder})" if holder else "") + ".")
        f.seek(0)
        f.truncate()
        f.write(f"{owner}, pid {os.getpid()}, since {now_ist():%Y-%m-%d %H:%M:%S} IST".encode())
        f.flush()
        self.f = f
        return self

    def _lock(self, f, on):
        if os.name == "nt":
            import msvcrt
            os.lseek(f.fileno(), self.OFFSET, os.SEEK_SET)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK if on else msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(f.fileno(), (fcntl.LOCK_EX | fcntl.LOCK_NB) if on else fcntl.LOCK_UN)

    def release(self):
        if self.f:
            try:
                self._lock(self.f, False)
            except OSError:
                pass
            self.f.close()
            self.f = None


def fno_folder(underlying, source="kite"):
    return f"{underlying}{FOLDERS[source]}"


def fno_root(data_root, underlying, source="kite"):
    return os.path.join(data_root, fno_folder(underlying, source))


def acquire_lock(data_root, underlying, owner, source="kite"):
    """Take the run lock for this underlying (and source) or raise LockBusy."""
    return RunLock(os.path.join(fno_root(data_root, underlying, source), ".lock")).acquire(owner)


# --- day folders: <YYYY-MM-DD>_exp-<nearest option expiry>_<withZeroVol|noZeroVol>_dl-<YYYY-MM-DD>_<HHMM> ----------
# dl is when the folder was first created (IST). Older days are plain <YYYY-MM-DD>. A day's folder is found
# by its date; the only rename ever made is noZeroVol -> withZeroVol (mark_with_zero_volume).

ZERO_PARTS = {True: "withZeroVol", False: "noZeroVol"}
DAY_FOLDER_RE = re.compile(r"(\d{4}-\d{2}-\d{2})(?:_exp-(\d{4}-\d{2}-\d{2})_(withZeroVol|noZeroVol)"
                           r"_dl-(\d{4}-\d{2}-\d{2})_(\d{2})(\d{2}))?")


class DayFolderError(RuntimeError):
    """A trading day has more than one folder in a root."""


def day_folder_name(day, expiry, keep_zero, created_at):
    """e.g. 2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-13_2319; created_at is shown in IST."""
    return f"{as_date(day)}_exp-{as_date(expiry)}_{ZERO_PARTS[bool(keep_zero)]}_dl-{to_ist(created_at):%Y-%m-%d_%H%M}"


def parse_day_folder(name):
    """(trading day, expiry, keep zero-volume True/False, download time IST) of a day folder name; all but
    the day are None for an old-style <YYYY-MM-DD> name. None when the name is not a day folder."""
    m = DAY_FOLDER_RE.fullmatch(name)
    if not m:
        return None
    try:
        day = date.fromisoformat(m.group(1))
        if not m.group(2):
            return day, None, None, None
        created = datetime.strptime(f"{m.group(4)} {m.group(5)}{m.group(6)}", "%Y-%m-%d %H%M").replace(tzinfo=IST)
        return day, date.fromisoformat(m.group(2)), m.group(3) == ZERO_PARTS[True], created
    except ValueError:
        return None


def mark_with_zero_volume(day_dir):
    """A noZeroVol day folder becomes withZeroVol once a run with keep zero-volume ON has written it (its
    zero-volume files are never deleted later). Returns the folder's path from now on: the new one, or
    day_dir unchanged for any other name. The caller holds the root's lock; files inside must be closed."""
    parsed = parse_day_folder(os.path.basename(day_dir))
    if not parsed or parsed[2] is not False:
        return day_dir
    new = os.path.join(os.path.dirname(day_dir), day_folder_name(parsed[0], parsed[1], True, parsed[3]))
    os.rename(day_dir, new)
    return new


def day_folders(root):
    """{trading day: folder name} for every day folder in root (old and new names). Creates nothing;
    raises DayFolderError when a day has two folders."""
    if not os.path.isdir(root):
        return {}
    found = {}
    for n in sorted(os.listdir(root)):
        parsed = parse_day_folder(n)
        if parsed and os.path.isdir(os.path.join(root, n)):
            found.setdefault(parsed[0], []).append(n)
    dups = {d: ns for d, ns in found.items() if len(ns) > 1}
    if dups:
        raise DayFolderError(f"More than one folder for the same trading day in {root}: " +
                             "; ".join(", ".join(ns) for _, ns in sorted(dups.items())) +
                             ". Keep one per day (move or merge the others) and run again.")
    return {d: ns[0] for d, ns in found.items()}


def find_day_dir(root, day):
    """Full path of the existing folder for day in root, or None. Raises DayFolderError on duplicates."""
    name = day_folders(root).get(as_date(day))
    return os.path.join(root, name) if name else None


def day_dir(data_root, underlying, run_date, source="kite"):
    """The saved folder of run_date for this underlying and source, or None."""
    return find_day_dir(fno_root(data_root, underlying, source), run_date)


def nearest_option_expiry(instruments, underlying, run_date):
    """Earliest CE/PE expiry on or after run_date in the instrument list, or None."""
    exp = [as_date(i["expiry"]) for i in instruments if i.get("name") == underlying
           and i.get("instrument_type") in ("CE", "PE") and i.get("expiry") and as_date(i["expiry"]) >= run_date]
    return min(exp) if exp else None


def exchange_of(underlying, source="kite"):
    """Kite's exchange (NFO / BFO), or on Dhan its segment (NSE_FNO / BSE_FNO)."""
    exch = UNDERLYINGS[underlying][0]
    return DHAN_SEGMENT_OF[exch] if source == "dhan" else exch


def run_key(underlying, run_date, source="kite"):
    prefix = "fno" if source == "kite" else f"fno-{source}"
    return f"{prefix}:{underlying}:{exchange_of(underlying, source)}:{run_date}"


def job_fields(underlying, run_date, data_root, speed, keep_zero=False, source="kite", label="missing",
               trim_to_bhav=False):
    """JOB values for job_begin() when an F&O run starts; run_date None for a missing-days run or a
    date range (label "<from>_to_<to>")."""
    day = run_date.isoformat() if run_date else ""
    return {"kind": "fno", "source": source, "run": run_key(underlying, day or label, source), "folder": data_root,
            "speed": speed, "keep_zero": keep_zero, "trim_to_bhav": bool(trim_to_bhav),
            "fno_date": day, "fno_underlying": underlying,
            "fno_exchange": exchange_of(underlying, source), "fno_summary": [], "fno_futures": [],
            "out_dirs": [os.path.join(fno_folder(underlying, source), day)] if day else []}


# --- the run --------------------------------------------------------------------------------

class RunLog:
    """Appends timestamped lines to run.log and echoes them to the console."""

    def __init__(self, path, echo):
        self.f, self.echo = open(path, "a", encoding="utf-8"), echo

    def __call__(self, msg=""):
        for line in str(msg).split("\n"):
            self.f.write(f"{now_ist():%Y-%m-%d %H:%M:%S} {line}\n")
            self.echo(line)
        self.f.flush()

    def close(self):
        self.f.close()


def save_instruments(path, instruments):
    if not instruments:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(instruments[0]), lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(instruments)
    os.replace(tmp, path)


def fmt_eta(seconds):
    seconds = int(seconds)
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


def connect(api_key, access_token, echo, report=True):
    """KiteConnect with the token checked by kite.profile(): (kite, user id, "") or (None, "", status).
    report=False leaves JOB alone (a single-contract re-download is not a job); the message is echoed."""
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    try:
        return kite, kite.profile().get("user_id", ""), ""
    except (kex.TokenException, kex.PermissionException) as e:
        msg, status = f"Kite access token is invalid or expired ({e}). Sign in again on the dashboard.", "auth_failed"
    except Exception as e:
        msg, status = f"Could not reach Kite to check the token: {type(e).__name__}: {e}", "error"
    echo(msg)
    if report:
        job_update(state="stopped", message=msg)
    return None, msg, status


def stop_with(echo, status, msg):
    echo(msg)
    job_update(state="stopped", message=msg)
    return status


def exchange_name(underlying):
    """NSE or BSE: whose bhavcopy lists the underlying's contracts."""
    return EXCHANGE_OF.get(underlying, "NSE")


def keep_listed(contracts, names, underlying):
    """(the contracts whose name is in names, the names of all contracts). A contract whose name can't be
    built is kept: it fails as a naming error later, as without the trim."""
    kept, ours = [], set()
    for c in contracts:
        try:
            name = contract_name(c["instrument_type"], c["expiry"], c.get("strike"), underlying)
        except ValueError:
            kept.append(c)
            continue
        ours.add(name)
        if name in names:
            kept.append(c)
    return kept, ours


def trim_contracts(contracts, run_date, day_dir, underlying, source, rlog):
    """(contracts, "ON", "") with only the contracts the exchange listed on run_date (every row of its bhavcopy,
    traded or not), or (contracts unchanged, "FALLBACK", reason) without a bhavcopy (not published yet, 404,
    an error, or slower than bhav.TRIM_BUDGET). Never refuses the run."""
    from . import bhav   # bhav imports this module
    exch = exchange_name(underlying)
    job_update(message=f"Getting {exch}'s contract list for the day...")
    try:
        names, reason = bhav.listed_names(run_date, day_dir, underlying, log=rlog)
    except Exception as e:   # never fail a run over the bhavcopy
        names, reason = None, f"{type(e).__name__}: {e}"
    if names is None:
        rlog(f"Bhavcopy for {run_date} not available ({reason}): downloading all {len(contracts):,} contracts "
             f"{BROKER[source]} lists")
        return contracts, "FALLBACK", reason
    kept, ours = keep_listed(contracts, names, underlying)
    rlog(f"Trimmed to {len(kept):,} contracts listed in {exch}'s bhavcopy for {run_date} ({BROKER[source]} listed "
         f"{len(contracts):,}; {len(contracts) - len(kept):,} not listed by {exch} that day were skipped)")
    if names - ours:
        rlog(f"{len(names - ours):,} contracts in the bhavcopy are not in {BROKER[source]}'s list")
    return kept, "ON", ""


def fallback_note(reason):
    return f"Bhavcopy not available ({reason}): full contract list used"


def fetch_day_bhav(run_date, day_dir, base, underlying, rlog):
    """Make sure <day>/bhavcopy/ has the exchange's (NSE/BSE) bhavcopy for run_date (the saved one, else a
    download of at most bhav.CHECK_BUDGET seconds). "" when it is there, else why not; that is also logged to
    bhavcopy.log, and the day stays pending for the dashboard's Check buttons."""
    from . import bhav   # bhav imports this module
    job_update(message=f"Getting {exchange_name(underlying)}'s bhavcopy for {run_date}...")
    try:
        names, reason = bhav.listed_names(run_date, day_dir, underlying, log=rlog, budget=bhav.CHECK_BUDGET)
    except Exception as e:   # never fail a finished download over the bhavcopy
        names, reason = None, f"{type(e).__name__}: {e}"
    if names is not None:
        return ""
    rlog(f"Bhavcopy for {run_date} not saved: {reason}. Check it later from the dashboard or nifty_bhav.py.")
    bhav.log_line(os.path.join(base, "bhavcopy.log"), f"{run_date}: {reason}")
    return reason


def check_day_bhav(day_dir, run_date, underlying, echo, rows, line):
    """bhav.check_saved for the day just downloaded: (summary rows, bhavcopy line, True), or the given
    rows, the line with the error and False when the check fails."""
    from . import bhav   # bhav imports this module
    # check_saved's source is where the bhavcopy came from: kept from an earlier check, else the exchange's URL
    checked = os.path.exists(os.path.join(day_dir, "bhavcopy", "bhav_check.json"))
    url = "" if checked else bhav.bhav_url(run_date, underlying)
    try:
        _, line, summary = bhav.check_saved(day_dir, run_date, underlying, url, log=echo)
        return summary or rows, line, True
    except Exception as e:
        echo(f"Bhavcopy check failed: {type(e).__name__}: {e}")
        return rows, f"{line} (check failed: {type(e).__name__}: {e})", False


class DuplicateWrite(Exception):
    """save() was asked to write a file another contract already wrote in this run."""

    def __init__(self, owner, name):
        super().__init__(f"{name} already written by {owner}")
        self.owner, self.name = owner, name


def run_daily(api_key, access_token, run_date, data_root, speed, underlying="NIFTY", log=print,
              weekday=WEEKLY_EXPIRY_WEEKDAY, kite=None, user="", instruments=None, window=None,
              label="", last=True, keep_zero=False, refetch=False, source="kite", trim_to_bhav=None,
              bhav_check=None):
    """Download day run_date for every contract that was live on it. The caller holds the lock
    (acquire_lock) and has called job_begin(**job_fields(...)). run_missing passes kite, instruments
    and window to share them between days; last=False keeps the job running after this day.
    keep_zero writes a file for contracts whose candles all have volume 0 (otherwise they get none);
    with it on, DONE contracts without a file whose reason is ZERO_VOLUME are requested again.
    refetch requests every DONE contract without a file again, whatever its reason (refetch_no_trade).
    source "dhan" (kite = a backfill.dhan_api.DhanClient) writes <UNDERLYING>_FnO_1min_Dhan/ under its own run keys.
    trim_to_bhav (Kite and Dhan; None = the setting, trim_default()) keeps only contracts in the day's bhavcopy
    (NSE/BSE), traded or not; when the bhavcopy is not available within bhav.TRIM_BUDGET seconds the full list is
    used (Trim_to_bhav FALLBACK in summary.csv, and the job message says so).
    bhav_check (default: on for Kite and Dhan, off with refetch, which checks by itself) gets the day's NSE/BSE bhavcopy
    into <day>/bhavcopy/ after the download (at most bhav.CHECK_BUDGET seconds) and runs bhav.check_saved;
    when the exchange has not published it the day stays pending. It never changes the returned status.
    Returns "finished", "finished_failures", "holiday", "too_early", "outside_window",
    "auth_failed", "stopped" or "error". Nothing is written for the last four before the run starts."""
    step = UNDERLYINGS[underlying][1]
    exchange = exchange_of(underlying, source)
    run = run_key(underlying, run_date, source)
    echo = lambda m: log(f"[{run}] {m}")
    start = datetime.combine(run_date, WINDOW[0])
    end = datetime.combine(run_date, WINDOW[1])
    msg = too_early(run_date)
    if msg:
        return stop_with(echo, "too_early", msg)

    # The token and the date window are checked before anything is created for the day
    if kite is None:
        kite, user, status = connect(api_key, access_token, echo)
        if status:
            return status
    if instruments is None:
        try:
            job_update(message="Loading instrument list...")
            instruments, window = load_window(kite, data_root, underlying, weekday, source)
        except (kex.TokenException, kex.PermissionException) as e:
            return stop_with(echo, "auth_failed", f"{BROKER[source]} access token is invalid or expired ({e}).")
        except Exception as e:
            return stop_with(echo, "error", f"Could not load the instrument list: {type(e).__name__}: {e}")
    msg = window_error(run_date, window)
    if msg:
        return stop_with(echo, "outside_window", msg)

    # The day's folder: the existing one (any name), else a new one named with the nearest option expiry
    # in this run's full instrument list (before the bhavcopy trim), the zero-volume setting and the time
    today = today_ist()
    base = fno_root(data_root, underlying, source)
    try:
        day = find_day_dir(base, run_date)
    except DayFolderError as e:
        return stop_with(echo, "error", str(e))
    created = day is None
    if created:
        expiry = nearest_option_expiry(instruments, underlying, run_date)
        if expiry is None:
            return stop_with(echo, "error", f"No {underlying} option expiry on or after {run_date} in the "
                                            f"{BROKER[source]} instrument list; nothing was saved.")
        day = os.path.join(base, day_folder_name(run_date, expiry, keep_zero, now_ist()))
    job_update(run=run, fno_date=run_date.isoformat(), fno_summary=[], fno_futures=[],
               out_dirs=[os.path.join(fno_folder(underlying, source), os.path.basename(day))])
    for sub in ("futures", "options"):
        os.makedirs(os.path.join(day, sub), exist_ok=True)
    db = open_db(data_root)
    rlog = RunLog(os.path.join(day, "run.log"), echo)
    limiter = RateLimiter(speed)
    current = ""   # contract being fetched, for errors.log
    try:
        rlog(f"=== {underlying} F&O 1-minute run for {run_date} ({exchange}), started "
             f"{now_ist():%Y-%m-%d %H:%M:%S} IST, {BROKER[source]} user {user}, speed {speed} req/s"
             + (", re-checking contracts without a file" if refetch else ""))
        rlog(f"Keep zero-volume contracts: {on_off(keep_zero)}")
        rlog(f"Date window {window.min_date} to {window.max_date} (previous expiry {window.previous_expiry}, "
             f"current week expiry {window.current_expiry})")
        if window.estimated:
            rlog(window_note(window))
        # The list is today's, so a past day's copy is named after the day it was fetched
        dump_name = (f"instruments_{exchange}.csv" if run_date == today
                     else f"instruments_{exchange}_fetched_{today}.csv")
        save_instruments(os.path.join(day, dump_name), instruments)
        contracts = select_contracts(instruments, underlying, run_date)
        if trim_to_bhav is None:
            trim_to_bhav = trim_default()
        trim_note = ""
        if trim_to_bhav:
            listed = len(contracts)
            contracts, trim_mode, trim_reason = trim_contracts(contracts, run_date, day, underlying, source, rlog)
            if trim_mode == "ON":
                rlog(f"Contracts: trimmed to bhavcopy (kept {len(contracts):,} of {listed:,})")
            else:
                rlog(f"Contracts: full list (bhavcopy not available: {trim_reason})")
                trim_note = fallback_note(trim_reason)
                rlog(trim_note)
                job_update(message=trim_note)
        else:
            trim_mode = "OFF"
            rlog("Contracts: full list (trim OFF)")
        futures = [c for c in contracts if c["instrument_type"] == "FUT"]
        if not futures:
            raise RuntimeError(f"No live {underlying} futures in the {exchange} instrument list.")

        # tradingsymbol -> (name, path) / error. A tradingsymbol or file name shared by two contracts would
        # send one contract's candles into another's file, so every contract involved fails instead.
        files, name_errors, by_name, dup_syms = {}, {}, {}, set()
        sym_count = {}
        for c in contracts:
            sym_count[c["tradingsymbol"]] = sym_count.get(c["tradingsymbol"], 0) + 1
        for c in contracts:
            sym = c["tradingsymbol"]
            if sym_count[sym] > 1:
                name_errors[sym] = f"{DUPLICATE_NAME}: tradingsymbol {sym} is shared by {sym_count[sym]} contracts"
                dup_syms.add(sym)
                continue
            try:
                name, path = contract_file(day, c, underlying)
            except ValueError as e:
                name_errors[sym] = f"naming error: {e}"
                continue
            by_name.setdefault(name, []).append(sym)
            files[sym] = (name, path)
        for name, syms in by_name.items():
            if len(syms) > 1:
                for sym in syms:
                    name_errors[sym] = f"{DUPLICATE_NAME}: file name {name} is also built for " + \
                                       ", ".join(s for s in syms if s != sym)
                    dup_syms.add(sym)
                    files.pop(sym)
        for sym in sorted(dup_syms):
            rlog(f"FAILED {sym}: {name_errors[sym]}")
        n_fut = len(futures)
        rlog(f"{len(contracts):,} live contracts: {n_fut} futures, {len(contracts) - n_fut:,} options "
             f"(window {WINDOW[0]:%H:%M}-{WINDOW[1]:%H:%M} IST, OI on)")

        db.executemany("INSERT OR IGNORE INTO chunks (run, symbol, token, chunk_from, chunk_to, status) "
                       "VALUES (?, ?, ?, ?, ?, 'PENDING')",
                       [(run, c["tradingsymbol"], c["instrument_token"], str(start), str(end)) for c in contracts])
        db.commit()
        status_of, reason_of = {}, {}
        for sym, st, reason in db.execute("SELECT symbol, status, reason FROM chunks WHERE run = ?", (run,)):
            status_of[sym], reason_of[sym] = st, reason
        has_file = lambda sym: sym in files and os.path.exists(files[sym][1])

        # Contracts done without a file are requested again: all of them when refetching (days saved
        # before reasons were stored have none), else the zero-volume ones once keep_zero is on
        redo = [c for c in contracts if status_of.get(c["tradingsymbol"]) == "DONE" and not has_file(c["tradingsymbol"])
                and (refetch or (keep_zero and reason_of.get(c["tradingsymbol"]) == ZERO_VOLUME))]
        if redo:
            db.executemany("UPDATE chunks SET status = 'PENDING', reason = NULL WHERE run = ? AND symbol = ?",
                           [(run, c["tradingsymbol"]) for c in redo])
            db.commit()
            for c in redo:
                status_of[c["tradingsymbol"]] = "PENDING"
            rlog(f"Requesting {len(redo):,} contracts without a file again")

        by_sym = {c["tradingsymbol"]: c for c in contracts}
        written = {}   # path -> tradingsymbol of every file written by this run

        def mark(c, st, error=None, reason=None):
            db.execute("UPDATE chunks SET status = ?, tries = tries + 1, error = ?, reason = ?, updated = ? "
                       "WHERE run = ? AND symbol = ? AND chunk_from = ?",
                       (st, error, reason, datetime.now().isoformat(timespec="seconds"), run, c["tradingsymbol"],
                        str(start)))
            db.commit()
            status_of[c["tradingsymbol"]] = st

        counts = {"written": 0, "present": 0, "no_candles": 0, "zero_kept": 0, "zero_skipped": 0}
        zero_found = {}   # name -> zero_volume_contracts.csv row, this run

        def fetch(c):
            return fetch_chunk(kite, limiter, c["instrument_token"], start, end, "minute",
                               oi=True, max_tries=MAX_TRIES)

        def save(c, candles):
            """Write the file; returns why there is none (NO_CANDLES / ZERO_VOLUME), or None."""
            name, path = files[c["tradingsymbol"]]
            owner = written.get(path)
            if owner is not None and owner != c["tradingsymbol"]:
                raise DuplicateWrite(owner, name)
            zero = bool(candles) and candle_volume(candles) == 0
            bars = write_contract(path, name, candles, keep_zero)
            if bars:
                written[path] = c["tradingsymbol"]
            if zero:
                zero_found[name] = {"Contract": name, **zero_volume_info(candles), "Kept": "Y" if bars else "N"}
                key = "zero_kept" if bars else "zero_skipped"
                counts[key] += 1
                job_incr(key)
            if bars:
                counts["written"] += 1
                with JOB_LOCK:
                    JOB["completed"].append([name, datetime.now().isoformat(timespec="seconds")])
                return None
            if zero:
                return ZERO_VOLUME
            counts["no_candles"] += 1
            job_incr("empty")
            return NO_CANDLES

        # Holiday check: the nearest future has no candles on a market holiday
        near = futures[0]
        near_sym = near["tradingsymbol"]
        if status_of.get(near_sym) != "DONE" and not (near_sym in files and os.path.exists(files[near_sym][1])):
            current = near_sym
            job_update(message="Checking the market was open...", symbol=files.get(near_sym, (near_sym,))[0])
            try:
                candles = fetch(near)
            except ChunkFailed as e:
                raise RuntimeError(f"Holiday check failed for {near_sym} (token {near['instrument_token']}): {e}")
            if not candles and not spot_traded(kite, limiter, underlying, source, start, end):
                return holiday(base, day, created, run, run_date, near, rlog, db, last)
            if not candles:
                rlog(f"{files.get(near_sym, (near_sym,))[0]} has no candles but {underlying} spot traded: not a holiday")
            if near_sym in files:
                mark(near, "DONE", reason=save(near, candles))

        # Files already on disk count as done (resume after a crash)
        for c in contracts:
            sym = c["tradingsymbol"]
            if status_of.get(sym) != "DONE" and has_file(sym):
                mark(c, "DONE")
                counts["present"] += 1
        n_done = sum(1 for c in contracts if status_of.get(c["tradingsymbol"]) == "DONE")
        n_failed = sum(1 for c in contracts if status_of.get(c["tradingsymbol"]) == "FAILED")
        total = len(contracts)
        job_update(stocks_total=total, chunks_total=total, stocks_done=n_done, chunks_done=n_done,
                   failed=n_failed, message="")
        if n_done:
            rlog(f"{n_done:,} contracts already done, skipping them")

        part = lambda: f"{label}{underlying} F&O {run_date}: {JOB['stocks_done']:,} / {total:,} contracts"
        t0, processed = time.monotonic(), 0
        for pass_no in (1, 2):   # pass 2 retries the contracts that failed
            todo = [c for c in contracts if status_of.get(c["tradingsymbol"]) != "DONE"]
            if not todo:
                break
            job_update(**{"pass": pass_no})
            if pass_no == 2:
                rlog(f"Retrying {len(todo)} failed contracts")
            for c in todo:
                if STOP.is_set():
                    raise StopJob()
                sym = c["tradingsymbol"]
                current = sym
                prev = status_of.get(sym)
                name = files.get(sym, (sym,))[0]
                job_update(symbol=name, part=part())
                error, reason = name_errors.get(sym), None
                if sym in dup_syms:
                    reason = DUPLICATE_NAME
                elif error is None:
                    try:
                        reason = save(c, fetch(c))
                    except DuplicateWrite as e:
                        # never overwrite: the file written earlier this run is removed and both contracts fail
                        error, reason = f"{DUPLICATE_NAME}: file {e.name} was already written by {e.owner}", DUPLICATE_NAME
                        other_path = files[e.owner][1]
                        if os.path.exists(other_path):
                            os.remove(other_path)
                        counts["written"] -= 1
                        other_error = f"{DUPLICATE_NAME}: file {e.name} is also built for {sym}"
                        mark(by_sym[e.owner], "FAILED", other_error, DUPLICATE_NAME)
                        log_error(data_root, run, e.owner, str(run_date), other_error)
                        rlog(f"FAILED {e.owner}: {other_error}")
                        written.pop(other_path, None)
                        with JOB_LOCK:
                            JOB["stocks_done"] -= 1
                            JOB["chunks_done"] -= 1
                            JOB["failed"] += 1
                    except ChunkFailed as e:
                        error = str(e)
                    except (AuthStop, StopJob):
                        raise
                    except Exception as e:
                        error = f"{type(e).__name__}: {e}"
                if error:
                    mark(c, "FAILED", error, reason if reason == DUPLICATE_NAME else None)
                    log_error(data_root, run, sym, str(run_date), error)
                    if reason == DUPLICATE_NAME and pass_no == 1 and sym not in dup_syms:
                        rlog(f"FAILED {sym}: {error}")
                    if prev != "FAILED":
                        job_incr("failed")
                else:
                    mark(c, "DONE", reason=reason)
                    with JOB_LOCK:
                        JOB["stocks_done"] += 1
                        JOB["chunks_done"] += 1
                        if prev == "FAILED":
                            JOB["failed"] -= 1
                processed += 1
                left = total - JOB["chunks_done"]
                job_update(eta=fmt_eta((time.monotonic() - t0) / processed * left))
                if processed % 100 == 0:
                    echo(f"{part()} | written {counts['written']:,} | no candles {counts['no_candles']:,} | "
                         f"zero-volume {counts['zero_kept'] + counts['zero_skipped']:,} | "
                         f"failed {JOB['failed']} | ETA {JOB['eta']}")

        write_zero_volume(day, zero_found)
        if bhav_check is None:
            bhav_check = AUTO_BHAV and not refetch
        bhav_note = fetch_day_bhav(run_date, day, base, underlying, rlog) if bhav_check else ""
        rows, fut_rows, bhav_line = summary_rows(day, contracts, files, step, underlying, on_off(keep_zero), trim_mode)
        write_csv(os.path.join(day, "summary.csv"), SUMMARY_COLUMNS, rows)
        logged = False
        if bhav_check and not bhav_note:
            # check_saved adds its line to run.log itself, so its other output goes to the console only
            rows, bhav_line, logged = check_day_bhav(day, run_date, underlying, echo, rows, bhav_line)
        elif bhav_note:
            bhav_line = f"{bhav_line} ({bhav_note})"
        failures = db.execute("SELECT symbol, token, error FROM chunks WHERE run = ? AND status = 'FAILED' "
                              "ORDER BY symbol", (run,)).fetchall()
        rlog(f"Contracts requested: {total:,}")
        rlog(f"Files written this run: {counts['written']:,}; already on disk: {counts['present']:,}")
        rlog(f"No candles (no file): {counts['no_candles']:,}")
        rlog(f"Zero-volume candles: kept {counts['zero_kept']:,} / skipped {counts['zero_skipped']:,} "
             f"(keep zero-volume contracts {on_off(keep_zero)}; see {ZERO_VOLUME_FILE})")
        rlog(f"Failed: {len(failures)}")
        for sym, token, error in failures:
            rlog(f"  FAILED {sym} (token {token}): {error}")
        rlog("Summary by option expiry:\n" + table(SUMMARY_COLUMNS, rows))
        rlog("Futures:\n" + table(FUTURE_COLUMNS, fut_rows))
        if not logged:   # check_saved adds its line to run.log itself
            rlog(bhav_line)
        rlog(f"=== Ended {now_ist():%Y-%m-%d %H:%M:%S} IST")
        msg = (f"Saved {underlying} F&O for {run_date}: {counts['written'] + counts['present']:,} contract files, "
               f"{counts['no_candles']:,} without candles, {counts['zero_kept'] + counts['zero_skipped']:,} with "
               f"zero-volume candles ({counts['zero_kept']:,} kept, {counts['zero_skipped']:,} skipped).")
        if failures:
            msg += f" {len(failures)} contracts failed (see run.log); run again to retry them."
        msg += f" {bhav_line}."
        if trim_note:
            msg += f" {trim_note}."
        job_update(state="finished" if last else "running", symbol="", chunk="", part="", eta="", message=msg,
                   fno_summary=rows, fno_futures=fut_rows)
        return "finished_failures" if failures else "finished"
    except StopJob:
        rlog("Stopped by the user.")
        job_update(state="stopped", message="Stopped. Start again to continue; finished contracts are skipped.")
        return "stopped"
    except AuthStop as e:
        log_error(data_root, run, current, str(run_date), str(e))
        rlog(f"Stopped: {e}")
        job_update(state="stopped", message=f"{e} Finished contracts are kept.")
        return "auth_failed"
    except Exception as e:
        log_error(data_root, run, current, str(run_date), f"{type(e).__name__}: {e}")
        rlog(f"Stopped: {type(e).__name__}: {e}")
        job_update(state="stopped", message=f"Stopped: {type(e).__name__}: {e}")
        return "error"
    finally:
        db.close()
        rlog.close()
        finish_day_folder(day, base, created, keep_zero, echo, underlying, source)


def finish_day_folder(day, base, created, keep_zero, echo, underlying, source, report=True):
    """After a run (files closed): a noZeroVol folder written with keep zero-volume ON becomes withZeroVol,
    and the root's day count is rewritten when a folder was created (or removed again on a holiday) or
    renamed. A failed rename (e.g. the folder open in Explorer) is reported; the next ON run tries again.
    Returns the day folder's path from now on."""
    renamed = False
    if keep_zero and os.path.isdir(day):
        try:
            new = mark_with_zero_volume(day)
        except OSError as e:
            msg = (f"Could not rename {os.path.basename(day)} to withZeroVol ({e}); close it in Explorer. "
                   f"The next run with keep zero-volume ON renames it.")
            echo(msg)
            if report:
                with JOB_LOCK:
                    JOB["message"] = f"{JOB['message']} {msg}".strip()
        else:
            renamed, day = new != day, new
            if renamed:
                echo(f"Day folder renamed to {os.path.basename(day)}")
                if report:
                    job_update(out_dirs=[os.path.join(fno_folder(underlying, source), os.path.basename(day))])
    if created or renamed:
        from . import fno_days   # fno_days imports this module
        try:
            fno_days.write_day_count(base)
        except (OSError, DayFolderError) as e:
            echo(f"Day count not updated: {e}")
    return day


def spot_traded(kite, limiter, underlying, source, start, end):
    """True if the underlying's spot index has candles in [start, end]: Kite's SPOT_TOKENS, on Dhan
    DhanClient.spot_token() (IDX_I). Raises RuntimeError like the future's holiday check when the request fails."""
    if source == "kite":
        token = SPOT_TOKENS.get(underlying)
    else:
        token = kite.spot_token(underlying) if hasattr(kite, "spot_token") else None
    if token is None:
        return False
    try:
        return bool(fetch_chunk(kite, limiter, token, start, end, "minute", max_tries=MAX_TRIES))
    except ChunkFailed as e:
        raise RuntimeError(f"Holiday check failed for {underlying} spot (token {token}): {e}")


def holiday(base, day, created, run, run_date, near, rlog, db, last=True):
    """No candles for the nearest future: log the holiday and remove the day folder made by this run."""
    msg = (f"{run_date}: no candles for {near['tradingsymbol']} (token {near['instrument_token']}) "
           f"between {WINDOW[0]:%H:%M} and {WINDOW[1]:%H:%M} IST, so the market was closed.")
    rlog(msg)
    rlog.close()
    with open(os.path.join(base, "holidays.log"), "a", encoding="utf-8") as f:
        f.write(f"{now_ist():%Y-%m-%d %H:%M:%S} {msg}\n")
    db.execute("DELETE FROM chunks WHERE run = ?", (run,))
    db.commit()
    has_files = any(n.endswith(".csv") for sub in ("futures", "options")
                    for n in os.listdir(os.path.join(day, sub)))
    if created and not has_files:
        shutil.rmtree(day)
        rlog.echo("Day folder removed.")
    else:
        rlog.echo(f"Day folder kept ({'it holds contract files' if has_files else 'it existed before this run'}).")
    job_update(state="finished" if last else "running", symbol="", part="", eta="",
               message=f"No trading on {run_date} (market holiday). Nothing was saved.")
    return "holiday"


def run_missing(api_key, access_token, data_root, speed, underlying="NIFTY", log=print,
                weekday=WEEKLY_EXPIRY_WEEKDAY, keep_zero=False, kite=None, user="", source="kite", dates=None,
                trim_to_bhav=None):
    """Download every date in the window that has no complete day folder, oldest first, sharing
    one Kite session and instrument list. Weekends and holidays end at the holiday check (and are
    logged, so the next run skips them). Same statuses as run_daily.

    dates: run these days instead (a range picked on the dashboard), minus logged holidays and
    today before READY_AT; saved days run again and skip their saved contracts.
    trim_to_bhav (None = the setting) is used for every day."""
    if trim_to_bhav is None:
        trim_to_bhav = trim_default()
    what = f"{min(dates)}_to_{max(dates)}" if dates else "missing"
    echo = lambda m: log(f"[{run_key(underlying, what, source)}] {m}")
    if kite is None:
        kite, user, status = connect(api_key, access_token, echo)
        if status:
            return status
    try:
        job_update(message="Loading instrument list...")
        instruments, window = load_window(kite, data_root, underlying, weekday, source)
    except (kex.TokenException, kex.PermissionException) as e:
        return stop_with(echo, "auth_failed", f"{BROKER[source]} access token is invalid or expired ({e}).")
    except Exception as e:
        return stop_with(echo, "error", f"Could not load the instrument list: {type(e).__name__}: {e}")
    if window.estimated:
        echo(window_note(window))
    if dates:
        picked, skip = sorted(dates), holidays_logged(data_root, underlying, source)
        bad = [d for d in picked if window_error(d, window)]
        if bad:
            return stop_with(echo, "outside_window", window_error(bad[0], window))
        dates = [d for d in picked if d not in skip and not too_early(d)]
        kind = "Range"
    else:
        dates = missing_dates(data_root, underlying, window, source=source)
        kind = "Missing"
    if not dates:
        msg = ("Nothing to download: every picked day is a holiday or not ready yet." if kind == "Range"
               else f"Nothing missing: every day from {window.min_date} to {window.max_date} is saved or a holiday.")
        echo(msg)
        job_update(state="finished", message=msg)
        return "finished"
    echo(f"{kind} days: {', '.join(map(str, dates))}")
    results = {}
    for n, d in enumerate(dates, 1):
        if STOP.is_set():
            return stop_with(echo, "stopped", "Stopped. Start again to continue; finished days are skipped.")
        st = run_daily(api_key, access_token, d, data_root, speed, underlying, log, weekday, kite, user,
                       instruments, window, label=f"Day {n} of {len(dates)}, ", last=n == len(dates),
                       keep_zero=keep_zero, source=source, trim_to_bhav=trim_to_bhav)
        results[d] = st
        if st in ("auth_failed", "stopped", "error"):
            return st
    saved = sum(1 for st in results.values() if st.startswith("finished"))
    hol = sum(1 for st in results.values() if st == "holiday")
    bad = [str(d) for d, st in results.items() if st == "finished_failures"]
    msg = (f"{kind} days done: {saved} saved, {hol} holiday/weekend"
           + (f"; failed contracts on {', '.join(bad)} (run again to retry)." if bad else "."))
    echo(msg)
    job_update(state="finished", message=msg)
    return "finished_failures" if bad else "finished"


# --- diagnostics (nifty_daily.py --inspect / --inspect-missing): nothing is written ---------------

INSPECT_COLUMNS = ["Contract", "Kite_candles", "Kite_volume", "Kite_last_close", "Bhav_volume", "Bhav_last", "Result"]


def find_contracts(instruments, names, underlying="NIFTY"):
    """Our contract names -> instrument rows of the live dump, matched by contract_name() of
    type/expiry/strike (never by Kite's tradingsymbol). Names not found are left out."""
    wanted, out = set(names), {}
    for i in instruments:
        if i.get("name") != underlying or i.get("instrument_type") not in TYPES or not i.get("expiry"):
            continue
        try:
            name = contract_name(i["instrument_type"], i["expiry"], i.get("strike"), underlying)
        except ValueError:
            continue
        if name in wanted:
            out[name] = i
    return out


def inspect_result(candles):
    """NO_CANDLES, ZERO_VOLUME_CANDLES or TRADED for one contract's candles."""
    if not candles:
        return "NO_CANDLES"
    return "ZERO_VOLUME_CANDLES" if candle_volume(candles) == 0 else "TRADED"


def missing_in_kite(day_dir):
    """Contract names marked MISSING_IN_KITE in the day's bhav_compare.csv."""
    path = os.path.join(day_dir, "bhavcopy", "bhav_compare.csv")
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return [r["Contract"] for r in csv.DictReader(f) if "MISSING_IN_KITE" in r["Status"]]


def inspect_contracts(api_key, access_token, run_date, data_root, names, underlying="NIFTY", speed=2.5,
                      log=print, verbose=True, kite=None, source="kite"):
    """Fetch run_date's raw 1-minute candles (with OI) for each contract name and print them, with the
    saved bhavcopy row when there is one. Writes nothing to the data folder. Returns (status, rows in
    INSPECT_COLUMNS); status "" when Kite was reached, else the connect() status. With kite= (a
    DhanClient, source="dhan") the caller has checked the token and the Dhan day is read."""
    from . import bhav   # bhav imports this module
    if kite is None:
        kite, msg, status = connect(api_key, access_token, log, report=False)
        if status:
            return status, []
    instruments = kite.instruments(UNDERLYINGS[underlying][0])
    found = find_contracts(instruments, names, underlying)
    folder = day_dir(data_root, underlying, run_date, source)
    items = bhav.saved_items(folder, underlying) if folder else None
    bhav_of = {i["name"]: i for i in items or []}
    start, end = datetime.combine(run_date, WINDOW[0]), datetime.combine(run_date, WINDOW[1])
    limiter, out = RateLimiter(speed), []
    for name in names:
        b = bhav_of.get(name)
        row = {"Contract": name, "Kite_candles": "", "Kite_volume": "", "Kite_last_close": "",
               "Bhav_volume": fmt_num(b["volume"]) if b else "", "Bhav_last": fmt_num(b["last"]) if b else ""}
        inst = found.get(name)
        if inst is None:
            out.append({**row, "Result": "NOT_IN_LIVE_LIST"})
            log(f"{name}: not in today's live {exchange_of(underlying, source)} instrument list")
            continue
        try:
            candles = fetch_chunk(kite, limiter, inst["instrument_token"], start, end, "minute",
                                  oi=True, max_tries=MAX_TRIES)
        except (AuthStop, ChunkFailed, StopJob) as e:
            out.append({**row, "Result": f"FAILED ({e})"})
            log(f"{name}: {e}")
            continue
        out.append({**row, "Kite_candles": len(candles), "Kite_volume": fmt_num(candle_volume(candles)),
                    "Kite_last_close": fmt_num(candles[-1]["close"]) if candles else "",
                    "Result": inspect_result(candles)})
        if verbose:
            log(f"{name} ({inst['tradingsymbol']}, {BROKER[source]} token {inst['instrument_token']}) on {run_date}, "
                f"{WINDOW[0]:%H:%M}-{WINDOW[1]:%H:%M} IST:")
            log(table(["Time", "Open", "High", "Low", "Close", "Volume", "OI"],
                      [{"Time": f"{to_ist(c['date']):%H:%M:%S}", "Open": fmt_num(c["open"]), "High": fmt_num(c["high"]),
                        "Low": fmt_num(c["low"]), "Close": fmt_num(c["close"]), "Volume": fmt_num(c["volume"]),
                        "OI": fmt_num(c.get("oi"))} for c in candles]) if candles else "  (no candles)")
            log(f"Candles: {len(candles)}, total volume: {fmt_num(candle_volume(candles))}")
            if b:
                log(f"Bhavcopy: volume {fmt_num(b['volume'])} contracts, last {fmt_num(b['last'])}, "
                    f"close {fmt_num(b['close'])}, settle {fmt_num(b['settle'])}, OI {fmt_num(b['oi'])}, lot {fmt_num(b['lot'])}")
            elif items is None:
                log("Bhavcopy: not saved for this day")
            else:
                log("Bhavcopy: contract not listed")
            log("")
    return "", out


def refetch_no_trade(api_key, access_token, run_date, data_root, speed, underlying="NIFTY", log=print,
                     weekday=WEEKLY_EXPIRY_WEEKDAY, kite=None, user="", source="kite", trim_to_bhav=None):
    """Request every contract of a saved day that has no file again, with keep zero-volume ON
    (contracts with a file are skipped), then re-run the bhavcopy check if the day's bhavcopy is saved.
    The caller holds the lock and has called job_begin(**job_fields(..., keep_zero=True)). source "dhan"
    (kite = a DhanClient) works on the Dhan folder.
    Returns (run_daily status or "not_downloaded", bhavcopy line or "")."""
    from . import bhav   # bhav imports this module
    day = day_dir(data_root, underlying, run_date, source)
    if not day or not os.path.exists(os.path.join(day, "summary.csv")):
        return stop_with(lambda m: log(f"[{run_key(underlying, run_date, source)}] {m}"), "not_downloaded",
                         f"{run_date} has not been downloaded in this folder yet; download the day first."), ""
    status = run_daily(api_key, access_token, run_date, data_root, speed, underlying, log, weekday,
                       kite=kite, user=user, keep_zero=True, refetch=True, source=source, trim_to_bhav=trim_to_bhav)
    day = day_dir(data_root, underlying, run_date, source)   # the run may have renamed it to withZeroVol
    if not status.startswith("finished") or bhav.saved_items(day, underlying) is None:
        return status, ""
    _, line, rows = bhav.check_saved(day, run_date, underlying, log=log)
    with JOB_LOCK:
        JOB["message"] += f" Checked again against the saved bhavcopy: {line}."
        if rows is not None:
            JOB["fno_summary"] = rows
    return status, line


def redownload_contract(api_key, access_token, run_date, data_root, contract, underlying="NIFTY",
                        speed=2.5, log=print, keep_zero=False, kite=None, source="kite"):
    """Fetch one contract (our file name, e.g. NIFTY2500013OCT26CE) for run_date again, e.g. one the
    bhavcopy check found missing. Only while Kite (or Dhan, source="dhan" with kite=DhanClient) still
    serves it (expiry today or later). The caller holds the lock. Returns (saved, message)."""
    broker = BROKER[source]
    folder = day_dir(data_root, underlying, run_date, source)
    dumps = sorted(glob.glob(os.path.join(folder, "instruments_*.csv"))) if folder else []
    if not dumps:
        return False, f"{run_date} has no saved instrument list; download the day first."
    with open(dumps[0], newline="", encoding="utf-8") as f:
        saved_list = list(csv.DictReader(f))
    contracts = select_contracts(saved_list, underlying, run_date)
    found = None
    for c in contracts:
        try:
            name, path = contract_file(folder, c, underlying)
        except ValueError:
            continue
        if name == contract:
            found = (c, path)
            break
    if found is None:
        return False, f"{contract} is not in the instrument list saved for {run_date}."
    c, path = found
    if as_date(c["expiry"]) < today_ist():
        return False, f"{contract} expired on {as_date(c['expiry'])}; {broker} no longer serves it."
    echo = lambda m: log(f"[{run_key(underlying, run_date, source)}] {m}")
    if kite is None:
        kite, msg, status = connect(api_key, access_token, echo, report=False)
        if status:
            return False, msg
    if hasattr(kite, "learn"):   # DhanClient: instrument type per id from the saved list
        kite.learn(saved_list)
    from . import bhav   # bhav imports this module
    bhav.log_line(os.path.join(folder, "run.log"),
                  f"=== Re-download of {contract} for {run_date} ({broker}), started {now_ist():%Y-%m-%d %H:%M:%S} IST, "
                  f"keep zero-volume contracts {on_off(keep_zero)}")
    start, end = datetime.combine(run_date, WINDOW[0]), datetime.combine(run_date, WINDOW[1])
    token = c["instrument_token"] if source == "dhan" else int(c["instrument_token"])
    try:
        candles = fetch_chunk(kite, RateLimiter(speed), token, start, end, "minute",
                              oi=True, max_tries=MAX_TRIES)
    except (AuthStop, ChunkFailed, StopJob) as e:
        return False, f"{contract}: {e}"
    bars = write_contract(path, contract, candles, keep_zero)
    zero = bool(candles) and candle_volume(candles) == 0
    reason = None if bars else ZERO_VOLUME if zero else NO_CANDLES
    if zero:
        write_zero_volume(folder, {contract: {"Contract": contract, **zero_volume_info(candles),
                                               "Kept": "Y" if bars else "N"}})
    db = open_db(data_root)
    try:
        run = run_key(underlying, run_date, source)
        db.execute("INSERT OR IGNORE INTO chunks (run, symbol, token, chunk_from, chunk_to, status) "
                   "VALUES (?, ?, ?, ?, ?, 'PENDING')", (run, c["tradingsymbol"], c["instrument_token"], str(start), str(end)))
        db.execute("UPDATE chunks SET status = 'DONE', tries = tries + 1, error = NULL, reason = ?, updated = ? "
                   "WHERE run = ? AND symbol = ?",
                   (reason, datetime.now().isoformat(timespec="seconds"), run, c["tradingsymbol"]))
        db.commit()
    finally:
        db.close()
    if keep_zero:   # noZeroVol -> withZeroVol, as after a run with the setting ON
        finish_day_folder(folder, os.path.dirname(folder), False, True, echo, underlying, source, report=False)
    if bars:
        msg = f"{contract}: saved {bars} bars for {run_date}" + (" (all with volume 0)." if zero else ".")
    elif zero:
        msg = (f"{contract}: {broker} returned {len(candles)} candles, all with volume 0, for {run_date}; no file "
               f"(keep zero-volume contracts is OFF).")
    else:
        msg = f"{contract}: {broker} returned no candles for {run_date}, so no file was written."
    echo(msg)
    return bool(bars), msg
