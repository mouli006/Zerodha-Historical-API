"""NSE equity (cash market) bhavcopy, 2015 -> today, from NSE's public archive (no broker, no login).

Store: <folder>/NSE_CM_bhav/ (folder = .env EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data):
    raw/<YYYY>/<zip as downloaded>   never changed after download; a zip saved there by hand is used as is
    <YYYY>.parquet                   every series of every ok day of the year (SCHEMA), rebuilt from raw/
    days.csv                         one row per weekday checked (DAYS_COLUMNS)
    holidays.log                     weekdays NSE answers 404 for (not asked again unless force)
    cm_bhav.log                      run log
    cm_bhav_progress.sqlite          this downloader's own progress (table days)
    .lock                            one run at a time (backfill.fno.RunLock, its own file)
    reference/                       nse_reference.py's folder, never touched here

Two formats (SOURCES): the old cmDDMONYYYYbhav.csv.zip (to early July 2024) and the UDiFF
BhavCopy_NSE_CM_0_0_0_<YYYYMMDD>_F_0000.csv.zip (from then on). Inside SWITCH_FROM..SWITCH_TO the UDiFF file is
tried first and the old one after a 404; days.csv records the format each day used.
Only a real HTTP 404 makes a past weekday a holiday. A page instead of the zip (200 + HTML), 401/403 or network
errors make the day "failed" (retried next run, listed with the path to save the file under). Today counts only
after READY_AT (19:00 IST); before that, or on a 404 for today, it is "pending".
The bhavcopy is the exchange's official record and belongs to no broker: broker data is checked against it."""
import csv
import io
import os
import re
import sqlite3
import statistics
import time
import zipfile
from datetime import date, datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from backfill.fno import LockBusy, RunLock  # noqa: F401 - LockBusy is re-exported for the CLI
from backfill.nse_ref.fetch import Blocked, FetchError, NotFound, NseClient, Throttle, write_bytes

IST = ZoneInfo("Asia/Kolkata")
START = date(2015, 1, 1)
READY_AT = dtime(19, 0)   # NSE publishes the day's file in the evening
SLEEP = 1.5               # seconds between request starts
TRIES = 3                 # tries with requests (backoff 2 s, 4 s) before headless Chrome takes over
SWITCH_FROM, SWITCH_TO = date(2024, 7, 1), date(2024, 7, 12)   # UDiFF first, then the old file
HOLIDAY_FLAG = 18         # more weekday holidays than this in a year is suspicious (NSE has about 12-16)
STORE = "NSE_CM_bhav"
MAIN_SERIES = ("EQ", "BE", "BZ")
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

OLD_HEADER = ["SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL",
              "TIMESTAMP", "TOTALTRADES", "ISIN"]
UDIFF_HEADER = ["TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN", "TckrSymb", "SctySrs",
                "XpryDt", "FininstrmActlXpryDt", "StrkPric", "OptnTp", "FinInstrmNm", "OpnPric", "HghPric",
                "LwPric", "ClsPric", "LastPric", "PrvsClsgPric", "UndrlygPric", "SttlmPric", "OpnIntrst",
                "ChngInOpnIntrst", "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd", "SsnId", "NewBrdLotQty",
                "Rmks", "Rsvd1", "Rsvd2", "Rsvd3", "Rsvd4"]

# Per format: URL, file name, header and our column -> theirs (fin_id None: not in that format)
SOURCES = {
    "old": {"url": "https://nsearchives.nseindia.com/content/historical/EQUITIES/{yyyy}/{mon}/{name}",
            "name": "cm{dd}{mon}{yyyy}bhav.csv.zip",
            "name_re": r"cm(?P<dd>\d{2})(?P<mon>[A-Z]{3})(?P<yyyy>\d{4})bhav\.csv\.zip",
            "header": OLD_HEADER, "trailing_empty": True, "date": ("TIMESTAMP", "%d-%b-%Y"),
            "columns": {"symbol": "SYMBOL", "series": "SERIES", "isin": "ISIN", "open": "OPEN", "high": "HIGH",
                        "low": "LOW", "close": "CLOSE", "last": "LAST", "prev_close": "PREVCLOSE",
                        "volume": "TOTTRDQTY", "turnover": "TOTTRDVAL", "trades": "TOTALTRADES", "fin_id": None}},
    "udiff": {"url": "https://nsearchives.nseindia.com/content/cm/{name}",
              "name": "BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip",
              "name_re": r"BhavCopy_NSE_CM_0_0_0_(?P<ymd>\d{8})_F_0000\.csv\.zip",
              "header": UDIFF_HEADER, "trailing_empty": False, "date": ("TradDt", "%Y-%m-%d"),
              "fixed": {"Sgmt": "CM", "FinInstrmTp": "STK"},
              "columns": {"symbol": "TckrSymb", "series": "SctySrs", "isin": "ISIN", "open": "OpnPric",
                          "high": "HghPric", "low": "LwPric", "close": "ClsPric", "last": "LastPric",
                          "prev_close": "PrvsClsgPric", "volume": "TtlTradgVol", "turnover": "TtlTrfVal",
                          "trades": "TtlNbOfTxsExctd", "fin_id": "FinInstrmId"}},
}

PRICES = ["open", "high", "low", "close", "last", "prev_close"]
SCHEMA = pa.schema([("date", pa.date32()), ("symbol", pa.string()), ("series", pa.string()), ("isin", pa.string()),
                    ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
                    ("last", pa.float64()), ("prev_close", pa.float64()), ("volume", pa.int64()),
                    ("turnover", pa.float64()), ("trades", pa.int64()), ("fin_id", pa.string()),
                    ("format", pa.string())])
COLUMNS = SCHEMA.names
DAYS_COLUMNS = ["date", "status", "format", "rows", "eq_be_bz_rows", "file", "downloaded_at", "note"]
STATUSES = ("ok", "holiday", "failed", "pending")


class BadFile(Exception):
    """A bhavcopy that fails validation; the message names the file."""


def now_ist():
    return datetime.now(IST)


def today_ist(now=None):
    return (now or now_ist()).astimezone(IST).date()


def store_dir(folder):
    return os.path.join(folder, STORE)


def weekdays(first, last):
    d = first
    while d <= last:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


# --- sources ----------------------------------------------------------------------------------

def formats_for(day):
    """The formats to try for day, in order."""
    if day < SWITCH_FROM:
        return ["old"]
    if day <= SWITCH_TO:
        return ["udiff", "old"]
    return ["udiff"]


def _parts(day):
    return {"dd": f"{day.day:02d}", "mon": MONTHS[day.month - 1], "yyyy": f"{day.year}", "ymd": f"{day:%Y%m%d}"}


def file_name(day, fmt):
    return SOURCES[fmt]["name"].format(**_parts(day))


def url_for(day, fmt):
    p = _parts(day)
    return SOURCES[fmt]["url"].format(name=file_name(day, fmt), **p)


def raw_path(store, day, fmt):
    return os.path.join(store, "raw", f"{day.year}", file_name(day, fmt))


def parse_file_name(name):
    """(day, format) of a bhavcopy zip name, or None."""
    for fmt, src in SOURCES.items():
        m = re.fullmatch(src["name_re"], name)
        if not m:
            continue
        g = m.groupdict()
        try:
            if "ymd" in g:
                return datetime.strptime(g["ymd"], "%Y%m%d").date(), fmt
            return date(int(g["yyyy"]), MONTHS.index(g["mon"]) + 1, int(g["dd"])), fmt
        except ValueError:
            return None
    return None


# --- download ---------------------------------------------------------------------------------

def is_zip(data):
    return data[:2] == b"PK"


class CmClient(NseClient):
    """NseClient for the bhavcopy zips: TRIES tries with backoff, the body must be a zip; only a 404 is NotFound
    (a page instead of the zip is treated as blocked, so the browser is tried and the day fails if that fails)."""

    def __init__(self, sleep=SLEEP, tries=TRIES, pause=time.sleep, **kw):
        kw.setdefault("throttle", Throttle(gap=sleep))
        super().__init__(**kw)
        self.tries, self.pause = tries, pause

    def _requests_get(self, url, kind):
        err = ""
        for n in range(self.tries):
            if n:
                self.pause(2 ** n)
            self.throttle.wait()
            self.requests += 1
            try:
                r = self.session.get(url, timeout=60)
            except requests.RequestException as e:
                err = f"{type(e).__name__}: {e}"
                continue
            if r.status_code == 404:
                raise NotFound("404 not found")
            if r.status_code in (401, 403):
                raise Blocked(f"HTTP {r.status_code}")
            if r.ok:
                if is_zip(r.content):
                    return r.content
                raise Blocked("a page instead of the zip")
            err = f"HTTP {r.status_code}"
        raise Blocked(err)

    def _browser_get(self, url, kind):
        data = super()._browser_get(url, kind)
        if not is_zip(data):
            raise FetchError("browser: a page instead of the zip")
        return data


# --- parse and validate -----------------------------------------------------------------------

def _num(value, kind, name, symbol, column):
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise BadFile(f"{name}: {symbol}: {column} is not a number ({value!r})") from None
    return int(v) if kind == "int" else v


def _few(items, n=5):
    return ", ".join(items[:n]) + (f" (+{len(items) - n} more)" if len(items) > n else "")


def parse_zip(path, day, fmt):
    """The day's rows in COLUMNS (every series kept), validated; BadFile naming the file on any problem."""
    name = os.path.basename(path)
    src = SOURCES[fmt]
    try:
        with zipfile.ZipFile(path) as z:
            members = [n for n in z.namelist() if not n.endswith("/")]
            if len(members) != 1:
                raise BadFile(f"{name}: {len(members)} files in the zip, expected one CSV")
            raw = z.read(members[0])
    except zipfile.BadZipFile:
        raise BadFile(f"{name}: not a valid zip file") from None
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise BadFile(f"{name}: empty file")
    header = [h.strip() for h in rows[0]]
    width = len(header)
    if src["trailing_empty"] and header and header[-1] == "":
        header = header[:-1]   # every line of the old format ends with a comma
    expected = src["header"]
    if header != expected:
        unknown = [h for h in header if h not in expected]
        missing = [h for h in expected if h not in header]
        what = "; ".join(x for x in (f"unknown columns {unknown}" if unknown else "",
                                      f"missing columns {missing}" if missing else "") if x) or "columns out of order"
        raise BadFile(f"{name}: unexpected header ({what})")
    data = [r for r in rows[1:] if any(c.strip() for c in r)]
    if not data:
        raise BadFile(f"{name}: no rows")
    bad = [i + 2 for i, r in enumerate(data) if len(r) != width]
    if bad:
        raise BadFile(f"{name}: {len(bad)} line(s) with {width} fields expected (line {_few([str(b) for b in bad])})")
    idx = {h: i for i, h in enumerate(header)}
    for col, value in src.get("fixed", {}).items():
        other = sorted({r[idx[col]].strip() for r in data} - {value})
        if other:
            raise BadFile(f"{name}: {col} must be {value}, found {other}")
    dcol, dfmt = src["date"]
    dates = set()
    for r in data:
        v = r[idx[dcol]].strip()
        try:
            dates.add(datetime.strptime(v.title() if dfmt.startswith("%d-%b") else v, dfmt).date())
        except ValueError:
            raise BadFile(f"{name}: bad {dcol} {v!r}") from None
    if dates != {day}:
        raise BadFile(f"{name}: dates {sorted(d.isoformat() for d in dates)} in the file, expected {day}")
    cols = src["columns"]
    out = []
    for r in data:
        g = {k: (r[idx[c]].strip() if c else "") for k, c in cols.items()}
        sym = f"{g['symbol']} {g['series']}"
        row = {"date": day, "symbol": g["symbol"], "series": g["series"], "isin": g["isin"]}
        for k in PRICES:
            row[k] = _num(g[k], "float", name, sym, cols[k])
        row["volume"] = _num(g["volume"], "int", name, sym, cols["volume"])
        row["turnover"] = round(_num(g["turnover"], "float", name, sym, cols["turnover"]), 2)
        row["trades"] = _num(g["trades"], "int", name, sym, cols["trades"])
        row["fin_id"] = g["fin_id"]
        row["format"] = fmt
        out.append(row)
    keys = [(r["symbol"], r["series"]) for r in out]
    dupes = sorted({f"{s} {t}" for (s, t) in keys if keys.count((s, t)) > 1}) if len(set(keys)) != len(keys) else []
    if dupes:
        raise BadFile(f"{name}: duplicate symbol/series rows: {_few(dupes)}")
    nonpos = [f"{r['symbol']} {r['series']}" for r in out if min(r["open"], r["high"], r["low"], r["close"]) <= 0]
    if nonpos:
        raise BadFile(f"{name}: {len(nonpos)} row(s) with open/high/low/close <= 0: {_few(nonpos)}")
    inverted = [f"{r['symbol']} {r['series']}" for r in out if r["high"] < r["low"]]
    if inverted:
        raise BadFile(f"{name}: {len(inverted)} row(s) with high < low: {_few(inverted)}")
    df = pd.DataFrame(out, columns=COLUMNS)
    return df.sort_values(["symbol", "series"], kind="stable").reset_index(drop=True)


def eq_be_bz(df):
    return int(df["series"].isin(MAIN_SERIES).sum())


# --- progress db, days.csv, parquet ----------------------------------------------------------

def open_db(store):
    con = sqlite3.connect(os.path.join(store, "cm_bhav_progress.sqlite"))
    con.execute("CREATE TABLE IF NOT EXISTS days (date TEXT PRIMARY KEY, status TEXT NOT NULL, format TEXT, "
                "file TEXT, url TEXT, rows INTEGER, eq_be_bz_rows INTEGER, downloaded_at TEXT, note TEXT)")
    con.commit()
    return con


def get_day(con, day):
    r = con.execute("SELECT status, format, file, url, rows, eq_be_bz_rows, downloaded_at, note FROM days "
                    "WHERE date = ?", (day.isoformat(),)).fetchone()
    return dict(zip(["status", "format", "file", "url", "rows", "eq_be_bz_rows", "downloaded_at", "note"], r)) if r else None


def set_day(con, day, status, fmt="", file="", url="", rows=None, main=None, downloaded_at="", note=""):
    con.execute("INSERT OR REPLACE INTO days VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (day.isoformat(), status, fmt, file, url, rows, main, downloaded_at, note))
    con.commit()


def all_days(con):
    cols = ["date", "status", "format", "file", "url", "rows", "eq_be_bz_rows", "downloaded_at", "note"]
    return [dict(zip(cols, r)) for r in con.execute(f"SELECT {', '.join(cols)} FROM days ORDER BY date")]


def write_text_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.replace(tmp, path)


def write_days_csv(store, con):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(DAYS_COLUMNS)
    for d in all_days(con):
        w.writerow(["" if d[c] is None else d[c] for c in DAYS_COLUMNS])
    write_text_atomic(os.path.join(store, "days.csv"), buf.getvalue())


def year_path(store, year):
    return os.path.join(store, f"{year}.parquet")


def parquet_dates(store, year):
    path = year_path(store, year)
    if not os.path.exists(path):
        return None
    col = pq.read_table(path, columns=["date"]).column("date").to_pylist()
    return {d.isoformat() for d in col}


def write_year(store, year, frames):
    df = pd.concat(frames, ignore_index=True).sort_values(["date", "symbol", "series"], kind="stable")
    table = pa.Table.from_pandas(df[COLUMNS], schema=SCHEMA, preserve_index=False)
    path = year_path(store, year)
    tmp = path + ".tmp"
    pq.write_table(table, tmp)
    os.replace(tmp, path)
    return len(df)


def rebuild_year(store, con, year, log=print):
    """Re-parse every ok day of year from raw/ into <year>.parquet; a day that no longer parses becomes failed."""
    frames = []
    for d in all_days(con):
        if d["status"] != "ok" or not d["date"].startswith(f"{year}-"):
            continue
        day = date.fromisoformat(d["date"])
        try:
            frames.append(parse_zip(os.path.join(store, d["file"]), day, d["format"]))
        except (BadFile, OSError) as e:
            set_day(con, day, "failed", d["format"], d["file"], d["url"], None, None, d["downloaded_at"], str(e))
            log(f"  {day} failed on rebuild: {e}")
    if not frames:
        if os.path.exists(year_path(store, year)):
            os.remove(year_path(store, year))
        return 0
    return write_year(store, year, frames)


def stale_years(store, con, touched=()):
    """Years whose parquet does not hold exactly the ok days in the db, plus touched."""
    ok = {}
    for d in all_days(con):
        if d["status"] == "ok":
            ok.setdefault(int(d["date"][:4]), set()).add(d["date"])
    years = set(touched)
    for year, dates in ok.items():
        if parquet_dates(store, year) != dates:
            years.add(year)
    for name in os.listdir(store):
        m = re.fullmatch(r"(\d{4})\.parquet", name)
        if m and int(m.group(1)) not in ok:
            years.add(int(m.group(1)))
    return sorted(years)


# --- report ------------------------------------------------------------------------------------

def report(con):
    """(text, flagged): per year counts and EQ/BE/BZ stocks per day, the format switch, years with too many
    weekday holidays (> HOLIDAY_FLAG, with their dates)."""
    days = all_days(con)
    years = sorted({d["date"][:4] for d in days})
    lines = [f"{'Year':<6}{'Trading':>8}{'Holidays':>9}{'Failed':>7}{'Pending':>8}{'Rows':>11}"
             f"{'EQ/BE/BZ min':>13}{'median':>8}{'max':>7}"]
    flagged = {}
    for y in years:
        ds = [d for d in days if d["date"].startswith(y)]
        by = {s: [d for d in ds if d["status"] == s] for s in STATUSES}
        main = [d["eq_be_bz_rows"] for d in by["ok"] if d["eq_be_bz_rows"] is not None]
        rows = sum(d["rows"] or 0 for d in by["ok"])
        stats = (f"{min(main):>13,}{int(statistics.median(main)):>8,}{max(main):>7,}" if main
                 else f"{'-':>13}{'-':>8}{'-':>7}")
        mark = "  <- check holidays" if len(by["holiday"]) > HOLIDAY_FLAG else ""
        lines.append(f"{y:<6}{len(by['ok']):>8}{len(by['holiday']):>9}{len(by['failed']):>7}{len(by['pending']):>8}"
                     f"{rows:>11,}{stats}{mark}")
        if mark:
            flagged[y] = [d["date"] for d in by["holiday"]]
    old = [d["date"] for d in days if d["status"] == "ok" and d["format"] == "old"]
    new = [d["date"] for d in days if d["status"] == "ok" and d["format"] == "udiff"]
    if new:
        lines.append(f"Format switch: first UDiFF day {new[0]}" + (f", last old-format day {old[-1]}" if old else ""))
    for y, dates in flagged.items():
        lines.append(f"{y}: {len(dates)} weekday holidays (NSE usually has 12-16), check them: {', '.join(dates)}")
    return "\n".join(lines), flagged


# --- run ---------------------------------------------------------------------------------------

class RunLog:
    def __init__(self, store, log):
        self.path, self.echo = os.path.join(store, "cm_bhav.log"), log

    def __call__(self, msg, echo=True):
        with open(self.path, "a", encoding="utf-8", newline="\n") as f:
            f.write(f"{now_ist():%Y-%m-%d %H:%M:%S} {msg}\n")
        if echo:
            self.echo(msg)


def log_holiday(store, day, urls, now):
    path = os.path.join(store, "holidays.log")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            if any(line.startswith(day.isoformat()) for line in f):
                return
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(f"{day} {day:%a} holiday: 404 for {' and '.join(urls)} (checked {now:%Y-%m-%d %H:%M} IST)\n")


def get_file(store, day, client, force, now):
    """(status, fmt, path, url, note, failures) for day: the file in raw/ (downloaded if missing or force)."""
    fmts = formats_for(day)
    if not force:
        for fmt in fmts:
            if os.path.exists(raw_path(store, day, fmt)):
                return "file", fmt, raw_path(store, day, fmt), url_for(day, fmt), "", []
    not_found, failures = [], []
    for fmt in fmts:
        url, path = url_for(day, fmt), raw_path(store, day, fmt)
        if client is None:
            failures.append((day, url, path, "not in raw/ (offline)"))
            continue
        try:
            data = client.get(url, "zip")
        except NotFound:
            not_found.append(url)
            continue
        except FetchError as e:
            failures.append((day, url, path, str(e)))
            continue
        write_bytes(path, data)
        return "downloaded", fmt, path, url, "", []
    if force:   # a failed re-download keeps the file already there
        for fmt in fmts:
            if os.path.exists(raw_path(store, day, fmt)):
                return "file", fmt, raw_path(store, day, fmt), url_for(day, fmt), "re-download failed, saved file used", []
    if failures:
        os.makedirs(os.path.dirname(failures[0][2]), exist_ok=True)   # ready for the file saved by hand
        return "failed", "", "", failures[0][1], "; ".join(f[3] for f in failures), failures
    if day < today_ist(now):
        return "holiday", "", "", not_found[0], "404 for " + " and ".join(not_found), []
    return "pending", "", "", not_found[0], "404: not yet published", []


def process_day(store, con, day, client, force, now, rlog):
    """Fetch, validate and record one weekday; returns (status, failures)."""
    saved = any(os.path.exists(raw_path(store, day, f)) for f in formats_for(day))
    if day == today_ist(now) and now.astimezone(IST).time() < READY_AT and not saved:
        set_day(con, day, "pending", note=f"not yet published (NSE publishes after {READY_AT:%H:%M} IST)")
        rlog(f"{day} pending: before {READY_AT:%H:%M} IST")
        return "pending", []
    kind, fmt, path, url, note, failures = get_file(store, day, client, force, now)
    if kind in ("failed", "pending", "holiday"):
        set_day(con, day, kind, url=url, note=note)
        if kind == "holiday":
            log_holiday(store, day, note[len("404 for "):].split(" and "), now.astimezone(IST))
        rlog(f"{day} {kind}: {note}")
        return kind, failures
    rel = os.path.relpath(path, store).replace(os.sep, "/")
    old = get_day(con, day)
    stamp = f"{now_ist():%Y-%m-%d %H:%M:%S}" if kind == "downloaded" else (
        old["downloaded_at"] if old and old.get("downloaded_at") else
        f"{datetime.fromtimestamp(os.path.getmtime(path), IST):%Y-%m-%d %H:%M:%S}")
    try:
        df = parse_zip(path, day, fmt)
    except BadFile as e:
        set_day(con, day, "failed", fmt, rel, url, None, None, stamp, str(e))
        rlog(f"{day} failed: {e}")
        return "failed", [(day, url, path, str(e))]
    set_day(con, day, "ok", fmt, rel, url, len(df), eq_be_bz(df), stamp, note)
    rlog(f"{day} ok ({fmt}, {len(df):,} rows, {eq_be_bz(df):,} EQ/BE/BZ{', ' + kind if kind == 'downloaded' else ', from raw/'})")
    return "ok", []


def scan_raw(store):
    """{day: [(fmt, path)]} of every bhavcopy zip in raw/<YYYY>/."""
    found = {}
    raw = os.path.join(store, "raw")
    if not os.path.isdir(raw):
        return found
    for year in sorted(os.listdir(raw)):
        sub = os.path.join(raw, year)
        if not os.path.isdir(sub):
            continue
        for name in sorted(os.listdir(sub)):
            got = parse_file_name(name)
            if got:
                found.setdefault(got[0], []).append((got[1], os.path.join(sub, name)))
    return found


def run(folder, days=(), force=False, offline=False, sleep=SLEEP, use_browser=True, client=None, now=None,
        log=print):
    """Download/validate days (weekdays; ok and holiday days skipped unless force) or, offline, every zip in raw/;
    then rebuild the parquet of every stale year, days.csv and the report. LockBusy when another run holds the lock.
    Returns {"counts", "failures" [(day, url, path, error)], "report", "flagged", "store", "years", "requests"}."""
    store = store_dir(folder)
    os.makedirs(store, exist_ok=True)
    lock = RunLock(os.path.join(store, ".lock")).acquire("cm_bhav.py")
    own_client = False
    con = None
    try:
        con = open_db(store)
        rlog = RunLog(store, log)
        now = now or now_ist()
        counts = dict.fromkeys(STATUSES + ("skipped",), 0)
        failures, touched = [], set()
        if offline:
            client = None
            found = scan_raw(store)
            days = sorted(found)
            rlog(f"=== offline run: {len(days)} file day(s) in raw/")
        else:
            days = sorted(d for d in set(days) if d.weekday() < 5)
            rlog(f"=== run started: {len(days)} weekday(s)"
                 + (f", {days[0]} to {days[-1]}" if days else "") + (", force" if force else ""))
            if client is None and days:
                client = CmClient(sleep=sleep, use_browser=use_browser, log=log)
                own_client = True
        try:
            for day in days:
                prev = get_day(con, day)
                if not offline and not force and prev and prev["status"] in ("ok", "holiday"):
                    counts["skipped"] += 1
                    continue
                status, fails = process_day(store, con, day, client, force and not offline, now, rlog)
                counts[status] += 1
                failures += fails
                if status == "ok":
                    touched.add(day.year)
        finally:
            years = stale_years(store, con, touched)
            for y in years:
                n = rebuild_year(store, con, y, rlog)
                rlog(f"{y}.parquet rebuilt ({n:,} rows)" if n else f"{y}: no ok days, no parquet")
            write_days_csv(store, con)
        text, flagged = report(con)
        rlog(f"=== run ended: " + ", ".join(f"{k} {v}" for k, v in counts.items()), echo=False)
        return {"counts": counts, "failures": failures, "report": text, "flagged": flagged, "store": store,
                "years": years, "requests": getattr(client, "requests", 0)}
    finally:
        if con is not None:
            con.close()
        if own_client:
            client.close()
        lock.release()
