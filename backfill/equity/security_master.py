"""Security master: one stable security_id per company, built from the saved NSE equity bhavcopy (no network).

Input: <folder>/NSE_CM_bhav/<YYYY>.parquet (backfill/cm_bhav.py; only read, never written), series EQ/BE/BZ by
default. Output: <folder>/security_master/ (every file written atomically, rebuilt in full on each build):
    securities.csv/.parquet       one row per security (SECURITY_COLUMNS)
    symbol_history.csv/.parquet   one row per period of constant (symbol, isin, series) (HISTORY_COLUMNS)
    events.csv                    LISTED / RENAME / ISIN_CHANGE / SERIES_CHANGE / GAP (EVENT_COLUMNS)
    review.csv                    links left for a human (REVIEW_COLUMNS): both_changed? / symbol_reused?
    build_report.txt              totals and the known-case checks
    build_info.json               series, link gap and data range of the build (read by the loaders)
    .lock                         one build at a time (backfill.locks.RunLock)

Linking walks the trading days in date order (see link()): the same ISIN is always the same security (renames,
series changes); a new ISIN under the symbol of a security whose old ISIN has stopped trading, seen within
link_gap trading days, is the same security (ISIN_CHANGE, a split candidate: the closes either side are recorded,
no factor is computed); anything else is a new security. Symbol and ISIN changing on the same day, or a symbol
coming back with a new ISIN, are never linked: they go to review.csv. Exits, split factors and index membership
are later steps."""
import json
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backfill.cm_bhav import store_dir
from backfill.locks import LockBusy, RunLock, held  # noqa: F401 - LockBusy is re-exported for the CLI
from config import equity_data_dir

IST = ZoneInfo("Asia/Kolkata")
OUT = "security_master"
SERIES = ("EQ", "BE", "BZ")
LINK_GAP = 10            # trading days: a new ISIN under a stopped security's symbol links within this gap
GAP_DAYS = 20            # trading days absent before a return is a GAP event
PRICE_TOL = 0.005        # both_changed?: first prev_close within 0.5% of the old last close / k
PRICE_DIVISORS = (1, 2, 3, 4, 5, 10)
BHAV_COLUMNS = ["date", "symbol", "series", "isin", "open", "close", "prev_close"]

SECURITY_COLUMNS = ["security_id", "first_date", "last_date", "trading_days", "first_symbol", "last_symbol",
                    "last_isin", "last_series", "symbols", "isins", "series_seen", "last_close", "days_since_last"]
HISTORY_COLUMNS = ["security_id", "symbol", "isin", "series", "from_date", "to_date", "trading_days"]
EVENT_COLUMNS = ["date", "security_id", "type", "old_symbol", "new_symbol", "old_isin", "new_isin", "old_series",
                 "new_series", "old_close", "new_prev_close", "new_open", "ratio_prev_close", "ratio_open",
                 "gap_days", "note"]
REVIEW_COLUMNS = ["kind", "date", "security_id_old", "security_id_new", "details"]
EVENT_TYPES = ["LISTED", "RENAME", "ISIN_CHANGE", "SERIES_CHANGE", "GAP"]

# Known cases in the real data (build_report.txt and `check`); a miss is reported, never fixed by a rule change.
KNOWN_RENAMES = [("AMARAJABAT", "ARE&M"), ("ADANITRANS", "ADANIENSOL"), ("AKZOINDIA", "JSWDULUX")]
KNOWN_ISIN_CHANGES = ["TATASTEEL", "IRCTC", "BAJFINANCE", "KOTAKBANK", "DRREDDY", "NAUKRI"]
KNOWN_BONUSES = {"RELIANCE": "INE002A01018", "HDFCBANK": "INE040A01034"}
KNOWN_ENDED = {"HDFC": "2023-07-12", "MINDTREE": None}
ENDED_TOL_DAYS = 7       # calendar days around an expected last date


class SecurityMasterError(Exception):
    pass


class ConflictError(SecurityMasterError):
    pass


class BhavcopyBusy(SecurityMasterError):
    pass


class NotBuilt(SecurityMasterError):
    pass


def now_ist():
    return datetime.now(IST)


def out_dir(folder=None):
    return os.path.join(folder or equity_data_dir(), OUT)


def year_files(store):
    if not os.path.isdir(store):
        return []
    names = sorted(n for n in os.listdir(store) if len(n) == 12 and n[:4].isdigit() and n.endswith(".parquet"))
    return [os.path.join(store, n) for n in names]


def load_bhav(store, series=SERIES):
    """(rows, days): the needed columns of every yearly file, rows of the given series only, sorted by date, ISIN,
    symbol; days = every date in the files whatever the series (the trading-day calendar for gaps)."""
    files = year_files(store)
    if not files:
        raise SecurityMasterError(f"No bhavcopy parquet files in {store}. Run cm_bhav.py first.")
    tables = [pq.read_table(f, columns=BHAV_COLUMNS, filters=[("series", "in", list(series))]) for f in files]
    df = pa.concat_tables(tables).to_pandas()
    if df.empty:
        raise SecurityMasterError(f"No rows of series {', '.join(series)} in {store}.")
    days = sorted({d for f in files for d in pq.read_table(f, columns=["date"]).column("date").unique().to_pylist()})
    return df.sort_values(["date", "isin", "symbol"], kind="stable").reset_index(drop=True), days


def check_conflicts(df):
    """One ISIN on two symbols, or one symbol on two ISINs, on the same day -> ConflictError listing them."""
    lines = []
    for key, other, what in (("isin", "symbol", "ISIN on two symbols"), ("symbol", "isin", "symbol with two ISINs")):
        n = df.groupby(["date", key])[other].nunique()
        for (day, value), _ in n[n > 1].items():
            names = sorted(df.loc[(df["date"] == day) & (df[key] == value), other].unique())
            lines.append(f"{day} {what}: {value} -> {', '.join(names)}")
    if lines:
        shown = "\n".join(lines[:50]) + (f"\n... and {len(lines) - 50} more" if len(lines) > 50 else "")
        raise ConflictError(f"{len(lines)} conflicts in the bhavcopy (nothing written):\n{shown}")


def ratio(a, b):
    return round(a / b, 6) if b else None


def event(day, sid, kind, **fields):
    row = dict.fromkeys(EVENT_COLUMNS)
    row.update(date=day, security_id=sid, type=kind, **fields)
    return row


def link(df, link_gap=LINK_GAP, days=None):
    """Walk the trading days and give every row a security. Returns (securities, events, reviews, days): securities
    = state dicts in id order (with their (symbol, isin, series) periods), days = the trading-day calendar (default:
    the dates of df). Gaps are counted in these days. Rows must be free of conflicts (check_conflicts)."""
    days = sorted(df["date"].unique()) if days is None else list(days)
    day_index = {d: i for i, d in enumerate(days)}
    di = df["date"].map(day_index).tolist()
    isin_last = pd.Series(di).groupby(df["isin"].to_numpy()).max().to_dict()
    secs, events, reviews = [], [], []
    isin_owner, symbol_owner = {}, {}

    def new_security(d, sym, isin, ser, close, prev, opn, note=None):
        sid = f"S{len(secs) + 1:06d}"
        s = {"id": sid, "first_d": d, "last_d": d, "days": 1, "first_symbol": sym, "symbol": sym, "isin": isin,
             "series": ser, "close": close, "first_prev": prev, "symbols": [sym], "isins": [isin], "series_seen": [ser],
             "periods": [[sym, isin, ser, d, d, 1]]}
        secs.append(s)
        isin_owner[isin] = s
        symbol_owner[sym] = s
        events.append(event(days[d], sid, "LISTED", new_symbol=sym, new_isin=isin, new_series=ser,
                            new_prev_close=prev, new_open=opn, note=note))
        return s

    def seen_again(s, d, sym, isin, ser, close):
        """Shared by rules 1 and 2: GAP / RENAME / SERIES_CHANGE events, then the state and the period."""
        day = days[d]
        absent = d - s["last_d"] - 1
        if absent > GAP_DAYS:
            events.append(event(day, s["id"], "GAP", gap_days=absent,
                                note=f"absent {days[s['last_d'] + 1]} to {days[d - 1]}"))
        if sym != s["symbol"]:
            events.append(event(day, s["id"], "RENAME", old_symbol=s["symbol"], new_symbol=sym, old_isin=s["isin"],
                                new_isin=isin))
            if symbol_owner.get(s["symbol"]) is s:
                del symbol_owner[s["symbol"]]
            symbol_owner[sym] = s
        if ser != s["series"]:
            events.append(event(day, s["id"], "SERIES_CHANGE", old_symbol=s["symbol"], new_symbol=sym,
                                old_series=s["series"], new_series=ser))
        for key, value in (("symbols", sym), ("isins", isin), ("series_seen", ser)):
            if value not in s[key]:
                s[key].append(value)
        p = s["periods"][-1]
        if (p[0], p[1], p[2]) == (sym, isin, ser):
            p[4], p[5] = d, p[5] + 1
        else:
            s["periods"].append([sym, isin, ser, d, d, 1])
        s.update(symbol=sym, isin=isin, series=ser, close=close, last_d=d, days=s["days"] + 1)

    start = 0
    cols = [df[c].tolist() for c in ("symbol", "series", "isin", "open", "close", "prev_close")]
    n = len(df)
    while start < n:
        d = di[start]
        end = start
        while end < n and di[end] == d:
            end += 1
        known, fresh = [], []
        for i in range(start, end):
            (known if cols[2][i] in isin_owner else fresh).append(i)
        for i in known:                                    # rule 1: same ISIN, same security
            sym, ser, isin, opn, close, prev = (c[i] for c in cols)
            seen_again(isin_owner[isin], d, sym, isin, ser, close)
        for i in fresh:                                    # sorted by (isin, symbol) already
            sym, ser, isin, opn, close, prev = (c[i] for c in cols)
            s = symbol_owner.get(sym)
            if s is None:                                  # rule 3
                new_security(d, sym, isin, ser, close, prev, opn, note="in data from first day" if d == 0 else None)
                continue
            stopped = isin_last[s["isin"]] < d
            gap = d - s["last_d"]
            if stopped and gap <= link_gap and s["last_d"] < d:   # rule 2: ISIN change (split candidate)
                old_close, old_isin = s["close"], s["isin"]
                events.append(event(days[d], s["id"], "ISIN_CHANGE", old_symbol=s["symbol"], new_symbol=sym,
                                    old_isin=old_isin, new_isin=isin, old_series=s["series"], new_series=ser,
                                    old_close=old_close, new_prev_close=prev, new_open=opn,
                                    ratio_prev_close=ratio(old_close, prev), ratio_open=ratio(old_close, opn)))
                isin_owner[isin] = s
                seen_again(s, d, sym, isin, ser, close)
                continue
            why = (f"old ISIN {s['isin']} still trades" if not stopped
                   else f"{gap} trading days since {s['symbol']} was last seen ({days[s['last_d']]}), more than {link_gap}")
            t = new_security(d, sym, isin, ser, close, prev, opn)                    # rule 5
            reviews.append({"kind": "symbol_reused?", "date": days[d], "security_id_old": s["id"],
                            "security_id_new": t["id"],
                            "details": f"{sym}: new ISIN {isin}, earlier {s['isin']} ({why})"})
        start = end
    reviews += review_both_changed(secs, days)
    return secs, events, reviews, days


def review_both_changed(secs, days):
    """Rule 4: a security ending on day d and another starting on d or d+1 with another symbol and ISIN, whose first
    prev_close is within PRICE_TOL of the old last close / k (k in PRICE_DIVISORS). Never linked, only listed."""
    starts = {}
    for s in secs:
        starts.setdefault(s["first_d"], []).append(s)
    rows = []
    for old in secs:
        d = old["last_d"]
        if d == len(days) - 1 or not old["close"]:
            continue
        for new in starts.get(d, []) + starts.get(d + 1, []):
            if new is old or new["first_symbol"] == old["symbol"] or new["isins"][0] == old["isin"]:
                continue
            prev = new["first_prev"]
            for k in PRICE_DIVISORS:
                target = old["close"] / k
                if prev and abs(prev - target) <= PRICE_TOL * target:
                    rows.append({"kind": "both_changed?", "date": days[new["first_d"]], "security_id_old": old["id"],
                                 "security_id_new": new["id"],
                                 "details": f"{old['symbol']} {old['isin']} last {days[d]} close {old['close']:g} -> "
                                            f"{new['first_symbol']} {new['isins'][0]} first prev_close {prev:g}"
                                            + (f" (close / {k})" if k > 1 else "")})
                    break
    return rows


def frames(secs, events, reviews, days):
    last = len(days) - 1
    sec_rows, hist_rows = [], []
    for s in secs:
        sec_rows.append([s["id"], days[s["first_d"]], days[s["last_d"]], s["days"], s["first_symbol"], s["symbol"],
                         s["isin"], s["series"], "|".join(s["symbols"]), "|".join(s["isins"]),
                         "|".join(s["series_seen"]), s["close"], last - s["last_d"]])
        for sym, isin, ser, a, b, n in s["periods"]:
            hist_rows.append([s["id"], sym, isin, ser, days[a], days[b], n])
    securities = pd.DataFrame(sec_rows, columns=SECURITY_COLUMNS)
    history = pd.DataFrame(hist_rows, columns=HISTORY_COLUMNS)
    ev = pd.DataFrame(events, columns=EVENT_COLUMNS)
    ev["gap_days"] = ev["gap_days"].astype("Int64")
    rv = pd.DataFrame(reviews, columns=REVIEW_COLUMNS).sort_values(["date", "kind", "security_id_new"], kind="stable")
    return securities, history, ev, rv.reset_index(drop=True)


def write_atomic(path, write):
    tmp = path + ".tmp"
    try:
        write(tmp)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def write_csv(df, path):
    write_atomic(path, lambda tmp: df.to_csv(tmp, index=False, lineterminator="\n", encoding="utf-8"))


def write_parquet(df, path):
    write_atomic(path, lambda tmp: pq.write_table(pa.Table.from_pandas(df, preserve_index=False), tmp))


def write_text(path, text):
    def put(tmp):
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
    write_atomic(path, put)


def known_checks(securities, events):
    """(lines, ok) for the KNOWN_* cases on built frames."""
    lines, ok = [], True
    ev = events.astype({"date": str})

    def mark(found, text):
        nonlocal ok
        ok &= found
        lines.append(f"  [{'found' if found else 'NOT FOUND'}] {text}")

    lines.append("Renames linked into one security:")
    for a, b in KNOWN_RENAMES:
        hit = ev[(ev["type"] == "RENAME") & (ev["old_symbol"] == a) & (ev["new_symbol"] == b)]
        mark(not hit.empty, f"{a} -> {b}" + "".join(f": {r.date}, {r.security_id}, ISIN {r.new_isin}"
                                                    for r in hit.itertuples()))
    lines.append("ISIN changes (ISIN_CHANGE event, ratio_prev_close / ratio_open):")
    for sym in KNOWN_ISIN_CHANGES:
        hit = ev[(ev["type"] == "ISIN_CHANGE") & ((ev["new_symbol"] == sym) | (ev["old_symbol"] == sym))]
        mark(not hit.empty, sym + "".join(f"\n      {r.date} {r.security_id} {r.old_isin} -> {r.new_isin}: old_close "
                                          f"{r.old_close:g}, new_prev_close {r.new_prev_close:g}, new_open "
                                          f"{r.new_open:g}, ratios {r.ratio_prev_close} / {r.ratio_open}"
                                          for r in hit.itertuples()))
    lines.append("Bonuses (one ISIN, no ISIN change):")
    for sym, isin in KNOWN_BONUSES.items():
        rows = securities[securities["symbols"].str.split("|").apply(lambda xs: sym in xs)]
        good = [r for r in rows.itertuples() if r.isins == isin
                and ev[(ev["security_id"] == r.security_id) & (ev["type"] == "ISIN_CHANGE")].empty]
        mark(bool(good), f"{sym} {isin}" + "".join(f": {r.security_id}, {r.first_date} to {r.last_date}" for r in good)
             + ("" if good else "".join(f"; seen as {r.security_id} with ISINs {r.isins}" for r in rows.itertuples())))
    lines.append("Ended securities (last date):")
    for sym, expected in KNOWN_ENDED.items():
        rows = securities[(securities["last_symbol"] == sym) & (securities["days_since_last"] > 0)]
        good = [r for r in rows.itertuples() if expected is None
                or abs((pd.Timestamp(r.last_date) - pd.Timestamp(expected)).days) <= ENDED_TOL_DAYS]
        text = f"{sym}" + (f" (expected around {expected})" if expected else "")
        text += "".join(f": {r.security_id} last {r.last_date}, {r.days_since_last} trading days ago" for r in rows.itertuples())
        mark(bool(good), text if not rows.empty else text + ": no ended security with this last symbol")
    return lines, ok


def report_text(securities, events, reviews, info, checks):
    lines = ["Security master build", f"Built: {info['built_at']}",
             f"Data: {info['first_date']} to {info['last_date']}, {info['trading_days']} trading days, "
             f"{info['rows']:,} rows, series {','.join(info['series'])}, link gap {info['link_gap']} trading days", "",
             "Totals:", f"  securities: {len(securities):,}"]
    counts = events["type"].value_counts()
    lines += [f"  {t} events: {int(counts.get(t, 0)):,}" for t in EVENT_TYPES]
    rk = reviews["kind"].value_counts()
    lines.append(f"  review rows: {len(reviews):,}"
                 + "".join(f" ({k}: {int(rk.get(k, 0)):,})" for k in ("both_changed?", "symbol_reused?")))
    first = pd.to_datetime(securities["first_date"]).dt.year.value_counts()
    last = pd.to_datetime(securities["last_date"]).dt.year.value_counts()
    lines += ["", "Securities per year:", "  year  first_seen  last_seen"]
    lines += [f"  {y}  {int(first.get(y, 0)):>10,}  {int(last.get(y, 0)):>9,}" for y in sorted(set(first.index) | set(last.index))]
    lines += ["  (last_seen in the final year includes every security still trading)", "", "Known-case checks:"]
    lines += checks[0]
    lines.append(f"Known-case checks: {'all found' if checks[1] else 'SOME NOT FOUND'}")
    return "\n".join(lines) + "\n"


def build(folder=None, series=SERIES, link_gap=LINK_GAP, now=None, log=print):
    """Full rebuild of <folder>/security_master/ from the bhavcopy parquet. Refuses while the bhavcopy store's lock is
    held (BhavcopyBusy) or another build runs (LockBusy). Returns a dict with the counts and the report."""
    folder = folder or equity_data_dir()
    store, out = store_dir(folder), out_dir(folder)
    if held(os.path.join(store, ".lock")):
        raise BhavcopyBusy(f"A bhavcopy download (cm_bhav.py) holds {os.path.join(store, '.lock')}; build after it ends.")
    lock = RunLock(os.path.join(out, ".lock")).acquire("security_master.py build")
    try:
        log(f"Reading {store} (series {','.join(series)})")
        df, calendar = load_bhav(store, series)
        check_conflicts(df)
        log(f"Linking {len(df):,} rows")
        secs, events, reviews, days = link(df, link_gap, calendar)
        securities, history, ev, rv = frames(secs, events, reviews, days)
        info = {"series": list(series), "link_gap": link_gap, "first_date": str(days[0]), "last_date": str(days[-1]),
                "trading_days": len(days), "rows": len(df),
                "built_at": f"{(now or now_ist()):%Y-%m-%d %H:%M:%S} IST"}
        checks = known_checks(securities, ev)
        report = report_text(securities, ev, rv, info, checks)
        write_parquet(securities, os.path.join(out, "securities.parquet"))
        write_csv(securities, os.path.join(out, "securities.csv"))
        write_parquet(history, os.path.join(out, "symbol_history.parquet"))
        write_csv(history, os.path.join(out, "symbol_history.csv"))
        write_csv(ev, os.path.join(out, "events.csv"))
        write_csv(rv, os.path.join(out, "review.csv"))
        write_text(os.path.join(out, "build_info.json"), json.dumps(info, indent=2) + "\n")
        write_text(os.path.join(out, "build_report.txt"), report)
        _CACHE.clear()
        return {"securities": len(securities), "events": len(ev), "reviews": len(rv), "report": report,
                "checks_ok": checks[1], "out": out}
    finally:
        lock.release()


# ---- loaders (read the built files) ----

_CACHE = {}


def load_master(folder=None):
    """The built frames plus point-in-time lookup intervals, cached until the files change. NotBuilt if missing."""
    out = out_dir(folder)
    paths = [os.path.join(out, n) for n in ("securities.parquet", "symbol_history.parquet", "build_info.json")]
    if not all(os.path.isfile(p) for p in paths):
        raise NotBuilt(f"No security master in {out}. Run: python security_master.py build")
    key = (out, tuple(os.path.getmtime(p) for p in paths))
    if key not in _CACHE:
        _CACHE.clear()
        securities = pq.read_table(paths[0]).to_pandas()
        history = pq.read_table(paths[1]).to_pandas()
        with open(paths[2], encoding="utf-8") as f:
            info = json.load(f)
        _CACHE[key] = {"securities": securities, "history": history, "info": info,
                       "symbol": intervals(history, "symbol"), "isin": intervals(history, "isin"),
                       "isin_owner": {i: r.security_id for r in securities.itertuples() for i in r.isins.split("|")}}
    return _CACHE[key]


def intervals(history, field):
    """{value: [(from, until or None, security_id)]}: a security's symbol (or ISIN) is valid from the first day of a
    run of periods carrying it until the next period of that security with another value starts, or until another
    security takes the value over (a reused symbol), whichever comes first."""
    out = {}
    for sid, rows in history.sort_values(["security_id", "from_date"], kind="stable").groupby("security_id", sort=False):
        runs = []
        for value, start in zip(rows[field], rows["from_date"]):
            if not runs or runs[-1][0] != value:
                runs.append([value, start])
        for j, (value, start) in enumerate(runs):
            until = runs[j + 1][1] if j + 1 < len(runs) else None
            out.setdefault(value, []).append((start, until, sid))
    for value, spans in out.items():
        spans.sort(key=lambda x: x[0])
        for j in range(len(spans) - 1):
            start, until, sid = spans[j]
            nxt = spans[j + 1][0]
            if until is None or nxt < until:
                spans[j] = (start, nxt, sid)
    return out


def as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def security_id_for(symbol_or_isin, on_date, folder=None):
    """The security_id that symbol or ISIN meant on on_date (point in time), else None."""
    m = load_master(folder)
    day, key = as_date(on_date), str(symbol_or_isin).strip().upper()
    for field in ("isin", "symbol"):
        for start, until, sid in m[field].get(key, []):
            if start <= day and (until is None or day < until):
                return sid
    return None


def lookup(symbol_or_isin, folder=None):
    """Every security that ever had this symbol or ISIN, in id order."""
    m = load_master(folder)
    key = str(symbol_or_isin).strip().upper()
    ids = {sid for field in ("isin", "symbol") for _, _, sid in m[field].get(key, [])}
    return sorted(ids)


def _read_years(folder, filters, first=None, last=None):
    files = year_files(store_dir(folder or equity_data_dir()))
    files = [f for f in files if (first is None or int(os.path.basename(f)[:4]) >= first.year)
             and (last is None or int(os.path.basename(f)[:4]) <= last.year)]
    tables = [pq.read_table(f, filters=filters) for f in files]
    return pa.concat_tables(tables).to_pandas() if tables else pd.DataFrame()


def history(security_id, start=None, end=None, folder=None):
    """That security's bhavcopy rows across all its symbols, ISINs and series (raw prices), sorted by date."""
    m = load_master(folder)
    row = m["securities"][m["securities"]["security_id"] == security_id]
    if row.empty:
        raise KeyError(f"unknown security_id {security_id}")
    r = row.iloc[0]
    first = max(as_date(r.first_date), as_date(start)) if start else as_date(r.first_date)
    last = min(as_date(r.last_date), as_date(end)) if end else as_date(r.last_date)
    filters = [("isin", "in", r.isins.split("|")), ("series", "in", m["info"]["series"]),
               ("date", ">=", first), ("date", "<=", last)]
    df = _read_years(folder, filters, first, last)
    if df.empty:
        return df
    df.insert(0, "security_id", security_id)
    return df.sort_values("date", kind="stable").reset_index(drop=True)


def universe(on_date, series=SERIES, folder=None):
    """Bhavcopy rows of the securities that traded on on_date in series, with security_id (unknown ISINs dropped)."""
    m = load_master(folder)
    day = as_date(on_date)
    df = _read_years(folder, [("date", "==", day), ("series", "in", list(series))], day, day)
    if df.empty:
        return df
    df.insert(0, "security_id", df["isin"].map(m["isin_owner"]))
    return df.dropna(subset=["security_id"]).sort_values("security_id", kind="stable").reset_index(drop=True)


def check(folder=None):
    """Re-run the known-case checks on the built files: (lines, ok)."""
    m = load_master(folder)
    events_path = os.path.join(out_dir(folder), "events.csv")
    ev = pd.read_csv(events_path, dtype=str, keep_default_na=False)
    for c in ("old_close", "new_prev_close", "new_open"):
        ev[c] = pd.to_numeric(ev[c])
    return known_checks(m["securities"].astype({"first_date": str, "last_date": str}), ev)
