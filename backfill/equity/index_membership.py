"""Index membership over time: NIFTY indices (point in time, from the current constituent lists walked back through
NSE Indices' change notices) and the AMFI large / mid / small cap lists (half-yearly). No network here: the files are
downloaded by index_fetch.py (index_membership.py fetch); the build and the loaders only read.

Inputs, all only read: <folder>/index_membership/input/ (amfi/, nifty_current/, nifty_changes/, snapshots/,
manual_changes.csv), the security master (security_master/) and the bhavcopy calendar (NSE_CM_bhav/<YYYY>.parquet).
Output <folder>/index_membership/, rebuilt in full, every file atomic: nifty_membership.parquet/.csv,
nifty_changes.csv, amfi.parquet/.csv, review.csv, calendar.csv, build_report.txt, build_info.json (text_cache/ keeps
the PDF text between builds).

Point in time: a membership is valid from its effective date (in the index from that date, out from the date of an
exclusion); an AMFI category from the list's valid_from (the first trading day of the month after the half year
plus amfi_lag trading days). Nothing after a date is used for it; a date outside the covered range is an error."""
import json
import os
import re
from bisect import bisect_left, bisect_right
from collections import namedtuple
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from backfill.cm_bhav import store_dir
from backfill.equity import index_fetch as fe
from backfill.equity import index_notices as nt
from backfill.equity import security_master as sm
from backfill.locks import LockBusy, RunLock, held  # noqa: F401 - LockBusy is re-exported for the CLI
from config import equity_data_dir

OUT = fe.OUT
SIZES = nt.SIZES
INDEXES = nt.INDEXES
# Midcap 150 and Smallcap 250 were first published on 2016-04-01 (ind_prs22022016_1.pdf); the others (NIFTY Midcap
# 100 as CNX Midcap / Nifty Free Float Midcap 100) existed before the data starts.
INDEX_START = {"NIFTY MIDCAP 150": date(2016, 4, 1), "NIFTY SMALLCAP 250": date(2016, 4, 1)}
AMFI_LAG = 5             # trading days after the first trading day of the month after the half year
NOT_TRADING_DAYS = 20    # a member without a bhavcopy row for more than this many trading days -> review
SNAPSHOT_TIMING = 5      # a snapshot difference within this many trading days of a change is timing
REVIEW_EXAMPLES = 10
RELIABLE_SHARE = 0.98    # reliable_from: at least this share of days at the expected size from then on
ADDITIONAL_CLASS = re.compile(r"DVR$")   # a second share class counted on top of the size (Tata Motors DVR)
REIT_RX = re.compile(r"REIT|InvIT|Real Estate (Investment )?Trust|Infrastructure Investment Trust", re.I)
EXPECTED_RULES = [
    "additional share class: each member whose symbol ends in DVR (Tata Motors DVR) adds 1 to the expected size",
    "REIT / InvIT: each current-list row of a REIT or InvIT (not in the equity master) takes 1 from the expected "
    "size from the effective date of its inclusion in that index (else from the coverage start)",
    "nothing else is expected; every other day off the size counts against reliable_from",
]
COMPOSITES = [  # (name, whole, parts, from): whole = union of the disjoint parts (NSE methodology, Sept 2026)
    ("NIFTY 100 = NIFTY 50 + NIFTY NEXT 50", "NIFTY 100", ["NIFTY 50", "NIFTY NEXT 50"], None),
    # before 2018-04-02 NIFTY 200 was NIFTY 100 + the (ceased) Nifty Full Midcap 100 (ind_prs05012018.pdf)
    ("NIFTY 200 = NIFTY 100 + NIFTY MIDCAP 100", "NIFTY 200", ["NIFTY 100", "NIFTY MIDCAP 100"], date(2018, 4, 2)),
    ("NIFTY 500 = NIFTY 100 + NIFTY MIDCAP 150 + NIFTY SMALLCAP 250", "NIFTY 500",
     ["NIFTY 100", "NIFTY MIDCAP 150", "NIFTY SMALLCAP 250"], date(2016, 4, 1)),
]
MEMBERSHIP_COLUMNS = ["index", "security_id", "symbol", "isin", "from_date", "to_date", "source"]
CHANGE_COLUMNS = ["index", "effective_date", "action", "symbol", "isin", "security_id", "source", "announced_date",
                  "note", "status", "file", "company"]
AMFI_COLUMNS = ["period", "period_end", "valid_from", "valid_to", "rank", "category", "security_id", "isin", "symbol",
                "company", "avg_mcap", "file"]
REVIEW_COLUMNS = ["kind", "index", "date", "security_id", "symbol", "details"]
REVIEW_KINDS = ["unmapped_current", "unmapped_change", "undo_mismatch", "not_parsed", "not_applied", "conflict",
                "count_mismatch", "member_not_trading", "snapshot_mismatch", "amfi_unmatched", "amfi_duplicate",
                "amfi_count"]
MANUAL_COLUMNS = ["index", "effective_date", "action", "symbol", "isin", "note"]
MANUAL_ACTIONS = ("IN", "OUT", "IGNORE")
MANUAL_TEMPLATE = (
    "index,effective_date,action,symbol,isin,note\n"
    "# Manual index changes, added to the parsed notices (index_membership.py build). Lines starting with # are\n"
    "# ignored. index: NIFTY 50, NIFTY NEXT 50, NIFTY 100, NIFTY 200, NIFTY 500, NIFTY MIDCAP 150, NIFTY SMALLCAP 250,\n"
    "# NIFTY MIDCAP 100. effective_date: the first day in (IN) or out (OUT), YYYY-MM-DD. action: IN, OUT, or IGNORE\n"
    "# (drops the parsed row of that index, date and symbol; with symbol and isin blank, every parsed row of that\n"
    "# index and date). isin is optional (used before the symbol).\n"
    "# Example (not active):\n"
    "# NIFTY 50,2023-07-13,IN,LTIM,INE214T01019,replaces HDFC (ind_prs04072023.pdf)\n")

# Known cases in the real data (build_report.txt and `check`); a miss is reported, never fixed by a rule change.
KNOWN_ALWAYS = [("RELIANCE", "NIFTY 50")]
KNOWN_LARGE = ["RELIANCE"]
KNOWN_HDFC = {"out": "HDFC", "index": "NIFTY 50", "last_day": "2023-07-12", "in_from": "2023-07-13", "in": "LTIM"}
KNOWN_AMFI_PERIODS = 18


class IndexMembershipError(Exception):
    pass


class StoreBusy(IndexMembershipError):
    pass


class InputStale(IndexMembershipError):
    pass


class NotBuilt(IndexMembershipError):
    pass


class OutOfCoverage(IndexMembershipError, ValueError):
    pass


def out_dir(folder=None):
    return os.path.join(folder or equity_data_dir(), OUT)


def as_date(value):
    return sm.as_date(value)


def until(series):
    """to_date with open periods as date.max (for comparisons)."""
    return series.where(series.notna(), date.max)


def index_name(text):
    """'nifty50', 'NIFTY_MIDCAP_150', 'Nifty Midcap 150' -> the index key; KeyError if unknown."""
    key = re.sub(r"[\s_]+", "", str(text)).upper()
    for name in INDEXES:
        if name.replace(" ", "") == key:
            return name
    raise KeyError(f"unknown index {text!r}; one of {', '.join(INDEXES)}")


# ---- inputs ----

def check_inputs(folder):
    """Refuse while a bhavcopy download or a security master build holds its lock (StoreBusy), without a master
    (NotBuilt) or with a master older than the newest bhavcopy year file (InputStale)."""
    store, master = store_dir(folder), sm.out_dir(folder)
    for lock, what in ((os.path.join(store, ".lock"), "a bhavcopy download (cm_bhav.py)"),
                       (os.path.join(master, ".lock"), "a security master build (security_master.py)")):
        if held(lock):
            raise StoreBusy(f"{what} holds {lock}; build after it ends.")
    info = os.path.join(master, "build_info.json")
    need = [os.path.join(master, n) for n in ("securities.parquet", "symbol_history.parquet")] + [info]
    if not all(os.path.isfile(p) for p in need):
        raise NotBuilt(f"No security master in {master}. Run: python security_master.py build")
    years = sm.year_files(store)
    if not years:
        raise IndexMembershipError(f"No bhavcopy parquet files in {store}. Run cm_bhav.py first.")
    newest = max(years, key=os.path.getmtime)
    if os.path.getmtime(info) < os.path.getmtime(newest):
        raise InputStale(f"{info} is older than {newest}. Rebuild it first: python security_master.py build")


class Calendar:
    """The trading days (every date in the bhavcopy, any series). Past the data, weekdays count on."""

    def __init__(self, days):
        self.days = list(days)
        self.index = {d: i for i, d in enumerate(self.days)}

    @property
    def first(self):
        return self.days[0]

    @property
    def last(self):
        return self.days[-1]

    def pos(self, day):
        """Index of the trading day, or of the next one; past the data, weekdays are counted on."""
        if day > self.days[-1]:
            return len(self.days) - 1 + int(np.busday_count(self.days[-1], day))
        return bisect_left(self.days, day)

    def at(self, i):
        """The i-th trading day; past the data, weekdays count on."""
        if i < len(self.days):
            return self.days[max(i, 0)]
        return pd.Timestamp(np.busday_offset(np.datetime64(self.days[-1]), i - len(self.days) + 1)).date()

    def before(self, day):
        """The trading day before day (the last day still in, for an exclusion effective on day)."""
        i = bisect_left(self.days, day)
        if i == 0:
            return day - timedelta(days=1)
        return self.days[i - 1]

    def on_or_before(self, day):
        i = bisect_right(self.days, day)
        return self.days[i - 1] if i else None

    def between(self, a, b):
        """Trading days in [a, b]."""
        return self.days[bisect_left(self.days, a):bisect_right(self.days, b)]

    def distance(self, a, b):
        return self.pos(b) - self.pos(a)


def read_calendar(folder):
    files = sm.year_files(store_dir(folder))
    days = set()
    for f in files:
        days.update(pq.read_table(f, columns=["date"]).column("date").unique().to_pylist())
    return Calendar(sorted(days))


def trading_positions(folder, isins_by_sid, series, cal):
    """{security_id: sorted numpy array of calendar positions it traded on (master series)}."""
    owner = {i: sid for sid, isins in isins_by_sid.items() for i in isins}
    wanted = pa.array(sorted(owner), type=pa.string())
    out = {}
    for f in sm.year_files(store_dir(folder)):
        t = pq.read_table(f, columns=["date", "isin", "series"])
        t = t.filter(pc.and_(pc.is_in(t.column("isin"), value_set=wanted),
                             pc.is_in(t.column("series"), value_set=pa.array(list(series)))))
        df = t.select(["date", "isin"]).to_pandas()
        for isin, d in zip(df["isin"], df["date"]):
            out.setdefault(owner[isin], set()).add(cal.index[d])
    return {sid: np.array(sorted(v)) for sid, v in out.items()}


# ---- the security master ----

class Master:
    """Point-in-time mapping of symbols and ISINs to security_id (the security master's loaders) plus names on a
    date."""

    def __init__(self, folder):
        self.folder = folder
        self.m = sm.load_master(folder)
        h = self.m["history"].sort_values(["security_id", "from_date"], kind="stable")
        self.periods = {sid: list(zip(g["from_date"], g["to_date"], g["symbol"], g["isin"]))
                        for sid, g in h.groupby("security_id", sort=False)}

    def by_isin(self, isin):
        return self.m["isin_owner"].get(str(isin or "").strip().upper())

    def by_symbol(self, symbol, days):
        """The security the symbol meant on the first of days that knows it, else the only security that ever had
        it; (sid, how) or (None, why)."""
        for d in days:
            if d is None:
                continue
            sid = sm.security_id_for(symbol, d, self.folder)
            if sid:
                return sid, f"symbol on {d}"
        ids = [s for s in sm.lookup(symbol, self.folder) if s in self.periods]
        if len(ids) == 1:
            return ids[0], "the only security with that symbol"
        return None, ("no security ever had that symbol" if not ids else f"symbol used by {', '.join(ids)}")

    def name_on(self, sid, day):
        """(symbol, isin) of the security on day (else its last before day, else its first)."""
        rows = self.periods.get(sid, [])
        if not rows:
            return "", ""
        best = rows[0]
        for f, t, s, i in rows:
            if f <= day:
                best = (f, t, s, i)
        return best[2], best[3]

    def isins(self, sid):
        return sorted({r[3] for r in self.periods.get(sid, [])})


# ---- PDF text cache ----

def notice_stream(path, cache_dir):
    """The extract_stream() items of a notice PDF, cached in cache_dir by file size and STREAM_VERSION."""
    name = os.path.basename(path)
    cache = os.path.join(cache_dir, name + ".json")
    size = os.path.getsize(path)
    if os.path.isfile(cache):
        try:
            with open(cache, encoding="utf-8") as f:
                c = json.load(f)
            if c.get("size") == size and c.get("version") == nt.STREAM_VERSION:
                return c["items"]
        except (OSError, ValueError, KeyError):
            pass
    with open(path, "rb") as f:
        data = f.read()
    try:
        items = nt.extract_stream(data)
    except Exception as e:   # noqa: BLE001 - a broken PDF is reported, the build goes on
        raise nt.NoticeError(f"{name}: {type(e).__name__}: {e}") from None
    os.makedirs(cache_dir, exist_ok=True)
    sm.write_text(cache, json.dumps({"size": size, "version": nt.STREAM_VERSION, "items": items}))
    return items


def notice_titles(folder):
    path = os.path.join(fe.input_dir(folder, "nifty_changes"), "notices.csv")
    if not os.path.isfile(path):
        return {}
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return dict(zip(df["file"], df["title"]))


def read_notices(folder, log=print):
    """(changes, notices, problems): parsed IN / OUT rows of every notice PDF in nifty_changes/, one summary per
    notice, and [(file, error)] for PDFs that could not be read."""
    sub = fe.input_dir(folder, "nifty_changes")
    cache = os.path.join(out_dir(folder), "text_cache")
    titles = notice_titles(folder)
    files = sorted((n for n in (os.listdir(sub) if os.path.isdir(sub) else []) if n.lower().endswith(".pdf")),
                   key=lambda n: (fe.notice_date(n) or date.min, n))
    streams, problems = {}, []
    log(f"Reading {len(files)} notice PDFs in {sub}")
    for n in files:
        if fe.notice_date(n) is None:
            problems.append((n, "file name is not ind_prsDDMMYYYY.pdf (announcement date unknown)"))
            continue
        try:
            streams[n] = notice_stream(os.path.join(sub, n), cache)
        except nt.NoticeError as e:
            problems.append((n, str(e)))
    changes, notices = [], []
    for n, items in streams.items():
        day = fe.notice_date(n)
        p = nt.parse_notice(items, day, titles.get(n, ""), n)
        ours = sorted({idx for _, idx in p["headings"] if idx})
        notices.append({"file": n, "announced_date": str(day), "title": titles.get(n, ""),
                        "dates": [str(d) for d in p["dates"]], "review": p["review"], "demerger": p["demerger"],
                        "replaces": p["replaces"], "no_text": p["no_text"],
                        "indexes": ours, "unresolved": p["unresolved"], "changes": len(p["changes"]),
                        "not_applied": p["not_applied"]})
        changes += p["changes"]
    return changes, notices, problems


def read_manual(folder):
    """(rows, bad): manual_changes.csv (created once with a header and a commented example, never overwritten)."""
    path = os.path.join(fe.input_dir(folder), "manual_changes.csv")
    if not os.path.isfile(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        sm.write_text(path, MANUAL_TEMPLATE)
    rows, bad = [], []
    with open(path, encoding="utf-8-sig") as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    if not lines:
        return rows, bad
    import csv
    reader = csv.DictReader(lines)
    for k, r in enumerate(reader, 1):
        r = {(c or "").strip().lower(): (v or "").strip() for c, v in r.items() if c}
        try:
            index = index_name(r.get("index", ""))
            day = date.fromisoformat(r.get("effective_date", ""))
            action = r.get("action", "").upper()
            if action not in MANUAL_ACTIONS:
                raise ValueError(f"action {action!r} is not IN, OUT or IGNORE")
            symbol = nt.clean_symbol(r.get("symbol", ""))
            isin = r.get("isin", "").upper()
            if not symbol and not isin and action != "IGNORE":
                raise ValueError("no symbol or isin")
        except (KeyError, ValueError) as e:
            bad.append((k, str(e).strip("'\"")))
            continue
        rows.append({"index": index, "effective_date": day, "action": action, "symbol": symbol, "isin": isin,
                     "note": r.get("note", ""), "row": k})
    return rows, bad


def current_lists(folder):
    """{index: (list date, path)} of the newest nifty_current/<INDEX>_<YYYY-MM-DD>.csv per index."""
    sub = fe.input_dir(folder, "nifty_current")
    out = {}
    for index in INDEXES:
        path = fe.newest(sub, fe.file_key(index), ".csv")
        if path:
            stamp = os.path.basename(path)[len(fe.file_key(index)) + 1:-4]
            out[index] = (date.fromisoformat(stamp), path)
    return out


def read_file(path):
    with open(path, "rb") as f:
        return f.read()


# ---- AMFI ----

def amfi_valid_from(end, cal, lag):
    """First trading day of the month after the half year ending on end, plus lag trading days."""
    first = date(end.year + (end.month == 12), 1 if end.month == 12 else end.month + 1, 1)
    return cal.at(cal.pos(first) + lag)


def build_amfi(folder, cal, master, lag, review, log=print):
    """(frame, periods): every AMFI list with valid_from / valid_to and security_id; periods = per-period summary."""
    sub = fe.input_dir(folder, "amfi")
    files = sorted(n for n in (os.listdir(sub) if os.path.isdir(sub) else []) if n.lower().endswith(".xlsx"))
    by_end = {}
    for n in files:
        try:
            end, rows, how = nt.read_amfi(read_file(os.path.join(sub, n)), n)
        except nt.NoticeError as e:
            review.append(row("amfi_count", "", "", "", "", f"{n}: not read: {e}"))
            continue
        if end in by_end:
            review.append(row("amfi_count", "", str(end), "", "",
                              f"two files for the half year ending {end}: {by_end[end][0]} and {n}; {n} not used"))
            continue
        by_end[end] = (n, rows, how)
    ends = sorted(by_end)
    starts = {e: amfi_valid_from(e, cal, lag) for e in ends}
    frames, periods = [], []
    for k, end in enumerate(ends):
        n, rows, how = by_end[end]
        label = nt.period_label(end)
        vf = starts[end]
        vt = cal.before(starts[ends[k + 1]]) if k + 1 < len(ends) else None
        matched, unmatched, how_matched = 0, 0, {"isin": 0, "symbol": 0}
        seen = {}
        out = []
        for r in rows:
            sid = master.by_isin(r["isin"])
            if sid:
                how_matched["isin"] += 1
            elif r["symbol"]:
                sid, _ = master.by_symbol(r["symbol"], [vf, end])
                if sid:
                    how_matched["symbol"] += 1
            if sid:
                matched += 1
                if sid in seen:
                    review.append(row("amfi_duplicate", "", str(vf), sid, r["symbol"],
                                      f"{label}: ranks {seen[sid]} and {r['rank']} map to the same security"))
                seen.setdefault(sid, r["rank"])
            else:
                unmatched += 1
                if r["rank"] <= 500:
                    review.append(row("amfi_unmatched", "", str(vf), "", r["symbol"],
                                      f"{label} rank {r['rank']} {r['company']} ISIN {r['isin'] or '-'} "
                                      f"BSE {r['bse_symbol'] or '-'}: no security with this ISIN or NSE symbol"))
            out.append({"period": label, "period_end": end, "valid_from": vf, "valid_to": vt, "rank": r["rank"],
                        "category": r["category"], "security_id": sid or "", "isin": r["isin"], "symbol": r["symbol"],
                        "company": r["company"], "avg_mcap": r["avg_mcap"], "file": n})
        cats = pd.Series([r["category"] for r in rows]).value_counts()
        large, mid = int(cats.get("LARGE", 0)), int(cats.get("MID", 0))
        if large != 100 or mid != 150:
            review.append(row("amfi_count", "", str(vf), "", "", f"{label}: {large} LARGE and {mid} MID (expected 100 / 150)"))
        top = [r for r in rows if r["rank"] <= 500]
        top_matched = sum(1 for r in out if r["rank"] <= 500 and r["security_id"])
        periods.append({"period": label, "period_end": str(end), "valid_from": str(vf),
                        "valid_to": str(vt) if vt else "", "file": n, "category_from": how, "rows": len(rows),
                        "large": large, "mid": mid, "small": int(cats.get("SMALL", 0)), "matched": matched,
                        "unmatched": unmatched, "by_isin": how_matched["isin"], "by_symbol": how_matched["symbol"],
                        "top500": len(top), "top500_matched": top_matched})
        frames.append(pd.DataFrame(out, columns=AMFI_COLUMNS))
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=AMFI_COLUMNS)
    log(f"AMFI: {len(periods)} half years, {len(frame):,} rows")
    return frame, periods


# ---- changes ----

def row(kind, index, day, sid, symbol, details):
    return {"kind": kind, "index": index, "date": str(day) if day else "", "security_id": sid or "",
            "symbol": symbol or "", "details": details}


def assemble_changes(parsed, manual, bad_manual, master, cal, review, replacing=()):
    """One list of change dicts (parsed + manual) with status, security_id and note. replacing: files whose lists
    replace the earlier notices' lists of the same index and effective date."""
    out = []
    seen = set()
    for c in parsed:
        key = (c["index"], c["effective_date"], c["action"], c["symbol"], c["file"])
        if key in seen:
            continue
        seen.add(key)
        out.append({**c, "isin": "", "security_id": "", "source": "file", "note": "", "status": "applied"})
    # superseded: the same index + symbol + action announced again before the first one's effective date
    groups = {}
    for c in out:
        groups.setdefault((c["index"], c["symbol"], c["action"]), []).append(c)
    for rows in groups.values():
        rows.sort(key=lambda c: (c["announced_date"], c["file"]))
        for a, b in zip(rows, rows[1:]):
            if a["status"] == "applied" and a["announced_date"] < b["announced_date"] < a["effective_date"] \
                    and a["file"] != b["file"]:
                a["status"] = "superseded"
                a["note"] = f"superseded by {b['file']} (effective {b['effective_date']})"
    # a notice saying the earlier list "stands replaced": earlier rows of its indices and dates are superseded
    for f in replacing:
        mine = [c for c in out if c["file"] == f]
        keys = {(c["index"], c["effective_date"]) for c in mine}
        when = min((c["announced_date"] for c in mine), default=None)
        for c in out:
            if c["file"] != f and (c["index"], c["effective_date"]) in keys and c["announced_date"] < when                     and c["status"] == "applied":
                c["status"] = "superseded"
                c["note"] = f"list replaced by {f}"
    for k, e in bad_manual:
        review.append(row("conflict", "", "", "", "", f"manual_changes.csv row {k} not used: {e}"))
    for m in manual:
        same = [c for c in out if c["source"] == "file" and c["index"] == m["index"]
                and c["effective_date"] == m["effective_date"]
                and (c["symbol"] == m["symbol"] or (m["action"] == "IGNORE" and not m["symbol"] and not m["isin"]))]
        if m["action"] == "IGNORE":
            hit = [c for c in same if c["status"] in ("applied", "superseded")]
            if not hit:
                review.append(row("conflict", m["index"], m["effective_date"], "", m["symbol"],
                                  f"manual_changes.csv row {m['row']}: IGNORE matches no parsed row"))
            for c in hit:
                c["status"] = "ignored"
                c["note"] = f"IGNORE in manual_changes.csv row {m['row']}" + (f": {m['note']}" if m["note"] else "")
            continue
        opposite = [c for c in same if c["action"] != m["action"] and c["status"] == "applied"]
        for c in opposite:
            c["status"] = "ignored"
            c["note"] = f"overridden by manual_changes.csv row {m['row']}"
            review.append(row("conflict", m["index"], m["effective_date"], "", m["symbol"],
                              f"manual {m['action']} (row {m['row']}) contradicts {c['action']} in {c['file']}; "
                              "the manual row is used"))
        dup = [c for c in same if c["action"] == m["action"] and c["status"] == "applied"]
        out.append({"index": m["index"], "effective_date": m["effective_date"], "action": m["action"],
                    "symbol": m["symbol"], "company": "", "announced_date": None, "file": "manual_changes.csv",
                    "isin": m["isin"], "security_id": "", "source": "manual",
                    "note": (m["note"] + ("; " if m["note"] and dup else "")
                             + (f"same as {dup[0]['file']}" if dup else "")),
                    "status": "duplicate" if dup else "applied"})
    for c in out:
        if c["status"] not in ("applied", "superseded"):
            continue
        sid = master.by_isin(c["isin"]) if c["isin"] else None
        if not sid:
            days = [c["announced_date"], cal.before(c["effective_date"]), c["effective_date"]]
            sid, why = master.by_symbol(c["symbol"], days)
        if sid:
            c["security_id"] = sid
            if not c["isin"]:
                c["isin"] = master.name_on(sid, c["effective_date"])[1]
        elif c["status"] == "applied":
            c["status"] = "unmapped"
            c["note"] = (c["note"] + "; " if c["note"] else "") + why
            review.append(row("unmapped_change", c["index"], c["effective_date"], "", c["symbol"],
                              f"{c['action']} in {c['file']} ({c['company'] or 'no name'}): {why}"))
    out.sort(key=lambda c: (c["effective_date"], c["index"], c["action"], c["symbol"]))
    return out


def map_current(index, rows, list_date, master, review):
    """({security_id: symbol}, [unmapped rows]) of a constituent list."""
    sids, unmapped = {}, []
    for r in rows:
        sid = master.by_isin(r["isin"])
        if not sid:
            sid, why = master.by_symbol(r["symbol"], [list_date])
        if not sid:
            review.append(row("unmapped_current", index, list_date, "", r["symbol"],
                              f"{r['company']} ISIN {r['isin']}: {why}"))
            unmapped.append(r)
            continue
        sids[sid] = r["symbol"]
    return sids, unmapped


def walk(index, current, list_date, start, changes, cal, review):
    """Membership periods [(security_id, from, to or None, start source, end source)] from the current list walked
    back through the applied changes effective after start and up to list_date."""
    by_day = {}
    for c in changes:
        if c["index"] == index and c["status"] == "applied" and start < c["effective_date"] <= list_date:
            by_day.setdefault(c["effective_date"], []).append(c)
    state = set(current)
    end = {sid: (None, f"current list {list_date}") for sid in current}
    periods = []
    for day in sorted(by_day, reverse=True):
        rows = by_day[day]
        for c in (x for x in rows if x["action"] == "IN"):
            sid = c["security_id"]
            if sid in state:
                state.remove(sid)
                periods.append((sid, day, end[sid][0], c["file"], end[sid][1]))
                c["walk"] = "undone"
            else:
                c["walk"] = "skipped"
                review.append(row("undo_mismatch", index, day, sid, c["symbol"],
                                  f"IN in {c['file']} but not a member from that date (rebuilt back from the current "
                                  "list); not applied"))
        for c in (x for x in rows if x["action"] == "OUT"):
            sid = c["security_id"]
            if sid not in state:
                state.add(sid)
                end[sid] = (cal.before(day), c["file"])
                c["walk"] = "undone"
            else:
                c["walk"] = "skipped"
                review.append(row("undo_mismatch", index, day, sid, c["symbol"],
                                  f"OUT in {c['file']} but still a member after that date; not applied"))
    for sid in state:
        periods.append((sid, start, end[sid][0], "coverage start", end[sid][1]))
    return [p for p in periods if p[2] is None or p[1] <= p[2]]


# ---- checks ----

def daily_sets(periods, cal, first, last):
    """[set of members] per trading day in [first, last] (the calendar's days)."""
    days = cal.between(first, last)
    pos0 = cal.index[days[0]] if days else 0
    adds, drops = {}, {}
    for sid, f, t in periods:
        f = max(f, first)
        t = min(t, last) if t else last
        if f > t:
            continue
        a = bisect_left(cal.days, f) - pos0
        b = bisect_right(cal.days, t) - 1 - pos0
        adds.setdefault(a, []).append(sid)
        drops.setdefault(b + 1, []).append(sid)
    out, cur = [], set()
    for i in range(len(days)):
        for sid in drops.get(i, ()):
            cur.discard(sid)
        for sid in adds.get(i, ()):
            cur.add(sid)
        out.append(set(cur))
    return days, out


def ranges(days, flags, values=None):
    """[(first, last, value)] runs of consecutive days where flags is true (value constant within a run)."""
    out = []
    for i, d in enumerate(days):
        if not flags[i]:
            continue
        v = values[i] if values is not None else None
        if out and out[-1][3] == i - 1 and out[-1][2] == v:
            out[-1] = (out[-1][0], d, v, i)
        else:
            out.append((d, d, v, i))
    return [(a, b, v) for a, b, v, _ in out]


def expected_sizes(index, days, sets, extra_class, reits):
    """The size each day should have (EXPECTED_RULES): SIZES + members of an additional share class - REIT / InvIT
    rows included by then. extra_class: security_ids; reits: [inclusion date]."""
    return [SIZES[index] + len(s & extra_class) - sum(1 for d0 in reits if d0 <= d) for d, s in zip(days, sets)]


def size_check(index, days, sets, review, expected=None):
    """Raw (count = size) and adjusted (count = expected size) exact days. Days off the expected size go to review
    (count_mismatch); "ok" (per day, adjusted) is used for reliable_from."""
    counts = [len(s) for s in sets]
    expected = expected or [SIZES[index]] * len(days)
    raw_bad = [c != SIZES[index] for c in counts]
    bad = [c != e for c, e in zip(counts, expected)]
    notes = [f"{c} members" + (f" (expected {e})" if e != SIZES[index] else "") for c, e in zip(counts, expected)]
    rs = ranges(days, bad, notes)
    for a, b, v in rs:
        review.append(row("count_mismatch", index, a, "", "", f"{v} (size {SIZES[index]}) from {a} to {b}"))
    ok, raw_ok = len(days) - sum(bad), len(days) - sum(raw_bad)
    return {"index": index, "days": len(days), "exact": raw_ok, "share": raw_ok / len(days) if days else 0.0,
            "exact_adjusted": ok, "share_adjusted": ok / len(days) if days else 0.0,
            "ranges": [(str(a), str(b), v) for a, b, v in rs], "ok": [not x for x in bad]}


def reliable_from(days, ok, snaps, share=RELIABLE_SHARE):
    """(day or None, why): the earliest day d after the latest compared snapshot with a real mismatch (timing
    differences allowed) such that the index is at its expected size on at least share of the days from d on."""
    bad_snaps = [as_date(x["day"]) for x in snaps if x.get("status") == "compared" and x["mismatch"]]
    after = max(bad_snaps) if bad_snaps else None
    n = len(days)
    suffix = [0] * (n + 1)
    for i in range(n - 1, -1, -1):
        suffix[i] = suffix[i + 1] + (1 if ok[i] else 0)
    for i in range(n):
        if after is not None and days[i] <= after:
            continue
        if suffix[i] >= share * (n - i):
            lead = f"after the snapshot mismatch on {after}; " if after else ""
            return days[i], f"{lead}{suffix[i] / (n - i):.1%} of {n - i:,} days at the expected size from then"
    lead = f" after the snapshot mismatch on {after}" if after else ""
    return None, f"no day{lead} from which {share:.0%} of the days are at the expected size"


def composite_check(name, whole, parts, first, daysets, master):
    common = None
    for k in [whole] + parts:
        ds = set(daysets[k][0]) if k in daysets else set()
        common = ds if common is None else common & ds
    days = sorted(d for d in (common or []) if first is None or d >= first)
    if not days:
        return {"name": name, "from": str(first or ""), "days": 0, "holds": 0, "share": 0.0, "ranges": []}
    look = {k: dict(zip(*daysets[k])) for k in [whole] + parts}
    flags, notes = [], []
    for d in days:
        union, overlap = set(), False
        for p in parts:
            s = look[p][d]
            overlap |= bool(union & s)
            union |= s
        ok = (union == look[whole][d]) and not overlap
        flags.append(not ok)
        if ok:
            notes.append(None)
        else:
            extra = sorted(look[whole][d] - union)
            missing = sorted(union - look[whole][d])
            notes.append(("in whole only: " + ", ".join(master.name_on(s, d)[0] for s in extra[:5]) if extra else "")
                         + ("; " if extra and missing else "")
                         + ("in parts only: " + ", ".join(master.name_on(s, d)[0] for s in missing[:5]) if missing else "")
                         + ("; overlap" if overlap else ""))
    rs = ranges(days, flags, notes)
    holds = len(days) - sum(flags)
    return {"name": name, "from": str(first or days[0]), "days": len(days), "holds": holds, "share": holds / len(days),
            "ranges": [(str(a), str(b), v) for a, b, v in rs]}


def not_trading_check(index, periods, positions, cal, cover_last, master, review):
    found = 0
    last_pos = cal.pos(cover_last) if cover_last in cal.index else bisect_right(cal.days, cover_last) - 1
    for sid, f, t, _, _ in periods:
        a = cal.pos(f)
        b = min(cal.pos(t) if t else last_pos, last_pos, len(cal.days) - 1)
        if a > b:
            continue
        traded = positions.get(sid, np.array([], dtype=int))
        inside = traded[(traded >= a) & (traded <= b)]
        marks = np.concatenate([[a - 1], inside, [b + 1]])
        gaps = np.diff(marks) - 1
        for k in np.nonzero(gaps > NOT_TRADING_DAYS)[0]:
            g0, g1 = cal.days[int(marks[k]) + 1], cal.days[int(marks[k + 1]) - 1]
            review.append(row("member_not_trading", index, g0, sid, master.name_on(sid, g0)[0],
                              f"a member from {f} to {t or 'now'} with no bhavcopy row for {int(gaps[k])} trading days "
                              f"({g0} to {g1})"))
            found += 1
    return found


def snapshot_check(folder, membership, changes, cal, coverage, master, review):
    """Compare the rebuilt members with each archived constituent list (a check only)."""
    sub = fe.input_dir(folder, "snapshots")
    results = []
    if not os.path.isdir(sub):
        return results
    for name in sorted(os.listdir(sub)):
        m = re.fullmatch(r"(NIFTY_[A-Z0-9_]+)_(\d{14})\.csv", name)
        if not m:
            continue
        index = m.group(1).replace("_", " ")
        if index not in SIZES:
            continue
        ts = datetime.strptime(m.group(2), "%Y%m%d%H%M%S") + timedelta(hours=5, minutes=30)   # UTC -> IST
        day = cal.on_or_before(ts.date())
        first, last = coverage[index]
        res = {"index": index, "file": name, "captured": f"{ts:%Y-%m-%d %H:%M} IST", "day": str(day) if day else ""}
        if day is None or first is None or not (first <= day <= last):
            res["status"] = "outside coverage"
            results.append(res)
            continue
        try:
            rows = nt.read_constituents(read_file(os.path.join(sub, name)))
        except (OSError, ValueError) as e:
            res["status"] = f"not read: {e}"
            results.append(res)
            continue
        snap, unmapped = set(), 0
        for r in rows:
            sid = master.by_isin(r["isin"]) or master.by_symbol(r["symbol"], [day])[0]
            if sid:
                snap.add(sid)
            else:
                unmapped += 1
        mem = membership[membership["index"] == index]
        ours = set(mem[(mem["from_date"] <= day) & (until(mem["to_date"]) >= day)]["security_id"])
        timing, real = [], []
        near = {}
        for c in changes:
            if c["index"] == index and c["status"] == "applied" and c["security_id"]:
                near.setdefault(c["security_id"], []).append(c["effective_date"])
        for sid in sorted(ours ^ snap):
            close = any(abs(cal.distance(d, day)) <= SNAPSHOT_TIMING for d in near.get(sid, []))
            side = "rebuilt only" if sid in ours else "snapshot only"
            (timing if close else real).append((sid, side))
            if not close:
                review.append(row("snapshot_mismatch", index, day, sid, master.name_on(sid, day)[0],
                                  f"{side} vs {name} (captured {res['captured']})"))
        res.update({"status": "compared", "rows": len(rows), "unmapped": unmapped, "same": len(ours & snap),
                    "timing": len(timing), "mismatch": len(real),
                    "examples": [f"{master.name_on(s, day)[0]} ({side})" for s, side in real[:5]]})
        results.append(res)
    return results


# ---- known cases ----

def member_periods(membership, index, sid):
    m = membership[(membership["index"] == index) & (membership["security_id"] == sid)]
    return sorted((r.from_date, r.to_date if pd.notna(r.to_date) else None) for r in m.itertuples())


def known_checks(membership, changes, amfi, info, master_lookup):
    """(lines, ok). master_lookup(symbol, day) -> security_id."""
    lines, ok = [], True
    cover = {k: (as_date(v["first"]), as_date(v["last"])) for k, v in info["coverage"].items() if v["first"]}

    def mark(found, text):
        nonlocal ok
        ok &= bool(found)
        lines.append(f"  [{'found' if found else 'NOT FOUND'}] {text}")

    lines.append("Always members:")
    for sym, index in KNOWN_ALWAYS:
        if index not in cover:
            mark(False, f"{sym} in {index}: {index} not covered")
            continue
        first, last = cover[index]
        sid = master_lookup(sym, last)
        ps = member_periods(membership, index, sid) if sid else []
        whole = len(ps) == 1 and ps[0][0] <= first and (ps[0][1] is None or ps[0][1] >= last)
        mark(whole, f"{sym} ({sid}) in {index} from {first} to {last}: periods "
             + (", ".join(f"{a} to {b or 'now'}" for a, b in ps) or "none"))
    lines.append("AMFI LARGE in every half year:")
    periods = sorted(amfi["period"].unique()) if not amfi.empty else []
    for sym in KNOWN_LARGE:
        sid = master_lookup(sym, cover.get("NIFTY 50", (None, None))[1] or date.today())
        mine = amfi[amfi["security_id"] == sid]
        cats = dict(zip(mine["period"], mine["category"]))
        large = [p for p in periods if cats.get(p) == "LARGE"]
        mark(len(large) == len(periods) and periods,
             f"{sym}: LARGE in {len(large)} of {len(periods)} half years"
             + ("" if len(large) == len(periods) else f" (not in {', '.join(p for p in periods if p not in large)})"))
    k = KNOWN_HDFC
    lines.append(f"{k['out']} leaves {k['index']} on its merger, {k['in']} replaces it:")
    out_sid = master_lookup(k["out"], as_date(k["last_day"]))
    ps = member_periods(membership, k["index"], out_sid) if out_sid else []
    ended = any(b is not None and str(b) == k["last_day"] for _, b in ps)
    mark(ended, f"{k['out']} ({out_sid}) in {k['index']} until {k['last_day']}: periods "
         + (", ".join(f"{a} to {b or 'now'}" for a, b in ps) or "none"))
    rep = [c for c in changes if c["index"] == k["index"] and c["action"] == "IN" and c["status"] == "applied"
           and str(c["effective_date"]) == k["in_from"]]
    names = [f"{c['symbol']} ({c['file']})" for c in rep]
    in_sid = master_lookup(k["in"], as_date(k["in_from"]))
    started = in_sid and any(str(a) == k["in_from"] for a, _ in member_periods(membership, k["index"], in_sid))
    mark(started and any(c["symbol"] == k["in"] for c in rep),
         f"included in {k['index']} from {k['in_from']}: {', '.join(names) or 'none'} (expected {k['in']})")
    lines.append(f"AMFI half years: {len(periods)} (expected {KNOWN_AMFI_PERIODS}, H2-2017 to H1-2026)")
    mark(len(periods) >= KNOWN_AMFI_PERIODS, f"{len(periods)} half years")
    for p in info.get("amfi_periods", []):
        good = p["large"] == 100 and p["mid"] == 150
        mark(good, f"{p['period']}: valid from {p['valid_from']}, {p['large']} LARGE / {p['mid']} MID / "
             f"{p['small']} SMALL ({p['category_from']})")
    lines.append("NIFTY 50 March and September reviews per year:")
    reviews = info.get("reviews", {})
    for year in sorted(reviews):
        r = reviews[year]
        for half in ("March", "September"):
            got = r.get(half)
            if got is None and year == max(reviews) and half == "September" and \
                    date.today() < date(int(year), 9, 30):
                continue
            mark(got, f"{year} {half}: " + (f"{got['file']} effective {got['effective']}, NIFTY 50 "
                                            f"{'changed' if got['nifty50'] else 'unchanged'}" if got else "no review notice"))
    lines.append("A stock from outside NIFTY 100 / AMFI MID entering NIFTY 50 at a review:")
    pick = info.get("midcap_entry")
    mark(pick, pick or "none found")
    return lines, ok


# ---- report ----

def report_text(info, checks, sizes, comps, snaps, review, counts, changes_per_year, amfi_periods, problems,
                not_applied):
    L = ["Index membership build", f"Built: {info['built_at']}",
         f"Data: bhavcopy {info['data_first']} to {info['data_last']}; AMFI lag {info['amfi_lag']} trading days", "",
         "Sources:"]
    for k, v in info["inputs"].items():
        L.append(f"  {k}: {v}")
    L += ["", "Coverage (reliable from the sources):"]
    for index in INDEXES:
        c = info["coverage"][index]
        L.append(f"  {index:20} {c['first'] or '-'} to {c['last'] or '-'}  (current list {c['list'] or 'none'}; "
                 f"{c['why']})")
    L += ["", "Members per index on the last trading day of each year:"]
    years = sorted({y for v in counts.values() for y in v})
    L.append("  " + "index".ljust(20) + "".join(f"{y:>6}" for y in years))
    for index in INDEXES:
        L.append("  " + index.ljust(20) + "".join(f"{counts[index].get(y, ''):>6}" for y in years))
    L += ["", "Applied changes per index per year (IN/OUT); reviews found (M = March, S = September):"]
    years = sorted({y for v in changes_per_year.values() for y in v})
    L.append("  " + "index".ljust(20) + "".join(f"{y:>9}" for y in years))
    for index in INDEXES:
        L.append("  " + index.ljust(20) + "".join(f"{changes_per_year[index].get(y, ''):>9}" for y in years))
    rv = info.get("reviews", {})
    L.append("  reviews".ljust(22) + "".join(f"{('M' if rv.get(str(y), {}).get('March') else '-') + ('S' if rv.get(str(y), {}).get('September') else '-'):>9}" for y in years))
    st = pd.Series([c["status"] for c in info["_changes"]]).value_counts()
    L.append("  change rows by status: " + ", ".join(f"{k} {int(v):,}" for k, v in st.items()))
    if info.get("pending"):
        L.append("  pending (effective after the current list): " + "; ".join(info["pending"]))
    L += ["", "Size check (members on each trading day of the coverage; raw = the index size, adjusted = the "
          "expected size):"]
    for s in sizes:
        L.append(f"  {s['index']:20} raw {s['exact']:,} of {s['days']:,} days ({s['share']:.1%}), adjusted "
                 f"{s['exact_adjusted']:,} ({s['share_adjusted']:.1%})"
                 + (f"; {len(s['ranges'])} ranges off, first: " + "; ".join(f"{a}..{b}: {v}" for a, b, v in s['ranges'][:3])
                    if s["ranges"] else ""))
    L += ["", f"Reliable from (at the expected size on at least {RELIABLE_SHARE:.0%} of the days from then to the end, "
          "and every snapshot from then on matching, timing differences allowed):"]
    for index in INDEXES:
        c = info["coverage"][index]
        L.append(f"  {index:20} {c.get('reliable_from') or 'not reliable':12}  {c.get('reliable_why', '')}"
                 + (f"; snapshots from then: {c['snapshots_after']}" if c.get("snapshots_after") else ""))
    L.append("  expected deviations:")
    L += [f"    - {x}" for x in EXPECTED_RULES]
    L += ["", "Composite checks (days where every index in them is covered):"]
    for c in comps:
        L.append(f"  {c['name']} (from {c['from']}): holds on {c['holds']:,} of {c['days']:,} days ({c['share']:.1%})")
        for a, b, v in c["ranges"][:4]:
            L.append(f"      {a}..{b}: {v}")
    L += ["", "Snapshots (Wayback copies of the constituent lists, a check only):"]
    compared = [s for s in snaps if s.get("status") == "compared"]
    L.append(f"  {len(snaps)} files, {len(compared)} inside the coverage")
    for s in compared:
        L.append(f"  {s['index']:20} {s['day']} ({s['file']}): same {s['same']}, timing {s['timing']}, "
                 f"mismatch {s['mismatch']}" + (f" ({', '.join(s['examples'])})" if s["examples"] else "")
                 + (f", {s['unmapped']} rows unmapped" if s["unmapped"] else ""))
    L += ["", "AMFI half years:"]
    L.append("  period  valid_from  valid_to    rows   LARGE MID  SMALL  matched  top500 matched  category from")
    for p in amfi_periods:
        L.append(f"  {p['period']}  {p['valid_from']}  {p['valid_to'] or 'open':10}  {p['rows']:>5}  {p['large']:>4} "
                 f"{p['mid']:>4} {p['small']:>5}  {p['matched'] / max(p['rows'], 1):>6.1%}  "
                 f"{p['top500_matched']:>3}/{p['top500']:<3} ({p['top500_matched'] / max(p['top500'], 1):.1%})  {p['category_from']}")
    L += ["", f"Notices: {info['notices']['files']} PDFs, {info['notices']['with_changes']} with changes to these "
          f"indices, {info['notices']['reviews']} periodic reviews"]
    for f, e in problems:
        L.append(f"  not read: {f}: {e}")
    if not_applied:
        L.append(f"  demerger / corporate-adjustment notices naming these indices (not applied): {len(not_applied)}")
        for x in not_applied[:12]:
            L.append(f"    {x}")
    L += ["", "Known-case checks:"] + checks[0]
    L.append(f"Known-case checks: {'all found' if checks[1] else 'SOME NOT FOUND'}")
    kinds = pd.Series([r["kind"] for r in review]).value_counts()
    L += ["", f"Review rows: {len(review):,}" + "".join(f" ({k}: {int(kinds.get(k, 0)):,})" for k in REVIEW_KINDS
                                                      if kinds.get(k, 0))]
    for r in review[:REVIEW_EXAMPLES]:
        L.append(f"  {r['kind']} | {r['index']} | {r['date']} | {r['security_id']} | {r['symbol']} | {r['details']}")
    return "\n".join(L) + "\n"


# ---- build ----

def reviews_per_year(notices, changes):
    """{year: {"March"/"September": {file, effective, nifty50}}}: periodic-review notices of the broad indices."""
    n50 = {c["file"] for c in changes if c["index"] == "NIFTY 50" and c["source"] == "file"}
    out = {}
    for n in notices:
        if not n["review"] or not n["dates"] or not (set(n["indexes"]) & set(INDEXES)):
            continue
        d = as_date(n["dates"][0])
        half = "March" if d.month in (3, 4) else "September" if d.month in (9, 10) else None
        if half is None:
            continue
        cur = out.setdefault(str(d.year), {}).get(half)
        if cur is None or (n["file"] in n50 and not cur["nifty50"]):
            out[str(d.year)][half] = {"file": n["file"], "effective": str(d), "nifty50": n["file"] in n50}
    return out


def pick_midcap_entry(changes, membership, amfi):
    """A NIFTY 50 inclusion at a review of a stock not in NIFTY 100 the day before (else AMFI MID then)."""
    m100 = membership[membership["index"] == "NIFTY 100"]
    best = None
    for c in changes:
        if c["index"] != "NIFTY 50" or c["action"] != "IN" or c["status"] != "applied" or not c["security_id"]:
            continue
        d = c["effective_date"]
        before = d - timedelta(days=1)
        mine = m100[m100["security_id"] == c["security_id"]]
        in100 = ((mine["from_date"] <= before) & (until(mine["to_date"]) >= before)).any()
        cat = ""
        a = amfi[(amfi["security_id"] == c["security_id"]) & (amfi["valid_from"] <= d)
                 & (until(amfi["valid_to"]) >= d)]
        if not a.empty:
            cat = a.sort_values("rank").iloc[0]["category"]
        if not in100 or cat == "MID":
            best = (f"{c['symbol']} ({c['security_id']}) into NIFTY 50 from {d} ({c['file']}): "
                    f"{'not in NIFTY 100' if not in100 else 'in NIFTY 100'} the day before, AMFI {cat or '-'} then")
    return best


def build(folder=None, amfi_lag=AMFI_LAG, now=None, log=print):
    """Full rebuild of <folder>/index_membership/ (offline). Refuses while the bhavcopy or security master lock is
    held (StoreBusy), without a master (NotBuilt), with a master older than the bhavcopy (InputStale) or while
    another index_membership run holds its lock (LockBusy)."""
    folder = folder or equity_data_dir()
    check_inputs(folder)
    out = out_dir(folder)
    os.makedirs(out, exist_ok=True)
    lock = RunLock(os.path.join(out, ".lock")).acquire("index_membership.py build")
    try:
        return _build(folder, out, amfi_lag, now, log)
    finally:
        lock.release()


def _build(folder, out, amfi_lag, now, log):
    cal = read_calendar(folder)
    master = Master(folder)
    review = []
    amfi, amfi_periods = build_amfi(folder, cal, master, amfi_lag, review, log)
    parsed, notices, problems = read_notices(folder, log)
    manual, bad_manual = read_manual(folder)
    for n in notices:
        for idx in n["unresolved"]:
            review.append(row("not_parsed", idx, n["announced_date"], "", "",
                              f"{n['file']}: the date for {idx} is not clear (effective dates {', '.join(n['dates'])}); "
                              "add its changes to manual_changes.csv"))
        if not n["dates"] and n["indexes"]:
            review.append(row("not_parsed", ", ".join(n["indexes"]), n["announced_date"], "", "",
                              f"{n['file']}: no effective date found"))
    not_applied = []
    for n in notices:
        for x in n["not_applied"]:
            not_applied.append(f"{n['file']} ({n['title'] or 'no title'}): {x['index']} from {x['effective_date']}")
            review.append(row("not_applied", x["index"], x["effective_date"], "", "",
                              f"{n['file']}: {n['title'] or 'index-name table'}; a demerger / corporate adjustment "
                              "notice is not applied (add manual rows if wanted)"))
    for f, e in problems:
        review.append(row("not_parsed", "", fe.notice_date(f) or "", "", "", f"{f}: {e}"))
    for n in notices:
        if n["no_text"]:
            review.append(row("not_parsed", "", n["announced_date"], "", "",
                              f"{n['file']} ({n['title'] or 'no title'}): no text layer (image PDF); add its changes to "
                              "manual_changes.csv"))
    changes = assemble_changes(parsed, manual, bad_manual, master, cal, review,
                               [n["file"] for n in notices if n["replaces"]])

    lists = current_lists(folder)
    coverage, periods_all, rows_out, pending, reit_rows = {}, {}, [], [], {}
    for index in INDEXES:
        if index not in lists:
            coverage[index] = {"first": None, "last": None, "list": None, "why": "no current list"}
            continue
        list_date, path = lists[index]
        if index in INDEX_START:
            start = cal.at(cal.pos(max(cal.first, INDEX_START[index])))
            why = f"first published {INDEX_START[index]}"
        else:
            start, why = cal.first, "from the data start"
        last = cal.on_or_before(list_date)
        cur, unmapped = map_current(index, nt.read_constituents(read_file(path)), list_date, master, review)
        reit_rows[index] = [r for r in unmapped if REIT_RX.search(r["company"])]
        for c in changes:
            if c["index"] == index and c["status"] == "applied" and c["effective_date"] > list_date:
                c["status"] = "pending"
                pending.append(f"{index} {c['action']} {c['symbol']} {c['effective_date']}")
        periods = walk(index, cur, list_date, start, changes, cal, review)
        periods_all[index] = periods
        coverage[index] = {"first": str(start), "last": str(last), "list": str(list_date), "why": why}
        for sid, f, t, s0, s1 in periods:
            sym, isin = master.name_on(sid, t or last)
            rows_out.append({"index": index, "security_id": sid, "symbol": sym, "isin": isin, "from_date": f,
                             "to_date": t, "source": f"{s0} -> {s1}"})
    for c in changes:
        if c["status"] == "applied" and c.get("walk") == "skipped":
            c["status"] = "skipped"
        elif c["status"] == "applied" and c["source"] == "file" and c.get("walk") is None and c["index"] in coverage \
                and coverage[c["index"]]["first"] and c["effective_date"] <= as_date(coverage[c["index"]]["first"]):
            c["status"] = "before coverage"
    membership = pd.DataFrame(rows_out, columns=MEMBERSHIP_COLUMNS).sort_values(
        ["index", "from_date", "security_id"], kind="stable").reset_index(drop=True)

    log("Checks")
    sizes, daysets = [], {}
    extra_class = {sid for sid, rows in master.periods.items() if any(ADDITIONAL_CLASS.search(r[2]) for r in rows)}
    for index, periods in periods_all.items():
        c = coverage[index]
        days, sets = daily_sets([(p[0], p[1], p[2]) for p in periods], cal, as_date(c["first"]), as_date(c["last"]))
        daysets[index] = (days, sets)
        reits = []
        for r in reit_rows.get(index, []):
            ins = [x["effective_date"] for x in changes if x["index"] == index and x["symbol"] == r["symbol"]
                   and x["action"] == "IN" and x["status"] == "unmapped"]
            reits.append(max(ins) if ins else as_date(c["first"]))
        sizes.append(size_check(index, days, sets, review, expected_sizes(index, days, sets, extra_class, reits)))
    comps = [composite_check(n, w, p, f, daysets, master) for n, w, p, f in COMPOSITES
             if w in daysets and all(x in daysets for x in p)]
    member_ids = sorted(set(membership["security_id"]))
    positions = trading_positions(folder, {s: master.isins(s) for s in member_ids}, master.m["info"]["series"], cal)
    for index, periods in periods_all.items():
        not_trading_check(index, periods, positions, cal, as_date(coverage[index]["last"]), master, review)
    cov_dates = {k: (as_date(v["first"]) if v["first"] else None, as_date(v["last"]) if v["last"] else None)
                 for k, v in coverage.items()}
    snaps = snapshot_check(folder, membership, changes, cal, cov_dates, master, review)
    for sz in sizes:
        index = sz["index"]
        mine = [x for x in snaps if x["index"] == index]
        day, why = reliable_from(daysets[index][0], sz.pop("ok"), mine)
        later = [x for x in mine if x.get("status") == "compared" and day and as_date(x["day"]) >= day]
        coverage[index].update({"reliable_from": str(day) if day else None, "reliable_why": why,
                                "snapshots_after": f"{sum(1 for x in later if not x['mismatch'])} of {len(later)} match"})
    for c in coverage.values():
        c.setdefault("reliable_from", None)
        c.setdefault("reliable_why", c["why"])

    counts = {index: {} for index in INDEXES}
    for index, (days, sets) in daysets.items():
        for d, s in zip(days, sets):
            counts[index][d.year] = len(s)
    per_year = {index: {} for index in INDEXES}
    for c in changes:
        if c["status"] == "applied" and c.get("walk") == "undone":
            y = c["effective_date"].year
            i_, o_ = per_year[c["index"]].get(y, (0, 0))
            per_year[c["index"]][y] = (i_ + (c["action"] == "IN"), o_ + (c["action"] == "OUT"))
    per_year = {k: {y: f"{a}/{b}" for y, (a, b) in v.items()} for k, v in per_year.items()}

    info = {"built_at": f"{(now or sm.now_ist()):%Y-%m-%d %H:%M:%S} IST", "amfi_lag": amfi_lag,
            "data_first": str(cal.first), "data_last": str(cal.last), "series": master.m["info"]["series"],
            "coverage": coverage,
            "amfi_coverage": {"first": amfi_periods[0]["valid_from"] if amfi_periods else None,
                              "last": str(cal.last) if amfi_periods else None},
            "amfi_periods": amfi_periods,
            "inputs": {"AMFI": f"{len(amfi_periods)} half-year xlsx in input\\amfi ({fe.AMFI_PAGE})",
                       "current lists": ", ".join(f"{k} {v[0]}" for k, v in lists.items()) or "none",
                       "notices": f"{len(notices)} PDFs in input\\nifty_changes ({fe.PRESS_PAGE})",
                       "manual_changes.csv": f"{len(manual)} rows" + (f", {len(bad_manual)} not used" if bad_manual else ""),
                       "snapshots": f"{len(snaps)} files in input\\snapshots (web.archive.org)"},
            "notices": {"files": len(notices), "with_changes": sum(1 for n in notices if n["changes"]),
                        "reviews": sum(1 for n in notices if n["review"])},
            "reviews": reviews_per_year(notices, changes), "pending": pending,
            "midcap_entry": pick_midcap_entry(changes, membership, amfi),
            "size_checks": [{k: v for k, v in s.items() if k != "ranges"} for s in sizes],
            "reliable_share": RELIABLE_SHARE, "expected_rules": EXPECTED_RULES,
            "composite_checks": [{k: v for k, v in c.items() if k != "ranges"} for c in comps]}
    checks = known_checks(membership, changes, amfi, info, lambda s, d: sm.security_id_for(s, d, folder))
    info["_changes"] = changes
    report = report_text(info, checks, sizes, comps, snaps, review, counts, per_year, amfi_periods, problems,
                         not_applied)
    del info["_changes"]

    ch = pd.DataFrame([{k: c.get(k) for k in CHANGE_COLUMNS} for c in changes], columns=CHANGE_COLUMNS)
    rv = pd.DataFrame(review, columns=REVIEW_COLUMNS)
    sm.write_parquet(membership, os.path.join(out, "nifty_membership.parquet"))
    sm.write_csv(membership, os.path.join(out, "nifty_membership.csv"))
    sm.write_csv(ch, os.path.join(out, "nifty_changes.csv"))
    sm.write_parquet(amfi, os.path.join(out, "amfi.parquet"))
    sm.write_csv(amfi, os.path.join(out, "amfi.csv"))
    sm.write_csv(rv, os.path.join(out, "review.csv"))
    sm.write_csv(pd.DataFrame({"date": cal.days}), os.path.join(out, "calendar.csv"))
    sm.write_text(os.path.join(out, "build_report.txt"), report)
    sm.write_text(os.path.join(out, "build_info.json"), json.dumps(info, indent=2, default=str) + "\n")
    _CACHE.clear()
    return {"membership": len(membership), "changes": len(ch), "amfi": len(amfi), "reviews": len(rv),
            "report": report, "checks_ok": checks[1], "out": out}


# ---- loaders (read the built files) ----

_CACHE = {}


def load_built(folder=None):
    out = out_dir(folder)
    paths = [os.path.join(out, n) for n in ("nifty_membership.parquet", "amfi.parquet", "build_info.json")]
    if not all(os.path.isfile(p) for p in paths):
        raise NotBuilt(f"No index membership in {out}. Run: python index_membership.py build")
    key = (out, tuple(os.path.getmtime(p) for p in paths))
    if key not in _CACHE:
        _CACHE.clear()
        with open(paths[2], encoding="utf-8") as f:
            info = json.load(f)
        _CACHE[key] = {"membership": pq.read_table(paths[0]).to_pandas(), "amfi": pq.read_table(paths[1]).to_pandas(),
                       "info": info}
    return _CACHE[key]


Coverage = namedtuple("Coverage", ["first", "last", "reliable_from"])


def coverage(index, folder=None):
    """Coverage(first, last, reliable_from): the dates the sources cover, and the first date from which the
    membership passed the checks (None: never)."""
    c = load_built(folder)["info"]["coverage"][index_name(index)]
    if not c["first"]:
        raise OutOfCoverage(f"{index_name(index)} is not covered: {c['why']}")
    rel = c.get("reliable_from")
    return Coverage(as_date(c["first"]), as_date(c["last"]), as_date(rel) if rel else None)


def _in_coverage(index, day, folder, allow_unreliable=False):
    first, last, rel = coverage(index, folder)
    if not first <= day <= last:
        raise OutOfCoverage(f"{day} is outside the coverage of {index_name(index)} ({first} to {last})")
    if not allow_unreliable and (rel is None or day < rel):
        raise OutOfCoverage(f"{day} is before {index_name(index)} is reliable "
                            f"({'from ' + str(rel) if rel else 'on no day'}); allow_unreliable=True uses it anyway")


def members(index, on_date, folder=None, allow_unreliable=False):
    """The security_ids in index on on_date (point in time), sorted. A date before the index's reliable_from date
    raises OutOfCoverage unless allow_unreliable."""
    index, day = index_name(index), as_date(on_date)
    _in_coverage(index, day, folder, allow_unreliable)
    m = load_built(folder)["membership"]
    m = m[m["index"] == index]
    hit = m[(m["from_date"] <= day) & (until(m["to_date"]) >= day)]
    return sorted(hit["security_id"])


def is_member(security_id, index, on_date, folder=None, allow_unreliable=False):
    return security_id in members(index, on_date, folder, allow_unreliable)


def _amfi_period(day, folder):
    b = load_built(folder)
    cov = b["info"]["amfi_coverage"]
    if not cov["first"] or not (as_date(cov["first"]) <= day <= as_date(cov["last"])):
        raise OutOfCoverage(f"{day} is outside the AMFI coverage ({cov['first']} to {cov['last']})")
    a = b["amfi"]
    return a[(a["valid_from"] <= day) & (until(a["valid_to"]) >= day)]


def amfi_category(security_id, on_date, folder=None):
    """(LARGE / MID / SMALL or None, period) of the security on on_date (the list valid then)."""
    rows = _amfi_period(as_date(on_date), folder)
    period = rows["period"].iloc[0] if not rows.empty else None
    mine = rows[rows["security_id"] == security_id].sort_values("rank")
    return (mine["category"].iloc[0] if not mine.empty else None), period


def amfi_list(on_date, category=None, folder=None):
    """The AMFI list valid on on_date (every row, or one category), by rank."""
    rows = _amfi_period(as_date(on_date), folder)
    if category:
        rows = rows[rows["category"] == category.upper()]
    return rows.sort_values("rank").reset_index(drop=True)


def check(folder=None):
    """Re-run the known-case checks on the built files: (lines, ok)."""
    b = load_built(folder)
    path = os.path.join(out_dir(folder), "nifty_changes.csv")
    ch = pd.read_csv(path, dtype=str, keep_default_na=False)
    changes = [{**r, "effective_date": as_date(r["effective_date"])} for r in ch.to_dict("records")]
    return known_checks(b["membership"], changes, b["amfi"], b["info"],
                        lambda s, d: sm.security_id_for(s, d, folder))
