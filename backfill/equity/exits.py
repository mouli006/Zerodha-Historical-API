"""Exit list and listing types: who left, when and why; who arrived, and how.

No network, no broker. Only reads the bhavcopy parquet (NSE_CM_bhav/<YYYY>.parquet, every series), the security
master (security_master/), the adjustment factors (adjustments/) and the NSE reference lists
(NSE_CM_bhav/reference/). Output: <folder>/exits/ (written atomically, rebuilt in full):
    exits.csv / .parquet      one row per EXITED security: reason, acquirer, distress hints, suggested treatment
    status.csv / .parquet     every security: ACTIVE / DORMANT / EXITED and its listing kind
    listings.csv              batch-admission days and SME migrations with counts and examples
    review.csv                unknown large exits, conflicting matches, unresolved acquirers, unmatched list rows
    calendar.csv              the trading days of the store (read by status_on)
    build_report.txt          counts, reference headers, known cases, review examples
    build_info.json           parameters and data range of the build
    .lock                     one build at a time (backfill.locks.RunLock)

The suggested treatment is a suggestion only: the backtest decides what an exited holding is worth."""
import json
import os
import re
from bisect import bisect_left
from datetime import timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from backfill.cm_bhav import store_dir
from backfill.equity import adjustments as adj
from backfill.equity import security_master as sm
from backfill.locks import LockBusy, RunLock, held  # noqa: F401 - LockBusy is re-exported for the CLI
from config import equity_data_dir

OUT = "exits"
REFERENCE = "reference"
ACTIVE_DAYS = 20         # ACTIVE: traded within the last 20 trading days (days_since_last < 20)
EXIT_DAYS = 60           # EXITED: last traded more than 60 trading days ago; DORMANT in between
BATCH_MIN = 20           # a day with at least this many STOCK first appearances is a batch admission
START_DAYS = 20          # first seen within the first 20 trading days of the data: IN_DATA_FROM_START
MERGER_WINDOW = 10       # mergers: last_trading_date within +-10 trading days of the last date
DELIST_WINDOW = 60       # delistings: 0 to 60 trading days after the last date
SUSPEND_WINDOW = 10      # suspensions: 0 to 10 trading days after the last date
ANY_LATER = ("LIQUIDATION", "COMPULSORY_DELISTING")   # these match any date after the last date (orders come late)
NEAR_LAST = 60           # symbols used in the last 60 trading days of a security are matched against the lists
DISTRESS_WINDOW = 60     # distress hints over the last 60 trading days
YEAR_DAYS = 250          # ... against the 250 trading days before them (turnover)
DISTRESS_DROP = -50.0    # price change % at or below this is a distress hint
DISTRESS_BE_BZ = 30      # ... as are this many days in BE / BZ
DISTRESS_TURNOVER = 0.25  # ... or a median turnover at most this share of the year before
REVIEW_TOP = 50          # UNKNOWN exits listed in review, largest by median turnover
SME_SERIES = ("SM", "ST")

STATUSES = ["ACTIVE", "DORMANT", "EXITED"]
REASONS = ["MERGER", "VOLUNTARY_DELISTING", "COMPULSORY_DELISTING", "LIQUIDATION", "MOVED_TO_SME", "RELISTED",
           "SUSPENDED", "RIGHTS_ENDED", "ETF_ENDED", "UNKNOWN"]          # in priority order
TREATMENTS = ["CONVERT", "CASH_AT_LAST", "WRITE_DOWN", "REVIEW", "NOT_APPLICABLE"]
LISTING_KINDS = ["IN_DATA_FROM_START", "SME_MIGRATION", "RELISTING", "BATCH_ADMISSION", "NEW_LISTING"]
REVIEW_KINDS = ["unknown_large", "conflict", "unresolved_acquirer", "unmatched_reference"]
# matches from two of these groups for one security are a conflict (SUSPENDED, RIGHTS / ETF fit with anything)
CONFLICT_GROUP = {"MERGER": "merger", "VOLUNTARY_DELISTING": "voluntary", "COMPULSORY_DELISTING": "write_down",
                  "LIQUIDATION": "write_down", "MOVED_TO_SME": "sme", "RELISTED": "relisted"}
LIQUIDATION_RX = re.compile(r"liquidat|winding\s*-?\s*up|wound\s*up|\bIBC\b|NCLT|insolven", re.I)

EXIT_COLUMNS = ["security_id", "last_symbol", "symbols", "isins", "instrument_kind", "first_date", "last_date",
                "last_close", "last_close_adj", "last_series", "reason", "reason_source", "reason_date",
                "reason_distance", "acquirer_security_id", "acquirer_symbol", "swap_ratio", "relisted_as",
                "distress_price_change", "distress_be_bz_days", "distress_turnover_ratio", "suggested_treatment",
                "note"]
STATUS_COLUMNS = ["security_id", "status", "last_date", "days_since_last", "listing_kind", "listing_note"]
LISTING_COLUMNS = ["kind", "date", "count", "examples"]
REVIEW_COLUMNS = ["kind", "security_id", "date", "symbol", "details"]

# The reference lists (nse_reference.py) and the columns this step needs from each.
REFERENCE_FILES = {
    "mergers.csv": ["circular_no", "old_symbol", "scheme_type", "exit_expected", "new_company", "new_symbol",
                    "swap_ratio", "new_per_old", "suspended_from", "last_trading_date", "record_date"],
    "manual_mergers.csv": ["old_symbol", "new_company", "new_symbol", "swap_ratio"],
    "voluntary_delisting.csv": ["list", "symbol", "isin", "board", "delisting_date", "delisting_type"],
    "delisting_circulars.csv": ["circular_no", "circular_date", "symbol", "delisting_type", "effective_date"],
    "compulsory_delisting.csv": ["order_date", "symbol"],
    "suspensions.csv": ["circular_no", "circular_date", "status", "symbol", "company", "effective_date", "reason",
                        "reason_text", "cancels_circular"],
}
CIRCULAR_REASON = {"voluntary": "VOLUNTARY_DELISTING", "compulsory": "COMPULSORY_DELISTING",
                   "liquidation": "LIQUIDATION", "ibc": "LIQUIDATION", "other": None, "on hold": None}

# Known cases in the real data (build_report.txt and `check`); a miss is reported, never fixed by a rule change.
KNOWN_MERGERS = [  # symbol, last date (prefix), acceptable acquirer symbols (then or now), swap ratio, treatment
    ("HDFC", "2023-07-12", ("HDFCBANK",), "42 for 25", "CONVERT"),
    ("MINDTREE", "2022-11", ("LTI", "LTIM"), None, None),
    ("SHRIRAMCIT", None, None, None, None),
    ("TATASTLBSL", None, None, None, None),
    ("GSKCONS", None, None, None, None),
    ("CAIRN", None, None, None, None),
    ("IDFC", None, None, None, None),
]
KNOWN_SME = ["ANNAPURNA", "SWARAJ"]
KNOWN_BATCH_DAYS = {"2026-04-20": 88, "2026-08-17": 126}
KNOWN_ACTIVE = ["RELIANCE", "HDFCBANK"]
KNOWN_RELISTED = ["VERTOZ", "SUMEETINDS"]


class ExitsError(Exception):
    pass


class StoreBusy(ExitsError):
    pass


class InputStale(ExitsError):
    pass


class NotBuilt(ExitsError):
    pass


class ReferenceListError(ExitsError):
    pass


def out_dir(folder=None):
    return os.path.join(folder or equity_data_dir(), OUT)


def reference_dir(folder):
    return os.path.join(store_dir(folder), REFERENCE)


# ---- inputs ----

def check_inputs(folder):
    """Refuse while a bhavcopy download or a security master / adjustments build holds its lock (StoreBusy), when
    one of those builds is missing (NotBuilt), or older than the newest bhavcopy year file or the adjustments
    older than the master (InputStale)."""
    store, master, factors = store_dir(folder), sm.out_dir(folder), adj.out_dir(folder)
    for lock, what in ((os.path.join(store, ".lock"), "a bhavcopy download (cm_bhav.py)"),
                       (os.path.join(master, ".lock"), "a security master build (security_master.py)"),
                       (os.path.join(factors, ".lock"), "an adjustments build (adjustments.py)")):
        if held(lock):
            raise StoreBusy(f"{what} holds {lock}; build after it ends.")
    need = adj.master_paths(folder) + [os.path.join(master, "review.csv")]
    if not all(os.path.isfile(p) for p in need):
        raise NotBuilt(f"No security master in {master}. Run: python security_master.py build")
    need_adj = [os.path.join(factors, n) for n in ("factors.parquet", "events.csv", "build_info.json")]
    if not all(os.path.isfile(p) for p in need_adj):
        raise NotBuilt(f"No adjustment factors in {factors}. Run: python adjustments.py build")
    years = sm.year_files(store)
    if not years:
        raise ExitsError(f"No bhavcopy parquet files in {store}. Run cm_bhav.py first.")
    newest = max(years, key=os.path.getmtime)
    master_info, adj_info = os.path.join(master, "build_info.json"), need_adj[2]
    for info, cmd in ((master_info, "security_master.py"), (adj_info, "adjustments.py")):
        if os.path.getmtime(info) < os.path.getmtime(newest):
            raise InputStale(f"{info} is older than {newest}. Rebuild it first: python {cmd} build")
    if os.path.getmtime(adj_info) < os.path.getmtime(master_info):
        raise InputStale(f"The adjustments ({adj_info}) are older than the security master. "
                         "Rebuild them first: python adjustments.py build")


def read_reference(folder):
    """({file: DataFrame}, {file: header}): every reference list as text. A missing list or column ->
    ReferenceListError (manual_mergers.csv may be missing: no manual rows)."""
    ref = reference_dir(folder)
    frames, headers = {}, {}
    for name, need in REFERENCE_FILES.items():
        path = os.path.join(ref, name)
        if not os.path.isfile(path):
            if name == "manual_mergers.csv":
                frames[name], headers[name] = pd.DataFrame(columns=need), []
                continue
            raise ReferenceListError(f"No {path}. Run: python nse_reference.py")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        headers[name] = list(df.columns)
        df.columns = [c.strip().lower() for c in df.columns]
        missing = [c for c in need if c not in df.columns]
        if missing:
            raise ReferenceListError(f"{path}: missing column(s) {', '.join(missing)}")
        frames[name] = df.apply(lambda c: c.str.strip())
    return frames, headers


def read_store(folder, master_series, isins):
    """(days, rows, sme, later, seen): the trading days (every date in the files, every series); the rows of the
    given ISINs in the master's series (date, isin, series, close, turnover); every SM / ST row (date, isin,
    series); the rows of the given ISINs in any other series (date, isin, series); every symbol and ISIN in the
    files, any series."""
    files = sm.year_files(store_dir(folder))
    wanted = pa.array(sorted(isins), type=pa.string())
    days, rows, sme, later, seen = set(), [], [], [], set()
    for f in files:
        t = pq.read_table(f, columns=["date", "symbol", "isin", "series", "close", "turnover"])
        days.update(t.column("date").unique().to_pylist())
        for c in ("symbol", "isin"):
            seen.update(str(v).upper() for v in t.column(c).unique().to_pylist() if v)
        t = t.drop_columns(["symbol"])
        mine = pc.is_in(t.column("isin"), value_set=wanted)
        in_master = pc.is_in(t.column("series"), value_set=pa.array(list(master_series)))
        in_sme = pc.is_in(t.column("series"), value_set=pa.array(list(SME_SERIES)))
        rows.append(t.filter(pc.and_(mine, in_master)))
        sme.append(t.filter(in_sme).select(["date", "isin", "series"]))
        later.append(t.filter(pc.and_(mine, pc.invert(pc.or_(in_master, in_sme)))).select(["date", "isin", "series"]))

    def frame(parts, cols):
        return pa.concat_tables(parts).to_pandas() if parts else pd.DataFrame(columns=cols)
    return (sorted(days), frame(rows, ["date", "isin", "series", "close", "turnover"]),
            frame(sme, ["date", "isin", "series"]), frame(later, ["date", "isin", "series"]), seen)


# ---- trading-day arithmetic ----

class Calendar:
    def __init__(self, days):
        self.days = list(days)
        self.index = {d: i for i, d in enumerate(self.days)}

    def pos(self, day):
        """Index of the trading day (or the next one); past the data, weekdays are counted on."""
        if day > self.days[-1]:
            return len(self.days) - 1 + int(np.busday_count(self.days[-1], day))
        if day < self.days[0]:
            return -int(np.busday_count(day, self.days[0]))
        return bisect_left(self.days, day)

    def distance(self, a, b):
        """Trading days from a to b (positive when b is later)."""
        return self.pos(b) - self.pos(a)

    def at(self, i):
        return self.days[min(max(i, 0), len(self.days) - 1)]


def parse_day(text):
    try:
        return sm.as_date(text) if text else None
    except ValueError:
        return None


# ---- Part 1: status ----

def status_of(days_since_last, active_days=ACTIVE_DAYS, exit_days=EXIT_DAYS):
    if days_since_last < active_days:
        return "ACTIVE"
    return "DORMANT" if days_since_last <= exit_days else "EXITED"


def read_gaps(folder):
    """{security_id: [(absent_first, absent_last, days)]} from the master's GAP events."""
    ev = pd.read_csv(os.path.join(sm.out_dir(folder), "events.csv"), dtype=str, keep_default_na=False)
    out = {}
    for r in ev[ev["type"] == "GAP"].itertuples():
        m = re.match(r"absent (\S+) to (\S+)", r.note)
        if m:
            out.setdefault(r.security_id, []).append((sm.as_date(m.group(1)), sm.as_date(m.group(2)),
                                                      int(r.gap_days or 0)))
    return out


# ---- Part 2: why each exited security left ----

def match(sid, reason, source, row, day, distance, **extra):
    return {"security_id": sid, "reason": reason, "source": source, "row": row,
            "date": day, "distance": distance, **extra}


def near_symbols(sid, history, cal, last_date):
    """The symbols the security used in its last NEAR_LAST trading days."""
    since = cal.at(cal.pos(last_date) - NEAR_LAST)
    h = history[(history["security_id"] == sid) & (history["to_date"].map(sm.as_date) >= since)]
    return set(h["symbol"])


def reference_matches(refs, exited, cal, isin_owner):
    """(matches, unmatched, skipped): every reference row that fits an exited security under its rule, the rows
    that fit none (with why), and the counts of rows not considered."""
    by_symbol = {}
    for sid, e in exited.items():
        for s in e["near"]:
            by_symbol.setdefault(s, []).append(sid)
    matches, unmatched, skipped = [], [], {}

    def skip(what):
        skipped[what] = skipped.get(what, 0) + 1

    def try_row(name, i, keys, day, lo, hi, reason, **extra):
        """Match row i of list `name` (keys = its symbols / ISINs) to exited securities with day in [lo, hi]
        trading days of their last date (hi None: any later day), never on or after the first date of a later
        security with the same symbol (a relisting). Unmatched rows are kept with the reason."""
        sids = []
        for k in keys:
            if not k:
                continue
            sids += by_symbol.get(k, [])
            if k in isin_owner and isin_owner[k] in exited:
                sids.append(isin_owner[k])
        hit = False
        near = []
        for sid in dict.fromkeys(sids):
            e = exited[sid]
            dist = cal.distance(e["last_date"], day)
            relist = e.get("relist")
            if relist and day >= relist[1]:
                near.append(f"{sid} last {e['last_date']} ({dist:+d} trading days), after relisting as "
                            f"{relist[0]} on {relist[1]}")
            elif lo <= dist and (hi is None or dist <= hi):
                matches.append(match(sid, reason, name, i, day, dist, **extra))
                hit = True
            else:
                near.append(f"{sid} last {e['last_date']} ({dist:+d} trading days)")
        if not hit:
            known = [k for k in keys if k]
            why = ("date outside the window: " + "; ".join(near)) if near else \
                f"no exited security with {' / '.join(known)}"
            unmatched.append({"file": name, "row": i, "date": day, "symbol": known[0] if known else "",
                              "why": why, "keys": known})

    mg = refs["mergers.csv"]
    manual = {r.old_symbol.upper(): r for r in refs["manual_mergers.csv"].itertuples() if r.old_symbol}
    in_mergers = set()
    for i, r in enumerate(mg.itertuples(), 1):
        if r.exit_expected.upper() == "N":
            skip("mergers.csv: exit_expected N")
            continue
        day = parse_day(r.last_trading_date) or parse_day(r.suspended_from) or parse_day(r.record_date)
        if day is None or not r.old_symbol:
            skip("mergers.csv: no symbol or date")
            continue
        in_mergers.add(r.old_symbol.upper())
        m = manual.get(r.old_symbol.upper())
        fill = {f: getattr(r, f) or (getattr(m, f, "") if m is not None else "")
                for f in ("new_symbol", "new_company", "swap_ratio")}
        try_row("mergers.csv", i, [r.old_symbol.upper()], day, -MERGER_WINDOW, MERGER_WINDOW, "MERGER",
                scheme_type=r.scheme_type, **fill)
    for i, r in enumerate(refs["manual_mergers.csv"].itertuples(), 1):
        sym = r.old_symbol.upper()
        if not sym or sym in in_mergers:
            continue
        if getattr(r, "exit_expected", "").upper() == "N":
            skip("manual_mergers.csv: exit_expected N")
            continue
        sids = [sid for sid in by_symbol.get(sym, [])]
        if not sids:
            unmatched.append({"file": "manual_mergers.csv", "row": i, "date": None, "symbol": sym,
                              "why": "no exited security with this symbol", "keys": [sym]})
            continue
        sid = max(sids, key=lambda s: exited[s]["last_date"])
        matches.append(match(sid, "MERGER", "manual_mergers.csv", i, None, None, new_symbol=r.new_symbol,
                             new_company=r.new_company, swap_ratio=r.swap_ratio,
                             scheme_type=getattr(r, "scheme_type", ""), extra_note="manual, no date"))

    for i, r in enumerate(refs["voluntary_delisting.csv"].itertuples(), 1):
        if r.list.lower() != "delisted":
            skip(f"voluntary_delisting.csv: list {r.list or 'blank'}")
            continue
        if r.board and r.board.lower() != "main board":
            skip(f"voluntary_delisting.csv: board {r.board}")
            continue
        day, kind = parse_day(r.delisting_date), r.delisting_type.lower()
        if day is None:
            skip("voluntary_delisting.csv: no date")
            continue
        keys = [r.symbol.upper(), r.isin.upper()]
        if "compulsory" in kind:
            try_row("voluntary_delisting.csv", i, keys, day, 0, None, "COMPULSORY_DELISTING")
        elif "liquidation" in kind:
            try_row("voluntary_delisting.csv", i, keys, day, 0, None, "LIQUIDATION")
        elif "voluntary" in kind:
            try_row("voluntary_delisting.csv", i, keys, day, 0, DELIST_WINDOW, "VOLUNTARY_DELISTING")
        else:
            try_row("voluntary_delisting.csv", i, keys, day, 0, DELIST_WINDOW, None,
                    extra_note=f"delisting type {r.delisting_type or 'blank'}")

    for i, r in enumerate(refs["delisting_circulars.csv"].itertuples(), 1):
        kind = r.delisting_type.lower()
        if kind == "withdrawal":
            skip("delisting_circulars.csv: withdrawal")
            continue
        day = parse_day(r.effective_date) or parse_day(r.circular_date)
        if day is None or not r.symbol:
            skip("delisting_circulars.csv: no symbol or date")
            continue
        reason = CIRCULAR_REASON.get(kind)
        try_row("delisting_circulars.csv", i, [r.symbol.upper()], day, 0,
                None if reason in ANY_LATER else DELIST_WINDOW, reason,
                extra_note=f"circular {r.circular_no} ({kind or 'no type'})")

    for i, r in enumerate(refs["compulsory_delisting.csv"].itertuples(), 1):
        day = parse_day(r.order_date)
        if day is None or not r.symbol:
            skip("compulsory_delisting.csv: no symbol or date")
            continue
        try_row("compulsory_delisting.csv", i, [r.symbol.upper()], day, 0, None, "COMPULSORY_DELISTING")

    sp = refs["suspensions.csv"]
    cancelled = {(r.cancels_circular, r.symbol.upper() or r.company.upper())
                 for r in sp.itertuples() if r.status.upper() == "CANCELLED" and r.cancels_circular}
    for i, r in enumerate(sp.itertuples(), 1):
        status = r.status.upper()
        if status == "CANCELLED":
            skip("suspensions.csv: CANCELLED")
            continue
        if (r.circular_no, r.symbol.upper()) in cancelled or (r.circular_no, r.company.upper()) in cancelled:
            skip("suspensions.csv: cancelled later")
            continue
        if not r.symbol:
            skip("suspensions.csv: no symbol")
            continue
        day = parse_day(r.effective_date) or parse_day(r.circular_date)
        if day is None:
            skip("suspensions.csv: no date")
            continue
        # the classified reason decides; the free text only when it is blank (NCLT also approves capital reductions)
        reason = "LIQUIDATION" if LIQUIDATION_RX.search(r.reason or r.reason_text) else "SUSPENDED"
        n = len(unmatched)
        try_row("suspensions.csv", i, [r.symbol.upper()], day, 0, None if reason in ANY_LATER else SUSPEND_WINDOW,
                reason, extra_note=f"suspension: {r.reason or 'no reason'}")
        if len(unmatched) > n:
            u = unmatched.pop()       # a suspension that matches no exit is normal (trading came back) ...
            if getattr(r, "exit_expected", "").upper() == "Y":
                unmatched.append(u)   # ... unless the circular expected an exit
    return matches, unmatched, skipped


def relistings(exited, sm_review, securities, history):
    """{security_id: (new security_id, its first date, source)}: the earliest later security (first date after
    this one's last date) of a symbol_reused? row of the master, or holding one of its near symbols."""
    reused = {}
    for i, r in enumerate(sm_review.itertuples(), 1):
        if r.kind == "symbol_reused?":
            reused.setdefault(r.security_id_old, []).append((r.security_id_new, i))
    first = dict(zip(securities["security_id"], securities["first_date"].map(sm.as_date)))
    holders = {}
    for sym, sid in zip(history["symbol"], history["security_id"]):
        holders.setdefault(sym, set()).add(sid)
    out = {}
    for sid, e in exited.items():
        found = {}
        for new, i in reused.get(sid, []):
            found[new] = f"security_master review.csv row {i}"
        for sym in e["near"]:
            for other in holders.get(sym, ()):
                if other != sid and other not in found and first[other] > e["last_date"]:
                    found[other] = f"security_master symbol {sym}"
        if found:
            new = min(found, key=lambda s: (first[s], s))
            out[sid] = (new, first[new], found[new])
    return out


def data_matches(exited, cal, sme, later):
    """MOVED_TO_SME, RELISTED (exited[sid]["relist"]), RIGHTS_ENDED / ETF_ENDED and other later series, from the
    bhavcopy and the master."""
    matches, notes = [], {}
    sme_by_isin = {i: g for i, g in sme.groupby("isin")} if not sme.empty else {}
    later_by_isin = {i: g for i, g in later.groupby("isin")} if not later.empty else {}
    for sid, e in exited.items():
        last = e["last_date"]
        for isin in e["isins"]:
            g = sme_by_isin.get(isin)
            if g is not None:
                after = g[g["date"] > last]
                if not after.empty:
                    d = min(after["date"])
                    series = "/".join(sorted(set(after["series"])))
                    matches.append(match(sid, "MOVED_TO_SME", f"bhavcopy {series}", None, d, cal.distance(last, d),
                                         extra_note=f"{isin} trades in {series} from {d}"))
            g = later_by_isin.get(isin)
            if g is not None:
                after = g[g["date"] > last]
                for series, part in after.groupby("series"):
                    notes.setdefault(sid, []).append(f"{isin} trades later in series {series} from "
                                                     f"{min(part['date'])}")
        if e.get("relist"):
            new, d, source = e["relist"]
            matches.append(match(sid, "RELISTED", source, None, d, cal.distance(last, d), relisted_as=new))
        if e["kind"] == "RIGHTS":
            matches.append(match(sid, "RIGHTS_ENDED", "instrument_kind", None, None, None))
        elif e["kind"] == "ETF":
            matches.append(match(sid, "ETF_ENDED", "instrument_kind", None, None, None))
    return matches, notes


def pick(ms):
    """The match that decides the reason: the first reason in REASONS, then the closest date; except that a
    LIQUIDATION match beats a VOLUNTARY_DELISTING one (an IBC / liquidation case listed as delisted)."""
    ms = [m for m in ms if m["reason"]]
    if not ms:
        return None

    def key(m):
        return (REASONS.index(m["reason"]), abs(m["distance"]) if m["distance"] is not None else 10 ** 6,
                m["source"], m["row"] or 0)
    chosen = min(ms, key=key)
    liquidation = [m for m in ms if m["reason"] == "LIQUIDATION"]
    if chosen["reason"] == "VOLUNTARY_DELISTING" and liquidation:
        chosen = min(liquidation, key=key)
    return chosen


def source_text(m):
    return m["source"] + (f" row {m['row']}" if m["row"] else "")


def describe(m):
    d = f" {m['date']}" if m["date"] else ""
    dist = f" ({m['distance']:+d})" if m["distance"] is not None else ""
    return f"{m['reason'] or 'note'} {source_text(m)}{d}{dist}" + (f": {m['extra_note']}" if m.get("extra_note") else "")


def resolve_acquirer(m, last, cal, master, folder):
    """(security_id, symbol then, note) of the acquirer of a merger match, else (None, symbol or '', why)."""
    sym = (m.get("new_symbol") or "").strip().upper()
    if not sym:
        company = (m.get("new_company") or "").strip().upper()
        if company and company in master["symbol"]:
            sym = company
    if not sym:
        return None, "", "acquirer not named in mergers.csv / manual_mergers.csv"
    now_of = dict(zip(master["securities"]["security_id"], master["securities"]["last_symbol"]))
    for day in (last, last - timedelta(days=7), cal.at(cal.pos(last) + 1)):
        sid = sm.security_id_for(sym, day, folder)
        if sid:
            return sid, sym, (f"acquirer now {now_of[sid]}" if now_of[sid] != sym else "")
    ever = sm.lookup(sym, folder)
    if len(ever) == 1:            # named by a symbol it had at another time (e.g. its later name)
        return ever[0], sym, f"acquirer {sym} is not that security's symbol on {last}; now {now_of[ever[0]]}"
    return None, sym, f"acquirer {sym} not in the security master around {last}" + \
        (f" (held by {', '.join(ever)} at other times)" if ever else "")


def per_old(ratio):
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*(?:for|:)\s*(\d+(?:\.\d+)?)\s*$", ratio or "", re.I)
    if not m or float(m.group(2)) == 0:
        return None
    return float(m.group(1)) / float(m.group(2))


# ---- Part 3: facts for a backtest ----

def distress(rows, last, cal, factor):
    """(price change %, BE/BZ days, turnover ratio, median turnover) over the last DISTRESS_WINDOW trading days.
    rows: the security's master-series rows (date, series, close, turnover), sorted by date."""
    li = cal.pos(last)
    start, year_start = cal.at(li - DISTRESS_WINDOW + 1), cal.at(li - DISTRESS_WINDOW - YEAR_DAYS + 1)
    win = rows[(rows["date"] >= start) & (rows["date"] <= last)]
    year = rows[(rows["date"] >= year_start) & (rows["date"] < start)]
    if win.empty:
        return None, 0, None, None
    a, b = win.iloc[0], win.iloc[-1]
    ca, cb = a["close"] / factor(a["date"]), b["close"] / factor(b["date"])
    change = round(100 * (cb / ca - 1), 2) if ca else None
    be_bz = int(win["series"].isin(["BE", "BZ"]).sum())
    mw, my = win["turnover"].median(), (year["turnover"].median() if not year.empty else None)
    ratio = round(mw / my, 4) if my and my == my and my > 0 else None
    size = rows[rows["date"] >= year_start]["turnover"].median()
    return change, be_bz, ratio, size


def distressed(change, be_bz, ratio):
    return ((change is not None and change <= DISTRESS_DROP) or be_bz >= DISTRESS_BE_BZ
            or (ratio is not None and ratio <= DISTRESS_TURNOVER))


def treatment(reason, acquirer, swap, hint):
    """A suggestion only; the backtest decides the value."""
    if reason == "MERGER":
        if acquirer and per_old(swap):
            return "CONVERT"
        return "CASH_AT_LAST" if not per_old(swap) else "REVIEW"
    if reason in ("VOLUNTARY_DELISTING", "MOVED_TO_SME", "RELISTED"):
        return "CASH_AT_LAST"
    if reason in ("LIQUIDATION", "COMPULSORY_DELISTING") or (reason == "UNKNOWN" and hint):
        return "WRITE_DOWN"
    if reason in ("RIGHTS_ENDED", "ETF_ENDED"):
        return "NOT_APPLICABLE"
    return "REVIEW"


# ---- Part 4: listing kinds ----

def listing_kinds(securities, cal, sme, relisted_from, start_days, batch_min):
    """({security_id: (kind, note)}, batch_days {date: count}, start_tail [(date, stock count, all count)])."""
    first = securities["first_date"].map(sm.as_date)
    pos = first.map(cal.pos)
    stock = securities["instrument_kind"] == "STOCK"
    counts = first[stock & (pos >= start_days)].value_counts()
    batch = {d: int(n) for d, n in sorted(counts.items()) if n >= batch_min}
    tail = []
    for i in range(min(start_days, len(cal.days))):
        d = cal.days[i]
        tail.append((d, int(((first == d) & stock).sum()), int((first == d).sum())))
    sme_first = sme.groupby("isin")["date"].agg(["min", "max"]) if not sme.empty else pd.DataFrame()
    sme_series = sme.groupby("isin")["series"].agg(lambda s: "/".join(sorted(set(s)))) if not sme.empty else {}
    out = {}
    for r, d, p in zip(securities.itertuples(), first, pos):
        if p < start_days:
            out[r.security_id] = ("IN_DATA_FROM_START", "in data from the first day" if p == 0 else
                                  f"first seen on trading day {p + 1} of the data")
            continue
        earlier = []
        for isin in r.isins.split("|"):
            if isin in sme_first.index and sme_first.at[isin, "min"] < d:
                earlier.append(f"{isin} in {sme_series[isin]} {sme_first.at[isin, 'min']} to "
                               f"{min(sme_first.at[isin, 'max'], d)}")
        if earlier:
            out[r.security_id] = ("SME_MIGRATION", "; ".join(earlier))
        elif r.security_id in relisted_from:
            out[r.security_id] = ("RELISTING", relisted_from[r.security_id])
        elif r.instrument_kind == "STOCK" and d in batch:
            out[r.security_id] = ("BATCH_ADMISSION", f"one of {batch[d]} STOCK securities first seen on {d}")
        else:
            out[r.security_id] = ("NEW_LISTING", "")
    return out, batch, tail


# ---- the build ----

def build_frames(folder, active_days, exit_days, batch_min, start_days, log=print):
    m = sm.load_master(folder)
    secs = m["securities"].copy()
    history = m["history"]
    refs, headers = read_reference(folder)
    sm_review = pd.read_csv(os.path.join(sm.out_dir(folder), "review.csv"), dtype=str, keep_default_na=False)
    secs["status"] = secs["days_since_last"].map(lambda n: status_of(int(n), active_days, exit_days))
    ex = secs[secs["status"] == "EXITED"]
    isins = {i for x in ex["isins"] for i in x.split("|")}
    log(f"Reading {store_dir(folder)} (every series)")
    days, rows, sme, later, seen = read_store(folder, m["info"]["series"], isins)
    cal = Calendar(days)

    exited = {}
    for r in ex.itertuples():
        last = sm.as_date(r.last_date)
        exited[r.security_id] = {"last_date": last, "isins": r.isins.split("|"), "kind": r.instrument_kind,
                                 "near": near_symbols(r.security_id, history, cal, last) | {r.last_symbol}}
    for sid, relist in relistings(exited, sm_review, secs, history).items():
        exited[sid]["relist"] = relist
    log(f"Matching {len(exited):,} exited securities against the reference lists")
    ref_m, unmatched, skipped = reference_matches(refs, exited, cal, m["isin_owner"])
    never = {}                # rows naming a symbol / ISIN that never trades in the store: counted, not reviewed
    for x in unmatched:
        if not any(k in seen for k in x["keys"]):
            never[x["file"]] = never.get(x["file"], 0) + 1
    unmatched = [x for x in unmatched if any(k in seen for k in x["keys"])]
    data_m, later_notes = data_matches(exited, cal, sme, later)
    by_sid = {}
    for x in ref_m + data_m:
        by_sid.setdefault(x["security_id"], []).append(x)

    periods = adj.load_factors(folder)["periods"]
    rows = rows.assign(security_id=rows["isin"].map(m["isin_owner"])).sort_values(["security_id", "date"],
                                                                                    kind="stable")
    rows_by = {sid: g for sid, g in rows.groupby("security_id", sort=False)}
    out, reviews, relisted_from = [], [], {}
    for r in ex.itertuples():
        sid, last = r.security_id, exited[r.security_id]["last_date"]
        ms = by_sid.get(sid, [])
        chosen = pick(ms)
        reason = chosen["reason"] if chosen else "UNKNOWN"
        notes = []
        in_periods = sid in periods

        def factor(day, sid=sid):
            return adj.lookup_factor(periods, sid, day) if sid in periods else 1.0
        if not in_periods:
            notes.append(f"no adjustment factors for {r.instrument_kind}; factor 1")
        acq_id, acq_sym, swap, relisted_as = None, "", "", ""
        if reason == "MERGER":
            swap = chosen.get("swap_ratio", "") or ""
            acq_id, acq_sym, why = resolve_acquirer(chosen, last, cal, m, folder)
            if why:
                notes.append(why)
            if per_old(swap):
                notes.append(f"{per_old(swap):.6g} new per old")
            if not acq_id:
                reviews.append({"kind": "unresolved_acquirer", "security_id": sid, "date": last,
                                "symbol": r.last_symbol, "details": f"{describe(chosen)}; {why}"})
        relisted = [x for x in ms if x["reason"] == "RELISTED"]
        if relisted:
            relisted_as = relisted[0]["relisted_as"]
            relisted_from.setdefault(relisted_as, f"relisting of {sid} ({r.last_symbol}, last {last})")
        groups = {CONFLICT_GROUP[x["reason"]] for x in ms if x["reason"] in CONFLICT_GROUP}
        acquirers = {(x.get("new_symbol") or x.get("new_company") or "").upper() for x in ms if x["reason"] == "MERGER"}
        if len(groups) > 1 or len(acquirers - {""}) > 1:
            reviews.append({"kind": "conflict", "security_id": sid, "date": last, "symbol": r.last_symbol,
                            "details": f"chose {reason}; " + "; ".join(describe(x) for x in ms if x["reason"])})
        if chosen and chosen.get("extra_note"):
            notes.append(chosen["extra_note"])
        notes += [f"also {describe(x)}" for x in ms if x is not chosen]
        notes += later_notes.get(sid, [])
        g = rows_by.get(sid, pd.DataFrame(columns=["date", "series", "close", "turnover"]))
        change, be_bz, ratio, size = distress(g, last, cal, factor)
        hint = distressed(change, be_bz, ratio)
        if hint and reason == "UNKNOWN":
            notes.append("distress hints")
        f_last = factor(last)
        out.append({"security_id": sid, "last_symbol": r.last_symbol, "symbols": r.symbols, "isins": r.isins,
                    "instrument_kind": r.instrument_kind, "first_date": sm.as_date(r.first_date), "last_date": last,
                    "last_close": r.last_close, "last_close_adj": round(r.last_close / f_last, 4),
                    "last_series": r.last_series, "reason": reason,
                    "reason_source": source_text(chosen) if chosen else "",
                    "reason_date": chosen["date"] if chosen else None,
                    "reason_distance": chosen["distance"] if chosen else None,
                    "acquirer_security_id": acq_id or "", "acquirer_symbol": acq_sym, "swap_ratio": swap,
                    "relisted_as": relisted_as, "distress_price_change": change, "distress_be_bz_days": be_bz,
                    "distress_turnover_ratio": ratio,
                    "suggested_treatment": treatment(reason, acq_id, swap, hint), "note": "; ".join(notes),
                    "_size": size})
    exits = pd.DataFrame(out, columns=EXIT_COLUMNS + ["_size"])
    unknown = exits[exits["reason"] == "UNKNOWN"].sort_values("_size", ascending=False, kind="stable")
    for x in unknown.head(REVIEW_TOP).to_dict("records"):
        size = f"{x['_size']:,.0f}" if pd.notna(x["_size"]) else "-"
        reviews.append({"kind": "unknown_large", "security_id": x["security_id"], "date": x["last_date"],
                        "symbol": x["last_symbol"], "details": f"median daily turnover {size} over its last year; "
                        f"last close {x['last_close']:g} ({x['last_series']}); treatment {x['suggested_treatment']}"})
    exits = exits.drop(columns="_size").astype({"reason_distance": "Int64"})
    for x in unmatched:
        reviews.append({"kind": "unmatched_reference", "security_id": "", "date": x["date"], "symbol": x["symbol"],
                        "details": f"{x['file']} row {x['row']}: {x['why']}"})
    for sid, rel in reused_new(sm_review).items():
        relisted_from.setdefault(sid, rel)

    kinds, batch, tail = listing_kinds(secs, cal, sme, relisted_from, start_days, batch_min)
    gaps = read_gaps(folder)
    status = secs[["security_id", "status", "last_date", "days_since_last"]].copy()
    status["last_date"] = status["last_date"].map(sm.as_date)
    status["listing_kind"] = status["security_id"].map(lambda s: kinds[s][0])

    def lnote(sid):
        parts = [kinds[sid][1]] if kinds[sid][1] else []
        if sid in gaps:
            parts.append("gaps: " + ", ".join(f"{a} to {b} ({n} days)" for a, b, n in gaps[sid]))
        return "; ".join(parts)
    status["listing_note"] = status["security_id"].map(lnote)
    status = status[STATUS_COLUMNS]
    listings = listing_rows(secs, kinds, batch)
    rv = pd.DataFrame(reviews, columns=REVIEW_COLUMNS)
    rv["_k"] = rv["kind"].map(REVIEW_KINDS.index)
    rv = rv.sort_values(["_k"], kind="stable").drop(columns="_k").reset_index(drop=True)
    return {"exits": exits, "status": status, "listings": listings, "review": rv, "days": days, "batch": batch,
            "tail": tail, "headers": headers, "skipped": skipped, "never": never, "secs": secs, "refs": refs,
            "master_info": m["info"]}


def reused_new(sm_review):
    """{new security_id: note} for every symbol_reused? row of the master (the new side is a relisting)."""
    out = {}
    for r in sm_review.itertuples():
        if r.kind == "symbol_reused?":
            out[r.security_id_new] = f"symbol reused from {r.security_id_old} (security_master review.csv)"
    return out


def listing_rows(secs, kinds, batch):
    first = dict(zip(secs["security_id"], secs["first_date"].map(sm.as_date)))
    sym = dict(zip(secs["security_id"], secs["first_symbol"]))
    rows = []
    for d, n in batch.items():
        ids = sorted(s for s, (k, _) in kinds.items() if k == "BATCH_ADMISSION" and first[s] == d)
        rows.append({"kind": "BATCH_ADMISSION", "date": d, "count": n,
                     "examples": " ".join(sym[s] for s in ids[:10])})
    sme = {}
    for s, (k, _) in kinds.items():
        if k == "SME_MIGRATION":
            sme.setdefault(first[s], []).append(s)
    for d in sorted(sme):
        rows.append({"kind": "SME_MIGRATION", "date": d, "count": len(sme[d]),
                     "examples": " ".join(sym[s] for s in sorted(sme[d])[:10])})
    return pd.DataFrame(rows, columns=LISTING_COLUMNS)


def known_checks(exits, status, batch, secs):
    """(lines, ok) for the KNOWN_* cases on the built frames."""
    lines, ok = [], True
    batch = {str(d): n for d, n in batch.items()}
    st = status.merge(secs[["security_id", "last_symbol"]], on="security_id")

    def mark(found, text):
        nonlocal ok
        ok &= found
        lines.append(f"  [{'found' if found else 'NOT FOUND'}] {text}")

    def describe_exit(x):
        return (f"{x.security_id} last {x.last_date}, {x.reason}"
                + (f", acquirer {x.acquirer_symbol} {x.acquirer_security_id}" if x.acquirer_symbol else "")
                + (f", swap {x.swap_ratio}" if x.swap_ratio else "") + f", {x.suggested_treatment}"
                + (f" ({x.note})" if x.note else ""))
    lines.append("Mergers (EXITED, MERGER, acquirer found):")
    for sym, last, acquirers, swap, treat in KNOWN_MERGERS:
        rows = exits[exits["last_symbol"] == sym]
        good = []
        for x in rows.itertuples():
            note_now = re.search(r"acquirer now ([^\s;]+)", x.note or "")
            names = {x.acquirer_symbol} | ({note_now.group(1)} if note_now else set())
            if (x.reason == "MERGER" and x.acquirer_security_id
                    and (last is None or str(x.last_date).startswith(last))
                    and (acquirers is None or names & set(acquirers))
                    and (swap is None or x.swap_ratio == swap) and (treat is None or x.suggested_treatment == treat)):
                good.append(x)
        want = ", ".join(v for v in (last and f"last {last}", acquirers and f"acquirer {'/'.join(acquirers)}",
                                     swap and f"swap {swap}", treat) if v)
        text = sym + (f" (expected {want})" if want else "")
        text += "".join(f"\n      {describe_exit(x)}" for x in rows.itertuples()) if not rows.empty else \
            ": not among the exits"
        mark(bool(good), text)
    lines.append("Delistings (a real example from each list, after 2015):")
    for reason, source in (("VOLUNTARY_DELISTING", "voluntary_delisting.csv"),
                           ("COMPULSORY_DELISTING", "compulsory_delisting.csv")):
        rows = exits[(exits["reason"] == reason) & exits["reason_source"].str.startswith(source)
                     & (exits["last_date"].map(str) >= "2015-01-02")]
        rows = rows.sort_values("last_date", kind="stable")
        text = f"{reason} from {source}: {len(rows):,} exits"
        if not rows.empty:
            x = rows.iloc[len(rows) // 2]
            text += f"; example {x.last_symbol} {x.security_id} last {x.last_date}, {x.reason_source} " \
                    f"{x.reason_date} ({int(x.reason_distance):+d}), {x.suggested_treatment}"
        mark(not rows.empty, text)
    lines.append("SME migrations (listing_kind SME_MIGRATION):")
    for sym in KNOWN_SME:
        rows = st[st["last_symbol"] == sym]
        mark(bool((rows["listing_kind"] == "SME_MIGRATION").any()),
             sym + "".join(f": {x.security_id} {x.listing_kind} ({x.listing_note})" for x in rows.itertuples())
             + ("" if not rows.empty else ": no security with this symbol"))
    lines.append("Batch admission days (STOCK first appearances):")
    for d, n in KNOWN_BATCH_DAYS.items():
        mark(d in batch, f"{d} expected about {n}: " + (f"{batch[d]}" if d in batch else "not a batch day"))
    lines.append("Active:")
    for sym in KNOWN_ACTIVE:
        rows = st[st["last_symbol"] == sym]
        mark(bool((rows["status"] == "ACTIVE").any()),
             sym + "".join(f": {x.security_id} {x.status}, last {x.last_date}" for x in rows.itertuples()))
    lines.append("Relisted (old -> new security_id):")
    for sym in KNOWN_RELISTED:
        rows = exits[exits["last_symbol"] == sym]
        good = rows[(rows["reason"] == "RELISTED") & (rows["relisted_as"] != "")]
        mark(not good.empty, sym + "".join(f": {x.security_id} -> {x.relisted_as or '-'} ({x.reason}, last "
                                           f"{x.last_date})" for x in rows.itertuples())
             + ("" if not rows.empty else ": not among the exits"))
    return lines, ok


def report_text(info, f, checks):
    exits, status, rv = f["exits"], f["status"], f["review"]
    L = ["Exit list and listing types build", f"Built: {info['built_at']}",
         f"Data: {info['first_date']} to {info['last_date']}, {info['trading_days']} trading days; active days "
         f"{info['active_days']}, exit days {info['exit_days']}, batch min {info['batch_min']}, start days "
         f"{info['start_days']}",
         f"Security master built {info['master_built_at']}; adjustments built {info['adjustments_built_at']}", "",
         "Reference lists (headers found):"]
    for name, cols in f["headers"].items():
        L.append(f"  {name} ({len(f['refs'][name]):,} rows): {', '.join(cols) if cols else 'missing (no rows)'}")
    L.append("  rows not considered: " + (", ".join(f"{k} {v:,}" for k, v in sorted(f["skipped"].items())) or "none"))
    L.append("  rows matching no exit whose symbol / ISIN never appears in the bhavcopy (not in review.csv): "
             + (", ".join(f"{k} {v:,}" for k, v in sorted(f["never"].items())) or "none"))
    sc = status["status"].value_counts()
    L += ["", "Status (every security):"] + [f"  {s}: {int(sc.get(s, 0)):,}" for s in STATUSES]
    kinds = f["secs"].set_index("security_id")["instrument_kind"]
    L.append("  EXITED per kind: " + ", ".join(f"{k} {int((exits['instrument_kind'] == k).sum()):,}"
                                               for k in sm.KINDS))
    rc, tc = exits["reason"].value_counts(), exits["suggested_treatment"].value_counts()
    L += ["", "Reasons (EXITED):"] + [f"  {r}: {int(rc.get(r, 0)):,}" for r in REASONS]
    L += ["", "Suggested treatment (a suggestion only: the backtest decides the value):"]
    L += [f"  {t}: {int(tc.get(t, 0)):,}" for t in TREATMENTS]
    lc = status["listing_kind"].value_counts()
    stock = status["security_id"].map(kinds) == "STOCK"
    lcs = status[stock]["listing_kind"].value_counts()
    L += ["", "Listing kinds (all / STOCK):"]
    L += [f"  {k}: {int(lc.get(k, 0)):,} / {int(lcs.get(k, 0)):,}" for k in LISTING_KINDS]
    L.append("  A BATCH_ADMISSION or SME_MIGRATION start date is not an IPO date.")
    tail = f["tail"]
    after = sum(n for _, _, n in tail[1:])
    L += ["", f"Start window: the first {info['start_days']} trading days count as IN_DATA_FROM_START; "
              f"{after:,} securities first seen after day 1 within them. Per day (STOCK / all):"]
    L += [f"  day {i + 1:>2} {d}: {s:,} / {n:,}" for i, (d, s, n) in enumerate(tail)]
    L += ["", f"Batch admission days (at least {info['batch_min']} STOCK first appearances, after the start window):"]
    L += [f"  {d}: {n:,}" for d, n in f["batch"].items()] or ["  none"]
    sme = f["listings"][f["listings"]["kind"] == "SME_MIGRATION"]
    L.append(f"SME migrations: {int(sme['count'].sum()):,} securities on {len(sme):,} days")
    L += ["", "Known cases:"] + checks[0]
    L.append(f"Known cases: {'all found' if checks[1] else 'SOME NOT FOUND'}")
    L += ["", f"Review rows: {len(rv):,} (" + ", ".join(f"{k} {int((rv['kind'] == k).sum()):,}" for k in REVIEW_KINDS)
          + ")"]
    shown = pd.concat([rv[rv["kind"] == k].head(3) for k in REVIEW_KINDS]).head(10) if not rv.empty else rv
    L += [f"  {r.kind} {r.date or ''} {r.security_id or ''} {r.symbol or ''}: {r.details}" for r in shown.itertuples()]
    return "\n".join(L) + "\n"


def build(folder=None, active_days=ACTIVE_DAYS, exit_days=EXIT_DAYS, batch_min=BATCH_MIN, start_days=START_DAYS,
          now=None, log=print):
    """Full rebuild of <folder>/exits/. Refuses while an input's lock is held (StoreBusy), when an input is
    missing (NotBuilt) or stale (InputStale), a reference list lacks a column (ReferenceListError), or another
    build runs (LockBusy)."""
    folder = folder or equity_data_dir()
    check_inputs(folder)
    out = out_dir(folder)
    lock = RunLock(os.path.join(out, ".lock")).acquire("exits.py build")
    try:
        f = build_frames(folder, active_days, exit_days, batch_min, start_days, log)
        with open(os.path.join(adj.out_dir(folder), "build_info.json"), encoding="utf-8") as fh:
            adj_info = json.load(fh)
        days = f["days"]
        info = {"active_days": active_days, "exit_days": exit_days, "batch_min": batch_min, "start_days": start_days,
                "first_date": str(days[0]), "last_date": str(days[-1]), "trading_days": len(days),
                "master_built_at": f["master_info"].get("built_at"), "adjustments_built_at": adj_info.get("built_at"),
                "built_at": f"{(now or sm.now_ist()):%Y-%m-%d %H:%M:%S} IST"}
        checks = known_checks(f["exits"], f["status"], f["batch"], f["secs"])
        report = report_text(info, f, checks)
        sm.write_csv(f["exits"], os.path.join(out, "exits.csv"))
        sm.write_parquet(f["exits"], os.path.join(out, "exits.parquet"))
        sm.write_csv(f["status"], os.path.join(out, "status.csv"))
        sm.write_parquet(f["status"], os.path.join(out, "status.parquet"))
        sm.write_csv(f["listings"], os.path.join(out, "listings.csv"))
        sm.write_csv(f["review"], os.path.join(out, "review.csv"))
        sm.write_csv(pd.DataFrame({"date": days}), os.path.join(out, "calendar.csv"))
        sm.write_text(os.path.join(out, "build_info.json"), json.dumps(info, indent=2) + "\n")
        sm.write_text(os.path.join(out, "build_report.txt"), report)
        _CACHE.clear()
        return {"exits": len(f["exits"]), "securities": len(f["status"]), "reviews": len(f["review"]),
                "report": report, "checks_ok": checks[1], "out": out}
    finally:
        lock.release()


# ---- loaders (read the built files) ----

_CACHE = {}


def load_exits(folder=None):
    out = out_dir(folder)
    paths = [os.path.join(out, n) for n in ("exits.parquet", "status.parquet", "calendar.csv", "build_info.json")]
    if not all(os.path.isfile(p) for p in paths):
        raise NotBuilt(f"No exit list in {out}. Run: python exits.py build")
    key = (out, tuple(os.path.getmtime(p) for p in paths))
    if key not in _CACHE:
        _CACHE.clear()
        exits = pq.read_table(paths[0]).to_pandas()
        status = pq.read_table(paths[1]).to_pandas()
        days = [sm.as_date(d) for d in pd.read_csv(paths[2], dtype=str)["date"]]
        with open(paths[3], encoding="utf-8") as fh:
            info = json.load(fh)
        _CACHE[key] = {"exits": exits, "status": status, "calendar": Calendar(days), "info": info,
                       "exit_by": {r["security_id"]: r for r in exits.to_dict("records")},
                       "status_by": {r["security_id"]: r for r in status.to_dict("records")},
                       "gaps": read_gaps(folder or equity_data_dir())}
    return _CACHE[key]


def exit_info(security_id, folder=None):
    """The exits.csv row of the security as a dict, None if it has not exited."""
    return load_exits(folder)["exit_by"].get(security_id)


def exits_between(start, end, kinds=("STOCK",), folder=None):
    """The exits whose last date is in [start, end], only those whose instrument_kind is in kinds (None: all)."""
    e = load_exits(folder)["exits"]
    last = e["last_date"].map(sm.as_date)
    keep = (last >= sm.as_date(start)) & (last <= sm.as_date(end))
    if kinds is not None:
        keep &= e["instrument_kind"].isin(list(kinds))
    return e[keep].reset_index(drop=True)


def status_on(security_id, on_date, folder=None):
    """Point in time: None before the first date; ACTIVE while it trades; inside a GAP or after the last date,
    ACTIVE / DORMANT / EXITED by the trading days since its last trade before on_date. A date after the data ->
    ValueError; unknown id -> KeyError."""
    x = load_exits(folder)
    if security_id not in x["status_by"]:
        raise KeyError(f"unknown security_id {security_id}")
    cal, day = x["calendar"], sm.as_date(on_date)
    if day > cal.days[-1]:
        raise ValueError(f"{day} is after the data ({cal.days[-1]})")
    s = x["status_by"][security_id]
    first = sm.as_date(listing_info(security_id, folder)["first_date"])
    last = sm.as_date(s["last_date"])
    if day < first:
        return None
    traded = min(day, last)
    for a, b, _ in x["gaps"].get(security_id, []):
        if a <= traded <= b:
            traded = cal.at(cal.pos(a) - 1)
    return status_of(cal.distance(traded, day), x["info"]["active_days"], x["info"]["exit_days"])


def listing_info(security_id, folder=None):
    """{security_id, status, last_date, days_since_last, listing_kind, listing_note, first_date}."""
    x = load_exits(folder)
    if security_id not in x["status_by"]:
        raise KeyError(f"unknown security_id {security_id}")
    m = sm.load_master(folder)
    row = m["securities"][m["securities"]["security_id"] == security_id].iloc[0]
    return {**x["status_by"][security_id], "first_date": sm.as_date(row.first_date)}


def read_built(folder=None, name="exits.csv"):
    path = os.path.join(out_dir(folder), name)
    if not os.path.isfile(path):
        raise NotBuilt(f"No {name} in {out_dir(folder)}. Run: python exits.py build")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def check(folder=None):
    """(lines, ok): the known cases on the built files."""
    x = load_exits(folder)
    listings = read_built(folder, "listings.csv")
    batch = {d: int(n) for k, d, n in zip(listings["kind"], listings["date"], listings["count"])
             if k == "BATCH_ADMISSION"}
    exits = x["exits"].copy()
    for c in ("acquirer_security_id", "acquirer_symbol", "swap_ratio", "relisted_as", "note", "reason_source"):
        exits[c] = exits[c].fillna("")
    return known_checks(exits, x["status"], batch, sm.load_master(folder)["securities"])
