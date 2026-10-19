"""Split / bonus / consolidation adjustment factors, from the saved NSE equity bhavcopy and the security master.

No network, no broker. NSE_CM_bhav/ and security_master/ are only read. Output: <folder>/adjustments/ (written
atomically and rebuilt in full; input/ and manual_factors.csv are only read):
    events.csv                 every price candidate and every confirmed event (EVENT_COLUMNS)
    factors.parquet / .csv     cumulative factor per period of each security (FACTOR_COLUMNS)
    review.csv                 price-only candidates, corporate actions without a price move, input problems
    links.csv                  ISIN changes without an event, applied events of 2+ without an ISIN change (info)
    build_report.txt           prev_close measurement, missing sessions, counts, known cases, review examples
    build_info.json            parameters and data range of the build
    .lock                      one build at a time (backfill.locks.RunLock)

Measured on the real data (2026-10-17): NSE's prev_close is NOT adjusted on an ex-date (close(p) / prev_close(d)
is 1.0 on every known split and bonus); only the price moves. So events are found from the price
(close(p) / open(d)), and one is applied only when it is confirmed: (a) an ISIN_CHANGE of the security in the
security master within ISIN_WINDOW trading days whose ratio_open matches, or (b) a row of the NSE
corporate-actions CSV saved by hand in adjustments/input/. A price on day d divided by the factor of d puts it on
today's scale. Bhavcopy prices are never changed; adjusted prices are computed when loading. Dividends are not
adjusted."""
import json
import math
import os
import re
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backfill.cm_bhav import store_dir
from backfill.equity import security_master as sm
from backfill.locks import LockBusy, RunLock, held  # noqa: F401 - LockBusy is re-exported for the CLI
from config import equity_data_dir

OUT = "adjustments"
INPUT = "input"
MANUAL = "manual_factors.csv"
TOL = 0.25               # price candidate: |close(p) / open(d) - 1| above this (NSE price bands are 20% at most)
SNAP_TOL = 0.03          # a ratio snaps to RATIOS within 3% (the opening move is not exact, e.g. 9.76 for 10)
CA_TOL = 0.10            # a corporate action matches a price move within 10% of its factor (MINDACORP opened 4.7% off)
EXACT_DATE_TOL = 0.25    # on the CSV ex-date itself: a move within 25% of the factor (the stock may rally that day) ...
EXACT_DATE_MIN = 1.3     # ... in the right direction and at least 1.3 (or at most 1/1.3), so it is not noise
SMALL_FACTOR = 1.3       # a corporate action below this factor is applied on its ex-date without price evidence
MIN_PRICE = 5.0          # price candidates need close(p) >= this (tick noise on penny stocks)
KINDS = ("STOCK",)
ISIN_WINDOW = 10         # trading days between an event and an ISIN change of the same security
CA_WINDOW = 3            # trading days between a corporate action's ex-date and the price move
PREV_TOL = 1e-4          # measurement: close(p) / prev_close(d) within 0.01% of 1
MISMATCH_TOL = 1e-3      # a prev_close mismatch (0.1%)
MISSING_SESSION_SHARE = 0.5   # more than half the stocks mismatch -> a session missing from the store
MISSING_SESSION_MIN = 10      # ... out of at least this many stocks that day

EVENT_COLUMNS = ["security_id", "ex_date", "symbol", "isin", "series", "prev_day", "gap", "close_prev_day",
                 "prev_close", "ratio_prev_close", "open", "close", "ratio", "close_move", "snapped_to",
                 "ratio_label", "kind", "factor", "applied", "confirmed_by", "source", "near_isin_change",
                 "isin_change_distance", "ca_ex_date", "ca_purpose", "note"]
FACTOR_COLUMNS = ["security_id", "from_date", "to_date", "factor"]
REVIEW_COLUMNS = ["kind", "security_id", "date", "symbol", "details"]
REVIEW_KINDS = ["price_only", "ca_no_move", "ca_unparsed", "ca_unknown", "manual_error"]
LINK_COLUMNS = ["kind", "security_id", "date", "symbol", "details"]
MANUAL_COLUMNS = ["security_id", "ex_date", "action", "factor", "note"]
MANUAL_TEMPLATE = ("security_id,ex_date,action,factor,note\n"
                   "# One row per change; lines starting with # are ignored. action: ADD (a new event), REMOVE (drop a\n"
                   "# detected event), SET (change its factor and apply it, e.g. to approve a price_only row).\n"
                   "# Example (a 1:1 bonus on 2017-09-07, factor 2):\n"
                   "# S000123,2017-09-07,SET,2,bonus 1:1 checked on the NSE circular\n")
EVENT_KINDS = ["SPLIT_OR_BONUS", "CONSOLIDATION", "OTHER"]

# Known cases (build_report.txt and `check`): (symbol, window from, window to, expected factor). Reported only; a
# miss is never fixed by changing a rule.
KNOWN_ADJUSTMENTS = [("TATASTEEL", "2022-07-01", "2022-08-31", 10), ("IRCTC", "2021-10-01", "2021-11-30", 5),
                     ("DRREDDY", "2024-10-01", "2024-11-30", 5), ("NAUKRI", "2025-05-01", "2025-05-31", 5),
                     ("KOTAKBANK", "2026-01-01", "2026-01-31", 5), ("BAJFINANCE", "2016-09-01", "2016-09-30", 10),
                     ("BAJFINANCE", "2025-06-01", "2025-06-30", 10), ("HDFCBANK", "2019-09-01", "2019-09-30", 2),
                     ("HDFCBANK", "2025-01-01", "2025-12-31", 2), ("RELIANCE", "2017-09-01", "2017-09-30", 2),
                     ("RELIANCE", "2024-10-01", "2024-10-31", 2)]


class AdjustmentError(Exception):
    pass


class StoreBusy(AdjustmentError):
    pass


class MasterStale(AdjustmentError):
    pass


class NotBuilt(AdjustmentError):
    pass


class CorpActionError(AdjustmentError):
    pass


def out_dir(folder=None):
    return os.path.join(folder or equity_data_dir(), OUT)


# ---- ratio table ----

def ratio_table():
    """{value: [labels]}: splits, consolidations, bonuses a:b ((a+b)/b, a and b in 1..10) and combinations seen."""
    table = {}

    def add(value, label):
        labels = table.setdefault(round(value, 6), [])
        if label not in labels:
            labels.append(label)
    for k in (2, 2.5, 4, 5, 10):
        add(k, f"split 1:{k:g}")
    for k in (2, 5, 10):
        add(1 / k, f"consolidation {k}:1")
    for a in range(1, 11):
        for b in range(1, 11):
            if math.gcd(a, b) == 1:
                add((a + b) / b, f"bonus {a}:{b}")
    add(10, "split 1:2 + bonus 4:1")
    add(10, "split 1:5 + bonus 1:1")
    return dict(sorted(table.items()))


RATIOS = ratio_table()
_VALUES = np.array(list(RATIOS))


def snap(value, tol=SNAP_TOL):
    """The RATIOS value nearest to value (in log terms) if within tol, else None."""
    if not value or value <= 0 or not math.isfinite(value):
        return None
    j = int(np.argmin(np.abs(np.log(_VALUES / value))))
    v = float(_VALUES[j])
    return v if abs(value / v - 1) <= tol else None


def label_of(value):
    return " | ".join(RATIOS.get(round(value, 6), [])) if value else ""


def kind_of(value):
    if value is None:
        return "OTHER"
    return "SPLIT_OR_BONUS" if value > 1 else "CONSOLIDATION"


def close_to(value, target, tol):
    return bool(value) and bool(target) and abs(value / target - 1) <= tol


# ---- corporate actions (NSE CSV saved by hand) ----

BONUS_RE = re.compile(r"BONUS\D{0,30}?(\d+)\s*:\s*(\d+)", re.I)
RS = r"(?:RS|RE|INR|RUPEES?)\.?\s*([\d.]+)"
SPLIT_RE = re.compile(r"(?:SPLIT|SUB\s*-?\s*DIVISION).*?(?:FROM\s*)?" + RS + r".*?TO\s*" + RS, re.I)  # FROM optional
CONSOLIDATION_RE = re.compile(r"CONSOLIDATION.*?FROM\s*" + RS + r".*?TO\s*" + RS, re.I)
PART_SPLIT = re.compile(r"/(?!-)")
NOT_EQUITY = re.compile(r"DEBENTURE|NCRPS|NCDS?|PREFERENCE|PREF", re.I)
ACTION_WORDS = re.compile(r"BONUS|SPLIT|SUB\s*-?\s*DIVISION|CONSOLIDATION", re.I)
DATE_FORMATS = ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%y", "%d %b %Y")


def parse_purpose(text):
    """(actions, unparsed): actions = [(label, factor)] for the bonuses, splits and consolidations in the text, read
    part by part (parts are separated by '/', not '/-'); unparsed is True when a part names one of them but cannot be
    read. Dividends, and bonuses of debentures / NCRPS / preference shares (not equity), are ignored."""
    actions, unparsed = [], False
    for part in PART_SPLIT.split(text):
        if NOT_EQUITY.search(part):
            continue
        found = []
        for a, b in BONUS_RE.findall(part):
            if int(a) > 0 and int(b) > 0:
                found.append((f"bonus {int(a)}:{int(b)}", (int(a) + int(b)) / int(b)))
        for pattern, word in ((SPLIT_RE, "split"), (CONSOLIDATION_RE, "consolidation")):
            for x, y in pattern.findall(part):
                try:
                    fx, fy = float(x.rstrip(".")), float(y.rstrip("."))
                except ValueError:
                    continue
                if fx > 0 and fy > 0 and fx != fy:
                    found.append((f"{word} Rs {fx:g} -> Rs {fy:g}", fx / fy))
        unparsed |= not found and bool(ACTION_WORDS.search(part))
        actions += found
    return actions, unparsed


def parse_date(text):
    text = str(text).strip()
    if not text or text == "-":
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text.title() if "%b" in fmt else text, fmt).date()
        except ValueError:
            continue
    raise ValueError(text)


def header_key(name):
    return re.sub(r"[^A-Z]", "", str(name).upper())


def read_corp_actions(folder):
    """Every *.csv in adjustments/input/ as one frame (symbol, isin, series, purpose, ex_date_text, file), duplicates
    dropped. A missing or empty folder gives an empty frame. Missing columns -> CorpActionError."""
    path = os.path.join(out_dir(folder), INPUT)
    cols = ["symbol", "isin", "series", "purpose", "ex_date_text", "file"]
    if not os.path.isdir(path):
        return pd.DataFrame(columns=cols)
    frames = []
    for name in sorted(n for n in os.listdir(path) if n.lower().endswith(".csv")):
        df = pd.read_csv(os.path.join(path, name), dtype=str, keep_default_na=False, encoding="utf-8-sig")
        keys = {header_key(c): c for c in df.columns}
        missing = [k for k in ("SYMBOL", "PURPOSE", "EXDATE") if k not in keys]
        if missing:
            raise CorpActionError(f"{os.path.join(path, name)}: missing column(s) {', '.join(missing)}; "
                                  f"columns found: {', '.join(map(str, df.columns))}")
        frames.append(pd.DataFrame({
            "symbol": df[keys["SYMBOL"]].str.strip().str.upper(),
            "isin": df[keys["ISIN"]].str.strip().str.upper() if "ISIN" in keys else "",
            "series": df[keys["SERIES"]].str.strip().str.upper() if "SERIES" in keys else "",
            "purpose": df[keys["PURPOSE"]].map(lambda s: " ".join(str(s).split())),
            "ex_date_text": df[keys["EXDATE"]].str.strip(), "file": name}))
    if not frames:
        return pd.DataFrame(columns=cols)
    out = pd.concat(frames, ignore_index=True)
    key = out["purpose"].str.upper()
    return out[~pd.DataFrame({"s": out["symbol"], "d": out["ex_date_text"].str.upper(), "p": key}).duplicated()] \
        .reset_index(drop=True)


# ---- the build ----

def master_paths(folder):
    out = sm.out_dir(folder)
    return [os.path.join(out, n) for n in ("securities.parquet", "symbol_history.parquet", "build_info.json",
                                           "events.csv")]


def check_inputs(folder):
    """Refuse while the bhavcopy or security master lock is held (StoreBusy), when the master is missing (NotBuilt)
    or older than the newest bhavcopy year file (MasterStale)."""
    store, master = store_dir(folder), sm.out_dir(folder)
    for lock, what in ((os.path.join(store, ".lock"), "a bhavcopy download (cm_bhav.py)"),
                       (os.path.join(master, ".lock"), "a security master build (security_master.py)")):
        if held(lock):
            raise StoreBusy(f"{what} holds {lock}; build after it ends.")
    paths = master_paths(folder)
    if not all(os.path.isfile(p) for p in paths):
        raise NotBuilt(f"No security master in {master}. Run: python security_master.py build")
    years = sm.year_files(store)
    if not years:
        raise AdjustmentError(f"No bhavcopy parquet files in {store}. Run cm_bhav.py first.")
    newest = max(years, key=os.path.getmtime)
    if os.path.getmtime(paths[2]) < os.path.getmtime(newest):
        raise MasterStale(f"The security master ({paths[2]}) is older than {newest}. "
                          "Rebuild it first: python security_master.py build")


def ensure_manual(folder):
    """Create manual_factors.csv once (header plus a commented example); never overwritten."""
    path = os.path.join(out_dir(folder), MANUAL)
    if not os.path.exists(path):
        os.makedirs(out_dir(folder), exist_ok=True)
        sm.write_text(path, MANUAL_TEMPLATE)
    return path


def read_manual(folder):
    path = ensure_manual(folder)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, comment="#", skipinitialspace=True)
    df.columns = [c.strip().lower() for c in df.columns]
    missing = [c for c in MANUAL_COLUMNS if c not in df.columns]
    if missing:
        raise CorpActionError(f"{path}: missing column(s) {', '.join(missing)}")
    return df[MANUAL_COLUMNS].apply(lambda c: c.str.strip())


def price_rows(folder, m, kinds):
    """(x, days): the bhavcopy rows of the securities of the given kinds, sorted by security and date, with the
    previous row of the same security (p*), gap in trading days and the ratios."""
    df, days = sm.load_bhav(store_dir(folder), tuple(m["info"]["series"]))
    df["security_id"] = df["isin"].map(m["isin_owner"])
    df = df.dropna(subset=["security_id"])
    if kinds is not None:
        df = df[df["security_id"].map(m["kind"]).isin(list(kinds))]
    x = df.sort_values(["security_id", "date"], kind="stable").reset_index(drop=True)
    index = {d: i for i, d in enumerate(days)}
    x["di"] = x["date"].map(index)
    g = x.groupby("security_id", sort=False)
    x["prev_day"] = g["date"].shift()
    x["close_prev_day"] = g["close"].shift()
    x["gap"] = (x["di"] - g["di"].shift()).astype("Int64")
    has = x["close_prev_day"].notna()
    x["ratio_prev_close"] = np.where(has & (x["prev_close"] > 0), x["close_prev_day"] / x["prev_close"], np.nan)
    x["ratio"] = np.where(has, x["close_prev_day"] / x["open"], np.nan)
    x["close_move"] = np.where(has, x["close_prev_day"] / x["close"], np.nan)
    return x, days


def measure(x, days):
    """Part 1: close(p) / prev_close(d) per security-day, and the missing-session days."""
    y = x[x["ratio_prev_close"].notna()]
    r = y["ratio_prev_close"]
    within = (r - 1).abs() <= PREV_TOL
    mism = (r - 1).abs() > MISMATCH_TOL
    per_day = pd.DataFrame({"date": y["date"], "mism": mism}).groupby("date")["mism"].agg(["sum", "size"])
    missing = per_day[(per_day["size"] >= MISSING_SESSION_MIN) & (per_day["sum"] > MISSING_SESSION_SHARE * per_day["size"])]
    index = {d: i for i, d in enumerate(days)}
    sessions = []
    for d, row in missing.iterrows():
        prev = days[index[d] - 1] if index[d] else None
        between = []
        if prev:
            c = prev + timedelta(days=1)
            while c < d:
                between.append(c)
                c += timedelta(days=1)
        weekend = [c for c in between if c.weekday() >= 5] or between
        sessions.append({"date": d, "prev": prev, "likely": weekend, "mismatch": int(row["sum"]),
                         "stocks": int(row["size"])})
    out = y.loc[~within, "ratio_prev_close"]
    off = ~y["date"].isin(set(missing.index))
    return {"days": len(y), "within": int(within.sum()), "outside": int((~within).sum()),
            "outside_01": int(mism.sum()),
            "top": out.round(3).value_counts().head(30),
            "top_off": y.loc[~within & off, "ratio_prev_close"].round(3).value_counts().head(30),
            "outside_off": int((~within & off).sum()), "sessions": sessions}


def isin_changes(folder, m, days):
    """{security_id: [(day index, date, ratio_open or None)]} from the security master's events.csv."""
    ev = pd.read_csv(os.path.join(sm.out_dir(folder), "events.csv"), dtype=str, keep_default_na=False)
    ev = ev[ev["type"] == "ISIN_CHANGE"]
    out = {}
    for r in ev.itertuples():
        d = sm.as_date(r.date)
        ro = float(r.ratio_open) if r.ratio_open else None
        out.setdefault(r.security_id, []).append((bisect_left(days, d), d, ro))
    return out


def nearest_change(changes, di):
    """(date, distance) of the nearest ISIN change within ISIN_WINDOW trading days, else (None, None)."""
    best = None
    for ci, d, _ in changes:
        dist = ci - di
        if abs(dist) <= ISIN_WINDOW and (best is None or abs(dist) < abs(best[1])):
            best = (d, dist)
    return best or (None, None)


def event_from_row(x, i):
    row = dict.fromkeys(EVENT_COLUMNS)
    r = x.iloc[i]
    row.update(security_id=r.security_id, ex_date=r.date, symbol=r.symbol, isin=r["isin"], series=r.series,
               prev_day=r.prev_day, gap=int(r.gap) if pd.notna(r.gap) else None, close_prev_day=r.close_prev_day,
               prev_close=r.prev_close, ratio_prev_close=_round(r.ratio_prev_close), open=r.open, close=r.close,
               ratio=_round(r.ratio), close_move=_round(r.close_move), applied="N", confirmed_by="", source="detected")
    return row


def fallback(x, rows, ex, factor):
    """(row, confirmed_by, note) when the price search found no match: the security's row on the CSV ex-date, or its
    next trading day. csv_exact_date: the move there goes the right way, is within EXACT_DATE_TOL of the factor and at
    least EXACT_DATE_MIN (1/EXACT_DATE_MIN for a consolidation). csv_small: else a factor below SMALL_FACTOR is
    applied there without price evidence. (None, None, None) when neither applies."""
    later = [j for j in rows if x.at[j, "date"] >= ex]
    if not later:
        return None, None, None
    j = min(later, key=lambda k: x.at[k, "date"])
    for v in (x.at[j, "ratio"], x.at[j, "close_move"]):
        if v != v or not v:
            continue
        right_way = v > 1 if factor > 1 else v < 1
        big = v >= EXACT_DATE_MIN if factor > 1 else v <= 1 / EXACT_DATE_MIN
        if right_way and big and abs(v / factor - 1) <= EXACT_DATE_TOL:
            return j, "csv_exact_date", f"move {v:.4g} on the ex-date accepted (within {EXACT_DATE_TOL:.0%})"
    if 1 < factor < SMALL_FACTOR:
        return j, "csv_small", f"applied without price evidence (factor < {SMALL_FACTOR:g})"
    return None, None, None


def _round(v):
    return None if v is None or pd.isna(v) else round(float(v), 6)


def set_snap(e, tol):
    v = snap(e["ratio"], tol) or snap(e["close_move"], tol)
    e["snapped_to"] = v
    e["ratio_label"] = label_of(v)
    e["kind"] = kind_of(v)
    return v


def confirm(e, how, factor, note=None):
    e["confirmed_by"] = how if not e["confirmed_by"] else f"{e['confirmed_by']}+{how}"
    if e["applied"] == "Y" and e["factor"] and abs(e["factor"] / factor - 1) > 1e-9:
        note = f"{how} factor {factor:g} replaces {e['factor']:g}" + (f"; {note}" if note else "")
    e["factor"], e["applied"] = factor, "Y"
    if note:
        e["note"] = f"{e['note']}; {note}" if e["note"] else note


def detect(x, days, m, changes, ca, manual, tol=TOL, snap_tol=SNAP_TOL, min_price=MIN_PRICE, kinds=KINDS,
           folder=None, log=print, ca_tol=CA_TOL):
    """Events and review rows (lists of dicts) from the price rows, ISIN changes, corporate actions and manual rows."""
    events, reviews = {}, []
    has = x["ratio"].notna()
    cand = x.index[has & ((x["ratio"] - 1).abs() > tol) & (x["close_prev_day"] >= min_price)]
    for i in cand:
        e = event_from_row(x, i)
        v = set_snap(e, snap_tol)
        di = int(x.at[i, "di"])
        for ci, d, ro in changes.get(e["security_id"], []):           # (a) ISIN change with a matching ratio_open
            if v and abs(ci - di) <= ISIN_WINDOW and close_to(ro, v, snap_tol):
                confirm(e, "isin_change", v, f"ISIN change {d} ratio_open {ro:g}")
                break
        events[(e["security_id"], e["ex_date"])] = e
    log(f"{len(cand):,} price candidates")

    # (b) corporate actions
    rows_of = x[has].groupby("security_id").groups
    all_rows = x.groupby("security_id").groups
    series = set(m["info"]["series"])
    groups, skipped = {}, {"no action": 0, "other series": 0, "other kind": 0, "after the data": 0}
    for r in ca.itertuples():
        actions, unparsed = parse_purpose(r.purpose)
        try:
            ex = parse_date(r.ex_date_text)
        except ValueError:
            reviews.append(review("ca_unparsed", None, None, r.symbol, f"{r.file}: ex-date '{r.ex_date_text}' "
                                                                       f"not read; {r.purpose}"))
            continue
        if unparsed:
            reviews.append(review("ca_unparsed", None, ex, r.symbol, f"{r.file}: {r.purpose}"))
            continue
        if not actions or ex is None:
            skipped["no action"] += 1
            continue
        if ex > days[-1]:
            skipped["after the data"] += 1
            continue
        if r.series and r.series not in series:
            skipped["other series"] += 1
            continue
        sid = m["isin_owner"].get(r.isin) if r.isin else None
        sid = sid or sm.security_id_for(r.symbol, ex, folder) or sm.security_id_for(r.symbol, ex - timedelta(days=7), folder)
        if sid is None:
            reviews.append(review("ca_unknown", None, ex, r.symbol, f"{r.file}: no security with this symbol on "
                                                                    f"{ex}; {r.purpose}"))
            continue
        if kinds is not None and m["kind"].get(sid) not in kinds:
            skipped["other kind"] += 1
            continue
        g = groups.setdefault((sid, ex), {"symbol": r.symbol, "actions": [], "purposes": []})
        for a in actions:
            if a not in g["actions"]:
                g["actions"].append(a)
        if r.purpose not in g["purposes"]:
            g["purposes"].append(r.purpose)
    for (sid, ex), g in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        factor = math.prod(f for _, f in g["actions"])
        what = " + ".join(lbl for lbl, _ in g["actions"])
        purpose = " / ".join(g["purposes"])
        if sid not in all_rows:
            reviews.append(review("ca_unknown", sid, ex, g["symbol"], f"no price rows of this security; {purpose}"))
            continue
        target = bisect_left(days, ex)
        best = None
        for j in rows_of.get(sid, []):
            dist = int(x.at[j, "di"]) - target
            if abs(dist) > CA_WINDOW:
                continue
            # within ca_tol of the factor, and closer to it than to "no move" (else a 1.1 matches any quiet day)
            moves = [v for v in (x.at[j, "ratio"], x.at[j, "close_move"])
                     if abs(v / factor - 1) <= ca_tol and abs(math.log(v / factor)) < abs(math.log(v))]
            if not moves:
                continue
            err = min(abs(math.log(v / factor)) for v in moves)
            key = (dist != 0, err, abs(dist))
            if best is None or key < best[0]:
                best = (key, j)
        how, note = "corp_action", None
        if best is None:
            j, how, note = fallback(x, all_rows[sid], ex, factor)
            if j is None:
                reviews.append(review("ca_no_move", sid, ex, g["symbol"],
                                      f"{what} = {factor:g}: no price move of {factor:g} within {CA_WINDOW} trading "
                                      f"days; {purpose}"))
                continue
            best = (None, j)
        j = best[1]
        key = (sid, x.at[j, "date"])
        e = events.get(key)
        if e is None:
            e = event_from_row(x, j)
            set_snap(e, snap_tol)
            e["source"] = "corp_action"
            events[key] = e
        e["ca_ex_date"], e["ca_purpose"] = ex, purpose
        e["ratio_label"], e["kind"] = what, kind_of(factor)      # the corporate action names the event
        if ex != e["ex_date"]:
            note = f"{note}; corporate action ex-date {ex}" if note else f"corporate action ex-date {ex}"
        confirm(e, how, factor, note)

    # manual overrides
    known = set(m["kind"])
    for r in manual.itertuples():
        action = r.action.upper()
        try:
            ex = sm.as_date(r.ex_date)
        except ValueError:
            reviews.append(review("manual_error", r.security_id, None, None, f"bad ex_date '{r.ex_date}'"))
            continue
        problem = None
        factor = None
        if r.security_id not in known:
            problem = f"unknown security_id {r.security_id}"
        elif action not in ("ADD", "REMOVE", "SET"):
            problem = f"unknown action '{r.action}' (ADD, REMOVE or SET)"
        elif action in ("ADD", "SET"):
            try:
                factor = float(r.factor)
                if not factor > 0:
                    raise ValueError
            except ValueError:
                problem = f"{action} needs a positive factor, got '{r.factor}'"
        key = (r.security_id, ex)
        if not problem and action == "ADD" and key in events:
            problem = "ADD: an event already exists on this date (use SET)"
        if not problem and action in ("REMOVE", "SET") and key not in events:
            problem = f"{action}: no event of {r.security_id} on {ex}"
        if problem:
            reviews.append(review("manual_error", r.security_id, ex, None, f"{action} {r.factor}: {problem}"))
            continue
        note = f"manual {action}" + (f": {r.note}" if r.note else "")
        if action == "ADD":
            e = dict.fromkeys(EVENT_COLUMNS)
            hit = x.index[(x["security_id"] == r.security_id) & (x["date"] == ex)]
            if len(hit):
                e = event_from_row(x, hit[0])
                set_snap(e, snap_tol)
            e.update(security_id=r.security_id, ex_date=ex, kind=kind_of(factor), applied="N", confirmed_by="",
                     source="manual", note=None)
            confirm(e, "manual", factor, note)
            events[key] = e
        elif action == "SET":
            confirm(events[key], "manual", factor, note)
            events[key]["kind"] = kind_of(factor)
        else:
            e = events[key]
            e["applied"] = "N"
            e["note"] = f"{e['note']}; removed by {note}" if e["note"] else f"removed by {note}"

    # a ca_no_move settled by a manual row (same day, same security or symbol: the CSV may name an older ISIN)
    settled = {(e["security_id"], e["ex_date"]) for e in events.values() if e["applied"] == "Y"} | \
              {(e["symbol"], e["ex_date"]) for e in events.values() if e["applied"] == "Y" and e["symbol"]}
    reviews = [rv for rv in reviews if rv["kind"] != "ca_no_move"
               or ((rv["security_id"], rv["date"]) not in settled and (rv["symbol"], rv["date"]) not in settled)]

    # nearest ISIN change, review of the unconfirmed
    index = {d: i for i, d in enumerate(days)}
    for e in events.values():
        di = index.get(e["ex_date"], bisect_left(days, e["ex_date"]))
        e["near_isin_change"], e["isin_change_distance"] = nearest_change(changes.get(e["security_id"], []), di)
        if e["applied"] == "N" and "removed by" not in (e["note"] or ""):
            reviews.append(review("price_only", e["security_id"], e["ex_date"], e["symbol"],
                                  f"close {e['close_prev_day']:g} on {e['prev_day']} -> open {e['open']:g} / close "
                                  f"{e['close']:g}: ratio {e['ratio']:g}, "
                                  + (f"suggested {e['snapped_to']:g} ({e['ratio_label']})" if e["snapped_to"]
                                     else "not a known ratio (OTHER)")))
    return events, reviews, skipped


def review(kind, sid, day, symbol, details):
    return {"kind": kind, "security_id": sid, "date": day, "symbol": symbol, "details": details}


def factor_periods(events, m, prev_of):
    """factors frame: one row per period of every security in the master; the latest period has factor 1."""
    applied = {}
    for e in events.values():
        if e["applied"] == "Y":
            applied.setdefault(e["security_id"], []).append((e["ex_date"], e["factor"]))
    rows = []
    for s in m["securities"].itertuples():
        first, last = sm.as_date(s.first_date), sm.as_date(s.last_date)
        evs = sorted(applied.get(s.security_id, []))
        if not evs:
            rows.append([s.security_id, first, last, 1.0])
            continue
        start = first
        for j, (ex, _) in enumerate(evs):
            f = math.prod(fx for _, fx in evs[j:])
            end = prev_of(s.security_id, ex)
            rows.append([s.security_id, start, end, round(f, 9)])
            start = ex
        rows.append([s.security_id, start, last, 1.0])
    return pd.DataFrame(rows, columns=FACTOR_COLUMNS)


def links(events, changes, m, days):
    rows = []
    index = {d: i for i, d in enumerate(days)}
    by_sid = {}
    for e in events.values():
        by_sid.setdefault(e["security_id"], []).append(index.get(e["ex_date"], bisect_left(days, e["ex_date"])))
    symbols = dict(zip(m["securities"]["security_id"], m["securities"]["last_symbol"]))
    for sid, chs in sorted(changes.items()):
        if m["kind"].get(sid) not in KINDS:
            continue
        for ci, d, ro in chs:
            if not any(abs(ci - di) <= ISIN_WINDOW for di in by_sid.get(sid, [])):
                rows.append({"kind": "isin_change_no_event", "security_id": sid, "date": d, "symbol": symbols.get(sid),
                             "details": f"ratio_open {ro:g}" if ro else "ratio_open blank"})
    for e in sorted(events.values(), key=lambda e: (e["ex_date"], e["security_id"])):
        if e["applied"] == "Y" and e["factor"] >= 2 and e["near_isin_change"] is None:
            rows.append({"kind": "event_no_isin_change", "security_id": e["security_id"], "date": e["ex_date"],
                         "symbol": e["symbol"], "details": f"factor {e['factor']:g} {e['ratio_label'] or ''}".strip()})
    return pd.DataFrame(rows, columns=LINK_COLUMNS)


def events_frame(events):
    ev = pd.DataFrame(sorted(events.values(), key=lambda e: (e["ex_date"], e["security_id"])), columns=EVENT_COLUMNS)
    ev["gap"] = ev["gap"].astype("Int64")
    ev["isin_change_distance"] = ev["isin_change_distance"].astype("Int64")
    return ev


def periods_of(factors):
    """{security_id: (from_dates, to_dates, factors)} sorted by from_date."""
    out = {}
    for sid, g in factors.sort_values(["security_id", "from_date"], kind="stable").groupby("security_id", sort=False):
        out[sid] = ([sm.as_date(d) for d in g["from_date"]], [sm.as_date(d) for d in g["to_date"]],
                    [float(f) for f in g["factor"]])
    return out


def lookup_factor(periods, sid, day, before=False):
    """The factor of the period holding day (before=True: the period holding the day before)."""
    if sid not in periods:
        raise KeyError(f"unknown security_id {sid}")
    starts, _, factors = periods[sid]
    j = (bisect_left(starts, day) if before else bisect_right(starts, day)) - 1
    return factors[max(j, 0)]


def num(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def known_checks(ev, periods, folder=None):
    """(lines, ok) for KNOWN_ADJUSTMENTS on the events frame."""
    lines, ok = [], True
    ev = ev.copy()
    ev["ex_date"] = ev["ex_date"].map(sm.as_date)
    for sym, start, end, expected in KNOWN_ADJUSTMENTS:
        a, b = sm.as_date(start), sm.as_date(end)
        ids = sm.lookup(sym, folder)
        hit = ev[ev["security_id"].isin(ids) & (ev["ex_date"] >= a) & (ev["ex_date"] <= b)]
        applied = hit[hit["applied"] == "Y"]
        found = not applied.empty
        ok &= found
        head = f"  [{'found' if found else 'NOT FOUND'}] {sym} {start[:7]} expected {expected:g}"
        if hit.empty:
            lines.append(head + ": no event in the window")
            continue
        lines.append(head + ":")
        for r in hit.itertuples():
            f = num(r.factor)
            text = (f"      {r.ex_date} {r.security_id} ratio {r.ratio} -> {r.ratio_label or 'OTHER'}, kind {r.kind}, "
                    f"applied {r.applied}" + (f", factor {f:g}" if f else "")
                    + (f", confirmed by {r.confirmed_by}" if r.confirmed_by else ", not confirmed")
                    + ("" if not f or abs(f / expected - 1) < 1e-9 else f" (differs from expected {expected:g})"))
            lines.append(text)
            if f and num(r.close_prev_day):
                fp = lookup_factor(periods, r.security_id, sm.as_date(r.prev_day))
                fe = lookup_factor(periods, r.security_id, r.ex_date)
                lines.append(f"        close {float(r.close_prev_day):g} on {r.prev_day} / {f:g} = "
                             f"{float(r.close_prev_day) / f:.2f} vs ex-date open {float(r.open):g}, close "
                             f"{float(r.close):g}; on today's scale {float(r.close_prev_day) / fp:.2f} -> "
                             f"{float(r.close) / fe:.2f}")
    return lines, ok


def report_text(info, meas, ev, rv, lk, skipped, ca_rows, checks):
    L = ["Adjustment factors build", f"Built: {info['built_at']}",
         f"Data: {info['first_date']} to {info['last_date']}, {info['trading_days']} trading days; kinds "
         f"{','.join(info['kinds'])}; tol {info['tol']}, snap-tol {info['snap_tol']}, ca-tol {info['ca_tol']}, "
         f"min price {info['min_price']}",
         f"Security master built {info['master_built_at']}", "",
         "Part 1: close(previous trading day of the security) / prev_close(d)",
         f"  security-days with a previous day: {meas['days']:,}",
         f"  within 0.01% of 1: {meas['within']:,} ({100 * meas['within'] / max(meas['days'], 1):.4f}%)",
         f"  outside 0.01%: {meas['outside']:,} (outside 0.1%: {meas['outside_01']:,}); outside 0.01% and not on a "
         f"missing-session day: {meas['outside_off']:,}",
         "  top 30 values outside 0.01% (all days):"]
    L += [f"    {v:.3f}  {n:,}" for v, n in meas["top"].items()]
    L.append("  top 30 values outside 0.01% (without the missing-session days):")
    L += [f"    {v:.3f}  {n:,}" for v, n in meas["top_off"].items()]
    L += ["  NSE's prev_close is not adjusted on ex-dates (measured): events come from the price move.", "",
          f"Missing sessions (a day where more than {MISSING_SESSION_SHARE:.0%} of the stocks' prev_close differs from "
          "their previous close; the session between is not in NSE_CM_bhav):"]
    for s in meas["sessions"]:
        likely = " / ".join(f"{d:%a} {d}" for d in s["likely"]) or "-"
        L.append(f"  {s['date']} (previous day in the store {s['prev']}): likely session {likely}; "
                 f"{s['mismatch']:,} of {s['stocks']:,} stocks")
    if not meas["sessions"]:
        L.append("  none")
    applied = ev[ev["applied"] == "Y"]
    conf = ev["confirmed_by"].fillna("")
    det = ev[ev["source"] == "detected"]
    L += ["", "Events:",
          f"  price candidates: {len(det):,} (" + ", ".join(f"{k} {int((det['kind'] == k).sum()):,}" for k in EVENT_KINDS) + ")",
          f"  confirmed by (a) ISIN change: {int(conf.str.contains('isin_change').sum()):,}",
          f"  confirmed by (b) corporate action: {int(conf.str.contains('corp_action').sum()):,} "
          f"(of which found below the price threshold: {int((ev['source'] == 'corp_action').sum()):,})",
          f"  confirmed by both: {int((conf.str.contains('isin_change') & conf.str.contains('corp_action')).sum()):,}",
          f"  applied on the exact ex-date (csv_exact_date): {int(conf.str.contains('csv_exact_date').sum()):,}; "
          f"without price evidence (csv_small, factor < {SMALL_FACTOR:g}): {int(conf.str.contains('csv_small').sum()):,}",
          f"  manual: {int(conf.str.contains('manual').sum()):,}",
          f"  unconfirmed price candidates (not applied, review price_only): {int((rv['kind'] == 'price_only').sum()):,}",
          f"  removed by manual: {int(ev['note'].fillna('').str.contains('removed by').sum()):,}",
          f"  applied: {len(applied):,} (" + ", ".join(f"{k} {int((applied['kind'] == k).sum()):,}" for k in EVENT_KINDS)
          + f"), securities with a factor: {applied['security_id'].nunique():,}",
          f"  corporate-action rows read: {ca_rows:,}; skipped: "
          + ", ".join(f"{k} {v:,}" for k, v in skipped.items()), "",
          "Links (links.csv, information only):"]
    for kind in ("isin_change_no_event", "event_no_isin_change"):
        part = lk[lk["kind"] == kind]
        L.append(f"  {kind}: {len(part):,}")
        L += [f"    {r.date} {r.security_id} {r.symbol}: {r.details}" for r in part.head(20).itertuples()]
    L += ["", "Known cases:"] + checks[0]
    L.append(f"Known cases: {'all found' if checks[1] else 'SOME NOT FOUND'}")
    L += ["", f"Review rows: {len(rv):,} (" + ", ".join(f"{k} {int((rv['kind'] == k).sum()):,}" for k in REVIEW_KINDS) + ")"]
    L += [f"  {r.kind} {r.date} {r.security_id or ''} {r.symbol or ''}: {r.details}" for r in rv.head(10).itertuples()]
    return "\n".join(L) + "\n"


def build(folder=None, tol=TOL, snap_tol=SNAP_TOL, min_price=MIN_PRICE, kinds=KINDS, now=None, log=print,
          ca_tol=CA_TOL):
    """Full rebuild of <folder>/adjustments/. Refuses while the bhavcopy or security master lock is held
    (StoreBusy), when the master is missing (NotBuilt) or stale (MasterStale), or another build runs (LockBusy)."""
    folder = folder or equity_data_dir()
    check_inputs(folder)
    out = out_dir(folder)
    lock = RunLock(os.path.join(out, ".lock")).acquire("adjustments.py build")
    try:
        manual = read_manual(folder)
        ca = read_corp_actions(folder)
        m = sm.load_master(folder)
        log(f"Reading {store_dir(folder)}")
        x, days = price_rows(folder, m, kinds)
        meas = measure(x, days)
        changes = isin_changes(folder, m, days)
        events, reviews, skipped = detect(x, days, m, changes, ca, manual, tol, snap_tol, min_price, kinds, folder, log,
                                          ca_tol)
        prev = {(s, d): p for s, d, p in zip(x["security_id"], x["date"], x["prev_day"]) if p == p and p is not None}

        def prev_of(sid, ex):
            p = prev.get((sid, ex))
            if p is None:
                j = bisect_left(days, ex) - 1
                p = days[j] if j >= 0 else ex - timedelta(days=1)
            return p
        factors = factor_periods(events, m, prev_of)
        ev = events_frame(events)
        rv = pd.DataFrame(reviews, columns=REVIEW_COLUMNS)
        rv["_k"] = rv["kind"].map(REVIEW_KINDS.index)
        rv = rv.sort_values(["_k", "date", "security_id"], kind="stable", na_position="first") \
            .drop(columns="_k").reset_index(drop=True)
        lk = links(events, changes, m, days)
        info = {"kinds": list(kinds) if kinds else None, "tol": tol, "snap_tol": snap_tol, "ca_tol": ca_tol,
                "min_price": min_price,
                "first_date": str(days[0]), "last_date": str(days[-1]), "trading_days": len(days),
                "master_built_at": m["info"].get("built_at"),
                "built_at": f"{(now or sm.now_ist()):%Y-%m-%d %H:%M:%S} IST"}
        if info["kinds"] is None:
            info["kinds"] = ["all"]
        checks = known_checks(ev, periods_of(factors), folder)
        report = report_text(info, meas, ev, rv, lk, skipped, len(ca), checks)
        sm.write_csv(ev, os.path.join(out, "events.csv"))
        sm.write_parquet(factors, os.path.join(out, "factors.parquet"))
        sm.write_csv(factors, os.path.join(out, "factors.csv"))
        sm.write_csv(rv, os.path.join(out, "review.csv"))
        sm.write_csv(lk, os.path.join(out, "links.csv"))
        sm.write_text(os.path.join(out, "build_info.json"), json.dumps(info, indent=2) + "\n")
        sm.write_text(os.path.join(out, "build_report.txt"), report)
        _CACHE.clear()
        return {"events": len(ev), "applied": int((ev["applied"] == "Y").sum()), "reviews": len(rv),
                "manual_errors": int((rv["kind"] == "manual_error").sum()), "report": report, "checks_ok": checks[1],
                "out": out}
    finally:
        lock.release()


# ---- loaders (read the built files) ----

_CACHE = {}


def load_factors(folder=None):
    out = out_dir(folder)
    paths = [os.path.join(out, n) for n in ("factors.parquet", "events.csv", "build_info.json")]
    if not all(os.path.isfile(p) for p in paths):
        raise NotBuilt(f"No adjustment factors in {out}. Run: python adjustments.py build")
    key = (out, tuple(os.path.getmtime(p) for p in paths))
    if key not in _CACHE:
        _CACHE.clear()
        factors = pq.read_table(paths[0]).to_pandas()
        _CACHE[key] = {"factors": factors, "periods": periods_of(factors)}
    return _CACHE[key]


def factor_on(security_id, on_date, folder=None):
    """The cumulative factor for that date: a price on that date divided by it is on today's scale."""
    return lookup_factor(load_factors(folder)["periods"], security_id, sm.as_date(on_date))


def adjusted_history(security_id, start=None, end=None, folder=None):
    """sm.history() rows plus adj_open/high/low/close (price / factor of the day), adj_prev_close (prev_close / factor
    of the day before: NSE's prev_close is unadjusted on an ex-date) and adj_volume (volume * factor)."""
    periods = load_factors(folder)["periods"]
    if security_id not in periods:
        raise KeyError(f"unknown security_id {security_id}")
    df = sm.history(security_id, start, end, folder)
    if df.empty:
        return df
    days = [sm.as_date(d) for d in df["date"]]
    f = np.array([lookup_factor(periods, security_id, d) for d in days])
    fb = np.array([lookup_factor(periods, security_id, d, before=True) for d in days])
    for c in ("open", "high", "low", "close"):
        df[f"adj_{c}"] = df[c] / f
    df["adj_prev_close"] = df["prev_close"] / fb
    df["adj_volume"] = df["volume"] * f
    df["factor"] = f
    return df


def read_built(folder=None, name="events.csv"):
    path = os.path.join(out_dir(folder), name)
    if not os.path.isfile(path):
        raise NotBuilt(f"No {name} in {out_dir(folder)}. Run: python adjustments.py build")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def check(folder=None):
    """(lines, ok): the known cases on the built files, plus the manual errors in review.csv."""
    periods = load_factors(folder)["periods"]
    ev = read_built(folder)
    for c in ("factor", "close_prev_day", "open", "close"):
        ev[c] = pd.to_numeric(ev[c])
    lines, ok = known_checks(ev, periods, folder)
    rv = read_built(folder, "review.csv")
    errors = rv[rv["kind"] == "manual_error"]
    lines.append(f"Manual errors: {len(errors)}")
    lines += [f"  {r.security_id} {r.date}: {r.details}" for r in errors.itertuples()]
    return lines, ok and errors.empty
