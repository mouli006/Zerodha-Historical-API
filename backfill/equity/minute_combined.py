"""The combined Kite + ICICI 1-minute loader (#31, P74; docs/dev/equity_minute_combined.md). Read only: it writes no
store file and takes no lock.

One source per stock-day, never mixed:
    KITE   Kite's days.parquet row has minute_rows > 0 (every label, DAY_EMPTY / SHORT_DAY / a stale EXTRA_SESSION
           ..., is only a flag); with no conversion stored the day is NONE / KITE_NO_CONVERSION
    ICICI  otherwise, ICICI's row has minute_rows > 0 (its labels are flags too)
    NONE   otherwise, with a reason "<kite part>+<icici part>"
Trading days are the bhavcopy's (EQ / BE / BZ row with volume > 0 of one of the stock's ISINs). A day with minutes
but no such row is NO_BHAV_ROW: not loaded unless include_no_trade=True.

    day_sources(key, start, end)        one row per day: the source, the reason and both sides' labels
    load_minute_best(key, start, end)   bars of the chosen source per day (the existing loaders' conversion)
    python -m backfill.equity.minute_combined report [--folder F] [--out DIR] [--stocks A,B]

The loaders must not run while equity_minute.py update / download / a fix-* or icici_minute.py download writes the
stores: a stock's year files and its days.parquet are not written as one transaction."""
import argparse
import os
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config
from backfill.equity import icici_plan as ip
from backfill.equity import icici_store as st
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import minute_tokencheck as mt
from backfill.equity import security_master as sm
from backfill.equity import special_sessions as ss
from backfill.equity.update import LOCK as UPDATE_LOCK
from backfill.locks import held

KITE, ICICI, NONE = "KITE", "ICICI", "NONE"
SERIES = sm.SERIES                                   # EQ / BE / BZ: the vendor-list definition of a traded day
FROZEN = (mt.PENDING, mt.REJECTED)                   # an ACCEPTED review row is not frozen
REGULAR_FROM, REGULAR_TO = 9 * 60 + 15, 15 * 60 + 29  # 09:15 .. 15:29, minutes of the day
KITE_COLUMNS = ["date", "minute_rows", "check", "minute_volume", "kind", "ratio", "kite_multiplier",
                "kite_difference", "minute_scale"]
ICICI_COLUMNS = ["date", "minute_rows", "check", "minute_volume"]
DAY_COLUMNS = ["date", "traded", "source", "reason", "day_flags", "kite_label", "icici_label", "kite_bars",
               "icici_bars", "kite_volume", "icici_volume", "session_kind", "frozen"]
BAR_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "security_id", "symbol", "source", "day_flags"]
PRICES = ("raw", "ours")
NO_BHAV_ROW = "NO_BHAV_ROW"


class CombinedError(mp.MinutePlanError):
    pass


# ---- the rule (pure) ----

def traded_days(rows):
    """The trading days of bhavcopy rows (date, series, volume): an EQ / BE / BZ row with volume > 0."""
    if rows is None or not len(rows):
        return set()
    ok = rows["series"].isin(SERIES) & (pd.to_numeric(rows["volume"], errors="coerce") > 0)
    return {sm.as_date(d) for d in rows.loc[ok, "date"]}


def has_conversion(df):
    """Per Kite row: a price conversion is stored (minute_scale, and ratio or, ADDITIVE, kite_multiplier and
    kite_difference). Missing columns: no conversion."""
    need = ("kind", "ratio", "kite_multiplier", "kite_difference", "minute_scale")
    if any(c not in df for c in need):
        return np.zeros(len(df), bool)
    add = (df["kind"] == "ADDITIVE").to_numpy()
    mult_ok = df["ratio"].notna().to_numpy()
    add_ok = (df["kite_multiplier"].notna() & df["kite_difference"].notna()).to_numpy()
    return df["minute_scale"].notna().to_numpy() & np.where(add, add_ok, mult_ok)


def _side(df, kite):
    """A source's day rows indexed by date: label, bars, volume (and conv for Kite)."""
    if df is None:
        return pd.DataFrame({"label": pd.Series(dtype=object), "bars": pd.Series(dtype="int64"),
                             "volume": pd.Series(dtype=float), "conv": pd.Series(dtype=bool)})
    out = pd.DataFrame({"date": list(df["date"]),
                        "label": df["check"].fillna("").astype(str).to_numpy() if "check" in df else "",
                        "bars": pd.to_numeric(df["minute_rows"], errors="coerce").fillna(0).astype("int64")
                        .to_numpy(),
                        "volume": pd.to_numeric(df["minute_volume"], errors="coerce").to_numpy(float)
                        if "minute_volume" in df else np.nan,
                        "conv": has_conversion(df) if kite else np.ones(len(df), bool)})
    return out.drop_duplicates("date", keep="last").set_index("date")


def _has(labels, name):
    return np.array([name in str(x).split("|") for x in labels], bool)


def _flags(labels):
    return ["|".join(p for p in str(x).split("|") if p and p != "OK") for x in labels]


def decide(traded, kite, icici, ctx=None):
    """The day table of one stock (DAY_COLUMNS), one row per traded day plus one per non-traded day where a source
    has minutes, in date order.

    traded  the stock's trading days (traded_days)
    kite    Kite's days rows (date, minute_rows, check, minute_volume and the conversion columns), None = no folder
    icici   ICICI's days rows (date, minute_rows, check, minute_volume), None = no folder
    ctx     match_kind (the ICICI plan's, "" = not in the plan), plan_days (dates the plan asks for), special
            ({date: kind} of the special-sessions calendar), frozen (bool)"""
    ctx = ctx or {}
    traded = set(traded)
    k, i = _side(kite, True), _side(icici, False)
    dates = sorted(traded | set(k.index[k["bars"] > 0]) | set(i.index[i["bars"] > 0]))
    kk, ii = k.reindex(dates), i.reindex(dates)
    k_row, i_row = kk["bars"].notna().to_numpy(), ii["bars"].notna().to_numpy()
    k_bars = kk["bars"].fillna(0).astype("int64").to_numpy()
    i_bars = ii["bars"].fillna(0).astype("int64").to_numpy()
    k_label = kk["label"].fillna("").to_numpy(object)
    i_label = ii["label"].fillna("").to_numpy(object)
    k_conv = kk["conv"].eq(True).to_numpy()

    k_min = k_bars > 0
    use_kite = k_min & k_conv
    no_conv = k_min & ~k_conv
    use_icici = ~k_min & (i_bars > 0)
    source = np.where(use_kite, KITE, np.where(use_icici, ICICI, NONE)).astype(object)

    if kite is None:
        k_part = np.full(len(dates), "NO_KITE_FOLDER", object)
    else:
        k_part = np.where(~k_row, "NO_KITE_ROW",
                          np.where(_has(k_label, "MINUTE_EMPTY"), "KITE_MINUTE_EMPTY", "KITE_NO_MINUTES")).astype(object)
    plan_days = ctx.get("plan_days") or set()
    match = ctx.get("match_kind") or ""
    no_row = ("NOT_IN_ICICI" if match == "NOT_IN_ICICI" else "ICICI_REVIEW" if match == "REVIEW" else None)
    i_part = []
    for d, row, lab in zip(dates, i_row, i_label):
        if row:
            parts = str(lab).split("|")
            i_part.append("ICICI_BEFORE_START" if "ICICI_BEFORE_START" in parts else
                          "ICICI_EMPTY" if "ICICI_EMPTY" in parts else "ICICI_NO_MINUTES")
        else:
            i_part.append(no_row or ("ICICI_NOT_YET_FETCHED" if d in plan_days else "NOT_IN_ICICI_PLAN"))
    i_part = np.array(i_part, object)
    reason = np.where(use_kite, KITE, np.where(use_icici, k_part,
                      np.where(no_conv, "KITE_NO_CONVERSION", k_part + "+" + i_part))).astype(object)
    is_traded = np.array([d in traded for d in dates], bool)
    reason = np.where(is_traded, reason, NO_BHAV_ROW).astype(object)
    flags = np.where(use_kite, _flags(k_label), np.where(use_icici, _flags(i_label), "")).astype(object) \
        if len(dates) else np.array([], object)
    special = ctx.get("special") or {}
    return pd.DataFrame({
        "date": dates, "traded": is_traded, "source": source, "reason": reason, "day_flags": flags,
        "kite_label": k_label, "icici_label": i_label, "kite_bars": k_bars, "icici_bars": i_bars,
        "kite_volume": kk["volume"].to_numpy(float), "icici_volume": ii["volume"].to_numpy(float),
        "session_kind": [special.get(d, "") for d in dates],
        "frozen": np.full(len(dates), bool(ctx.get("frozen")))}, columns=DAY_COLUMNS)


# ---- reading (read only) ----

def read_side(path, columns):
    """The needed columns of a stock's days.parquet (never our_factor), date as datetime.date; None without a file."""
    p = os.path.join(path, ms.DAYS_FILE)
    if not os.path.isfile(p):
        return None
    names = set(pq.read_schema(p).names)
    df = pq.read_table(p, columns=[c for c in columns if c in names]).to_pandas()
    df["date"] = [sm.as_date(d) for d in df["date"]]
    return df


class Context:
    """What day_sources needs besides the stock's own files, read once: the special-sessions calendar, the ICICI plan
    (match kind and planned days; empty when not built) and the frozen stocks (Kite token review PENDING /
    REJECTED; none without the file)."""

    def __init__(self, folder):
        self.folder = folder
        cal = ss.load_special_sessions(folder)
        self.special = dict(zip(cal["date"], cal["kind"]))
        try:
            plan, days = ip.read_plan(folder)
            self.match = dict(zip(plan["security_id"], plan["match_kind"]))
            self.plan_days = {s: set(g) for s, g in days.groupby("security_id")["date"]}
        except mp.NotBuilt:
            self.match, self.plan_days = {}, {}
        rev = mt.active(folder)
        last = rev.groupby("security_id").tail(1) if len(rev) else rev
        self.frozen = set(last.loc[last["state"].isin(FROZEN), "security_id"]) if len(last) else set()

    def of(self, sid):
        return {"match_kind": self.match.get(sid, ""), "plan_days": self.plan_days.get(sid, set()),
                "special": self.special, "frozen": sid in self.frozen}


def _in_range(df, a, b):
    if df is None:
        return None
    keep = np.ones(len(df), bool)
    if a:
        keep &= np.array([d >= a for d in df["date"]], bool)
    if b:
        keep &= np.array([d <= b for d in df["date"]], bool)
    return df[keep]


def stock_sources(folder, sid, traded, ctx, a=None, b=None):
    """decide() on the stock's two days.parquet files, rows limited to a..b."""
    traded = {d for d in traded if (a is None or d >= a) and (b is None or d <= b)}
    kite = _in_range(read_side(ms.stock_dir(folder, sid), KITE_COLUMNS), a, b)
    icici = _in_range(read_side(st.stock_dir(folder, sid), ICICI_COLUMNS), a, b)
    return decide(traded, kite, icici, ctx.of(sid))


def day_sources(key, start=None, end=None, folder=None, context=None):
    """The day table (DAY_COLUMNS) of one company from start to end (dates, inclusive); key = security_id or any
    symbol / ISIN it ever had. attrs["security_id"] holds the id."""
    folder = folder or config.equity_data_dir()
    a = sm.as_date(start) if start else None
    b = sm.as_date(end) if end else None
    sid = ms.resolve(key, folder, a)
    try:
        rows = sm.history(sid, a, b, folder)
    except KeyError:
        raise ms.NoMinuteData(f"{sid}: not in the security master.") from None
    ctx = context or Context(folder)
    out = stock_sources(folder, sid, traded_days(rows), ctx, a, b)
    out.attrs["security_id"] = sid
    return out


def load_minute_best(key, start=None, end=None, prices="raw", clean=True, regular_hours=True, include_no_trade=False,
                     folder=None, context=None):
    """1-minute bars of one company, each day from exactly one source (day_sources): columns ts, open, high, low,
    close, volume, security_id, symbol, source (KITE / ICICI), day_flags. prices "raw" / "ours" and clean pass
    through to minute_store.load_minute and icici_store.load_minute_icici. regular_hours: bars outside 09:15-15:29
    dropped, except on a special-session day (the calendar). include_no_trade: also the NO_BHAV_ROW days.
    attrs["days"] = the day table, attrs["clean"] = {source: clean counts}. NoMinuteData when no day has bars."""
    if prices not in PRICES:
        raise ValueError(f"prices must be one of {', '.join(PRICES)}")
    folder = folder or config.equity_data_dir()
    days = day_sources(key, start, end, folder, context)
    sid = days.attrs["security_id"]
    use = days if include_no_trade else days[days["traded"]]
    parts, cleaned = [], {}
    for src, loader in ((KITE, ms.load_minute), (ICICI, st.load_minute_icici)):
        want = use[use["source"] == src]
        if not len(want):
            continue
        try:
            m = loader(sid, min(want["date"]), max(want["date"]), prices=prices, folder=folder, clean=clean)
        except ms.NoMinuteData:          # the ICICI one is a subclass
            continue
        cleaned[src] = m.attrs.get("clean")
        m = m[m["ts"].dt.date.isin(set(want["date"]))].copy()
        m["source"] = src
        parts.append(m[BAR_COLUMNS[:-1]])
    if not parts or not sum(len(p) for p in parts):
        raise ms.NoMinuteData(f"{sid}: no minute bars from Kite or ICICI between {start or 'start'} and "
                              f"{end or 'end'} (see day_sources for the reasons).")
    out = pd.concat(parts, ignore_index=True)
    d = out["ts"].dt.date
    if regular_hours:
        special = set(days.loc[days["session_kind"] != "", "date"])
        mins = (out["ts"].dt.hour * 60 + out["ts"].dt.minute).to_numpy()
        keep = ((mins >= REGULAR_FROM) & (mins <= REGULAR_TO)) | d.isin(special).to_numpy()
        out, d = out[keep], d[keep]
    out = out.assign(day_flags=d.map(dict(zip(days["date"], days["day_flags"]))).to_numpy())
    out = out.sort_values("ts", kind="stable").reset_index(drop=True)[BAR_COLUMNS]
    out.attrs = {"days": days, "clean": cleaned}
    return out


# ---- the report (read only; writes only into a new output folder) ----

def lock_paths(folder):
    return {"Kite store": mp.lock_path(folder), "ICICI store": st.lock_path(folder),
            "equity update": os.path.join(folder, UPDATE_LOCK),
            "bhavcopy store": os.path.join(mp.store_dir(folder), ".lock")}


def _mtime(p):
    try:
        return os.path.getmtime(p)
    except OSError:
        return None


def busy(folder):
    """The locks held now, as text lines (held() is read only and takes nothing)."""
    return [f"{name} lock held: {p}" for name, p in lock_paths(folder).items() if held(p)]


def traded_by_stock(folder, ids):
    """{security_id: sorted dates} of the traded days of the given securities, from the bhavcopy year files (read
    once, compact columns)."""
    m = sm.load_master(folder)
    owner = m["isin_owner"]
    parts = []
    for f in sm.year_files(mp.store_dir(folder)):
        t = pq.read_table(f, columns=["date", "isin", "series", "volume"],
                          filters=[("series", "in", list(SERIES)), ("volume", ">", 0)]).to_pandas()
        t["security_id"] = t["isin"].map(owner)
        t = t[t["security_id"].isin(ids)]
        parts.append(t[["security_id", "date"]].drop_duplicates())
    if not parts:
        return {}
    df = pd.concat(parts, ignore_index=True).drop_duplicates()
    df["date"] = [sm.as_date(d) for d in df["date"]]
    return {s: set(g) for s, g in df.groupby("security_id")["date"]}


def report(folder=None, out=None, stocks=None, log=print, now=None):
    """Counts per source and reason over every STOCK security (or the given ones). Exit code: 0 done, 1 a store
    lock was taken during the run (re-run), 2 refused (output folder exists, a lock held, inputs busy / stale)."""
    folder = folder or config.equity_data_dir()
    now = now or datetime.now(mp.IST)
    out = out or os.path.join(folder, "analysis", f"combined_{now:%Y-%m-%d}")
    if os.path.exists(out):
        log(f"Refused: {out} exists (each run writes a new folder; pass --out).")
        return 2
    lines = busy(folder)
    if lines:
        for x in lines:
            log(f"Refused: {x}; run after it ends.")
        return 2
    try:
        mp.check_inputs(folder)
    except mp.MinutePlanError as e:
        log(f"Refused: {e}")
        return 2
    before = {p: _mtime(p) for p in lock_paths(folder).values()}
    t0 = time.time()
    master = sm.load_master(folder)
    secs = master["securities"]
    symbol = dict(zip(secs["security_id"], secs["last_symbol"]))
    if stocks:
        ids = sorted({ms.resolve(k, folder) for k in stocks})
    else:
        ids = sorted(secs.loc[secs["instrument_kind"] == "STOCK", "security_id"])
    traded = traded_by_stock(folder, set(ids))
    ctx = Context(folder)
    report_lines = []

    def say(x=""):
        log(x)
        report_lines.append(str(x))

    cur, peak = md.memory()
    say(f"Combined Kite + ICICI day sources (read only), {now:%Y-%m-%d %H:%M} IST, folder {folder}")
    say(f"securities: {len(ids)}; with traded days {len(traded)}; memory after the bhavcopy {cur / 2**20:.0f} MB")
    summ, by_stock, none_rows, over_rows, nobhav_rows = [], [], [], [], []
    by_date = {}
    for n, sid in enumerate(ids, 1):
        t = stock_sources(folder, sid, traded.get(sid, set()), ctx)
        if not len(t):
            continue
        sym = symbol.get(sid, "")
        tr = t[t["traded"]]
        nb = t[~t["traded"]]
        both = tr[(tr["kite_bars"] > 0) & (tr["icici_bars"] > 0)]
        de = tr[(tr["source"] == KITE) & _has(tr["kite_label"], "DAY_EMPTY")]
        by_stock.append({"security_id": sid, "symbol": sym, "traded_days": len(tr),
                         "kite": int((tr["source"] == KITE).sum()), "icici": int((tr["source"] == ICICI).sum()),
                         "none": int((tr["source"] == NONE).sum()), "both": len(both),
                         "kite_day_empty": len(de), "no_bhav_row": len(nb), "frozen": bool(t["frozen"].iat[0]),
                         "first_day": min(tr["date"]) if len(tr) else "", "last_day": max(tr["date"]) if len(tr) else ""})
        if len(tr):
            g = tr.assign(year=[d.year for d in tr["date"]]).groupby(["year", "source", "reason"]).size()
            summ.append(g.rename("days").reset_index().assign(security_id=sid))
            for (d, src), k in tr.groupby(["date", "source"]).size().items():
                row = by_date.setdefault(d, {KITE: 0, ICICI: 0, NONE: 0, NO_BHAV_ROW: 0})
                row[src] += int(k)
        for d in nb["date"]:
            by_date.setdefault(d, {KITE: 0, ICICI: 0, NONE: 0, NO_BHAV_ROW: 0})[NO_BHAV_ROW] += 1
        x = tr[tr["source"] == NONE]
        if len(x):
            none_rows.append(x[["date", "reason", "kite_label", "icici_label"]].assign(security_id=sid, symbol=sym))
        if len(both):
            over_rows.append(both[["date", "source", "kite_label", "icici_label", "kite_bars", "icici_bars"]]
                             .assign(security_id=sid, symbol=sym))
        if len(nb):
            nobhav_rows.append(nb[["date", "source", "kite_label", "icici_label", "kite_bars", "icici_bars",
                                   "kite_volume", "icici_volume"]].assign(security_id=sid, symbol=sym))
        if n % 500 == 0:
            cur, peak = md.memory()
            log(f"  {n}/{len(ids)} stocks, memory {cur / 2**20:.0f} MB (peak {peak / 2**20:.0f})")

    bs = pd.DataFrame(by_stock, columns=["security_id", "symbol", "traded_days", "kite", "icici", "none", "both",
                                         "kite_day_empty", "no_bhav_row", "frozen", "first_day", "last_day"])
    sm_cols = ["year", "source", "reason", "stocks", "days"]
    if summ:
        s = pd.concat(summ, ignore_index=True)
        summary = (s.groupby(["year", "source", "reason"]).agg(stocks=("security_id", "nunique"), days=("days", "sum"))
                   .reset_index()[sm_cols])
    else:
        summary = pd.DataFrame(columns=sm_cols)

    def frame(rows, cols):
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.concat(rows, ignore_index=True)[cols]

    none_df = frame(none_rows, ["security_id", "symbol", "date", "reason", "kite_label", "icici_label"])
    over_df = frame(over_rows, ["security_id", "symbol", "date", "source", "kite_label", "icici_label", "kite_bars",
                                "icici_bars"])
    nb_df = frame(nobhav_rows, ["security_id", "symbol", "date", "source", "kite_label", "icici_label", "kite_bars",
                                "icici_bars", "kite_volume", "icici_volume"])
    if len(nb_df):
        nb_df.insert(3, "weekday", [d.strftime("%a") for d in nb_df["date"]])
    else:
        nb_df.insert(3, "weekday", pd.Series(dtype=object))
    bd = pd.DataFrame([{"date": d, "kite": v[KITE], "icici": v[ICICI], "none": v[NONE], "no_bhav_row": v[NO_BHAV_ROW]}
                       for d, v in sorted(by_date.items())], columns=["date", "kite", "icici", "none", "no_bhav_row"])

    tot = {c: int(bs[c].sum()) for c in ("traded_days", "kite", "icici", "none", "both", "kite_day_empty",
                                         "no_bhav_row")}
    by_src = summary.groupby("source")["days"].sum() if len(summary) else pd.Series(dtype=int)
    none_split = none_df["reason"].value_counts()
    say("")
    say(f"traded stock-days {tot['traded_days']:,} = KITE {tot['kite']:,} + ICICI {tot['icici']:,} + "
        f"NONE {tot['none']:,}; both sources have minutes on {tot['both']:,}")
    say(f"Kite DAY_EMPTY days loaded from Kite: {tot['kite_day_empty']:,}; NO_BHAV_ROW days (not loaded by default): "
        f"{tot['no_bhav_row']:,} in {int((bs['no_bhav_row'] > 0).sum())} stocks, "
        f"{int((nb_df['kite_volume'].fillna(0) > 0).sum()) if len(nb_df) else 0} of them with Kite volume")
    say(f"frozen stocks (token review PENDING / REJECTED): {int(bs['frozen'].sum())}")
    say("")
    say("NONE days by reason:")
    for r, k in none_split.items():
        say(f"  {r:<45} {k:>10,}")
    say("")
    say("days by source (summary.csv):")
    for src in (KITE, ICICI, NONE):
        say(f"  {src:<6} {int(by_src.get(src, 0)):>12,}")
    checks = [
        ("traded = KITE + ICICI + NONE", tot["traded_days"], tot["kite"] + tot["icici"] + tot["none"]),
        ("summary.csv days = by_stock traded_days", int(summary["days"].sum()) if len(summary) else 0,
         tot["traded_days"]),
        ("summary KITE = by_stock kite", int(by_src.get(KITE, 0)), tot["kite"]),
        ("summary ICICI = by_stock icici", int(by_src.get(ICICI, 0)), tot["icici"]),
        ("summary NONE = by_stock none", int(by_src.get(NONE, 0)), tot["none"]),
        ("none_days.csv rows = by_stock none", len(none_df), tot["none"]),
        ("NONE reasons sum = by_stock none", int(none_split.sum()), tot["none"]),
        ("overlap.csv rows = by_stock both", len(over_df), tot["both"]),
        ("no_bhav_row.csv rows = by_stock no_bhav_row", len(nb_df), tot["no_bhav_row"]),
        ("by_date traded = by_stock traded_days", int(bd[["kite", "icici", "none"]].to_numpy().sum()),
         tot["traded_days"]),
        ("by_date no_bhav_row = by_stock no_bhav_row", int(bd["no_bhav_row"].sum()), tot["no_bhav_row"]),
    ]
    say("")
    ok_all = True
    for name, a, b in checks:
        ok = a == b
        ok_all &= ok
        say(f"  {'OK  ' if ok else 'FAIL'} {name}: {a:,} vs {b:,}")

    changed = busy(folder) or [p for p, m in before.items() if _mtime(p) != m]
    cur, peak = md.memory()
    say("")
    say(f"stocks {len(bs)}, {time.time() - t0:.0f} s, peak memory {peak / 2**20:.0f} MB")
    if changed:
        say("STORES CHANGED DURING THE RUN (a lock was taken): re-run. " + "; ".join(map(str, changed)))
    os.makedirs(out)
    head = "# stores changed during the run: re-run\n" if changed else ""
    sm.write_text(os.path.join(out, "summary.csv"), head + summary.to_csv(index=False, lineterminator="\n"))
    sm.write_csv(bs, os.path.join(out, "by_stock.csv"))
    sm.write_csv(bd, os.path.join(out, "by_date.csv"))
    sm.write_csv(over_df, os.path.join(out, "overlap.csv"))
    sm.write_csv(none_df, os.path.join(out, "none_days.csv"))
    sm.write_csv(nb_df, os.path.join(out, "no_bhav_row.csv"))
    sm.write_text(os.path.join(out, "report.txt"), "\n".join(report_lines) + "\n")
    log(f"written: {out}")
    if changed:
        return 1
    return 0 if ok_all else 1


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m backfill.equity.minute_combined",
                                description="Combined Kite + ICICI 1-minute day sources (read only).")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("report", help="counts per source and reason; writes only a new output folder")
    r.add_argument("--folder", help="equity data folder (absolute; default EQUITY_DATA_DIR)")
    r.add_argument("--out", help="new output folder (default <folder>/analysis/combined_<IST date>)")
    r.add_argument("--stocks", help="comma-separated security_ids or symbols (default: every STOCK)")
    a = p.parse_args(argv)
    if a.folder and not os.path.isabs(a.folder):
        p.error("--folder must be absolute")
    stocks = [x.strip() for x in a.stocks.split(",") if x.strip()] if a.stocks else None
    try:
        return report(a.folder, a.out, stocks)
    except mp.MinutePlanError as e:
        print(f"Not run: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
