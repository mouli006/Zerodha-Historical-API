"""equity_minute.py fix-zero-padding [--apply]: the days.parquet rows of the Kite 1-minute store whose day aggregates
still include Kite's all-zero padding bars, fixed (docs/dev/equity_minute_zero_padding.md).

Kite pads some old sessions with all-zero bars (open = high = low = close = 0, volume 0). minute_aggregates drops them
since b52ca35 (ms.clean_bars), but the rows the full download built before still count them: minute_low = 0, so most
are HIGHLOW_DIFF. A row with minute_low <= 0 is
    PADDING   every stored bar of the day with a price <= 0 is an all-zero bar: fixed;
    OTHER     some bar with a price <= 0 has a non-zero field: reported only, never changed.
Per PADDING row only these columns change, from minute_aggregates on its stored bars and Calc.derive of the stock
(a fresh download's code):
    minute_rows, first_bar, last_bar, minute_open, minute_high, minute_low, minute_close, minute_scale_measured,
    check (CANDLE_UNCONFIRMED kept, as checks does), note
minute_volume and neg_volume_bars are never written (zero bars have volume 0; a day of only zero bars keeps 0 where a
fresh download has none). The conversion columns are never written: a whole-stock derive with every PADDING row fixed
is compared with the derive of the stored rows, and any change is listed as drift (scale_drift.csv). A PADDING row is
held (not changed) when its own conversion, the scales of the row before, a label it does not own (EXTRA_SESSION,
NEGATIVE_VOLUME, SCALE_STEP) or its note would change, when its stored conversion is not what the derive gives today,
or when its volume would change; the stock is derived again without the held rows until no fixed row moves.
The stored minute bars are never written. Dry run (default) writes only its report folder
(equity_1min/zeropad/dryrun_<time>/). apply: backs up each changed days.parquet to
equity_1min/archive/zeropad_<time>/<security_id>/, writes the new ones as .tmp, replaces them all at the end (any
error restores every file: ApplyFailed), writes zeropad.csv into the backup and rebuilds check/. Both take
equity_1min/.lock and refuse while a bhavcopy download / input build or a Kite F&O run holds its lock."""
import os
import shutil
import tempfile
import time
from datetime import date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.equity.minute_negvol import same
from backfill.equity.minute_repair import count_labels, different
from backfill.locks import RunLock

AGGREGATES = ["minute_rows", "first_bar", "last_bar", "minute_open", "minute_high", "minute_low", "minute_close"]
FIXED = AGGREGATES + ["minute_scale_measured", "check", "note"]
CONVERSION = ["our_factor", "ratio", "kite_multiplier", "kite_difference", "kind", "vol_ratio", "minute_scale",
              "minute_volume_scale", "conversion_from"]
LOADED = ["kind", "ratio", "kite_multiplier", "kite_difference", "vol_ratio", "minute_scale", "minute_volume_scale"]
SCALES = ["minute_scale", "minute_volume_scale"]
NOT_OWNED = {"EXTRA_SESSION", md.NEGATIVE, "SCALE_STEP"}      # labels the fix keeps (CANDLE_UNCONFIRMED put back)
CONVERTED = "converted as "
SCAN_COLUMNS = ["date", "minute_low", "minute_rows", "kite_close", "bhav_close", "check"]
KNOWN = [("JISLDVREQS", date(2015, 5, 15))]  # 375 bars, 5 of them all-zero (handover #38)
ROW_COLUMNS = ["security_id", "symbol", "date", "bars", "zero_bars", "all_zero"] + \
    [f"{p}_{c}" for c in FIXED for p in ("old", "new")] + ["changed"]
HELD_COLUMNS = ["security_id", "symbol", "date", "reason", "detail"]
OTHER_COLUMNS = ["security_id", "symbol", "date", "bars", "nonpositive_bars", "check", "reason"]
DRIFT_COLUMNS = ["security_id", "symbol", "from_date", "to_date", "rows", "old_minute_scale", "new_minute_scale",
                 "old_minute_volume_scale", "new_minute_volume_scale", "columns", "price_change_pct",
                 "volume_change_pct", "padding_rows", "held_padding_rows_stock", "other_rows_changed_stock",
                 "affects_loaded_prices"]
PROGRESS_EVERY = 250
SHOWN = 10
SAMPLE = 10                      # random fixed rows shown before / after
SEED = 0


def classify(bars):
    """(kind, zero bars, bars with a price <= 0, reason) of one day's stored bars whose row has minute_low <= 0."""
    if not len(bars):
        return "OTHER", 0, 0, "no stored bars for the day"
    o, h, lo, c = (bars[x].to_numpy(float) for x in ("open", "high", "low", "close"))
    v = bars["volume"].to_numpy()
    nonpos = (o <= 0) | (h <= 0) | (lo <= 0) | (c <= 0)
    zero = (o == 0) & (h == 0) & (lo == 0) & (c == 0) & (v == 0)
    if not nonpos.any():
        return "OTHER", 0, 0, "no bar with a price <= 0 (the stored row does not match the bars)"
    bad = nonpos & ~zero
    if bad.any():
        b = bars[bad].iloc[0]
        return "OTHER", int(zero.sum()), int(nonpos.sum()), (
            f"{int(bad.sum())} bar(s) with a price <= 0 and a non-zero field, e.g. {b['ts']:%H:%M} o={b['open']:g} "
            f"h={b['high']:g} l={b['low']:g} c={b['close']:g} v={b['volume']}")
    return "PADDING", int(zero.sum()), int(nonpos.sum()), ""


def labels_kept(check):
    return set(str(check).split("|")) & NOT_OWNED


def note_kept(note):
    """The note parts the fix does not own (all but "converted as ...", which follows DAY_EMPTY / EXTRA_SESSION)."""
    note = "" if note is None or (isinstance(note, float) and np.isnan(note)) else str(note)
    return [p for p in note.split("; ") if p and not p.startswith(CONVERTED)]


def empty_estimate(me, with_day, no_min, all_days):
    """(rows, days) of check/minute_empty_days.csv as md.rebuild_checks would write it: market-wide dates (more than
    EMPTY_SHARE of EMPTY_MIN+ stocks with a day candle have no minutes) one row each, else runs per stock. me:
    {sid: MINUTE_EMPTY dates}; all_days(sid) -> the stock's dates."""
    market = {d for d, k in no_min.items() if with_day.get(d, 0) >= md.EMPTY_MIN and k / with_day[d] > md.EMPTY_SHARE}
    rows, days = len(market), len(market)
    for sid, dates in me.items():
        if not dates:
            continue
        ds = all_days(sid)
        for _, _, n in mp.runs_of(ds, [d in dates and d not in market for d in ds]):
            rows += 1
            days += n
    return rows, days, market


def pct_change(old, new, close, volume):
    """Max |new / old - 1| in % of the loaded close and volume (ms.convert) of rows with conversion old vs new."""
    def conv(c):
        m = c[LOADED].reset_index(drop=True).assign(open=close, high=close, low=close, close=close, volume=volume)
        return ms.convert(m)
    a, b = conv(old), conv(new)
    out = []
    for k in ("close", "volume"):
        with np.errstate(divide="ignore", invalid="ignore"):
            x = np.abs(b[k] / a[k] - 1) * 100
        x = x[np.isfinite(x)]
        out.append(round(float(x.max()), 4) if len(x) else np.nan)
    return out


def spans(pos, sig):
    """[(first, last)] of consecutive positions with the same signature."""
    out = []
    for p in pos:
        if out and p == out[-1][1] + 1 and sig[p] == sig[out[-1][1]]:
            out[-1][1] = p
        else:
            out.append([p, p])
    return out


def _replace(tmp, path):
    os.replace(tmp, path)


class Stock:
    """One stock's fix: the PADDING rows' new inputs, the hold loop, the new frame and its guard."""

    def __init__(self, calc, sid, stored, padding):
        self.calc, self.sid, self.stored = calc, sid, stored
        self.symbol = stored["symbol"].iat[-1] if len(stored) else sid
        self.bhav = calc.bhav_of(sid)
        self.inputs = stored[md.INPUT_COLUMNS]
        self.base = calc.derive(sid, self.inputs, bhav=self.bhav)
        if list(self.base["date"]) != list(stored["date"]):
            raise md.ApplyFailed(f"{sid}: days.parquet is not sorted by date; not fixed")
        self.pos = {d: i for i, d in enumerate(stored["date"])}
        self.padding = padding                 # {date: (bars, zero_bars)}
        self.held, self.fixes = {}, {}
        dates = sorted(padding)
        agg = md.minute_aggregates(pd.concat([padding[d][0] for d in dates]).assign(run_id="", fetched_at=""))
        for d in dates:
            i, a = self.pos[d], agg.get(d)
            row = stored.iloc[i]
            if a:
                vals, mv, nb = {c: a[c] for c in AGGREGATES}, a["minute_volume"], int(a["neg_volume_bars"])
            else:                              # only zero bars: no minutes (as inputs_from), the volume stays 0
                vals = {"minute_rows": 0, "first_bar": "", "last_bar": "", "minute_open": np.nan,
                        "minute_high": np.nan, "minute_low": np.nan, "minute_close": np.nan}
                mv, nb = 0, 0
            if not same(row["minute_volume"], mv) or int(row["neg_volume_bars"]) != nb:
                self.held[d] = ("VOLUME_CHANGE", f"minute_volume {row['minute_volume']} -> {mv}, neg_volume_bars "
                                                 f"{row['neg_volume_bars']} -> {nb}")
                continue
            self.fixes[i] = vals
        self.trial1 = self.trial = self.derive(self.fixes)
        while True:                            # hold rows that would move, derive again without them
            bad = {i: r for i in self.fixes if (r := self.problem(i))}
            if not bad:
                break
            for i, r in bad.items():
                self.held[stored["date"].iat[i]] = r
                del self.fixes[i]
            self.trial = self.derive(self.fixes)

    def derive(self, fixes):
        if not fixes:
            return self.base
        t = self.inputs.copy()
        for i, vals in fixes.items():
            for c, v in vals.items():
                t.at[i, c] = v
        return self.calc.derive(self.sid, t, bhav=self.bhav)

    def problem(self, i):
        """(reason, detail) when PADDING row i cannot be fixed on the stored conversion, else None."""
        s, b, t = self.stored, self.base, self.trial
        for j, cols in ((i, CONVERSION), (i - 1, SCALES)):
            if j < 0:
                continue
            pre = [c for c in cols if not same(b.at[j, c], s.at[j, c])]
            if pre:
                return "PRE_EXISTING", f"{md.iso(s['date'].iat[j])}: the stored {', '.join(pre)} differ from a " \
                                       "derive of the stored rows"
            moved = [c for c in cols if not same(t.at[j, c], s.at[j, c])]
            if moved:
                return "SCALE_DRIFT", f"{md.iso(s['date'].iat[j])}: " + ", ".join(
                    f"{c} {s.at[j, c]} -> {t.at[j, c]}" for c in moved)
        if labels_kept(t.at[i, "check"]) != labels_kept(s.at[i, "check"]):
            return "LABEL_NOT_OWNED", f"check {s.at[i, 'check']} -> {t.at[i, 'check']}"
        if note_kept(t.at[i, "note"]) != note_kept(s.at[i, "note"]):
            return "LABEL_NOT_OWNED", f"note {s.at[i, 'note']!r} -> {t.at[i, 'note']!r}"
        return None

    def new_frame(self):
        """The days.parquet frame with the fixed rows; raises when anything else would change (the guard)."""
        s, t = self.stored, self.trial
        new = s.copy()
        for i in self.fixes:
            for c in FIXED:
                v = t.at[i, c]
                if c == "check" and md.UNCONFIRMED in str(s.at[i, "check"]):
                    v = md.add_check(v, md.UNCONFIRMED)          # set by update, never derived (as checks)
                new.at[i, c] = v
        for c in FIXED:                                          # keep the stored dtypes
            if new[c].dtype != s[c].dtype:
                try:
                    new[c] = new[c].astype(s[c].dtype)
                except (TypeError, ValueError):
                    pass
        fixed = np.zeros(len(s), bool)
        fixed[list(self.fixes)] = True
        other = different(s, new, ~fixed)[0]
        inner = [c for c in different(s, new, fixed)[0] if c not in FIXED]
        if other or inner:
            raise md.ApplyFailed(f"{self.sid}: the fix would change {sorted(set(other) | set(inner))} beyond the "
                                 "fixed columns of the PADDING rows; nothing written")
        return new, fixed

    def drift(self):
        """scale_drift.csv rows: where a derive with every PADDING row fixed (trial1) changes the loaded conversion
        against the derive of the stored rows (old values: the stored ones)."""
        s, b, t = self.stored, self.base, self.trial1
        moved = np.zeros(len(s), bool)
        for c in LOADED:
            moved |= ~np.array([same(x, y) for x, y in zip(b[c], t[c])])
        if not moved.any():
            return []
        pad = {self.pos[d] for d in self.padding}
        pos = np.flatnonzero(moved)
        sig = {p: tuple(str(x) for x in (s.at[p, "minute_scale"], t.at[p, "minute_scale"],
                                          s.at[p, "minute_volume_scale"], t.at[p, "minute_volume_scale"])) for p in pos}
        held = len(self.held)
        others = int(sum(1 for p in pos if p not in pad))
        out = []
        for a, z in spans(pos, sig):
            ix = list(range(a, z + 1))
            cols = sorted({c for c in LOADED for p in ix if not same(b.at[p, c], t.at[p, c])})
            price, vol = pct_change(s.loc[ix], t.loc[ix], t.loc[ix, "minute_close"].to_numpy(float),
                                    t.loc[ix, "minute_volume"].to_numpy(float))
            out.append({"security_id": self.sid, "symbol": self.symbol, "from_date": md.iso(s["date"].iat[a]),
                        "to_date": md.iso(s["date"].iat[z]), "rows": len(ix),
                        "old_minute_scale": s.at[a, "minute_scale"], "new_minute_scale": t.at[a, "minute_scale"],
                        "old_minute_volume_scale": s.at[a, "minute_volume_scale"],
                        "new_minute_volume_scale": t.at[a, "minute_volume_scale"], "columns": " ".join(cols),
                        "price_change_pct": price, "volume_change_pct": vol,
                        "padding_rows": sum(1 for p in ix if p in pad), "held_padding_rows_stock": held,
                        "other_rows_changed_stock": others, "affects_loaded_prices": "Y"})
        return out


def fix(folder=None, apply=False, now=None, log=print):
    """See the module docstring. Returns a dict with the counts, the rows (fixed, held, other), the drift list, the
    label counts and the report."""
    folder = folder or config.equity_data_dir()
    inp = mp.Inputs(folder, bhav=False)                    # refuses while a bhavcopy download / input build runs
    mp.check_fno(config.load_env())                        # ... and while a Kite F&O run holds its lock
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py fix-zero-padding" + (" --apply" if apply else ""))
    work, tmps, backup, backed = None, [], None, []
    started = time.time()
    try:
        now = md.as_ist(now)
        stamp = f"{now:%Y-%m-%d_%H%M%S}"
        base_dir = ms.data_dir(folder)
        sids = sorted(n for n in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, n, ms.DAYS_FILE))) \
            if os.path.isdir(base_dir) else []
        mode = "apply" if apply else "dry run, nothing written"
        log(f"Scanning {len(sids):,} built stocks for rows with minute_low <= 0 ({mode}) ...")
        if apply:
            calc = md.Calc(folder, inp, log)
            backup = os.path.join(md.out_dir(folder), "archive", f"zeropad_{stamp}")
        else:
            try:
                calc = md.Calc(folder, inp, write=False)
            except md.CacheStale:
                work = tempfile.mkdtemp(prefix="zeropad_")
                log("The bhavcopy cache is older than the store: a temporary one is built for the dry run.")
                calc = md.Calc(folder, inp, write=True, cache_out=os.path.join(work, md.CACHE))
        r = {"applied": apply, "stocks": len(sids), "rows": [], "held": [], "other": [], "drift": [],
             "store_before": {}, "before": {}, "after": {}, "changed_columns": {}, "years_read": 0,
             "years_written": 0, "refused_before": {}, "refused_after": {}, "next_update": [], "known": [],
             "zero_days": [], "changed_stocks": 0, "backup": None, "report_dir": None}
        with_day, no_min_before, no_min, me_before, me_after = {}, {}, {}, {}, {}
        for k, sid in enumerate(sids, 1):
            path = ms.stock_dir(folder, sid)
            t = pq.read_table(os.path.join(path, ms.DAYS_FILE), columns=SCAN_COLUMNS).to_pandas()
            t["date"] = [sm.as_date(d) for d in t["date"]]
            count_labels(r["store_before"], t["check"])
            for d, kd in zip(t["date"], t["kite_close"].notna() & t["bhav_close"].notna()):
                if kd:
                    with_day[d] = with_day.get(d, 0) + 1
            me = set(t.loc[t["check"].str.contains("MINUTE_EMPTY"), "date"])
            for d in me:
                no_min_before[d] = no_min_before.get(d, 0) + 1
                no_min[d] = no_min.get(d, 0) + 1
            if me:
                me_before[sid] = me_after[sid] = me
            cand = t[t["minute_low"].to_numpy(float) <= 0]
            if len(cand):
                one_stock(r, calc, sid, path, cand, apply, backup, backed, tmps, me_after, no_min)
            if k % PROGRESS_EVERY == 0:
                log(f"  {k:,} / {len(sids):,} stocks, {len(r['rows']):,} rows to fix, {time.time() - started:,.0f} s")
        after = dict(r["store_before"])
        for k2, v in r["before"].items():
            after[k2] = after.get(k2, 0) - v
        for k2, v in r["after"].items():
            after[k2] = after.get(k2, 0) + v
        r["store_after"] = {k2: v for k2, v in after.items() if v}

        def all_days(s):
            return [sm.as_date(d) for d in pq.read_table(os.path.join(ms.stock_dir(folder, s), ms.DAYS_FILE),
                                                         columns=["date"]).column("date").to_pylist()]
        r["empty_before"] = empty_estimate(me_before, with_day, no_min_before, all_days)
        r["empty_after"] = empty_estimate(me_after, with_day, no_min, all_days)
        empty_csv = os.path.join(md.check_dir(folder), "minute_empty_days.csv")
        r["empty_csv_rows"] = len(pd.read_csv(empty_csv, dtype=str)) if os.path.isfile(empty_csv) else None
        if apply:
            for tmp, p in tmps:
                _replace(tmp, p)
            tmps = []
            if r["rows"]:
                sm.write_csv(pd.DataFrame(r["rows"], columns=ROW_COLUMNS), os.path.join(backup, "zeropad.csv"))
                r["backup"] = backup
                prio = md.priorities(inp.membership)
                md.rebuild_checks(folder, {n: prio.get(n, 3) for n in sids})
        r["peak_mb"] = round(md.memory()[1] / 1e6, 1)
        r["seconds"] = round(time.time() - started)
        r["report_dir"] = os.path.join(md.out_dir(folder), "zeropad", f"{'apply' if apply else 'dryrun'}_{stamp}")
        r["report"] = report(r)
        write_report(r)
        log(r["report"], end="")
        return r
    except BaseException as e:
        if apply and backup:
            for tmp, _ in tmps:
                if os.path.exists(tmp):
                    os.remove(tmp)
            for sid in backed:
                src = os.path.join(backup, sid, ms.DAYS_FILE)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE))
            if isinstance(e, Exception):
                raise md.ApplyFailed(f"fix-zero-padding --apply failed ({type(e).__name__}: {e}); every "
                                     f"days.parquet restored from {backup}") from e
        raise
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)
        lock.release()


def one_stock(r, calc, sid, path, cand, apply, backup, backed, tmps, me_after, no_min):
    """Classify one stock's candidate rows, fix its PADDING rows (dry run: in memory only)."""
    years = ms.year_paths(path)
    bars = {}
    for y in sorted({d.year for d in cand["date"]}):
        if y not in years:
            continue
        m = pq.read_table(years[y]).to_pandas()
        r["years_read"] += 1
        day = m["ts"].dt.date
        for d in cand["date"]:
            if d.year == y:
                bars[d] = m[day == d].sort_values("ts", kind="stable").reset_index(drop=True)
        del m
    stored = ms.read_days(path)
    symbol = stored["symbol"].iat[-1] if len(stored) else sid
    padding = {}
    for d, chk in zip(cand["date"], cand["check"]):
        b = bars.get(d, pd.DataFrame(columns=ms.MINUTE_COLUMNS))
        kind, zero, nonpos, why = classify(b)
        if kind == "PADDING":
            padding[d] = (b, zero)
        else:
            r["other"].append({"security_id": sid, "symbol": symbol, "date": md.iso(d), "bars": len(b),
                               "nonpositive_bars": nonpos, "check": chk, "reason": why})
    if not padding:
        return
    st = Stock(calc, sid, stored, padding)
    for d, (why, detail) in sorted(st.held.items()):
        r["held"].append({"security_id": sid, "symbol": symbol, "date": md.iso(d), "reason": why, "detail": detail})
    r["drift"] += st.drift()
    ones = np.ones(len(stored), bool)
    pre = [c for c in different(stored, st.base, ones)[0] if c != "check"]
    if pre:
        r["refused_before"][sid] = (symbol, pre)
    new, fixed = st.new_frame() if st.fixes else (stored, np.zeros(len(stored), bool))
    post = [c for c in different(new, st.trial, ones)[0] if c != "check"]
    if post:
        r["refused_after"][sid] = (symbol, post)
    if not st.fixes:
        return
    last = len(stored) - 1
    if any(not same(st.trial.at[last, c], stored.at[last, c]) for c in SCALES):
        r["next_update"].append((symbol, sid))
    for i in sorted(st.fixes):
        d = stored["date"].iat[i]
        old, nw = stored.iloc[i], new.iloc[i]
        changed = [c for c in FIXED if not same(old[c], nw[c])]
        for c in changed:
            r["changed_columns"][c] = r["changed_columns"].get(c, 0) + 1
        b, zero = padding[d]
        rec = {"security_id": sid, "symbol": symbol, "date": md.iso(d), "bars": len(b), "zero_bars": zero,
               "all_zero": "Y" if zero == len(b) else "N", "changed": " ".join(changed)}
        for c in FIXED:
            rec[f"old_{c}"], rec[f"new_{c}"] = old[c], nw[c]
        r["rows"].append(rec)
        if (symbol, d) in KNOWN:
            r["known"].append((symbol, sid, d, old.to_dict(), nw.to_dict()))
        if zero == len(b):
            r["zero_days"].append({"year": d.year, "security_id": sid, "old": old["check"], "new": nw["check"],
                                   "kite_close": old["kite_close"]})
    count_labels(r["before"], stored.loc[fixed, "check"])
    count_labels(r["after"], new.loc[fixed, "check"])
    me = set(new.loc[new["check"].str.contains("MINUTE_EMPTY"), "date"])
    old_me = me_after.get(sid, set())
    for d in me - old_me:
        no_min[d] = no_min.get(d, 0) + 1
    for d in old_me - me:
        no_min[d] -= 1
    if me or old_me:
        me_after[sid] = me
    r["changed_stocks"] += 1
    if apply:
        dst = os.path.join(backup, sid, ms.DAYS_FILE)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(os.path.join(path, ms.DAYS_FILE), dst)
        backed.append(sid)
        tmp = os.path.join(path, ms.DAYS_FILE + ".tmp")
        ms.write_days(new, tmp)
        tmps.append((tmp, os.path.join(path, ms.DAYS_FILE)))


def by_year(rows, key="date"):
    out = {}
    for x in rows:
        y = str(x[key])[:4]
        n, s = out.get(y, (0, set()))
        s.add(x["security_id"])
        out[y] = (n + 1, s)
    return ", ".join(f"{y}: {n:,} rows / {len(s):,} stocks" for y, (n, s) in sorted(out.items())) or "none"


def fmt(v):
    if isinstance(v, float):
        return "NaN" if np.isnan(v) else f"{v:g}"
    return repr(v) if isinstance(v, str) else str(v)


def report(r):
    lines = []
    rows, held, other = r["rows"], r["held"], r["other"]
    stocks = len({x["security_id"] for x in rows + held})
    lines.append(("APPLIED" if r["applied"] else "DRY RUN, nothing written to the store")
                 + f": {r['stocks']:,} stocks scanned")
    lines.append("1. Rows with minute_low <= 0")
    lines.append(f"  PADDING: {len(rows) + len(held):,} rows in {stocks:,} stocks ({by_year(rows + held)})")
    lines.append(f"    to fix: {len(rows):,} rows in {r['changed_stocks']:,} stocks ({by_year(rows)})")
    reasons = {}
    for x in held:
        reasons[x["reason"]] = reasons.get(x["reason"], 0) + 1
    lines.append(f"    held (not changed): {len(held):,} rows in {len({x['security_id'] for x in held}):,} stocks"
                 + (" (" + ", ".join(f"{k} {v:,}" for k, v in sorted(reasons.items())) + ")" if held else ""))
    lines.append(f"  OTHER (reported only): {len(other):,} rows in {len({x['security_id'] for x in other}):,} "
                 f"stocks ({by_year(other)})")
    keys = ["OK"] + md.CHECKS
    lines.append("2. Checks, before -> after")
    lines.append("  whole Kite store:")
    for k in keys:
        if r["store_before"].get(k) or r["store_after"].get(k):
            lines.append(f"    {k:<18} {r['store_before'].get(k, 0):>9,} -> {r['store_after'].get(k, 0):,}")
    lines.append("  the fixed rows:")
    for k in keys:
        if r["before"].get(k) or r["after"].get(k):
            lines.append(f"    {k:<18} {r['before'].get(k, 0):>9,} -> {r['after'].get(k, 0):,}")
    lines.append("3. Known case")
    found = {(s, d) for s, _, d, _, _ in r["known"]}
    for symbol, d in KNOWN:
        if (symbol, d) not in found:
            lines.append(f"  {symbol} {md.iso(d)}: not among the fixed rows"
                         + (" (held)" if any(x["symbol"] == symbol and x["date"] == md.iso(d) for x in held) else ""))
    for symbol, sid, d, old, new in r["known"]:
        lines.append(f"  {symbol} ({sid}) {md.iso(d)}:")
        for c in md.DAY_COLUMNS:
            a, b = old.get(c), new.get(c)
            lines.append(f"    {c:<22} {fmt(a)}" + (f" -> {fmt(b)}" if not same(a, b) else ""))
    lines.append(f"4. {min(SAMPLE, len(rows))} random fixed rows (seed {SEED}), before -> after")
    if rows:
        pick = sorted(np.random.default_rng(SEED).choice(len(rows), size=min(SAMPLE, len(rows)), replace=False))
        for j in pick:
            x = rows[j]
            parts = [f"{c} {fmt(x['old_' + c])} -> {fmt(x['new_' + c])}" for c in
                     ("minute_rows", "first_bar", "minute_open", "minute_low", "minute_close", "check")
                     if not same(x["old_" + c], x["new_" + c])]
            lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}, {x['zero_bars']} of {x['bars']} bars "
                         f"zero: " + "; ".join(parts))
    lines.append("5. Columns changed on the fixed rows: "
                 + (", ".join(f"{c} {n:,}" for c, n in r["changed_columns"].items()) or "none"))
    lines.append(f"  minute_scale_measured written on {r['changed_columns'].get('minute_scale_measured', 0):,} rows")
    lines.append("  minute_volume, neg_volume_bars and every conversion column (" + ", ".join(CONVERSION)
                 + "): unchanged on every row (checked per stock before anything is written)")
    drift = r["drift"]
    dstocks = sorted({x["security_id"] for x in drift})
    lines.append(f"6. Scale drift (a derive with every PADDING row fixed; not written): {len(drift):,} spans in "
                 f"{len(dstocks):,} stocks" + (", ALL AFFECT LOADED PRICES" if drift else ""))
    if drift:
        pc = pd.to_numeric(pd.Series([x["price_change_pct"] for x in drift]), errors="coerce")
        vc = pd.to_numeric(pd.Series([x["volume_change_pct"] for x in drift]), errors="coerce")
        lines.append(f"  rows {sum(x['rows'] for x in drift):,}; max price change {pc.max():.4g}%, max volume "
                     f"change {vc.max():.4g}%; held PADDING rows in these stocks "
                     f"{sum({x['security_id']: x['held_padding_rows_stock'] for x in drift}.values()):,}; other "
                     f"rows changed {sum({x['security_id']: x['other_rows_changed_stock'] for x in drift}.values()):,}")
        for x in drift[:SHOWN * 3]:
            lines.append(f"  {x['symbol']} ({x['security_id']}) {x['from_date']}..{x['to_date']} ({x['rows']} rows): "
                         f"minute_scale {fmt(x['old_minute_scale'])} -> {fmt(x['new_minute_scale'])}, "
                         f"minute_volume_scale {fmt(x['old_minute_volume_scale'])} -> "
                         f"{fmt(x['new_minute_volume_scale'])}; {x['columns']}; price {fmt(x['price_change_pct'])}%, "
                         f"volume {fmt(x['volume_change_pct'])}%")
        if len(drift) > SHOWN * 3:
            lines.append(f"  ... {len(drift) - SHOWN * 3:,} more spans (scale_drift.csv)")
    after, before = r["refused_after"], r["refused_before"]
    lines.append(f"  `checks` would still refuse after the fix: {len(after):,} of the fixed stocks"
                 + (": " + ", ".join(f"{s} ({sid})" for sid, (s, _) in sorted(after.items())) if after else ""))
    diff = sorted(set(after) ^ set(dstocks))
    if not diff:
        lines.append("  = the stocks of the drift list")
    for sid in diff:
        if sid in after:
            why = (f"already refused before the fix ({', '.join(before[sid][1])})" if sid in before
                   else f"columns {', '.join(after[sid][1])}")
            lines.append(f"  {after[sid][0]} ({sid}): refused, not in the drift list: {why}")
        else:
            sym = next(x["symbol"] for x in drift if x["security_id"] == sid)
            why = ("the rows that cause it are held (they stay as stored)"
                   if any(x["security_id"] == sid for x in held) else "no row of it is fixed")
            lines.append(f"  {sym} ({sid}): in the drift list, not refused: {why}")
    if before:
        lines.append(f"  refused already before the fix (pre-existing, not caused by it): {len(before):,} stocks")
    if r["next_update"]:
        lines.append("  newest row's scale would differ (the next update's new days get the new level): "
                     + ", ".join(f"{s} ({sid})" for s, sid in r["next_update"]))
    z = r["zero_days"]
    lines.append(f"7. Days of only zero bars: {len(z):,} fixed ({by_year(z, 'year')})")
    trans = {}
    for x in z:
        trans[(x["old"], x["new"])] = trans.get((x["old"], x["new"]), 0) + 1
    for (a, b), n in sorted(trans.items(), key=lambda kv: -kv[1]):
        lines.append(f"    {a} -> {b}: {n:,}")
    lines.append(f"  minute_volume stays 0 (a fresh download stores none); minute_rows -> 0 on all {len(z):,}: they "
                 "leave the 'Kite has minutes' days of mp.stored_minute_days / icici_plan.kite_minute_days "
                 f"({sum(1 for x in z if pd.isna(x['kite_close'])):,} of them without a Kite day candle)")
    (rb, db, mb), (ra, da, ma) = r["empty_before"], r["empty_after"]
    lines.append(f"  estimate, check/minute_empty_days.csv (= gaps' 1MIN MINUTE_EMPTY_DAYS rows in kite_gaps.csv): "
                 f"{rb:,} rows / {db:,} days -> {ra:,} rows / {da:,} days (+{ra - rb:,} rows, +{da - db:,} days); "
                 f"market-wide dates {len(mb):,} -> {len(ma):,}"
                 + (f"; the current file has {r['empty_csv_rows']:,} rows" if r["empty_csv_rows"] is not None else "")
                 + ". gaps not run.")
    lines.append(f"8. OTHER rows (not changed): {len(other):,}")
    for x in other[:SHOWN * 5]:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']} [{x['check']}]: {x['reason']}")
    if len(other) > SHOWN * 5:
        lines.append(f"  ... {len(other) - SHOWN * 5:,} more (other_rows.csv)")
    if held:
        lines.append(f"  Held PADDING rows (not changed): {len(held):,} (held_rows.csv)")
        for x in held[:SHOWN]:
            lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: {x['reason']}: {x['detail']}")
    lines.append(f"9. Minute year files read {r['years_read']:,}, written {r['years_written']}")
    if r["backup"]:
        lines.append(f"Backup: {r['backup']} (zeropad.csv lists every fixed row)")
    lines.append(f"Report: {r['report_dir']}")
    lines.append(f"Peak memory {r['peak_mb'] / 1e3:.2f} GB, {r['seconds']:,} s")
    return "\n".join(lines) + "\n"


def write_report(r):
    out = r["report_dir"]
    os.makedirs(out, exist_ok=True)
    sm.write_text(os.path.join(out, "report.txt"), r["report"])
    for name, rows, cols in (("rows.csv", r["rows"], ROW_COLUMNS), ("held_rows.csv", r["held"], HELD_COLUMNS),
                             ("other_rows.csv", r["other"], OTHER_COLUMNS),
                             ("scale_drift.csv", r["drift"], DRIFT_COLUMNS)):
        sm.write_csv(pd.DataFrame(rows, columns=cols), os.path.join(out, name))
